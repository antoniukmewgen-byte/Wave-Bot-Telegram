import asyncio
import logging
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

from fastapi import FastAPI, Request
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, Update
from telegram.error import Forbidden, RetryAfter, TimedOut, NetworkError
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ContextTypes, MessageHandler, filters

from config import (
    BOT_TOKEN, AMO_SUBDOMAIN, HOT_STATUSES, ADMIN_IDS,
    TIMEOUT_PERSONAL, TIMEOUT_WARN, TIMEOUT_SOS, TIMEOUT_REBROADCAST,
    SCHEDULER_TICK, MANAGERS, WEBHOOK_PATH,
)
from db import (
    init_db, q, get_lead, get_taken, get_all_taken, get_all_availability, take_lead,
    get_msg_id, save_msg, get_all_msgs,
    mark_skipped, get_skipped,
    is_available, set_availability,
    mark_connected, get_connected,
)
from sheets import fetch_managers, warmup

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

_app: Application = None

# Зворотній словник id→name — будується один раз
MANAGERS_BY_ID: dict = {v: k for k, v in MANAGERS.items()}


# ─── СПОВІЩЕННЯ ПРО ПОМИЛКИ ──────────────────────────────────────────────────

async def send_long(message, text: str, parse_mode: str = 'HTML'):
    """Розбиває довге повідомлення на частини по межах блоків (не ріже теги)."""
    limit = 4096
    if len(text) <= limit:
        await message.reply_text(text, parse_mode=parse_mode)
        return

    # Розбиваємо по блоках \n\n щоб не розрізати HTML теги
    blocks = text.split('\n\n')
    chunk  = ''
    for block in blocks:
        if len(chunk) + len(block) + 2 > limit:
            if chunk:
                await message.reply_text(chunk.strip(), parse_mode=parse_mode)
            chunk = block
        else:
            chunk = chunk + '\n\n' + block if chunk else block
    if chunk:
        await message.reply_text(chunk.strip(), parse_mode=parse_mode)


async def notify_admins(text: str):
    """Надсилає повідомлення всім адмінам."""
    for admin_id in ADMIN_IDS:
        try:
            await _app.bot.send_message(chat_id=admin_id, text=text, parse_mode='HTML')
        except Exception:
            pass


async def notify_admin_error(where: str, error: Exception, manager_id: str = None):
    """Надсилає адміну повідомлення про помилку."""
    if not ADMIN_IDS or not _app:
        return
    mgr_part = ''
    if manager_id:
        name = MANAGERS_BY_ID.get(manager_id) or manager_id
        mgr_part = f"\n👤 Менеджер: <b>{name}</b> (<code>{manager_id}</code>)"
    text = (
        f"🚨 <b>Помилка бота</b>\n"
        f"📍 Місце: <code>{where}</code>{mgr_part}\n"
        f"❗ Помилка: <code>{type(error).__name__}: {error}</code>"
    )
    await notify_admins(text)


# ─── УТИЛІТИ ─────────────────────────────────────────────────────────────────

def month_key() -> str:
    d = datetime.now()
    return f"{d.year}-{d.month:02d}"


def day_key() -> str:
    d = datetime.now()
    return f"{d.year}-{d.month:02d}-{d.day:02d}"


def build_keyboard(lead_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Беру в роботу", callback_data=f"t:{lead_id}"),
        InlineKeyboardButton("❌ Не можу взяти", callback_data=f"s:{lead_id}"),
        InlineKeyboardButton("🔁 Дубль",         callback_data=f"d:{lead_id}"),
    ]])


def sorted_queue(exclude: list[str] = None, managers: dict = None) -> list[str]:
    if managers is None:
        managers = fetch_managers()
    month      = day_key()
    exclude    = set(exclude or [])
    taken_map  = get_all_taken(month)
    avail_map  = get_all_availability()

    # Рахуємо скільки заявок зараз персонально відправлено (але ще не взято)
    # broadcast не рахуємо — там заявка вже відкрита для всіх
    sent_rows = q(
        "SELECT manager_id, COUNT(*) as cnt FROM leads "
        "WHERE status = 'sent' AND manager_id IS NOT NULL "
        "GROUP BY manager_id",
        fetch='all',
    )
    sent_map = {r['manager_id']: r['cnt'] for r in sent_rows} if sent_rows else {}

    queue = []
    for tg_id, info in managers.items():
        if tg_id in exclude:
            continue
        if not avail_map.get(tg_id, False):
            continue
        taken     = taken_map.get(tg_id, 0)
        pending   = sent_map.get(tg_id, 0)
        max_leads = info['max_leads']
        # Пропускаємо якщо вже є персональна (неприйнята) заявка
        if pending > 0:
            continue
        if max_leads is not None and taken >= max_leads:
            continue
        queue.append((taken, tg_id))

    queue.sort()
    return [tg_id for _, tg_id in queue]


# ─── ВІДПРАВКА / РЕДАГУВАННЯ ─────────────────────────────────────────────────

async def _deactivate_blocked(manager_id: str):
    """Деактивує менеджера, що заблокував бота, та сповіщає адміна."""
    set_availability(manager_id, False)
    name = MANAGERS_BY_ID.get(manager_id, manager_id)
    logger.warning(f"{name} ({manager_id}) заблокував бота — деактивовано")
    await notify_admins(f"🚫 <b>{name}</b> заблокував бота — автоматично виведено з черги")


async def _tg_retry(coro_fn, manager_id: str):
    """3 спроби з exponential backoff (1s → 2s). Forbidden → деактивація і raise."""
    last_err: Exception = None
    for attempt in range(3):
        try:
            return await coro_fn()
        except Forbidden:
            await _deactivate_blocked(manager_id)
            raise
        except RetryAfter as e:
            last_err = e
            await asyncio.sleep(e.retry_after + 1)
        except (TimedOut, NetworkError) as e:
            last_err = e
            if attempt < 2:
                await asyncio.sleep(2 ** attempt)
    raise last_err


async def send_to(manager_id: str, lead_id: str, text: str) -> int:
    try:
        msg = await _tg_retry(
            lambda: _app.bot.send_message(
                chat_id=manager_id,
                text=text,
                reply_markup=build_keyboard(lead_id),
                parse_mode='HTML',
            ),
            manager_id,
        )
    except Exception as e:
        logger.error(f"send_to {manager_id}: {e}")
        await notify_admin_error("send_to (відправка заявки)", e, manager_id)
        raise
    save_msg(lead_id, manager_id, msg.message_id)
    return msg.message_id


async def edit_msg(manager_id: str, lead_id: str, text: str, keep_buttons: bool = False):
    msg_id = get_msg_id(lead_id, manager_id)
    if not msg_id:
        return
    try:
        await _app.bot.edit_message_text(
            chat_id=manager_id,
            message_id=msg_id,
            text=text,
            reply_markup=build_keyboard(lead_id) if keep_buttons else None,
            parse_mode='HTML',
        )
    except Forbidden:
        await _deactivate_blocked(manager_id)
    except Exception as e:
        logger.debug(f"edit_msg {manager_id}: {e}")


async def delete_and_send(manager_id: str, lead_id: str, text: str):
    """Видаляє старе повідомлення і відправляє нове (ескалація)."""
    msg_id = get_msg_id(lead_id, manager_id)
    if msg_id:
        try:
            await _app.bot.delete_message(chat_id=manager_id, message_id=msg_id)
        except Exception as e:
            logger.debug(f"delete_and_send: не вдалось видалити {msg_id} для {manager_id}: {e}")
    await send_to(manager_id, lead_id, text)


async def remove_from_others(lead_id: str, except_id: str = None, note: str = "✅ Заявку вже взято в роботу"):
    for m in get_all_msgs(lead_id):
        if m['manager_id'] == except_id:
            continue
        await edit_msg(m['manager_id'], lead_id, note)


# ─── ЛОГІКА ЧЕРГИ ────────────────────────────────────────────────────────────

async def assign_next(lead_id: str, exclude: list[str] = None):
    """Призначити заявку наступному менеджеру в черзі."""
    try:
        queue = sorted_queue(exclude=exclude)
    except Exception as e:
        await notify_admin_error("assign_next (читання черги)", e)
        return

    if not queue:
        logger.warning(f"Заявка {lead_id}: немає вільних менеджерів")
        lead = get_lead(lead_id)
        if lead and lead['status'] == 'queued':
            q("UPDATE leads SET status='no_managers' WHERE lead_id=?", (lead_id,))
            await notify_admins(
                f"⚠️ <b>Немає вільних менеджерів!</b>\n\n"
                f"{lead['title']} не розподілена.\n"
                f"Перевірте таблицю — можливо не заповнено поточний місяць."
            )
        return

    manager_id   = queue[0]
    managers     = fetch_managers()
    manager_name = managers.get(manager_id, {}).get('name', 'Менеджер')

    lead = get_lead(lead_id)
    if not lead:
        return

    text = (
        f"{lead['title']}\n"
        f"👤 <i>Черга: {manager_name}</i>"
    )

    try:
        await send_to(manager_id, lead_id, text)
        q("UPDATE leads SET status='sent', manager_id=?, sent_at=? WHERE lead_id=?",
          (manager_id, datetime.now().timestamp(), lead_id))
        logger.info(f"Заявка {lead_id} → {manager_name} ({manager_id})")
    except Exception as e:
        logger.error(f"assign_next відправка {lead_id} → {manager_id}: {e}")
        await notify_admin_error(f"assign_next (відправка заявки #{lead_id})", e, manager_id)


async def broadcast_to_all(lead_id: str):
    """Розіслати заявку всім вільним менеджерам (хто перший — того й тапки)."""
    lead = get_lead(lead_id)
    if not lead or lead['status'] in ('taken', 'duplicate'):
        return

    orig_manager = lead['manager_id']
    queue = sorted_queue(exclude=get_skipped(lead_id))
    text  = f"{lead['title']}\n👤 <i>Відкрита черга</i>"

    # Оновлюємо повідомлення оригінального менеджера (якщо є)
    if orig_manager:
        await edit_msg(orig_manager, lead_id, text, keep_buttons=True)

    for mid in queue:
        await delete_and_send(mid, lead_id, text)

    q("UPDATE leads SET status='broadcast', esc_level=1, sent_at=? WHERE lead_id=?",
      (datetime.now().timestamp(), lead_id))
    logger.info(f"Заявка {lead_id} розіслана всім ({len(queue)} менеджерів)")


async def escalate_warn(lead_id: str, title: str):
    warn = (
        f"⚠️⚠️⚠️ <b>ТЕРМІНОВО!</b>\n"
        f"Заявка вже <b>5 хвилин</b> без відповіді!\n\n{title}"
    )
    queue = sorted_queue(exclude=get_skipped(lead_id))
    for mid in queue:
        await delete_and_send(mid, lead_id, warn)
    q("UPDATE leads SET esc_level=2 WHERE lead_id=?", (lead_id,))
    logger.info(f"Заявка {lead_id}: 5-хвилинне попередження")


async def escalate_sos(lead_id: str, title: str):
    sos = (
        f"🆘🚨💀🔴 <b>SOS!!! ЗАЯВКА 10 ХВИЛИН!!!</b> 🔴💀🚨🆘\n"
        f"😱🔥💥 ХТОСЬ ВІЗЬМІТЬ ВЖЕ! 💥🔥😱\n\n{title}"
    )
    queue = sorted_queue(exclude=get_skipped(lead_id))
    for mid in queue:
        await delete_and_send(mid, lead_id, sos)
    now = datetime.now().timestamp()
    q("UPDATE leads SET esc_level=3, last_rebroadcast_at=? WHERE lead_id=?", (now, lead_id))
    logger.info(f"Заявка {lead_id}: SOS 10 хвилин")


async def rebroadcast_periodic(lead_id: str, title: str):
    """Повторна розсилка кожні 30 хв після SOS — до тих пір, поки заявку не візьмуть."""
    msg = (
        f"🔄 <b>Заявка досі не взята!</b>\n"
        f"⏰ Повторна розсилка — будь ласка, візьміть в роботу!\n\n{title}"
    )
    queue = sorted_queue(exclude=get_skipped(lead_id))
    for mid in queue:
        await delete_and_send(mid, lead_id, msg)
    q("UPDATE leads SET last_rebroadcast_at=? WHERE lead_id=?",
      (datetime.now().timestamp(), lead_id))
    logger.info(f"Заявка {lead_id}: повторна розсилка (кожні 30 хв)")


# ─── ПЛАНУВАЛЬНИК ─────────────────────────────────────────────────────────────

async def scheduler_loop():
    """Фонова задача: перевіряє застарілі заявки кожні SCHEDULER_TICK секунд."""
    last_cleanup = datetime.now().month
    while True:
        await asyncio.sleep(SCHEDULER_TICK)
        try:
            await _tick()
            now_month = datetime.now().month
            if now_month != last_cleanup:
                _cleanup_old_records()
                last_cleanup = now_month
        except Exception as e:
            logger.error(f"Scheduler помилка: {e}")
            await notify_admin_error("scheduler (фоновий планувальник)", e)


def _cleanup_old_records():
    """Видаляє записи старші за 2 місяці."""
    now            = datetime.now()
    two_months_ago = (now.replace(day=1) - timedelta(days=1)).replace(day=1) - timedelta(days=1)
    keep_from      = two_months_ago.replace(day=1).strftime('%Y-%m')

    q("DELETE FROM stats WHERE month < ?", (keep_from,))
    q("DELETE FROM leads WHERE created_at < ? AND status IN ('taken','duplicate','closed')",
      (datetime.now().timestamp() - 60 * 24 * 3600,))
    q("DELETE FROM messages WHERE lead_id NOT IN (SELECT lead_id FROM leads)")
    q("DELETE FROM skipped  WHERE lead_id NOT IN (SELECT lead_id FROM leads)")
    logger.info(f"БД: очищено записи старші за {keep_from}")


async def _tick():
    now   = datetime.now().timestamp()
    leads = q(
        "SELECT * FROM leads WHERE status NOT IN ('taken','duplicate','closed')",
        fetch='all',
    )
    for lead in leads:
        lid  = lead['lead_id']
        age  = now - lead['created_at']
        lvl  = lead['esc_level']
        last_rb = lead['last_rebroadcast_at']

        logger.debug(
            f"_tick | lead={lid} status={lead['status']} esc={lvl} "
            f"age={int(age)}s sent={'yes' if lead['sent_at'] else 'no'} "
            f"last_rb={int(now - last_rb)}s ago" if last_rb else
            f"_tick | lead={lid} status={lead['status']} esc={lvl} "
            f"age={int(age)}s sent={'yes' if lead['sent_at'] else 'no'} last_rb=none"
        )

        # ── Заявки без sent_at (ще не розіслані або no_managers) ──────────────
        if lead['status'] in ('queued', 'no_managers') and not lead['sent_at']:
            if age > 5:
                await assign_next(lid)
            continue

        if not lead['sent_at']:
            continue

        # ── Заявки з sent_at — ескалація ──────────────────────────────────────
        if lvl == 0 and age >= TIMEOUT_PERSONAL:
            await broadcast_to_all(lid)
        elif lvl == 1 and age >= TIMEOUT_WARN:
            await escalate_warn(lid, lead['title'])
        elif lvl == 2 and age >= TIMEOUT_SOS:
            await escalate_sos(lid, lead['title'])
        elif lvl >= 3:
            rb_base = last_rb or lead['sent_at'] or lead['created_at']
            if now - rb_base >= TIMEOUT_REBROADCAST:
                await rebroadcast_periodic(lid, lead['title'])


# ─── ОБРОБНИКИ CALLBACK ──────────────────────────────────────────────────────

def work_keyboard(is_active: bool) -> InlineKeyboardMarkup:
    if is_active:
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("🚫 Вийти з черги", callback_data="work:off"),
        ]])
    else:
        return InlineKeyboardMarkup([[
            InlineKeyboardButton("✅ Увійти в чергу", callback_data="work:on"),
        ]])


async def on_work_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    name    = MANAGERS_BY_ID.get(user_id)
    if not name:
        return

    text   = update.message.text
    active = text == "✅ Увійти в чергу"
    set_availability(user_id, active)

    kb = ReplyKeyboardMarkup(
        [[KeyboardButton("✅ Увійти в чергу"), KeyboardButton("🚫 Вийти з черги")]],
        resize_keyboard=True,
        is_persistent=True,
    )
    status = "✅ Ви в черзі — заявки надходитимуть" if active else "🚫 Ви вийшли з черги — заявки не надходитимуть"
    await update.message.reply_text(status, reply_markup=kb)
    logger.info(f"{name} {'увійшов в чергу' if active else 'вийшов з черги'}")


async def on_work(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    name    = MANAGERS_BY_ID.get(user_id)
    if not name:
        await update.message.reply_text("⛔ Ця команда вам недоступна.")
        return

    active = is_available(user_id)
    status = "✅ В черзі" if active else "🚫 Не в черзі"
    await update.message.reply_text(
        f"👤 <b>{name}</b>\nПоточний статус: {status}\n\nОберіть дію:",
        reply_markup=work_keyboard(active),
        parse_mode='HTML',
    )


async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id   = str(update.effective_user.id)
    user_name = update.effective_user.full_name

    if user_id not in MANAGERS.values() and user_id not in ADMIN_IDS:
        await update.message.reply_text("⛔ У вас немає доступу до цього бота.")
        return

    mgr_name = MANAGERS_BY_ID.get(user_id, user_name)
    mark_connected(user_id, mgr_name)

    if user_id in MANAGERS.values():
        active = is_available(user_id)
        status = "✅ В черзі" if active else "🚫 Не в черзі"
        kb = ReplyKeyboardMarkup(
            [[KeyboardButton("✅ Увійти в чергу"), KeyboardButton("🚫 Вийти з черги")]],
            resize_keyboard=True,
            is_persistent=True,
        )
        await update.message.reply_text(
            f"✅ Вітаю, {mgr_name}!\nПоточний статус: {status}",
            reply_markup=kb,
        )
    elif user_id in ADMIN_IDS:
        kb = ReplyKeyboardMarkup(
            [
                [KeyboardButton("👥 Статус менеджерів"), KeyboardButton("📊 Черга")],
                [KeyboardButton("📅 Статистика день"),  KeyboardButton("📆 Статистика місяць")],
                [KeyboardButton("📋 Активні заявки")],
                [KeyboardButton("🔌 Підключення")],
            ],
            resize_keyboard=True,
            is_persistent=True,
        )
        await update.message.reply_text("👋 Вітаю, адміне!\nОберіть дію:", reply_markup=kb)
    if user_id not in ADMIN_IDS:
        await notify_admins(f"✅ <b>{mgr_name}</b> підключив(ла) бота\n👤 ID: <code>{user_id}</code>")


async def on_admin_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        return

    text     = update.message.text
    managers = fetch_managers()
    month    = day_key()

    if text == "👥 Статус менеджерів":
        connected_ids = {r['manager_id'] for r in get_connected()}
        avail_map     = get_all_availability()
        lines = ["👥 <b>Статус менеджерів:</b>\n"]
        for name, tg_id in MANAGERS.items():
            if tg_id == '0':
                lines.append(f"❌ {name} — ID не вказано")
                continue
            if tg_id not in connected_ids:
                lines.append(f"❌ {name} — ще не підключився")
                continue
            taken     = get_taken(tg_id, month)
            info      = managers.get(tg_id, {})
            max_leads = info.get('max_leads')
            at_limit  = max_leads is not None and taken >= max_leads
            in_queue  = tg_id in managers and avail_map.get(tg_id, False) and not at_limit
            conv      = info.get('conversion', 0)
            payments  = info.get('payments', '?')
            hot_taken = info.get('hot_taken', '?')
            if at_limit:
                lines.append(
                    f"⛔ {name} — ліміт вичерпано ({taken}/{max_leads}) | "
                    f"конв. {conv}% | оплат: {payments} | лідів: {hot_taken}"
                )
            elif not in_queue:
                lines.append(
                    f"🚫 {name} — поза чергою "
                    f"(конв. {conv}% | оплат: {payments} | лідів: {hot_taken})"
                )
            else:
                limit_str = '∞' if max_leads is None else str(max_leads)
                basis     = f"конв. {conv}%" if payments else f"лідів: {hot_taken}"
                lines.append(
                    f"✅ {name} — взяв: {taken}/{limit_str} | {basis} | оплат: {payments}"
                )
        await send_long(update.message, '\n'.join(lines))

    elif text == "🔌 Підключення":
        connected = {r['manager_id']: r for r in get_connected()}
        lines = ["🔌 <b>Підключення менеджерів:</b>\n"]
        for name, tg_id in MANAGERS.items():
            if tg_id == '0':
                lines.append(f"❓ {name} — ID не вказано")
                continue
            if tg_id in connected:
                dt = datetime.fromtimestamp(connected[tg_id]['connected_at'])
                lines.append(f"✅ {name} — підключився {dt.strftime('%d.%m %H:%M')}")
            else:
                lines.append(f"❌ {name} — ще не підключився")
        await send_long(update.message, '\n'.join(lines))

    elif text == "📋 Активні заявки":
        rows = q(
            "SELECT * FROM leads WHERE status NOT IN ('taken','duplicate','closed') ORDER BY created_at",
            fetch='all',
        )
        if not rows:
            await update.message.reply_text("✅ Немає активних заявок")
            return
        lines = [f"📋 <b>Активні заявки ({len(rows)}):</b>\n"]
        for lead in rows:
            age_min    = int((datetime.now().timestamp() - lead['created_at']) / 60)
            status_map = {
                'queued':      '🕐 В черзі',
                'sent':        '📨 Відправлена',
                'broadcast':   '📢 Розіслана всім',
                'no_managers': '⚠️ Немає менеджерів',
            }
            status_str = status_map.get(lead['status'], lead['status'])
            mgr        = managers.get(lead['manager_id'] or '', {}).get('name', '—')
            lines.append(
                f"{lead['title']}\n"
                f"{status_str}\n"
                f"⏱ {age_min} хв\n"
                f"👤 {mgr}"
            )
        await send_long(update.message, '\n\n'.join(lines))

    elif text == "📅 Статистика день":
        now_dt      = datetime.now()
        today_start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
        today_str   = now_dt.strftime('%d.%m.%Y')

        today_rows = q(
            "SELECT * FROM leads WHERE created_at >= ? ORDER BY created_at",
            (today_start,), fetch='all',
        )

        if not today_rows:
            await update.message.reply_text(
                f"📅 <b>За сьогодні ({today_str})</b>\n\nЗаявок не було.",
                parse_mode='HTML',
            )
            return

        table_rows = []
        for lead in today_rows:
            recv_str = datetime.fromtimestamp(lead['created_at']).strftime('%H:%M')
            if lead['status'] == 'taken' and lead['taken_at'] and lead['created_at']:
                reaction_str = f"{max(0, int((lead['taken_at'] - lead['created_at']) / 60))} хв"
                status_str   = "Взято"
                mgr_name     = MANAGERS_BY_ID.get(lead['manager_id'], '—')
            else:
                reaction_str = "—"
                status_str   = "Не взято"
                mgr_name     = "—"
            table_rows.append((mgr_name, f"#{lead['lead_id']}", recv_str, reaction_str, status_str))

        headers = ["Менеджер", "Заявка", "Отримано", "Реакція", "Статус"]
        col_w   = [max(len(h), max(len(r[i]) for r in table_rows))
                   for i, h in enumerate(headers)]

        def fmt_row(cols):
            return " | ".join(c.ljust(w) for c, w in zip(cols, col_w))

        taken_today = sum(1 for r in today_rows if r['status'] == 'taken')
        await update.message.reply_text(
            f"📅 <b>За сьогодні ({today_str})</b>\n"
            f"Всього: {len(today_rows)} | Взято: {taken_today} | Не взято: {len(today_rows) - taken_today}\n\n"
            f"<pre>{fmt_row(headers)}\n"
            f"{'-+-'.join('-' * w for w in col_w)}\n"
            f"{chr(10).join(fmt_row(r) for r in table_rows)}</pre>",
            parse_mode='HTML',
        )

    elif text == "📆 Статистика місяць":
        now_dt      = datetime.now()
        month_start = now_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp()

        month_rows = q(
            "SELECT * FROM leads WHERE created_at >= ? ORDER BY created_at",
            (month_start,), fetch='all',
        )

        if not month_rows:
            await update.message.reply_text(
                f"📆 <b>За місяць ({month})</b>\n\nЗаявок ще не було.",
                parse_mode='HTML',
            )
            return

        mgr_taken     = defaultdict(int)
        mgr_not       = defaultdict(int)
        mgr_reactions = defaultdict(list)

        for lead in month_rows:
            mid = lead['manager_id'] or '—'
            if lead['status'] == 'taken':
                mgr_taken[mid] += 1
                if lead['taken_at'] and lead['created_at']:
                    mgr_reactions[mid].append(
                        max(0, int((lead['taken_at'] - lead['created_at']) / 60))
                    )
            else:
                mgr_not[mid] += 1

        m_rows        = []
        total_taken   = 0
        all_reactions = []
        for mgr_name, tg_id in MANAGERS.items():
            t = mgr_taken.get(tg_id, 0)
            if t == 0:
                continue
            total_taken += t
            reactions = mgr_reactions.get(tg_id, [])
            all_reactions.extend(reactions)
            avg_str = f"{int(sum(reactions)/len(reactions))} хв" if reactions else "—"
            m_rows.append((mgr_name, str(t), avg_str))

        not_taken_total = sum(mgr_not.values())
        overall_avg = (
            f"{int(sum(all_reactions)/len(all_reactions))} хв"
            if all_reactions else "—"
        )

        if not m_rows:
            await update.message.reply_text(
                f"📆 <b>За місяць ({month})</b>\n\nЗаявок ще не взято.",
                parse_mode='HTML',
            )
            return

        m_headers = ["Менеджер", "Взято", "Сер. реакція"]
        m_col_w   = [max(len(h), max(len(r[i]) for r in m_rows))
                     for i, h in enumerate(m_headers)]

        def fmt_m(cols):
            return " | ".join(c.ljust(w) for c, w in zip(cols, m_col_w))

        await update.message.reply_text(
            f"📆 <b>За місяць ({month})</b>\n"
            f"Всього: {len(month_rows)} | Взято: {total_taken} | "
            f"Не взято: {not_taken_total} | Сер. реакція: {overall_avg}\n\n"
            f"<pre>{fmt_m(m_headers)}\n"
            f"{'-+-'.join('-' * w for w in m_col_w)}\n"
            f"{chr(10).join(fmt_m(r) for r in m_rows)}</pre>",
            parse_mode='HTML',
        )

    elif text == "📊 Черга":
        queue = sorted_queue(managers=managers)
        if not queue:
            await update.message.reply_text("😶 Черга порожня — немає вільних менеджерів")
            return
        lines = ["📊 <b>Поточна черга:</b>\n"]
        for i, tg_id in enumerate(queue, 1):
            name  = managers.get(tg_id, {}).get('name', tg_id)
            taken = get_taken(tg_id, month)
            lines.append(f"{i}. {name} — взяв: {taken}")
        await send_long(update.message, '\n'.join(lines))



async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    try:
        action, lead_id = query.data.split(':', 1)
    except ValueError:
        await query.answer()
        return

    manager_id = str(query.from_user.id)
    lead       = get_lead(lead_id)

    if not lead:
        await query.answer("⚠️ Заявка не знайдена", show_alert=True)
        return

    if lead['status'] in ('taken', 'duplicate', 'closed'):
        await query.answer("❌ Цю заявку вже оброблено", show_alert=True)
        await edit_msg(manager_id, lead_id, "❌ Цю заявку вже оброблено")
        return

    managers = fetch_managers()
    mgr_name = managers.get(manager_id, {}).get('name', query.from_user.first_name or manager_id)

    await query.answer()

    try:
        if action in ('take', 't'):
            # Перевіряємо ліміт перед взяттям
            mgr_info  = managers.get(manager_id, {})
            max_leads = mgr_info.get('max_leads')
            if max_leads is not None:
                taken_today = get_taken(manager_id, day_key())
                if taken_today >= max_leads:
                    await query.answer("⛔ Ви вже взяли максимальну кількість лідів на сьогодні", show_alert=True)
                    await edit_msg(manager_id, lead_id, f"⛔ Ліміт вичерпано ({taken_today}/{max_leads})\n\n{lead['title']}")
                    return

            if not take_lead(lead_id, manager_id, day_key()):
                await edit_msg(manager_id, lead_id, "❌ Заявку вже взяв інший менеджер")
                return

            await edit_msg(manager_id, lead_id, f"✅ Ви взяли заявку в роботу!\n\n{lead['title']}")
            await remove_from_others(lead_id, except_id=manager_id,
                                     note=f"✅ Заявку взяв(ла) <b>{mgr_name}</b>")
            logger.info(f"Заявка {lead_id} взята {mgr_name} ({manager_id})")
            await notify_admins(f"✅ <b>{mgr_name}</b> взяв(ла) заявку в роботу\n\n{lead['title']}")

            # Перевіряємо чи менеджер досяг ліміту
            managers_info = fetch_managers()
            info      = managers_info.get(manager_id, {})
            max_leads = info.get('max_leads')
            if max_leads is not None:
                taken_today = get_taken(manager_id, day_key())
                if taken_today >= max_leads:
                    await _app.bot.send_message(
                        chat_id=manager_id,
                        text=f"⛔ Ви взяли максимальну кількість лідів на сьогодні ({max_leads}). "
                             f"Нові заявки надходитимуть завтра.",
                    )

        elif action in ('skip', 's'):
            mark_skipped(lead_id, manager_id)
            await edit_msg(manager_id, lead_id, f"⏭ Ви відмовились від заявки\n\n{lead['title']}")

            lead = get_lead(lead_id)
            if lead['status'] == 'sent':
                q("UPDATE leads SET status='queued', manager_id=NULL WHERE lead_id=?", (lead_id,))
                await assign_next(lead_id, exclude=get_skipped(lead_id))
            logger.info(f"Заявка {lead_id} відхилена {mgr_name}")

        elif action in ('dup', 'd'):
            q("UPDATE leads SET status='duplicate' WHERE lead_id=?", (lead_id,))
            await edit_msg(manager_id, lead_id, "🔁 Ви позначили заявку як дубль")
            await remove_from_others(lead_id, except_id=manager_id,
                                     note="🔁 Заявка закрита як дубль")
            logger.info(f"Заявка {lead_id} — дубль ({mgr_name})")

        elif action == 'work':
            name = MANAGERS_BY_ID.get(manager_id)
            if not name:
                return
            active = (lead_id == 'on')
            set_availability(manager_id, active)
            status = "✅ Ви в черзі — заявки надходитимуть" if active else "🚫 Ви вийшли з черги — заявки не надходитимуть"
            await query.edit_message_text(
                f"👤 <b>{name}</b>\n\n{status}\n\nЩоб змінити — напишіть /work",
                reply_markup=work_keyboard(active),
                parse_mode='HTML',
            )
            logger.info(f"{name} {'увійшов в чергу' if active else 'вийшов з черги'}")

    except Exception as e:
        logger.error(f"on_callback {action} {lead_id}: {e}")
        await notify_admin_error(f"on_callback (дія: {action}, заявка: {lead_id})", e, manager_id)


# ─── WEBHOOK AMOCRM ──────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(fastapi: FastAPI):
    global _app
    init_db()

    _app = Application.builder().token(BOT_TOKEN).build()
    _app.add_handler(CallbackQueryHandler(on_callback))
    _app.add_handler(CommandHandler('start', on_start))
    _app.add_handler(CommandHandler('work', on_work))
    _app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r'^(✅ Увійти в чергу|🚫 Вийти з черги)$'),
        on_work_button,
    ))
    _app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(
            r'^(👥 Статус менеджерів|📊 Черга|🔌 Підключення|📋 Активні заявки|📅 Статистика день|📆 Статистика місяць)$'
        ),
        on_admin_button,
    ))

    await _app.initialize()
    await _app.start()
    loop = asyncio.get_event_loop()
    loop.run_in_executor(None, warmup)
    asyncio.create_task(_app.updater.start_polling(allowed_updates=Update.ALL_TYPES))
    asyncio.create_task(scheduler_loop())
    logger.info("Бот запущено")
    yield
    await _app.updater.stop()
    await _app.stop()
    await _app.shutdown()
    logger.info("Бот зупинено")


fastapi_app = FastAPI(lifespan=lifespan)


@fastapi_app.post(f'/webhook/{WEBHOOK_PATH}')
async def amocrm_webhook(request: Request):
    try:
        data = await request.form()
    except Exception:
        return {'ok': True}

    lead_id     = (data.get('leads[status][0][id]')
                   or data.get('leads[add][0][id]'))
    status_id   = (data.get('leads[status][0][status_id]')
                   or data.get('leads[add][0][status_id]'))
    pipeline_id = (data.get('leads[status][0][pipeline_id]')
                   or data.get('leads[add][0][pipeline_id]'))

    logger.info(f"Webhook: lead_id={lead_id} status_id={status_id} pipeline_id={pipeline_id} keys={list(data.keys())[:6]}")

    if not lead_id:
        return {'ok': True}

    if str(pipeline_id) != '10815171':
        logger.info(f"Webhook: ігноруємо pipeline_id={pipeline_id} (не наша воронка)")
        return {'ok': True}

    if str(status_id) != '85731907':
        logger.info(f"Webhook: ігноруємо status_id={status_id} (не наш етап)")
        return {'ok': True}

    if get_lead(lead_id):
        return {'ok': True}

    raw_label = HOT_STATUSES.get(str(status_id), 'Нова заявка')
    # Формуємо заголовок рамки залежно від типу заявки
    if 'Гаряча' in raw_label:
        header = '🔥 ГАРЯЧА ЗАЯВКА'
    elif 'Кваліфікована' in raw_label:
        header = '⭐ КВАЛІФІКОВАНА ЗАЯВКА'
    else:
        header = '📋 НОВА ЗАЯВКА'
    lead_url = f"https://{AMO_SUBDOMAIN}.kommo.com/leads/detail/{lead_id}"
    title    = f'{header}\n🔗 <a href="{lead_url}">Угода #{lead_id}</a>'

    try:
        q("INSERT INTO leads (lead_id, status, created_at, title) VALUES (?,?,?,?)",
          (lead_id, 'queued', datetime.now().timestamp(), title))
    except Exception as e:
        logger.error(f"Webhook: не вдалось записати заявку {lead_id}: {e}")
        await notify_admin_error(f"webhook (запис заявки #{lead_id} в БД)", e)
        return {'ok': False}

    asyncio.create_task(assign_next(lead_id))
    return {'ok': True}


if __name__ == '__main__':
    import uvicorn
    uvicorn.run('main:fastapi_app', host='0.0.0.0', port=8080, reload=False)
