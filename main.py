import asyncio
import logging
from collections import defaultdict
from contextlib import asynccontextmanager
from datetime import datetime, timedelta

import aiohttp
from fastapi import FastAPI, Request
from telegram import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, Update
from telegram.error import Forbidden, RetryAfter, TimedOut, NetworkError
from telegram.ext import Application, CallbackQueryHandler, CommandHandler, ConversationHandler, ContextTypes, MessageHandler, filters

from config import (
    BOT_TOKEN, AMO_SUBDOMAIN, AMO_TOKEN, AMO_PIPELINE_ID, AMO_HOT_STATUS_ID,
    HOT_STATUSES, ADMIN_IDS,
    TIMEOUT_PERSONAL, TIMEOUT_WARN, TIMEOUT_SOS, TIMEOUT_REBROADCAST,
    SCHEDULER_TICK, MANAGERS, WEBHOOK_PATH, KOMMO_MANAGER_IDS,
)
from db import (
    init_db, q, get_lead, get_taken, get_all_taken, get_all_availability, take_lead,
    get_msg_id, save_msg, get_all_msgs,
    mark_skipped, get_skipped,
    is_available, set_availability, get_all_exit_reasons,
    mark_connected, get_connected,
    get_all_max_leads_overrides, set_max_leads_override, reset_all_limit_overrides,
    get_all_schedules, set_schedule, update_last_notified, init_default_schedules,
)
from sheets import fetch_managers, get_block_reason, warmup

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)

_app: Application = None

MANAGERS_BY_ID: dict = {v: k for k, v in MANAGERS.items()}


# ─── СПОВІЩЕННЯ ──────────────────────────────────────────────────────────────

async def send_long(message, text: str, parse_mode: str = 'HTML'):
    """Розбиває довге повідомлення на частини по межах блоків."""
    limit = 4096
    if len(text) <= limit:
        await message.reply_text(text, parse_mode=parse_mode)
        return

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
    for admin_id in ADMIN_IDS:
        try:
            await _app.bot.send_message(chat_id=admin_id, text=text, parse_mode='HTML')
        except Exception:
            pass


async def _set_kommo_responsible(lead_id: str, manager_id: str) -> bool:
    """Встановлює відповідального менеджера в Kommo після взяття заявки. Повертає True якщо успішно."""
    kommo_user_id = KOMMO_MANAGER_IDS.get(manager_id)
    if not kommo_user_id or not AMO_TOKEN:
        return False
    url = f"https://{AMO_SUBDOMAIN}.kommo.com/api/v4/leads"
    payload = [{"id": int(lead_id), "responsible_user_id": kommo_user_id}]
    headers = {"Authorization": f"Bearer {AMO_TOKEN}", "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession() as session:
            async with session.patch(url, json=payload, headers=headers) as resp:
                if resp.status not in (200, 202, 204):
                    body = await resp.text()
                    logger.error(f"Kommo responsible: HTTP {resp.status} для заявки {lead_id} | {body[:200]}")
                    return False
                logger.info(f"Kommo responsible: заявка {lead_id} → менеджер {kommo_user_id}")
                return True
    except Exception as e:
        logger.error(f"Kommo responsible: помилка для заявки {lead_id} | {e}")
        return False


async def notify_admin_error(where: str, error: Exception, manager_id: str = None):
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

def day_key() -> str:
    d = datetime.now()
    return f"{d.year}-{d.month:02d}-{d.day:02d}"


def build_keyboard(lead_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Беру в роботу", callback_data=f"t:{lead_id}"),
        InlineKeyboardButton("❌ Не можу взяти", callback_data=f"s:{lead_id}"),
        InlineKeyboardButton("🔁 Дубль",         callback_data=f"d:{lead_id}"),
    ]])


def _build_sent_map() -> dict:
    """Скільки заявок зараз персонально відправлено кожному менеджеру (status='sent')."""
    rows = q(
        "SELECT manager_id, COUNT(*) as cnt FROM leads "
        "WHERE status = 'sent' AND manager_id IS NOT NULL "
        "GROUP BY manager_id",
        fetch='all',
    )
    return {r['manager_id']: r['cnt'] for r in rows} if rows else {}


def sorted_queue(
    exclude: list[str] = None,
    managers: dict = None,
    taken_map: dict = None,
    avail_map: dict = None,
    overrides: dict = None,
    sent_map: dict = None,
) -> list[str]:
    """
    Повертає список tg_id менеджерів у порядку черги.
    Прийняті ззовні taken_map/avail_map/overrides/sent_map дозволяють
    уникнути зайвих запитів до БД, якщо черга будується для багатьох лідів підряд.
    """
    if managers is None:
        managers = fetch_managers()
    if taken_map is None:
        taken_map = get_all_taken(day_key())
    if avail_map is None:
        avail_map = get_all_availability()
    if overrides is None:
        overrides = get_all_max_leads_overrides()
    if sent_map is None:
        sent_map = _build_sent_map()

    exclude = set(exclude or [])

    queue = []
    for tg_id, info in managers.items():
        if tg_id in exclude:
            continue
        if not avail_map.get(tg_id, False):
            continue
        taken     = taken_map.get(tg_id, 0)
        pending   = sent_map.get(tg_id, 0)
        max_leads = overrides[tg_id] if tg_id in overrides else info['max_leads']
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
    set_availability(manager_id, False, reason='bot_blocked')
    name = MANAGERS_BY_ID.get(manager_id, manager_id)
    logger.warning(f"{name} ({manager_id}) заблокував бота — деактивовано")
    await notify_admins(f"🔕 <b>{name}</b> заблокував бота — автоматично виведено з черги")


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


async def broadcast_to_all(lead_id: str, **tick_ctx):
    """
    Розіслати заявку всім вільним менеджерам.
    Оригінальний менеджер отримує оновлення через edit_msg і явно виключається
    з queue, щоб не отримати повідомлення двічі.
    """
    lead = get_lead(lead_id)
    if not lead or lead['status'] in ('taken', 'duplicate', 'closed'):
        return

    orig_manager = lead['manager_id']
    skipped      = get_skipped(lead_id)

    # Виключаємо orig_manager з черги — він вже отримає edit_msg нижче
    exclude = list(set(skipped + ([orig_manager] if orig_manager else [])))
    queue   = sorted_queue(exclude=exclude, **tick_ctx)
    text    = f"{lead['title']}\n👤 <i>Відкрита черга</i>"

    if orig_manager:
        await edit_msg(orig_manager, lead_id, text, keep_buttons=True)

    for mid in queue:
        await delete_and_send(mid, lead_id, text)

    q("UPDATE leads SET status='broadcast', esc_level=1, sent_at=? WHERE lead_id=?",
      (datetime.now().timestamp(), lead_id))
    logger.info(f"Заявка {lead_id} розіслана всім ({len(queue)} менеджерів)")


async def escalate_warn(lead_id: str, title: str, **tick_ctx):
    lead = get_lead(lead_id)
    if not lead or lead['status'] in ('taken', 'duplicate', 'closed'):
        return
    warn = (
        f"⚠️⚠️⚠️ <b>ТЕРМІНОВО!</b>\n"
        f"Заявка вже <b>5 хвилин</b> без відповіді!\n\n{title}"
    )
    queue = sorted_queue(exclude=get_skipped(lead_id), **tick_ctx)
    for mid in queue:
        await delete_and_send(mid, lead_id, warn)
    q("UPDATE leads SET esc_level=2 WHERE lead_id=?", (lead_id,))
    logger.info(f"Заявка {lead_id}: 5-хвилинне попередження")


async def escalate_sos(lead_id: str, title: str, **tick_ctx):
    lead = get_lead(lead_id)
    if not lead or lead['status'] in ('taken', 'duplicate', 'closed'):
        return
    sos = (
        f"🆘🚨💀🔴 <b>SOS!!! ЗАЯВКА 10 ХВИЛИН!!!</b> 🔴💀🚨🆘\n"
        f"😱🔥💥 ХТОСЬ ВІЗЬМІТЬ ВЖЕ! 💥🔥😱\n\n{title}"
    )
    queue = sorted_queue(exclude=get_skipped(lead_id), **tick_ctx)
    for mid in queue:
        await delete_and_send(mid, lead_id, sos)
    now = datetime.now().timestamp()
    q("UPDATE leads SET esc_level=3, last_rebroadcast_at=? WHERE lead_id=?", (now, lead_id))
    logger.info(f"Заявка {lead_id}: SOS 10 хвилин")


async def rebroadcast_periodic(lead_id: str, title: str, **tick_ctx):
    lead = get_lead(lead_id)
    if not lead or lead['status'] in ('taken', 'duplicate', 'closed'):
        return
    msg = (
        f"🔄 <b>Заявка досі не взята!</b>\n"
        f"⏰ Повторна розсилка — будь ласка, візьміть в роботу!\n\n{title}"
    )
    queue = sorted_queue(exclude=get_skipped(lead_id), **tick_ctx)
    for mid in queue:
        await delete_and_send(mid, lead_id, msg)
    q("UPDATE leads SET last_rebroadcast_at=? WHERE lead_id=?",
      (datetime.now().timestamp(), lead_id))
    logger.info(f"Заявка {lead_id}: повторна розсилка (кожні 30 хв)")


# ─── ПЛАНУВАЛЬНИК ─────────────────────────────────────────────────────────────

async def _check_schedules():
    """Надсилає нагадування менеджерам на початку робочого дня."""
    from datetime import timezone, timedelta
    tz    = timezone(timedelta(hours=3))  # Europe/Kyiv (UTC+3)
    now   = datetime.now(tz)
    today = now.strftime('%Y-%m-%d')
    weekday = now.weekday()  # 0=пн, 6=нд
    current_time = now.strftime('%H:%M')

    schedules = get_all_schedules()
    for manager_id, sch in schedules.items():
        if not sch.get('enabled', 1):
            continue
        if sch.get('last_notified') == today:
            continue
        days = [int(d) for d in sch['days'].split(',') if d.strip()]
        if weekday not in days:
            continue
        if current_time != sch['start_time']:
            continue
        try:
            name = MANAGERS_BY_ID.get(manager_id, manager_id)
            await _app.bot.send_message(chat_id=manager_id, text="⏰")
            await asyncio.sleep(2)
            await _app.bot.send_message(chat_id=manager_id, text="⏰⏰")
            await asyncio.sleep(2)
            await _app.bot.send_message(
                chat_id=manager_id,
                text=f"⏰⏰⏰ <b>{name}</b>, твій робочий час почався!\nНе забудь увімкнути бота — натисни «✅ Увійти в чергу» якщо ще не зробив це.",
                parse_mode='HTML',
            )
            update_last_notified(manager_id, today)
            logger.info(f"Schedule: нагадування надіслано {name} ({manager_id})")
        except Exception as e:
            logger.warning(f"Schedule: не вдалось надіслати {manager_id}: {e}")


async def scheduler_loop():
    last_cleanup  = datetime.now().month
    last_day      = datetime.now().day
    last_sch_min  = ''
    while True:
        await asyncio.sleep(SCHEDULER_TICK)
        try:
            await _tick()
            now = datetime.now()
            if now.day != last_day:
                _reset_limit_overrides()
                last_day = now.day
            if now.month != last_cleanup:
                _cleanup_old_records()
                last_cleanup = now.month
            # Перевіряємо розклади раз на хвилину
            cur_min = now.strftime('%H:%M')
            if cur_min != last_sch_min:
                last_sch_min = cur_min
                await _check_schedules()
        except Exception as e:
            logger.error(f"Scheduler помилка: {e}")
            await notify_admin_error("scheduler (фоновий планувальник)", e)


def _reset_limit_overrides():
    reset_all_limit_overrides()
    logger.info("Ручні ліміти скинуто (новий день)")


def _cleanup_old_records():
    """Видаляє записи старші за 2 місяці (зберігає поточний + попередній)."""
    now       = datetime.now()
    keep_from = (now.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m')

    q("DELETE FROM stats WHERE month < ?", (keep_from,))
    q("DELETE FROM leads WHERE created_at < ? AND status IN ('taken','duplicate','closed')",
      (datetime.now().timestamp() - 60 * 24 * 3600,))
    q("DELETE FROM messages WHERE lead_id NOT IN (SELECT lead_id FROM leads)")
    q("DELETE FROM skipped  WHERE lead_id NOT IN (SELECT lead_id FROM leads)")
    logger.info(f"БД: очищено записи до {keep_from}")


async def _tick():
    now   = datetime.now().timestamp()
    leads = q(
        "SELECT * FROM leads WHERE status NOT IN ('taken','duplicate','closed') ORDER BY created_at DESC",
        fetch='all',
    )
    if not leads:
        return

    # Спільні дані для всіх лідів цього тіку — один набір запитів замість N
    managers  = fetch_managers()
    taken_map = get_all_taken(day_key())
    avail_map = get_all_availability()
    overrides = get_all_max_leads_overrides()
    sent_map  = _build_sent_map()
    tick_ctx  = dict(
        managers=managers,
        taken_map=taken_map,
        avail_map=avail_map,
        overrides=overrides,
        sent_map=sent_map,
    )

    for lead in leads:
        lid     = lead['lead_id']
        lvl     = lead['esc_level']
        sent_at = lead['sent_at']
        last_rb = lead['last_rebroadcast_at']

        logger.debug(
            f"_tick | lead={lid} status={lead['status']} esc={lvl} "
            f"sent={'yes' if sent_at else 'no'} "
            f"last_rb={int(now - last_rb)}s ago" if last_rb else
            f"_tick | lead={lid} status={lead['status']} esc={lvl} "
            f"sent={'yes' if sent_at else 'no'} last_rb=none"
        )

        # ── Заявки без sent_at (ще не розіслані або no_managers) ──────────────
        if lead['status'] in ('queued', 'no_managers') and not sent_at:
            if now - lead['created_at'] > 5:
                await assign_next(lid)
            continue

        if not sent_at:
            continue

        # ── Заявки з sent_at — ескалація ──────────────────────────────────────
        # Всі рівні рахуються від sent_at (оновлюється при кожній розсилці),
        # тому стара created_at із Kommo не впливає на таймаути.
        age = now - sent_at

        if lvl == 0 and age >= TIMEOUT_PERSONAL:
            # Розсилаємо тільки якщо немає іншої активної broadcast заявки
            active_broadcast = q(
                "SELECT lead_id FROM leads WHERE status='broadcast' AND lead_id != ? LIMIT 1",
                (lid,), fetch='one',
            )
            if not active_broadcast:
                waiting = q(
                    "SELECT COUNT(*) as cnt FROM leads WHERE status NOT IN ('taken','duplicate','closed','broadcast') AND esc_level=0 AND sent_at IS NOT NULL AND lead_id != ?",
                    (lid,), fetch='one',
                )
                waiting_count = waiting['cnt'] if waiting else 0
                if waiting_count > 0:
                    logger.info(f"Broadcast: заявка {lid} іде всім | в черзі чекають: {waiting_count}")
                await broadcast_to_all(lid, **tick_ctx)
        elif lvl == 1 and age >= TIMEOUT_WARN:
            await escalate_warn(lid, lead['title'], **tick_ctx)
        elif lvl == 2 and age >= TIMEOUT_SOS:
            await escalate_sos(lid, lead['title'], **tick_ctx)
        elif lvl >= 3:
            rb_base = last_rb or sent_at or lead['created_at']
            if now - rb_base >= TIMEOUT_REBROADCAST:
                await rebroadcast_periodic(lid, lead['title'], **tick_ctx)


# ─── ADMIN UI ─────────────────────────────────────────────────────────────────

ADMIN_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton("👥 Статус менеджерів"), KeyboardButton("📊 Черга")],
        [KeyboardButton("📅 Статистика день"),  KeyboardButton("📆 Статистика місяць")],
        [KeyboardButton("📋 Активні заявки"),   KeyboardButton("⚙️ Ліміти")],
        [KeyboardButton("🔄 Синхронізація"),    KeyboardButton("🔌 Підключення")],
        [KeyboardButton("⏰ Розклади")],
    ],
    resize_keyboard=True,
    is_persistent=True,
)

MANAGER_KB = ReplyKeyboardMarkup(
    [[KeyboardButton("✅ Увійти в чергу"), KeyboardButton("🚫 Вийти з черги")]],
    resize_keyboard=True,
    is_persistent=True,
)


def work_keyboard(is_active: bool) -> InlineKeyboardMarkup:
    label = "🚫 Вийти з черги" if is_active else "✅ Увійти в чергу"
    data  = "work:off"         if is_active else "work:on"
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=data)]])


# ─── ОБРОБНИКИ МЕНЕДЖЕРА ─────────────────────────────────────────────────────

async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id   = str(update.effective_user.id)
    user_name = update.effective_user.full_name

    is_admin   = user_id in ADMIN_IDS
    is_manager = user_id in MANAGERS.values()

    if not is_admin and not is_manager:
        await update.message.reply_text("⛔ У вас немає доступу до цього бота.")
        return

    mgr_name = MANAGERS_BY_ID.get(user_id, user_name)
    mark_connected(user_id, mgr_name)

    # Адмін завжди отримує адмін-панель (навіть якщо він є в MANAGERS)
    if is_admin:
        await update.message.reply_text("👋 Вітаю, адміне!\nОберіть дію:", reply_markup=ADMIN_KB)
    else:
        active = is_available(user_id)
        status = "✅ В черзі" if active else "🚫 Не в черзі"
        await update.message.reply_text(
            f"✅ Вітаю, {mgr_name}!\nПоточний статус: {status}",
            reply_markup=MANAGER_KB,
        )

    if not is_admin:
        await notify_admins(f"✅ <b>{mgr_name}</b> підключив(ла) бота\n👤 ID: <code>{user_id}</code>")


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


async def on_work_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    name    = MANAGERS_BY_ID.get(user_id)
    if not name:
        return

    active = update.message.text == "✅ Увійти в чергу"

    if active:
        managers = fetch_managers()
        if user_id not in managers:
            reason = get_block_reason(user_id) or "❌ Ви не можете увійти в чергу. Зверніться до керівника."
            await update.message.reply_text(reason, reply_markup=MANAGER_KB)
            set_availability(user_id, False, reason='blocked')
            return

    set_availability(user_id, active, reason=None if active else 'manual')

    status = "✅ Ви в черзі — заявки надходитимуть" if active else "🚫 Ви вийшли з черги — заявки не надходитимуть"
    await update.message.reply_text(status, reply_markup=MANAGER_KB)
    logger.info(f"{name} {'увійшов в чергу' if active else 'вийшов з черги'}")


# ─── ОБРОБНИКИ АДМІНА ────────────────────────────────────────────────────────

async def _handle_manager_status(message, managers: dict):
    month         = day_key()
    connected_ids = {r['manager_id'] for r in get_connected()}
    avail_map     = get_all_availability()
    overrides     = get_all_max_leads_overrides()
    taken_map     = get_all_taken(month)
    sent_map      = _build_sent_map()
    exit_reasons  = get_all_exit_reasons()

    lines = ["👥 <b>Статус менеджерів:</b>\n"]
    for name, tg_id in MANAGERS.items():
        if tg_id == '0':
            lines.append(f"❌ {name} — ID не вказано")
            continue
        if tg_id not in managers:
            continue
        if tg_id not in connected_ids:
            lines.append(f"(КОРИСТУВАЧ ❌) {name} — ще не підключився")
            continue
        taken     = taken_map.get(tg_id, 0)
        info      = managers.get(tg_id, {})
        max_leads = overrides[tg_id] if tg_id in overrides else info.get('max_leads')
        lim_mark  = " ✏️" if tg_id in overrides else ""
        limit_str = '∞' if max_leads is None else f"{max_leads}{lim_mark}"
        at_limit  = max_leads is not None and taken >= max_leads
        is_active = avail_map.get(tg_id, False)
        has_pending = sent_map.get(tg_id, 0) > 0

        if at_limit:
            lines.append(f"(БОТ ⛔) {name} — ліміт вичерпано | взяв: {taken}/{limit_str}")
        elif not is_active:
            reason = exit_reasons.get(tg_id)
            if reason == 'blocked':
                lines.append(f"(БОТ 🔒) {name} — недостатні показники | взяв: {taken}/{limit_str}")
            elif reason == 'bot_blocked':
                lines.append(f"(БОТ 🔕) {name} — заблокував бота | взяв: {taken}/{limit_str}")
            else:
                lines.append(f"(КОРИСТУВАЧ 🚫) {name} — не в роботі | взяв: {taken}/{limit_str}")
        elif has_pending:
            lines.append(f"(КОРИСТУВАЧ 📨) {name} — очікує відповіді | взяв: {taken}/{limit_str}")
        else:
            lines.append(f"(КОРИСТУВАЧ ✅) {name} — в роботі | взяв: {taken}/{limit_str}")
    await send_long(message, '\n'.join(lines))


async def _handle_connections(message):
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
    await send_long(message, '\n'.join(lines))


async def _handle_active_leads(message, managers: dict):
    rows = q(
        "SELECT * FROM leads WHERE status NOT IN ('taken','duplicate','closed') ORDER BY created_at DESC",
        fetch='all',
    )
    if not rows:
        await message.reply_text("✅ Немає активних заявок")
        return

    status_map = {
        'queued':      '🕐 В черзі',
        'sent':        '📨 Відправлена',
        'broadcast':   '📢 Розіслана всім',
        'no_managers': '⚠️ Немає менеджерів',
    }
    lines = [f"📋 <b>Активні заявки ({len(rows)}):</b>\n"]
    for lead in rows:
        age_min    = int((datetime.now().timestamp() - lead['created_at']) / 60)
        status_str = status_map.get(lead['status'], lead['status'])
        mgr        = '—' if lead['status'] == 'broadcast' else managers.get(lead['manager_id'] or '', {}).get('name', '—')
        lines.append(
            f"{lead['title']}\n"
            f"{status_str}\n"
            f"⏱ {age_min} хв\n"
            f"👤 {mgr}"
        )
    await send_long(message, '\n\n'.join(lines))


async def _handle_daily_stats(message):
    now_dt      = datetime.now()
    today_start = now_dt.replace(hour=0, minute=0, second=0, microsecond=0).timestamp()
    today_str   = now_dt.strftime('%d.%m.%Y')

    # Взяті сьогодні — по taken_at, щоб збігатись з лімітами черги
    taken_rows = q(
        "SELECT * FROM leads WHERE taken_at >= ? AND status = 'taken' ORDER BY taken_at",
        (today_start,), fetch='all',
    )
    # Активні зараз (ще не взяті)
    active_count = q(
        "SELECT COUNT(*) as cnt FROM leads WHERE status NOT IN ('taken','duplicate','closed')",
        fetch='one',
    )['cnt']

    if not taken_rows and active_count == 0:
        await message.reply_text(
            f"📅 <b>За сьогодні ({today_str})</b>\n\nЗаявок не було.",
            parse_mode='HTML',
        )
        return

    table_rows = []
    mgr_taken_d     = defaultdict(int)
    mgr_reactions_d = defaultdict(list)

    for lead in taken_rows:
        taken_str    = datetime.fromtimestamp(lead['taken_at']).strftime('%H:%M')
        reaction_min = max(0, int((lead['taken_at'] - lead['created_at']) / 60))
        reaction_str = f"{reaction_min} хв"
        mgr_name     = MANAGERS_BY_ID.get(lead['manager_id'], '—')
        table_rows.append((mgr_name, f"#{lead['lead_id']}", taken_str, reaction_str))

        mid = lead['manager_id'] or '—'
        mgr_taken_d[mid] += 1
        mgr_reactions_d[mid].append(reaction_min)

    summary_rows = []
    for mgr_name, tg_id in MANAGERS.items():
        t = mgr_taken_d.get(tg_id, 0)
        if t == 0:
            continue
        reactions = mgr_reactions_d.get(tg_id, [])
        avg_str = f"{int(sum(reactions)/len(reactions))} хв" if reactions else "—"
        summary_rows.append((mgr_name, str(t), avg_str))

    header_line = (
        f"📅 <b>За сьогодні ({today_str})</b>\n"
        f"Взято: {len(taken_rows)} | Активних зараз: {active_count}"
    )

    if not table_rows:
        await message.reply_text(header_line + "\n\nЩе ніхто не взяв заявок.", parse_mode='HTML')
        return

    headers = ["Менеджер", "Заявка", "Взято о", "Реакція"]
    col_w   = [max(len(h), max(len(r[i]) for r in table_rows))
               for i, h in enumerate(headers)]

    def fmt_row(cols):
        return " | ".join(c.ljust(w) for c, w in zip(cols, col_w))

    summary_block = ""
    if summary_rows:
        s_headers = ["Менеджер", "Взято", "Сер. реакція"]
        s_col_w   = [max(len(h), max(len(r[i]) for r in summary_rows))
                     for i, h in enumerate(s_headers)]
        def fmt_s(cols):
            return " | ".join(c.ljust(w) for c, w in zip(cols, s_col_w))
        summary_block = (
            f"\n\n📊 <b>По менеджерах:</b>\n"
            f"<pre>{fmt_s(s_headers)}\n"
            f"{'-+-'.join('-' * w for w in s_col_w)}\n"
            f"{chr(10).join(fmt_s(r) for r in summary_rows)}</pre>"
        )

    await send_long(
        message,
        f"{header_line}\n\n"
        f"<pre>{fmt_row(headers)}\n"
        f"{'-+-'.join('-' * w for w in col_w)}\n"
        f"{chr(10).join(fmt_row(r) for r in table_rows)}</pre>"
        f"{summary_block}",
    )


async def _handle_monthly_stats(message):
    now_dt      = datetime.now()
    month_start = now_dt.replace(day=1, hour=0, minute=0, second=0, microsecond=0).timestamp()
    month_label = now_dt.strftime('%m.%Y')

    # Взяті цього місяця — по taken_at, щоб збігатись з лімітами черги
    month_rows = q(
        "SELECT * FROM leads WHERE taken_at >= ? AND status = 'taken' ORDER BY taken_at",
        (month_start,), fetch='all',
    )
    if not month_rows:
        await message.reply_text(
            f"📆 <b>За місяць ({month_label})</b>\n\nЗаявок ще не взято.",
            parse_mode='HTML',
        )
        return

    mgr_taken     = defaultdict(int)
    mgr_reactions = defaultdict(list)
    all_reactions = []

    for lead in month_rows:
        mid = lead['manager_id'] or '—'
        mgr_taken[mid] += 1
        if lead['taken_at'] and lead['created_at']:
            reaction = max(0, int((lead['taken_at'] - lead['created_at']) / 60))
            mgr_reactions[mid].append(reaction)
            all_reactions.append(reaction)

    m_rows      = []
    total_taken = 0
    for mgr_name, tg_id in MANAGERS.items():
        t = mgr_taken.get(tg_id, 0)
        if t == 0:
            continue
        total_taken += t
        reactions = mgr_reactions.get(tg_id, [])
        avg_str = f"{int(sum(reactions)/len(reactions))} хв" if reactions else "—"
        m_rows.append((mgr_name, str(t), avg_str))

    overall_avg = (
        f"{int(sum(all_reactions)/len(all_reactions))} хв" if all_reactions else "—"
    )

    m_headers = ["Менеджер", "Взято", "Сер. реакція"]
    m_col_w   = [max(len(h), max(len(r[i]) for r in m_rows))
                 for i, h in enumerate(m_headers)]

    def fmt_m(cols):
        return " | ".join(c.ljust(w) for c, w in zip(cols, m_col_w))

    await send_long(
        message,
        f"📆 <b>За місяць ({month_label})</b>\n"
        f"Взято: {total_taken} | Сер. реакція: {overall_avg}\n\n"
        f"<pre>{fmt_m(m_headers)}\n"
        f"{'-+-'.join('-' * w for w in m_col_w)}\n"
        f"{chr(10).join(fmt_m(r) for r in m_rows)}</pre>",
    )


async def _handle_queue(message, managers: dict):
    month     = day_key()
    avail_map = get_all_availability()
    overrides = get_all_max_leads_overrides()
    taken_map = get_all_taken(month)
    sent_map  = _build_sent_map()

    # Всі активні менеджери відсортовані по кількості взятих
    active = []
    for tg_id, info in managers.items():
        if not avail_map.get(tg_id, False):
            continue
        taken     = taken_map.get(tg_id, 0)
        max_leads = overrides[tg_id] if tg_id in overrides else info['max_leads']
        if max_leads is not None and taken >= max_leads:
            continue
        active.append((taken, tg_id))
    active.sort()

    if not active:
        await message.reply_text("😶 Черга порожня — немає вільних менеджерів")
        return

    lines = ["📊 <b>Поточна черга:</b>\n"]
    for i, (taken, tg_id) in enumerate(active, 1):
        info      = managers.get(tg_id, {})
        name      = info.get('name', tg_id)
        max_leads = overrides[tg_id] if tg_id in overrides else info.get('max_leads')
        limit_str = '∞' if max_leads is None else str(max_leads)
        pending   = sent_map.get(tg_id, 0) > 0
        mark      = " 📨" if pending else ""
        lines.append(f"{i}. {name} — взяв: {taken}/{limit_str}{mark}")

    lines.append("\n<i>📨 — очікує відповіді на поточну заявку</i>")
    await send_long(message, '\n'.join(lines))


async def _handle_sync(message):
    if not AMO_TOKEN:
        await message.reply_text(
            "⚠️ <b>AMO_TOKEN не налаштовано</b>\n"
            "Додайте токен Kommo API в .env файл на сервері:\n"
            "<code>AMO_TOKEN=ваш_токен</code>",
            parse_mode='HTML',
        )
        return
    msg = await message.reply_text("🔄 Синхронізація... зачекайте")
    try:
        added, skipped, closed = await sync_from_kommo()
        await msg.edit_text(
            f"✅ <b>Синхронізацію завершено</b>\n"
            f"➕ Додано нових: <b>{added}</b>\n"
            f"⏭ Вже були в системі: <b>{skipped}</b>\n"
            f"🔄 Змінили статус в CRM: <b>{closed}</b>",
            parse_mode='HTML',
        )
    except Exception as e:
        await msg.edit_text(f"❌ Помилка синхронізації: {e}")
        logger.error(f"Sync error: {e}")


async def on_admin_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        return

    text     = update.message.text
    managers = fetch_managers()

    if text == "👥 Статус менеджерів":
        await _handle_manager_status(update.message, managers)
    elif text == "🔌 Підключення":
        await _handle_connections(update.message)
    elif text == "📋 Активні заявки":
        await _handle_active_leads(update.message, managers)
    elif text == "📅 Статистика день":
        await _handle_daily_stats(update.message)
    elif text == "📆 Статистика місяць":
        await _handle_monthly_stats(update.message)
    elif text == "📊 Черга":
        await _handle_queue(update.message, managers)
    elif text == "🔄 Синхронізація":
        await _handle_sync(update.message)


# ─── СИНХРОНІЗАЦІЯ З KOMMO ───────────────────────────────────────────────────

def _make_lead_title(status_id: str, lead_id: str) -> str:
    raw_label = HOT_STATUSES.get(str(status_id), 'Нова заявка')
    if 'Гаряча' in raw_label:
        header = '🔥 ГАРЯЧА ЗАЯВКА'
    elif 'Кваліфікована' in raw_label:
        header = '⭐ КВАЛІФІКОВАНА ЗАЯВКА'
    else:
        header = '📋 НОВА ЗАЯВКА'
    lead_url = f"https://{AMO_SUBDOMAIN}.kommo.com/leads/detail/{lead_id}"
    return f'{header}\n🔗 <a href="{lead_url}">Угода #{lead_id}</a>'


async def sync_from_kommo() -> tuple[int, int, int]:
    if not AMO_TOKEN:
        return 0, 0, 0

    url     = f"https://{AMO_SUBDOMAIN}.kommo.com/api/v4/leads"
    headers = {"Authorization": f"Bearer {AMO_TOKEN}"}
    added   = 0
    skipped = 0
    closed  = 0
    page    = 1
    kommo_ids: set[str] = set()

    async with aiohttp.ClientSession() as session:
        while True:
            params = {
                "filter[statuses][0][pipeline_id]": AMO_PIPELINE_ID,
                "filter[statuses][0][status_id]":   AMO_HOT_STATUS_ID,
                "limit": 250,
                "page":  page,
            }
            async with session.get(url, headers=headers, params=params) as resp:
                if resp.status == 204:
                    break
                if resp.status != 200:
                    logger.error(f"Kommo sync: HTTP {resp.status}")
                    break
                data  = await resp.json()
                leads = data.get("_embedded", {}).get("leads", [])
                if not leads:
                    break

                for lead in leads:
                    lead_id = str(lead["id"])
                    kommo_ids.add(lead_id)
                    if get_lead(lead_id):
                        skipped += 1
                        continue

                    title   = _make_lead_title(AMO_HOT_STATUS_ID, lead_id)
                    created = lead.get("created_at") or datetime.now().timestamp()
                    try:
                        q("INSERT INTO leads (lead_id, status, created_at, title) VALUES (?,?,?,?)",
                          (lead_id, "queued", created, title))
                        added += 1
                    except Exception as e:
                        logger.error(f"Kommo sync: не вдалось додати {lead_id}: {e}")

                if len(leads) < 250:
                    break
                page += 1

    # Закриваємо активні заявки яких вже немає в Kommo
    active_rows = q(
        "SELECT lead_id FROM leads WHERE status NOT IN ('taken','duplicate','closed')",
        fetch='all',
    )
    for row in (active_rows or []):
        if row['lead_id'] not in kommo_ids:
            q("UPDATE leads SET status='closed' WHERE lead_id=?", (row['lead_id'],))
            await remove_from_others(row['lead_id'], note="📋 Заявку закрито в CRM")
            closed += 1
            logger.info(f"Sync: заявка {row['lead_id']} відсутня в Kommo → закрито")

    return added, skipped, closed


# ─── CONVERSATION: ЛІМІТИ МЕНЕДЖЕРІВ ────────────────────────────────────────

LIMIT_SELECT, LIMIT_INPUT = range(2)


async def limits_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        return ConversationHandler.END

    managers  = fetch_managers()
    overrides = get_all_max_leads_overrides()

    buttons = []
    for tg_id, info in managers.items():
        name     = info['name']
        override = overrides.get(tg_id)
        if override is not None:
            lim_str = f"{override} ✏️"
        elif info['max_leads'] is None:
            lim_str = "∞"
        else:
            lim_str = str(info['max_leads'])
        buttons.append([InlineKeyboardButton(f"{name} — {lim_str}", callback_data=f"setlim:{tg_id}")])

    buttons.append([InlineKeyboardButton("❌ Скасувати", callback_data="setlim:cancel")])
    await update.message.reply_text(
        "⚙️ <b>Ліміти менеджерів</b>\n"
        "Виберіть менеджера для зміни ліміту:\n\n"
        "<i>✏️ = ручний ліміт | ∞ = без ліміту (з таблиці)</i>",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return LIMIT_SELECT


async def limits_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "setlim:cancel":
        await query.edit_message_text("❌ Скасовано")
        return ConversationHandler.END

    tg_id    = query.data.split(':', 1)[1]
    managers = fetch_managers()
    name     = managers.get(tg_id, {}).get('name', tg_id)

    context.user_data['limit_tg_id'] = tg_id
    context.user_data['limit_name']  = name

    await query.edit_message_text(
        f"⚙️ Менеджер: <b>{name}</b>\n\n"
        f"Введіть новий ліміт лідів на день:\n"
        f"• число (напр. <code>5</code>) — встановити ліміт\n"
        f"• <code>0</code> — без ліміту (∞, брати з таблиці)\n"
        f"• /cancel — скасувати",
        parse_mode='HTML',
    )
    return LIMIT_INPUT


async def limits_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        return ConversationHandler.END

    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("⚠️ Введіть ціле число або 0 для скидання ліміту")
        return LIMIT_INPUT

    value     = int(text)
    tg_id     = context.user_data.get('limit_tg_id')
    name      = context.user_data.get('limit_name', tg_id)
    max_leads = None if value == 0 else value

    set_max_leads_override(tg_id, max_leads)

    lim_str = "∞ (без ліміту, з таблиці)" if max_leads is None else str(max_leads)
    await update.message.reply_text(
        f"✅ Ліміт для <b>{name}</b> встановлено: <b>{lim_str}</b>",
        parse_mode='HTML',
        reply_markup=ADMIN_KB,
    )
    return ConversationHandler.END


async def limits_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Скасовано", reply_markup=ADMIN_KB)
    return ConversationHandler.END


# ─── CONVERSATION: РОЗКЛАДИ МЕНЕДЖЕРІВ ───────────────────────────────────────

SCHED_SELECT, SCHED_DAYS, SCHED_TIME = range(3)

DAYS_UA = {0: 'Пн', 1: 'Вт', 2: 'Ср', 3: 'Чт', 4: 'Пт', 5: 'Сб', 6: 'Нд'}


def _format_schedule(sch: dict) -> str:
    days = [DAYS_UA[int(d)] for d in sch['days'].split(',') if d.strip()]
    status = '✅' if sch.get('enabled', 1) else '❌'
    return f"{status} {', '.join(days)} о {sch['start_time']}"


async def schedules_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        return ConversationHandler.END

    schedules = get_all_schedules()
    buttons   = []
    for name, tg_id in sorted(MANAGERS.items(), key=lambda x: x[0]):
        sch = schedules.get(tg_id)
        sch_text = _format_schedule(sch) if sch else '—'
        buttons.append([InlineKeyboardButton(
            f"{name} | {sch_text}", callback_data=f"sched:{tg_id}"
        )])

    buttons.append([InlineKeyboardButton("❌ Скасувати", callback_data="sched:cancel")])
    await update.message.reply_text(
        "⏰ <b>Розклади менеджерів</b>\nОберіть менеджера для редагування:",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode='HTML',
    )
    return SCHED_SELECT


async def schedules_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    tg_id = query.data.split(':', 1)[1]

    if tg_id == 'cancel':
        await query.edit_message_text("❌ Скасовано")
        return ConversationHandler.END

    context.user_data['sched_manager_id'] = tg_id

    name = MANAGERS_BY_ID.get(tg_id, tg_id)
    schedules = get_all_schedules()
    sch = schedules.get(tg_id)
    current = _format_schedule(sch) if sch else 'не задано'

    cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Скасувати", callback_data="sched:cancel")]])
    await query.edit_message_text(
        f"⏰ <b>{name}</b>\nПоточний розклад: {current}\n\n"
        "Введіть робочі дні через кому:\n"
        "<code>0</code>=Пн <code>1</code>=Вт <code>2</code>=Ср <code>3</code>=Чт "
        "<code>4</code>=Пт <code>5</code>=Сб <code>6</code>=Нд\n\n"
        "Приклад: <code>0,1,2,3,4</code> (пн-пт)",
        parse_mode='HTML',
        reply_markup=cancel_kb,
    )
    return SCHED_DAYS


async def schedules_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        return ConversationHandler.END

    text = update.message.text.strip()
    try:
        days = [int(d.strip()) for d in text.split(',')]
        assert all(0 <= d <= 6 for d in days) and days
    except Exception:
        await update.message.reply_text("❌ Невірний формат. Введіть цифри від 0 до 6 через кому.\nПриклад: <code>0,1,2,3,4</code>", parse_mode='HTML')
        return SCHED_DAYS

    context.user_data['sched_days'] = ','.join(str(d) for d in sorted(set(days)))
    cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Скасувати", callback_data="sched:cancel")]])
    await update.message.reply_text(
        "Введіть час початку роботи у форматі <code>ГГ:ХХ</code>\nПриклад: <code>16:00</code>",
        parse_mode='HTML',
        reply_markup=cancel_kb,
    )
    return SCHED_TIME


async def schedules_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        return ConversationHandler.END

    text = update.message.text.strip()
    try:
        datetime.strptime(text, '%H:%M')
    except ValueError:
        await update.message.reply_text("❌ Невірний формат часу. Введіть у форматі <code>ГГ:ХХ</code>\nПриклад: <code>16:00</code>", parse_mode='HTML')
        return SCHED_TIME

    tg_id = context.user_data['sched_manager_id']
    days  = context.user_data['sched_days']
    set_schedule(tg_id, days, text)

    name = MANAGERS_BY_ID.get(tg_id, tg_id)
    days_str = ', '.join(DAYS_UA[int(d)] for d in days.split(','))
    await update.message.reply_text(
        f"✅ Розклад збережено!\n<b>{name}</b>: {days_str} о {text}",
        reply_markup=ADMIN_KB,
        parse_mode='HTML',
    )
    logger.info(f"Schedule: розклад {name} ({tg_id}) → {days} {text}")
    return ConversationHandler.END


async def schedules_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await update.message.reply_text("❌ Скасовано", reply_markup=ADMIN_KB)
    return ConversationHandler.END


# ─── CALLBACK ────────────────────────────────────────────────────────────────

async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    if query.data.startswith('setlim:'):
        await query.answer()
        return

    try:
        action, lead_id = query.data.split(':', 1)
    except ValueError:
        await query.answer()
        return

    manager_id = str(query.from_user.id)

    # ── work:on / work:off ────────────────────────────────────────────────────
    if action == 'work':
        try:
            name = MANAGERS_BY_ID.get(manager_id)
            if not name:
                await query.answer()
                return
            active = (lead_id == 'on')
            set_availability(manager_id, active, reason=None if active else 'manual')
            await query.answer()
            status = "✅ Ви в черзі — заявки надходитимуть" if active else "🚫 Ви вийшли з черги — заявки не надходитимуть"
            await query.edit_message_text(
                f"👤 <b>{name}</b>\n\n{status}\n\nЩоб змінити — напишіть /work",
                reply_markup=work_keyboard(active),
                parse_mode='HTML',
            )
            logger.info(f"{name} {'увійшов в чергу' if active else 'вийшов з черги'}")
        except Exception as e:
            logger.error(f"on_callback work: {e}")
            try:
                await query.answer()
            except Exception:
                pass
        return

    lead = get_lead(lead_id)
    if not lead:
        await query.answer("⚠️ Заявка не знайдена", show_alert=True)
        return

    if lead['status'] in ('taken', 'duplicate', 'closed'):
        await query.answer("❌ Цю заявку вже оброблено", show_alert=True)
        await edit_msg(manager_id, lead_id, "❌ Цю заявку вже оброблено")
        return

    managers = fetch_managers()
    mgr_name = managers.get(manager_id, {}).get('name', query.from_user.first_name or manager_id)

    try:
        if action in ('take', 't'):
            # Перевіряємо чи менеджер в черзі (активний + пройшов перевірку таблиці)
            if not is_available(manager_id) or manager_id not in managers:
                await query.answer("⛔ Ви поза чергою — заявку взяти неможливо", show_alert=True)
                return

            mgr_info  = managers.get(manager_id, {})
            overrides = get_all_max_leads_overrides()
            max_leads = overrides[manager_id] if manager_id in overrides else mgr_info.get('max_leads')

            if max_leads is not None:
                taken_today = get_taken(manager_id, day_key())
                if taken_today >= max_leads:
                    await query.answer("⛔ Ви вже взяли максимальну кількість лідів на сьогодні", show_alert=True)
                    await edit_msg(manager_id, lead_id, f"⛔ Ліміт вичерпано ({taken_today}/{max_leads})\n\n{lead['title']}")
                    return

            if not take_lead(lead_id, manager_id, day_key()):
                await query.answer("❌ Заявку вже взяв інший менеджер", show_alert=True)
                await edit_msg(manager_id, lead_id, "❌ Заявку вже взяв інший менеджер")
                return

            await query.answer()
            await edit_msg(manager_id, lead_id, f"✅ Ви взяли заявку в роботу!\n\n{lead['title']}")

            kommo_ok = await _set_kommo_responsible(lead_id, manager_id)
            if kommo_ok:
                await edit_msg(manager_id, lead_id, f"✅ Ви взяли заявку в роботу! | Відповідальний: {mgr_name}\n\n{lead['title']}")
            await remove_from_others(lead_id, except_id=manager_id,
                                     note=f"✅ Заявку взяв(ла) <b>{mgr_name}</b>")
            logger.info(f"Заявка {lead_id} взята {mgr_name} ({manager_id})")
            await notify_admins(f"✅ <b>{mgr_name}</b> взяв(ла) заявку в роботу\n\n{lead['title']}")

            # Сповіщаємо менеджера якщо він досяг ліміту
            if max_leads is not None:
                taken_today = get_taken(manager_id, day_key())
                if taken_today >= max_leads:
                    await _app.bot.send_message(
                        chat_id=manager_id,
                        text=f"⛔ Ви взяли максимальну кількість лідів на сьогодні ({max_leads}). "
                             f"Нові заявки надходитимуть завтра.",
                    )

        elif action in ('skip', 's'):
            # Якщо менеджер вийшов з черги — не дозволяємо відхиляти
            if not is_available(manager_id):
                await query.answer(
                    "⛔ Ви поза чергою. Щоб взаємодіяти із заявками — спочатку увійдіть у чергу (/work)",
                    show_alert=True,
                )
                return

            await query.answer()
            mark_skipped(lead_id, manager_id)
            await edit_msg(manager_id, lead_id, f"⏭ Ви відмовились від заявки\n\n{lead['title']}")

            lead = get_lead(lead_id)
            if lead and lead['status'] == 'sent':
                q("UPDATE leads SET status='queued', manager_id=NULL WHERE lead_id=?", (lead_id,))
                await assign_next(lead_id, exclude=get_skipped(lead_id))
            logger.info(f"Заявка {lead_id} відхилена {mgr_name}")

        elif action in ('dup', 'd'):
            if not is_available(manager_id):
                await query.answer(
                    "⛔ Ви поза чергою. Щоб взаємодіяти із заявками — спочатку увійдіть у чергу (/work)",
                    show_alert=True,
                )
                return

            await query.answer()
            q("UPDATE leads SET status='duplicate' WHERE lead_id=?", (lead_id,))
            await edit_msg(manager_id, lead_id, "🔁 Ви позначили заявку як дубль")
            await remove_from_others(lead_id, except_id=manager_id,
                                     note="🔁 Заявка закрита як дубль")
            logger.info(f"Заявка {lead_id} — дубль ({mgr_name})")

        else:
            await query.answer()

    except Exception as e:
        logger.error(f"on_callback {action} {lead_id}: {e}")
        try:
            await query.answer()
        except Exception:
            pass
        await notify_admin_error(f"on_callback (дія: {action}, заявка: {lead_id})", e, manager_id)


# ─── WEBHOOK AMOCRM ──────────────────────────────────────────────────────────

@asynccontextmanager
async def lifespan(fastapi: FastAPI):
    global _app
    init_db()
    init_default_schedules(MANAGERS)

    _app = Application.builder().token(BOT_TOKEN).build()

    _lim_entry = MessageHandler(filters.TEXT & filters.Regex(r'^⚙️ Ліміти$'), limits_start)
    _app.add_handler(ConversationHandler(
        entry_points=[_lim_entry],
        states={
            LIMIT_SELECT: [CallbackQueryHandler(limits_select, pattern=r'^setlim:'), _lim_entry],
            LIMIT_INPUT:  [_lim_entry, MessageHandler(filters.TEXT & ~filters.COMMAND, limits_input)],
        },
        fallbacks=[CommandHandler('cancel', limits_cancel)],
        per_user=True,
        allow_reentry=True,
    ))

    _sched_entry = MessageHandler(filters.TEXT & filters.Regex(r'^⏰ Розклади$'), schedules_start)
    _sched_cancel_cb = CallbackQueryHandler(schedules_select, pattern=r'^sched:cancel$')
    _app.add_handler(ConversationHandler(
        entry_points=[_sched_entry],
        states={
            SCHED_SELECT: [CallbackQueryHandler(schedules_select, pattern=r'^sched:'), _sched_entry],
            SCHED_DAYS:   [_sched_cancel_cb, _sched_entry, MessageHandler(filters.TEXT & ~filters.COMMAND, schedules_days)],
            SCHED_TIME:   [_sched_cancel_cb, _sched_entry, MessageHandler(filters.TEXT & ~filters.COMMAND, schedules_time)],
        },
        fallbacks=[CommandHandler('cancel', schedules_cancel)],
        per_user=True,
        allow_reentry=True,
    ))

    _app.add_handler(CallbackQueryHandler(on_callback))
    _app.add_handler(CommandHandler('start', on_start))
    _app.add_handler(CommandHandler('work', on_work))
    _app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r'^(✅ Увійти в чергу|🚫 Вийти з черги)$'),
        on_work_button,
    ))
    _app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(
            r'^(👥 Статус менеджерів|📊 Черга|🔌 Підключення|📋 Активні заявки|📅 Статистика день|📆 Статистика місяць|🔄 Синхронізація|⏰ Розклади)$'
        ),
        on_admin_button,
    ))

    await _app.initialize()
    await _app.start()

    from telegram import BotCommand, MenuButtonCommands
    await _app.bot.set_my_commands([
        BotCommand('start', '🔄 Головне меню / перезапуск'),
    ])
    await _app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())

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
                   or data.get('leads[add][0][id]')
                   or data.get('leads[delete][0][id]'))
    status_id   = (data.get('leads[status][0][status_id]')
                   or data.get('leads[add][0][status_id]'))
    pipeline_id = (data.get('leads[status][0][pipeline_id]')
                   or data.get('leads[add][0][pipeline_id]'))
    is_delete   = bool(data.get('leads[delete][0][id]'))

    logger.info(f"Webhook: lead_id={lead_id} status_id={status_id} pipeline_id={pipeline_id} delete={is_delete} keys={list(data.keys())[:6]}")

    if not lead_id:
        return {'ok': True}

    if is_delete:
        lead = get_lead(lead_id)
        if lead and lead['status'] not in ('taken', 'duplicate', 'closed'):
            q("UPDATE leads SET status='closed' WHERE lead_id=?", (lead_id,))
            await remove_from_others(lead_id, note="🗑 Заявку видалено в CRM")
            logger.info(f"Webhook: заявка {lead_id} видалена в CRM → закрито в боті")
        return {'ok': True}

    if str(pipeline_id) != AMO_PIPELINE_ID:
        # Якщо заявка є в нашій БД — закриваємо, бо вона пішла в іншу воронку
        lead = get_lead(lead_id)
        if lead and lead['status'] not in ('taken', 'duplicate', 'closed'):
            q("UPDATE leads SET status='closed' WHERE lead_id=?", (lead_id,))
            await remove_from_others(lead_id, note="📋 Заявку переміщено в іншу воронку CRM")
            logger.info(f"Webhook: заявка {lead_id} пішла в іншу воронку → закрито в боті")
        else:
            logger.info(f"Webhook: ігноруємо pipeline_id={pipeline_id} (не наша воронка)")
        return {'ok': True}

    if str(status_id) != AMO_HOT_STATUS_ID:
        lead = get_lead(lead_id)
        if lead and lead['status'] not in ('taken', 'duplicate', 'closed'):
            q("UPDATE leads SET status='closed' WHERE lead_id=?", (lead_id,))
            await remove_from_others(lead_id, note="📋 Заявку переміщено на інший етап в CRM")
            logger.info(f"Webhook: заявка {lead_id} змінила статус → закрито в боті")
        return {'ok': True}

    if get_lead(lead_id):
        return {'ok': True}

    title = _make_lead_title(status_id, lead_id)
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
