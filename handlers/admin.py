import logging
from collections import defaultdict
from datetime import datetime

from telegram import KeyboardButton, ReplyKeyboardMarkup, Update
from telegram.ext import ContextTypes

import state
from config import ADMIN_IDS, AMO_PIPELINE_ID, AMO_HOT_STATUS_ID, WEBHOOK_PATH, AMO_TOKEN
from db import (
    q, get_all_taken, get_all_availability, get_all_max_leads_overrides,
    get_all_exit_reasons, get_connected, get_managers_dict, get_all_managers,
    get_pending_managers, approve_manager, delete_manager,
)
from kommo import sync_from_kommo
from notifications import send_long, notify_admin_error
from queue_logic import day_key, _build_sent_map, cleanup_orphaned_manager_messages
from sheets import fetch_managers

logger = logging.getLogger(__name__)

ADMIN_KB = ReplyKeyboardMarkup(
    [
        [KeyboardButton("👥 Статус менеджерів"), KeyboardButton("📊 Черга")],
        [KeyboardButton("📅 Статистика день"),   KeyboardButton("📆 Статистика місяць")],
        [KeyboardButton("📋 Активні заявки"),    KeyboardButton("⚙️ Ліміти")],
        [KeyboardButton("🔄 Синхронізація"),     KeyboardButton("🔌 Підключення")],
        [KeyboardButton("⏰ Розклади"),           KeyboardButton("🔍 Діагностика")],
        [KeyboardButton("👤 Менеджери"),          KeyboardButton("🧹 Прибрати привиди")],
    ],
    resize_keyboard=True,
    is_persistent=True,
)


async def _handle_manager_status(message, managers: dict):
    month         = day_key()
    connected_ids = {r['manager_id'] for r in get_connected()}
    avail_map     = get_all_availability()
    overrides     = get_all_max_leads_overrides()
    taken_map     = get_all_taken(month)
    sent_map      = _build_sent_map()
    exit_reasons  = get_all_exit_reasons()

    lines = ["👥 <b>Статус менеджерів:</b>\n"]
    for name, tg_id in get_managers_dict().items():
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
            if reason == 'has_distributed':
                lines.append(f"(БОТ 📞) {name} — на зв'язку з клієнтом | взяв: {taken}/{limit_str}")
            elif reason == 'blocked':
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
    for name, tg_id in get_managers_dict().items():
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

    taken_rows = q(
        "SELECT * FROM leads WHERE taken_at >= ? AND status = 'taken' ORDER BY taken_at",
        (today_start,), fetch='all',
    )
    active_count = q(
        "SELECT COUNT(*) as cnt FROM leads WHERE status NOT IN ('taken','duplicate','closed')",
        fetch='one',
    )['cnt']

    if not taken_rows and active_count == 0:
        await message.reply_text(f"📅 <b>За сьогодні ({today_str})</b>\n\nЗаявок не було.", parse_mode='HTML')
        return

    table_rows      = []
    mgr_taken_d     = defaultdict(int)
    mgr_reactions_d = defaultdict(list)

    for lead in taken_rows:
        taken_str    = datetime.fromtimestamp(lead['taken_at']).strftime('%H:%M')
        reaction_min = max(0, int((lead['taken_at'] - lead['created_at']) / 60))
        mgr_name     = state.MANAGERS_BY_ID.get(lead['manager_id'], '—')
        table_rows.append((mgr_name, f"#{lead['lead_id']}", taken_str, f"{reaction_min} хв"))
        mid = lead['manager_id'] or '—'
        mgr_taken_d[mid] += 1
        mgr_reactions_d[mid].append(reaction_min)

    summary_rows = []
    for mgr_name, tg_id in get_managers_dict().items():
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
    col_w   = [max(len(h), max(len(r[i]) for r in table_rows)) for i, h in enumerate(headers)]

    def fmt_row(cols):
        return " | ".join(c.ljust(w) for c, w in zip(cols, col_w))

    summary_block = ""
    if summary_rows:
        s_headers = ["Менеджер", "Взято", "Сер. реакція"]
        s_col_w   = [max(len(h), max(len(r[i]) for r in summary_rows)) for i, h in enumerate(s_headers)]
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

    month_rows = q(
        "SELECT * FROM leads WHERE taken_at >= ? AND status = 'taken' ORDER BY taken_at",
        (month_start,), fetch='all',
    )
    if not month_rows:
        await message.reply_text(f"📆 <b>За місяць ({month_label})</b>\n\nЗаявок ще не взято.", parse_mode='HTML')
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
    for mgr_name, tg_id in get_managers_dict().items():
        t = mgr_taken.get(tg_id, 0)
        if t == 0:
            continue
        total_taken += t
        reactions = mgr_reactions.get(tg_id, [])
        avg_str = f"{int(sum(reactions)/len(reactions))} хв" if reactions else "—"
        m_rows.append((mgr_name, str(t), avg_str))

    overall_avg = f"{int(sum(all_reactions)/len(all_reactions))} хв" if all_reactions else "—"
    m_headers   = ["Менеджер", "Взято", "Сер. реакція"]
    m_col_w     = [max(len(h), max(len(r[i]) for r in m_rows)) for i, h in enumerate(m_headers)]

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


async def _handle_diagnostics(message):
    now = datetime.now().timestamp()
    lines = ["🔍 <b>Повна діагностика бота</b>\n"]

    all_leads = q(
        "SELECT * FROM leads WHERE status NOT IN ('taken','duplicate','closed') ORDER BY created_at DESC",
        fetch='all',
    ) or []
    msg_counts = {}
    for row in (q("SELECT lead_id, COUNT(*) as cnt FROM messages GROUP BY lead_id", fetch='all') or []):
        msg_counts[row['lead_id']] = row['cnt']

    lines.append(f"📋 <b>Активні заявки ({len(all_leads)}):</b>")
    if not all_leads:
        lines.append("  — немає")
    for lead in all_leads:
        age_s    = int(now - lead['created_at'])
        age_str  = f"{age_s//60}хв {age_s%60}с"
        sent_str = f"{int(now - lead['sent_at'])}с тому" if lead['sent_at'] else "❌ не відправлена"
        msgs_cnt = msg_counts.get(lead['lead_id'], 0)
        rb_str   = f" | перероз: {int(now - lead['last_rebroadcast_at'])}с тому" if lead['last_rebroadcast_at'] else ''
        lines.append(
            f"\n  <b>#{lead['lead_id']}</b> | {lead['status']} | esc={lead['esc_level']}\n"
            f"  вік: {age_str} | sent: {sent_str}\n"
            f"  повідомлень у менеджерів: {msgs_cnt}{rb_str}"
        )

    lines.append("\n\n📢 <b>Broadcast черга:</b>")
    bc_active = q("SELECT lead_id FROM leads WHERE status='broadcast' AND sent_at IS NOT NULL", fetch='all') or []
    bc_waiting = q("SELECT lead_id FROM leads WHERE status='broadcast' AND sent_at IS NULL ORDER BY created_at DESC", fetch='all') or []
    lines.append(f"  Активна (надіслана): {', '.join(r['lead_id'] for r in bc_active)}" if bc_active else "  Активна: немає")
    lines.append(f"  Чекають в черзі: {', '.join(r['lead_id'] for r in bc_waiting)}" if bc_waiting else "  Черга: порожня")

    lines.append("\n\n👥 <b>Менеджери:</b>")
    avail_map    = get_all_availability()
    overrides    = get_all_max_leads_overrides()
    taken_map    = get_all_taken(day_key())
    sent_map     = _build_sent_map()
    exit_reasons = get_all_exit_reasons()

    try:
        managers  = fetch_managers()
        sheets_ok = True
    except Exception as e:
        managers  = {}
        sheets_ok = False
        lines.append(f"  ⚠️ Google Sheets недоступний: {e}")

    for name, tg_id in get_managers_dict().items():
        active    = avail_map.get(tg_id, False)
        in_sheet  = tg_id in managers
        taken     = taken_map.get(tg_id, 0)
        pending   = sent_map.get(tg_id, 0)
        max_l     = overrides.get(tg_id) or (managers.get(tg_id, {}).get('max_leads') if in_sheet else '?')
        limit_str = '∞' if max_l is None else str(max_l)
        reason    = exit_reasons.get(tg_id, '')
        sheet_mark   = f"✅ таблиця | ліміт={limit_str} | взяв={taken}" if in_sheet else "❌ нема в таблиці"
        status_icon  = "🟢" if active else "🔴"
        pending_str  = " | 📨 чекає" if pending else ""
        reason_str   = f" ({reason})" if reason and not active else ""
        lines.append(f"  {status_icon} {name}{pending_str}{reason_str}\n     {sheet_mark}")

    from sheets import _cache_ts, _cache
    lines.append("\n\n📊 <b>Google Sheets:</b>")
    if sheets_ok:
        cache_age = int(now - _cache_ts) if _cache_ts else -1
        lines.append(f"  Менеджерів у кеші: {len(_cache)}")
        lines.append(f"  Кеш оновлено: {cache_age}с тому")
        if not _cache:
            lines.append("  ⚠️ КЕШ ПОРОЖНІЙ — жоден менеджер не пройде в чергу!")
    else:
        lines.append("  ❌ Не вдалось підключитись до таблиці")

    lines.append("\n\n🗄 <b>БД статистика:</b>")
    total_leads = q("SELECT COUNT(*) as cnt FROM leads", fetch='one')['cnt']
    taken_total = q("SELECT COUNT(*) as cnt FROM leads WHERE status='taken'", fetch='one')['cnt']
    dup_total   = q("SELECT COUNT(*) as cnt FROM leads WHERE status='duplicate'", fetch='one')['cnt']
    msg_total   = q("SELECT COUNT(*) as cnt FROM messages", fetch='one')['cnt']
    lines.append(f"  Всього заявок: {total_leads} (взято: {taken_total}, дублів: {dup_total})")
    lines.append(f"  Повідомлень у messages: {msg_total}")

    lines.append(f"\n\n🔗 <b>Webhook path:</b> <code>/webhook/{WEBHOOK_PATH}</code>")
    lines.append(f"AMO pipeline: <code>{AMO_PIPELINE_ID}</code> | status: <code>{AMO_HOT_STATUS_ID}</code>")

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


async def _handle_cleanup_orphans(message):
    """
    Одноразовий ручний cleanup: прибирає у Telegram + в БД "привиди" повідомлень
    у менеджерів, які зараз поза чергою, але ще тримають старі активні заявки
    (лишились від виходів ДО фіксу handle_manager_exit).
    """
    msg = await message.reply_text("🧹 Шукаю привидів... зачекайте")
    try:
        cleaned = await cleanup_orphaned_manager_messages()
        if cleaned:
            await msg.edit_text(f"✅ Прибрано привидів у <b>{cleaned}</b> неактивних менеджерів", parse_mode='HTML')
        else:
            await msg.edit_text("✅ Привидів не знайдено — все чисто")
    except Exception as e:
        await msg.edit_text(f"❌ Помилка cleanup: {e}")
        logger.error(f"Cleanup orphans error: {e}")


async def _handle_managers_list(message):
    """Показує список всіх менеджерів у БД та заявки на схвалення."""
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    all_mgrs  = get_all_managers(approved_only=False)
    pending   = [m for m in all_mgrs if not m['is_approved']]
    approved  = [m for m in all_mgrs if m['is_approved']]

    lines = ["👤 <b>Менеджери в системі</b>\n"]

    if pending:
        lines.append("⏳ <b>Очікують схвалення:</b>")
        for m in pending:
            lines.append(
                f"  • <b>{m['tg_name']}</b> | Sheets: {m['sheet_name'] or '?'} | "
                f"Kommo: {m['kommo_id'] or '?'} | ID: <code>{m['tg_id']}</code>"
            )
        lines.append("")

    lines.append(f"✅ <b>Схвалені ({len(approved)}):</b>")
    for m in approved:
        lines.append(
            f"  • <b>{m['sheet_name'] or m['tg_name']}</b> | "
            f"Kommo: {m['kommo_id'] or '?'} | ID: <code>{m['tg_id']}</code>"
        )

    if pending:
        buttons = []
        for m in pending:
            name = m['sheet_name'] or m['tg_name']
            buttons.append([
                InlineKeyboardButton(f"✅ {name}", callback_data=f"mgr_approve:{m['tg_id']}"),
                InlineKeyboardButton(f"❌ Відхилити", callback_data=f"mgr_reject:{m['tg_id']}"),
            ])
        await message.reply_text(
            '\n'.join(lines), parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup(buttons),
        )
    else:
        await send_long(message, '\n'.join(lines))


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
    elif text == "🔍 Діагностика":
        await _handle_diagnostics(update.message)
    elif text == "👤 Менеджери":
        await _handle_managers_list(update.message)
    elif text == "🧹 Прибрати привиди":
        await _handle_cleanup_orphans(update.message)
