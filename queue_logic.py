import asyncio
import logging
from datetime import datetime, timedelta, timezone as _timezone

# Єдина timezone для всього модуля (Київ, UTC+3)
_TZ = _timezone(timedelta(hours=3))

from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Forbidden

from config import (
    TIMEOUT_PERSONAL, TIMEOUT_WARN, TIMEOUT_SOS, TIMEOUT_REBROADCAST, SCHEDULER_TICK,
)
from db import (
    q, get_lead, get_all_taken, get_all_availability, get_all_max_leads_overrides,
    get_skipped, get_all_schedules, update_last_notified, reset_all_limit_overrides, get_msg_id,
    get_all_msgs, claim_lead_for_send, delete_msg, is_available, set_availability,
    get_all_managers, get_manager, get_exit_reason,
    add_distributed_lead, remove_distributed_lead, count_distributed_leads, get_distributed_lead,
    transfer_taken, get_connected, get_managers_dict, get_all_exit_reasons, get_status_chats,
)
from notifications import (
    notify_admins, notify_admin_error, send_to, edit_msg, delete_and_send, remove_from_others,
    cleanup_stale_messages, remove_buttons_for_manager, delete_messages_for_manager,
    send_long_to_chat, _deactivate_blocked,
)
from sheets import fetch_managers, fetch_managers_async

import state

logger = logging.getLogger(__name__)

# Захист від паралельного запуску _tick
_tick_lock = asyncio.Lock()


def day_key() -> str:
    d = datetime.now(_TZ)
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


def build_manager_status_text(managers: dict) -> str:
    """
    Формує той самий текст, що і кнопка "👥 Статус менеджерів" в адмінці —
    винесено сюди, щоб використовувати і по кнопці, і в періодичній розсилці в чат.
    """
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
            elif reason == 'schedule':
                lines.append(f"(БОТ 🌙) {name} — зміна закінчилась | взяв: {taken}/{limit_str}")
            else:
                lines.append(f"(КОРИСТУВАЧ 🚫) {name} — не в роботі | взяв: {taken}/{limit_str}")
        elif has_pending:
            lines.append(f"(КОРИСТУВАЧ 📨) {name} — очікує відповіді | взяв: {taken}/{limit_str}")
        else:
            lines.append(f"(КОРИСТУВАЧ ✅) {name} — в роботі | взяв: {taken}/{limit_str}")
    return '\n'.join(lines)


async def broadcast_manager_status():
    """Надсилає поточний статус менеджерів у всі чати, де увімкнена розсилка (/statuson)."""
    chat_ids = get_status_chats()
    if not chat_ids:
        return
    managers = await fetch_managers_async()
    text     = build_manager_status_text(managers)
    for chat_id in chat_ids:
        try:
            await send_long_to_chat(chat_id, text)
        except Exception as e:
            logger.warning(f"broadcast_manager_status: не вдалось надіслати в чат {chat_id}: {e}")


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
        if pending > 0:
            continue
        if max_leads is not None and taken >= max_leads:
            continue
        queue.append((taken, tg_id))

    queue.sort()
    return [tg_id for _, tg_id in queue]


async def assign_next(lead_id: str, exclude: list[str] = None):
    # Фетчимо менеджерів заздалегідь (в окремому потоці, не блокуючи event loop)
    # і передаємо в sorted_queue, щоб вона не робила це сама синхронно всередині.
    managers = await fetch_managers_async()
    try:
        queue = sorted_queue(exclude=exclude, managers=managers)
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
    manager_name = managers.get(manager_id, {}).get('name', 'Менеджер')

    lead = get_lead(lead_id)
    if not lead:
        return

    # Атомарно бронюємо заявку ПЕРЕД відправкою — захист від паралельних assign_next
    if not claim_lead_for_send(lead_id, manager_id):
        logger.info(f"assign_next: заявка {lead_id} вже зайнята іншим менеджером — пропускаємо")
        return

    text = f"{lead['title']}\n👤 <i>Черга: {manager_name}</i>"
    try:
        await send_to(manager_id, lead_id, text, build_keyboard(lead_id))
        logger.info(f"Заявка {lead_id} → {manager_name} ({manager_id})")
    except Exception as e:
        # Повертаємо заявку в чергу якщо відправка не вдалась
        q("UPDATE leads SET status='queued', manager_id=NULL, sent_at=NULL WHERE lead_id=?", (lead_id,))
        logger.error(f"assign_next відправка {lead_id} → {manager_id}: {e}")
        await notify_admin_error(f"assign_next (відправка заявки #{lead_id})", e, manager_id)


async def handle_manager_exit(manager_id: str):
    """
    При виході з черги (вручну або автоматично по розкладу) — видаляє всі активні
    повідомлення менеджера (особисті й broadcast) і, якщо серед них були особисті
    ("sent") заявки, повертає їх у чергу та передає іншому менеджеру —
    так само як це робить дія 'skip'.
    """
    rows = q("""
        SELECT lead_id FROM leads
        WHERE manager_id = ? AND status = 'sent'
    """, (manager_id,), fetch='all')
    personal_leads = [r['lead_id'] for r in (rows or [])]

    await delete_messages_for_manager(manager_id)

    for lead_id in personal_leads:
        lead = get_lead(lead_id)
        if lead and lead['status'] == 'sent' and lead['manager_id'] == manager_id:
            q("UPDATE leads SET status='queued', manager_id=NULL, sent_at=NULL WHERE lead_id=?", (lead_id,))
            await assign_next(lead_id, exclude=get_skipped(lead_id))


async def cleanup_orphaned_manager_messages() -> int:
    """
    Одноразовий (можна викликати вручну, напр. кнопкою в адмінці) cleanup для
    "привидів" — повідомлень, що лишились у менеджерів, які вийшли з черги ще
    ДО того як з'явився handle_manager_exit (тобто на старому коді), і тому
    ніколи не були видалені.

    Для кожного менеджера, який зараз is_active=0, але все ще має рядки в
    messages по активних (не taken/duplicate/closed) заявках — прибирає їх
    так само, як реальний вихід із черги: видаляє смс у Telegram + рядок з БД,
    а особисті ("sent") заявки повертає в чергу через assign_next.

    Повертає кількість менеджерів, для яких було що прибирати.
    """
    avail_map = get_all_availability()
    inactive  = [mid for mid, active in avail_map.items() if not active]

    cleaned = 0
    for manager_id in inactive:
        row = q("""
            SELECT COUNT(*) as cnt FROM messages m
            JOIN leads l ON l.lead_id = m.lead_id
            WHERE m.manager_id = ?
              AND l.status NOT IN ('taken', 'duplicate', 'closed')
        """, (manager_id,), fetch='one')
        if row and row['cnt']:
            await handle_manager_exit(manager_id)
            cleaned += 1

    if cleaned:
        logger.info(f"cleanup_orphaned_manager_messages: прибрано привидів у {cleaned} неактивних менеджерів")
    return cleaned


async def broadcast_to_all(lead_id: str, **tick_ctx):
    """
    Розіслати заявку всім вільним менеджерам.
    Статус завжди переходить в broadcast, але надсилання блокується якщо вже є активна broadcast заявка.
    """
    lead = get_lead(lead_id)
    if not lead or lead['status'] in ('taken', 'duplicate', 'closed'):
        return

    orig_manager = lead['manager_id']
    skipped      = get_skipped(lead_id)
    text         = f"{lead['title']}\n👤 <i>Відкрита черга</i>"
    kb           = build_keyboard(lead_id)

    # Активна broadcast — та що вже реально надіслана всім (є sent_at)
    active_broadcast = q(
        "SELECT lead_id FROM leads WHERE status='broadcast' AND sent_at IS NOT NULL AND lead_id != ? LIMIT 1",
        (lead_id,), fetch='one',
    )

    if active_broadcast:
        # sent_at=NULL щоб ескалація не починалась поки не надіслана реально
        q("UPDATE leads SET status='broadcast', esc_level=1, sent_at=NULL WHERE lead_id=?", (lead_id,))
        if orig_manager:
            msg_id = get_msg_id(lead_id, orig_manager)
            if msg_id:
                try:
                    await state._app.bot.delete_message(chat_id=orig_manager, message_id=msg_id)
                except Exception as e:
                    logger.debug(f"broadcast queue: не вдалось видалити повідомлення у {orig_manager}: {e}")
                # Видаляємо запис з messages незалежно від успіху видалення TG-повідомлення
                # (уникаємо "привидів" — записів без реального повідомлення в Telegram)
                delete_msg(lead_id, orig_manager)
        logger.info(f"Заявка {lead_id}: перейшла в broadcast, чекає черги (активна: {active_broadcast['lead_id']})")
        return

    exclude = list(set(skipped + ([orig_manager] if orig_manager else [])))
    queue   = sorted_queue(exclude=exclude, **tick_ctx)

    if orig_manager:
        await delete_and_send(orig_manager, lead_id, text, kb)

    for mid in queue:
        await delete_and_send(mid, lead_id, text, kb)

    q("UPDATE leads SET status='broadcast', esc_level=1, sent_at=? WHERE lead_id=?",
      (datetime.now().timestamp(), lead_id))
    logger.info(f"Заявка {lead_id} розіслана всім ({len(queue)} менеджерів)")


async def restore_buttons_for_manager(manager_id: str):
    """Відновлює кнопки на активних лідах коли менеджер входить в чергу.
    Також надсилає broadcast заявки що вже активні але ще не приходили цьому менеджеру."""

    # 1. Відновлюємо кнопки на вже надісланих повідомленнях
    rows = q("""
        SELECT l.* FROM leads l
        JOIN messages m ON m.lead_id = l.lead_id
        WHERE m.manager_id = ?
          AND l.status NOT IN ('taken', 'duplicate', 'closed')
    """, (manager_id,), fetch='all')

    for lead in (rows or []):
        lvl = lead['esc_level']
        if lvl <= 1:
            text = f"{lead['title']}\n👤 <i>Відкрита черга</i>"
        elif lvl == 2:
            text = f"⚠️⚠️⚠️ <b>ТЕРМІНОВО!</b>\nЗаявка без відповіді!\n\n{lead['title']}"
        else:
            text = f"🆘🚨💀🔴 <b>SOS!!!</b>\n\n{lead['title']}"
        await edit_msg(manager_id, lead['lead_id'], text, keyboard=build_keyboard(lead['lead_id']))

    # 2. Надсилаємо активні broadcast заявки що ще не приходили цьому менеджеру
    broadcast_leads = q("""
        SELECT l.* FROM leads l
        WHERE l.status = 'broadcast'
          AND l.sent_at IS NOT NULL
          AND l.lead_id NOT IN (
              SELECT lead_id FROM messages WHERE manager_id = ?
          )
          AND l.lead_id NOT IN (
              SELECT lead_id FROM skipped WHERE manager_id = ?
          )
    """, (manager_id, manager_id), fetch='all')

    for lead in (broadcast_leads or []):
        lvl = lead['esc_level']
        if lvl <= 1:
            text = f"{lead['title']}\n👤 <i>Відкрита черга</i>"
        elif lvl == 2:
            text = f"⚠️⚠️⚠️ <b>ТЕРМІНОВО!</b>\nЗаявка без відповіді!\n\n{lead['title']}"
        else:
            text = f"🆘🚨💀🔴 <b>SOS!!!</b>\n\n{lead['title']}"
        try:
            await send_to(manager_id, lead['lead_id'], text, build_keyboard(lead['lead_id']))
            logger.info(f"restore: надіслано broadcast заявку {lead['lead_id']} → {manager_id}")
        except Exception as e:
            logger.error(f"restore: не вдалось надіслати {lead['lead_id']} → {manager_id}: {e}")


def _update_offline(queue_set: set, lead_id: str, text: str):
    """Повертає coroutine-список для оновлення смс менеджерів поза чергою (без кнопок)."""
    return [
        edit_msg(m['manager_id'], lead_id, text)
        for m in get_all_msgs(lead_id)
        if m['manager_id'] not in queue_set
    ]


async def escalate_warn(lead_id: str, title: str, **tick_ctx):
    lead = get_lead(lead_id)
    if not lead or lead['status'] in ('taken', 'duplicate', 'closed'):
        return
    warn = (
        f"⚠️⚠️⚠️ <b>ТЕРМІНОВО!</b>\n"
        f"Заявка вже <b>5 хвилин</b> без відповіді!\n\n{title}"
    )
    kb        = build_keyboard(lead_id)
    queue     = sorted_queue(exclude=get_skipped(lead_id), **tick_ctx)
    queue_set = set(queue)
    for mid in queue:
        await delete_and_send(mid, lead_id, warn, kb)
    for coro in _update_offline(queue_set, lead_id, warn):
        await coro
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
    kb        = build_keyboard(lead_id)
    queue     = sorted_queue(exclude=get_skipped(lead_id), **tick_ctx)
    queue_set = set(queue)
    for mid in queue:
        await delete_and_send(mid, lead_id, sos, kb)
    for coro in _update_offline(queue_set, lead_id, sos):
        await coro
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
    kb        = build_keyboard(lead_id)
    queue     = sorted_queue(exclude=get_skipped(lead_id), **tick_ctx)
    queue_set = set(queue)
    for mid in queue:
        await delete_and_send(mid, lead_id, msg, kb)
    for coro in _update_offline(queue_set, lead_id, msg):
        await coro
    q("UPDATE leads SET last_rebroadcast_at=? WHERE lead_id=?",
      (datetime.now().timestamp(), lead_id))
    logger.info(f"Заявка {lead_id}: повторна розсилка (кожні 30 хв)")


async def _send_next_queued_broadcast(**tick_ctx):
    """Надсилає наступну broadcast заявку що чекає своєї черги (якщо немає активної)."""
    active = q(
        "SELECT 1 FROM leads WHERE status='broadcast' AND sent_at IS NOT NULL LIMIT 1",
        fetch='one',
    )
    if active:
        return

    # Затримка 10с після того як хтось взяв лід — щоб не засипати менеджера одразу
    recent = q(
        "SELECT MAX(taken_at) as last FROM leads WHERE taken_at IS NOT NULL",
        fetch='one',
    )
    if recent and recent['last'] and datetime.now().timestamp() - recent['last'] < 10:
        return

    waiting = q(
        "SELECT * FROM leads WHERE status='broadcast' AND sent_at IS NULL ORDER BY created_at DESC LIMIT 1",
        fetch='one',
    )
    if not waiting:
        return

    orig_manager = waiting['manager_id']
    skipped      = get_skipped(waiting['lead_id'])
    text         = f"{waiting['title']}\n👤 <i>Відкрита черга</i>"
    kb           = build_keyboard(waiting['lead_id'])
    exclude      = list(set(skipped + ([orig_manager] if orig_manager else [])))
    queue        = sorted_queue(exclude=exclude, **tick_ctx)

    if orig_manager:
        await delete_and_send(orig_manager, waiting['lead_id'], text, kb)

    for mid in queue:
        await delete_and_send(mid, waiting['lead_id'], text, kb)

    q("UPDATE leads SET sent_at=? WHERE lead_id=?",
      (datetime.now().timestamp(), waiting['lead_id']))
    logger.info(f"Заявка {waiting['lead_id']} надіслана всім з черги broadcast ({len(queue)} менеджерів)")


async def _tick():
    # Якщо попередній тік ще виконується — пропускаємо, не накопичуємо
    if _tick_lock.locked():
        logger.warning("_tick: попередній тік ще виконується, пропускаємо")
        return

    async with _tick_lock:
        now   = datetime.now().timestamp()
        leads = q(
            "SELECT * FROM leads WHERE status NOT IN ('taken','duplicate','closed') ORDER BY created_at DESC",
            fetch='all',
        )
        if not leads:
            return

        managers  = await fetch_managers_async()
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

            if lead['status'] in ('queued', 'no_managers') and not sent_at:
                if now - lead['created_at'] > 5:
                    await assign_next(lid)
                continue

            if not sent_at:
                continue

            age = now - sent_at

            if lvl == 0 and age >= TIMEOUT_PERSONAL:
                await broadcast_to_all(lid, **tick_ctx)
            elif lvl == 1 and age >= TIMEOUT_WARN:
                await escalate_warn(lid, lead['title'], **tick_ctx)
            elif lvl == 2 and age >= TIMEOUT_SOS:
                await escalate_sos(lid, lead['title'], **tick_ctx)
            elif lvl >= 3:
                rb_base = last_rb or sent_at or lead['created_at']
                if now - rb_base >= TIMEOUT_REBROADCAST:
                    await rebroadcast_periodic(lid, lead['title'], **tick_ctx)

        await _send_next_queued_broadcast(**tick_ctx)


async def _send_shift_reminder(manager_id: str, name: str):
    """Надсилає 3-хвильове нагадування про початок зміни у фоні."""
    try:
        await state._app.bot.send_message(chat_id=manager_id, text="⏰")
        await asyncio.sleep(2)
        await state._app.bot.send_message(chat_id=manager_id, text="⏰⏰")
        await asyncio.sleep(2)
        await state._app.bot.send_message(
            chat_id=manager_id,
            text=f"⏰⏰⏰ <b>{name}</b>, твій робочий час почався!\nНатисни «✅ Увійти в чергу» щоб почати отримувати заявки.",
            parse_mode='HTML',
        )
        logger.info(f"Schedule: нагадування надіслано {name} ({manager_id})")
    except Forbidden:
        logger.warning(f"Schedule: {name} ({manager_id}) заблокував бота — деактивуємо")
        await _deactivate_blocked(manager_id)
    except Exception as e:
        logger.warning(f"Schedule: не вдалось надіслати {manager_id}: {e}")


async def _check_schedules():
    """Надсилає нагадування на початку зміни та автоматично виводить з черги в кінці."""
    now          = datetime.now(_TZ)
    today        = now.strftime('%Y-%m-%d')
    weekday      = now.weekday()
    yesterday    = (weekday - 1) % 7
    current_time = now.strftime('%H:%M')

    schedules = get_all_schedules()
    for manager_id, sch in schedules.items():
        if not sch.get('enabled', 1):
            continue

        days      = [int(d) for d in sch['days'].split(',') if d.strip()]
        start     = sch.get('start_time', '16:00')
        end       = sch.get('end_time', '23:00')
        crosses   = end <= start  # зміна переходить через північ (напр. 22:00–05:00)

        # ── Авто-деактивація в кінці зміни ──────────────────────────────────
        # Спрацьовує навіть якщо менеджер зараз поза чергою через 'has_distributed' —
        # інакше після закриття заявки on_lead_undistributed поверне його в чергу
        # вночі, вже після завершення зміни. Виконується лише один раз за хвилину
        # завдяки самообмеженню: одразу після виклику reason стає 'schedule',
        # тож умова нижче більше не виконується на наступних тіках.
        if current_time == end:
            working_day = yesterday if crosses else weekday
            if working_day in days:
                if is_available(manager_id) or get_exit_reason(manager_id) == 'has_distributed':
                    set_availability(manager_id, False, reason='schedule')
                    await handle_manager_exit(manager_id)
                    name = state.MANAGERS_BY_ID.get(manager_id, manager_id)
                    try:
                        await state._app.bot.send_message(
                            chat_id=manager_id,
                            text=f"🌙 <b>{name}</b>, твоя зміна закінчилась.\nТебе автоматично виведено з черги.",
                            parse_mode='HTML',
                        )
                    except Exception:
                        pass
                    await notify_admins(f"🌙 <b>{name}</b> автоматично виведено з черги (кінець зміни)")
                    logger.info(f"Schedule: {name} ({manager_id}) — авто-деактивація о {end}")

        # ── Нагадування на початку зміни ─────────────────────────────────────
        if sch.get('last_notified') == today:
            continue
        if weekday not in days:
            continue
        if current_time != start:
            continue

        name = state.MANAGERS_BY_ID.get(manager_id, manager_id)
        update_last_notified(manager_id, today)

        # Якщо менеджер вже в черзі — мовчки відмічаємо як повідомлений
        if is_available(manager_id):
            logger.info(f"Schedule: {name} вже в черзі — нагадування пропущено")
            continue

        asyncio.create_task(_send_shift_reminder(manager_id, name))


def _reset_limit_overrides():
    reset_all_limit_overrides()
    logger.info("Ручні ліміти скинуто (новий день)")


def _cleanup_old_records():
    """Видаляє записи старші за 2 місяці."""
    now       = datetime.now()
    keep_from = (now.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m')
    q("DELETE FROM stats WHERE month < ?", (keep_from,))
    q("DELETE FROM leads WHERE created_at < ? AND status IN ('taken','duplicate','closed')",
      (datetime.now().timestamp() - 60 * 24 * 3600,))
    q("DELETE FROM messages WHERE lead_id NOT IN (SELECT lead_id FROM leads)")
    q("DELETE FROM skipped  WHERE lead_id NOT IN (SELECT lead_id FROM leads)")
    logger.info(f"БД: очищено записи до {keep_from}")


async def deactivate_out_of_schedule():
    """При старті сервера виводить з черги менеджерів що зараз поза робочим часом."""
    now          = datetime.now(_TZ)
    weekday      = now.weekday()
    yesterday    = (weekday - 1) % 7
    current_time = now.strftime('%H:%M')

    schedules = get_all_schedules()
    for manager_id, sch in schedules.items():
        if not sch.get('enabled', 1):
            continue
        if not is_available(manager_id) and get_exit_reason(manager_id) != 'has_distributed':
            continue

        days    = [int(d) for d in sch['days'].split(',') if d.strip()]
        start   = sch.get('start_time', '16:00')
        end     = sch.get('end_time', '23:00')
        crosses = end <= start  # зміна переходить через північ

        if crosses:
            in_shift = (current_time >= start and weekday in days) or \
                       (current_time < end and yesterday in days)
        else:
            in_shift = weekday in days and start <= current_time < end

        if not in_shift:
            set_availability(manager_id, False, reason='schedule')
            await handle_manager_exit(manager_id)
            name = state.MANAGERS_BY_ID.get(manager_id, manager_id)
            logger.info(f"Старт: {name} поза робочим часом → виведено з черги")
            try:
                await state._app.bot.send_message(
                    chat_id=manager_id,
                    text=f"🌙 <b>{name}</b>, твоя зміна закінчилась.\nТебе автоматично виведено з черги.",
                    parse_mode='HTML',
                )
            except Exception:
                pass
            await notify_admins(f"🌙 <b>{name}</b> автоматично виведено з черги (поза робочим часом при старті)")


_STATUS_BROADCAST_HOURS = {f"{h:02d}:00" for h in range(17, 23)}  # 17:00 .. 22:00 включно


async def _check_status_broadcast():
    """Раз на годину (17:00–22:00) шле статус менеджерів у зареєстровані чати."""
    now = datetime.now(_TZ)
    if now.strftime('%H:%M') in _STATUS_BROADCAST_HOURS:
        await broadcast_manager_status()


async def scheduler_loop():
    _now_tz          = lambda: datetime.now(_TZ)
    last_cleanup      = _now_tz().month
    last_day          = _now_tz().day
    last_sch_min      = ''
    last_msg_cleanup  = _now_tz().timestamp()
    while True:
        await asyncio.sleep(SCHEDULER_TICK)
        try:
            await _tick()
            now = _now_tz()
            if now.day != last_day:
                _reset_limit_overrides()
                last_day = now.day
            if now.month != last_cleanup:
                _cleanup_old_records()
                last_cleanup = now.month
            cur_min = now.strftime('%H:%M')
            if cur_min != last_sch_min:
                last_sch_min = cur_min
                await _check_schedules()
                await _check_status_broadcast()
            if now.timestamp() - last_msg_cleanup >= 300:
                await cleanup_stale_messages()
                last_msg_cleanup = now.timestamp()
        except Exception as e:
            logger.error(f"Scheduler помилка: {e}")
            await notify_admin_error("scheduler (фоновий планувальник)", e)


# ─── Розподілені заявки (Распределены) ───────────────────────────────────────

async def on_lead_distributed(lead_id: str):
    """
    Викликається коли заявка переходить в статус 'Распределены', а також коли
    в цьому статусі просто змінюють відповідального (Kommo шле ту саму
    вебхук-подію 'status' і на зміну responsible_user_id, без зміни стадії) —
    тому тут завжди перевіряємо ПОТОЧНОГО відповідального, а не тільки перший раз.
    """
    from kommo import get_lead_responsible

    responsible_kommo_id = await get_lead_responsible(lead_id)
    if not responsible_kommo_id:
        logger.warning(f"on_lead_distributed: не вдалось отримати responsible для заявки {lead_id}")
        return

    mgr = next(
        (m for m in get_all_managers(approved_only=True)
         if m['kommo_id'] == responsible_kommo_id),
        None,
    )
    new_manager_id = mgr['tg_id'] if mgr else None

    # Лід вже був закріплений за кимось раніше — перевіряємо, чи це той самий
    existing = get_distributed_lead(lead_id)
    if existing and existing['manager_id'] != new_manager_id:
        # Відповідального змінили, поки лід лишався в 'Распределены' —
        # звільняємо старого менеджера (і переносимо йому лічильник взятого)
        await _release_reassigned_manager(lead_id, existing['manager_id'], new_manager_id)

    if not mgr:
        logger.info(
            f"on_lead_distributed: менеджер з kommo_id={responsible_kommo_id} "
            f"не знайдений у БД (заявка {lead_id})"
        )
        return

    if existing and existing['manager_id'] == new_manager_id:
        # Той самий менеджер, як і був — нічого по суті не змінилось
        return

    manager_id = new_manager_id
    name       = mgr['sheet_name'] or mgr['tg_name'] or manager_id

    add_distributed_lead(lead_id, manager_id)
    # Виводимо з черги тільки якщо менеджер зараз активний —
    # якщо він вже вийшов вручну або за розкладом, не перетираємо його причину виходу
    if is_available(manager_id):
        set_availability(manager_id, False, reason='has_distributed')
    # Знімаємо кнопки з усіх активних заявок що вже надіслані менеджеру
    await remove_buttons_for_manager(manager_id)
    logger.info(f"on_lead_distributed: {name} ({manager_id}) → виведено з черги (заявка {lead_id})")

    try:
        await state._app.bot.send_message(
            chat_id=manager_id,
            text=(
                "🚫 <b>Вас виведено з черги</b>\n\n"
                "У вас є заявка в статусі <b>\"Распределены\"</b> в CRM.\n"
                "Після закриття або передачі заявки — ви повернетесь в чергу автоматично."
            ),
            parse_mode='HTML',
        )
    except Exception as e:
        logger.warning(f"on_lead_distributed: не вдалось повідомити {manager_id}: {e}")


async def _release_reassigned_manager(lead_id: str, old_manager_id: str, new_manager_id=None):
    """
    Лід був закріплений за old_manager_id, але зараз в Kommo відповідальний
    інший (new_manager_id, або взагалі не наш менеджер — тоді None).
    Прибираємо стару прив'язку, переносимо лічильник «взятих» заявок і,
    якщо у старого менеджера більше немає інших розподілених лідів,
    повертаємо його в чергу (тільки якщо exit_reason саме 'has_distributed').
    """
    remove_distributed_lead(lead_id)
    transfer_taken(old_manager_id, new_manager_id, day_key())

    remaining = count_distributed_leads(old_manager_id)
    if remaining > 0:
        logger.info(
            f"on_lead_distributed (переприз.): {old_manager_id} ще має {remaining} заявок у 'Распределены'"
        )
        return

    mgr = get_manager(old_manager_id)
    if not mgr:
        return
    name = mgr['sheet_name'] or mgr['tg_name'] or old_manager_id

    row = q("SELECT exit_reason FROM availability WHERE manager_id=?", (old_manager_id,), fetch='one')
    if row and row['exit_reason'] != 'has_distributed':
        logger.info(
            f"on_lead_distributed (переприз.): {name} ({old_manager_id}) — не в черзі з іншої причини "
            f"({row['exit_reason']}), не повертаємо"
        )
        return

    set_availability(old_manager_id, True)
    logger.info(
        f"on_lead_distributed (переприз.): {name} ({old_manager_id}) → "
        f"заявку {lead_id} передано іншому, повернуто в чергу"
    )

    try:
        await state._app.bot.send_message(
            chat_id=old_manager_id,
            text=(
                "↩️ <b>Заявку передано іншому менеджеру</b>\n\n"
                "Ця заявка більше не за вами — ви повернуті в чергу."
            ),
            parse_mode='HTML',
        )
    except Exception as e:
        logger.warning(f"on_lead_distributed (переприз.): не вдалось повідомити {old_manager_id}: {e}")


async def on_lead_undistributed(lead_id: str, manager_id: str):
    """
    Викликається коли заявка покидає статус 'Распределены'.
    Якщо у менеджера більше немає таких заявок — повертає його в чергу.
    """
    remove_distributed_lead(lead_id)

    remaining = count_distributed_leads(manager_id)
    if remaining > 0:
        logger.info(
            f"on_lead_undistributed: {manager_id} ще має {remaining} заявок у 'Распределены'"
        )
        return

    mgr = get_manager(manager_id)
    if not mgr:
        return

    name = mgr['sheet_name'] or mgr['tg_name'] or manager_id

    # Повертаємо в чергу тільки якщо причина виходу саме 'has_distributed' —
    # якщо менеджер вийшов вручну або за розкладом поки заявка була distributed, не чіпаємо
    row = q("SELECT exit_reason FROM availability WHERE manager_id=?", (manager_id,), fetch='one')
    if row and row['exit_reason'] != 'has_distributed':
        logger.info(
            f"on_lead_undistributed: {name} ({manager_id}) — не в черзі з іншої причини "
            f"({row['exit_reason']}), не повертаємо"
        )
        return

    set_availability(manager_id, True)
    logger.info(f"on_lead_undistributed: {name} ({manager_id}) → повернуто в чергу")

    try:
        await state._app.bot.send_message(
            chat_id=manager_id,
            text=(
                "✅ <b>Вас повернуто в чергу</b>\n\n"
                "Заявка «Распределены» закрита або передана — "
                "ви знову отримуватимете нові заявки."
            ),
            parse_mode='HTML',
        )
    except Exception as e:
        logger.warning(f"on_lead_undistributed: не вдалось повідомити {manager_id}: {e}")
