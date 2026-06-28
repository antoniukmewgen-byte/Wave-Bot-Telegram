import asyncio
import logging
from datetime import datetime, timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from config import (
    TIMEOUT_PERSONAL, TIMEOUT_WARN, TIMEOUT_SOS, TIMEOUT_REBROADCAST, SCHEDULER_TICK,
)
from db import (
    q, get_lead, get_all_taken, get_all_availability, get_all_max_leads_overrides,
    get_skipped, get_all_schedules, update_last_notified, reset_all_limit_overrides, get_msg_id,
    get_all_msgs, claim_lead_for_send, delete_msg,
)
from notifications import (
    notify_admins, notify_admin_error, send_to, edit_msg, delete_and_send, remove_from_others,
    cleanup_stale_messages,
)
from sheets import fetch_managers

import state

logger = logging.getLogger(__name__)

# Захист від паралельного запуску _tick
_tick_lock = asyncio.Lock()


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
        if pending > 0:
            continue
        if max_leads is not None and taken >= max_leads:
            continue
        queue.append((taken, tg_id))

    queue.sort()
    return [tg_id for _, tg_id in queue]


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
    """Відновлює кнопки на активних лідах коли менеджер входить в чергу."""
    rows = q("""
        SELECT l.* FROM leads l
        JOIN messages m ON m.lead_id = l.lead_id
        WHERE m.manager_id = ?
          AND l.status NOT IN ('taken', 'duplicate', 'closed')
    """, (manager_id,), fetch='all')

    if not rows:
        return

    for lead in rows:
        lvl = lead['esc_level']
        if lvl <= 1:
            text = f"{lead['title']}\n👤 <i>Відкрита черга</i>"
        elif lvl == 2:
            text = f"⚠️⚠️⚠️ <b>ТЕРМІНОВО!</b>\nЗаявка без відповіді!\n\n{lead['title']}"
        else:
            text = f"🆘🚨💀🔴 <b>SOS!!!</b>\n\n{lead['title']}"

        await edit_msg(manager_id, lead['lead_id'], text, keyboard=build_keyboard(lead['lead_id']))


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


async def _check_schedules():
    """Надсилає нагадування на початку зміни та автоматично виводить з черги в кінці."""
    from datetime import timezone
    from db import set_availability
    tz           = timezone(timedelta(hours=3))
    now          = datetime.now(tz)
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
        if current_time == end:
            # Звичайна зміна: сьогоднішній день має бути робочим
            # Нічна зміна (crosses midnight): вчорашній день має бути робочим
            working_day = yesterday if crosses else weekday
            if working_day in days:
                from db import is_available
                if is_available(manager_id):
                    set_availability(manager_id, False, reason='schedule')
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
        try:
            name = state.MANAGERS_BY_ID.get(manager_id, manager_id)
            await state._app.bot.send_message(chat_id=manager_id, text="⏰")
            await asyncio.sleep(2)
            await state._app.bot.send_message(chat_id=manager_id, text="⏰⏰")
            await asyncio.sleep(2)
            await state._app.bot.send_message(
                chat_id=manager_id,
                text=f"⏰⏰⏰ <b>{name}</b>, твій робочий час почався!\nНе забудь увімкнути бота — натисни «✅ Увійти в чергу» якщо ще не зробив це.",
                parse_mode='HTML',
            )
            update_last_notified(manager_id, today)
            logger.info(f"Schedule: нагадування надіслано {name} ({manager_id})")
        except Exception as e:
            logger.warning(f"Schedule: не вдалось надіслати {manager_id}: {e}")


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
    from datetime import timezone
    from db import is_available, set_availability
    tz           = timezone(timedelta(hours=3))
    now          = datetime.now(tz)
    weekday      = now.weekday()
    yesterday    = (weekday - 1) % 7
    current_time = now.strftime('%H:%M')

    schedules = get_all_schedules()
    for manager_id, sch in schedules.items():
        if not sch.get('enabled', 1):
            continue
        if not is_available(manager_id):
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


async def scheduler_loop():
    last_cleanup      = datetime.now().month
    last_day          = datetime.now().day
    last_sch_min      = ''
    last_msg_cleanup  = datetime.now().timestamp()
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
            cur_min = now.strftime('%H:%M')
            if cur_min != last_sch_min:
                last_sch_min = cur_min
                await _check_schedules()
            if now.timestamp() - last_msg_cleanup >= 300:
                await cleanup_stale_messages()
                last_msg_cleanup = now.timestamp()
        except Exception as e:
            logger.error(f"Scheduler помилка: {e}")
            await notify_admin_error("scheduler (фоновий планувальник)", e)
