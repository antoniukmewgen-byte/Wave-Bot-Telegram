import asyncio
import logging
from datetime import datetime, timedelta

from telegram import InlineKeyboardButton, InlineKeyboardMarkup

from config import (
    TIMEOUT_PERSONAL, TIMEOUT_WARN, TIMEOUT_SOS, TIMEOUT_REBROADCAST, SCHEDULER_TICK,
)
from db import (
    q, get_lead, get_all_taken, get_all_availability, get_all_max_leads_overrides,
    get_skipped, get_all_schedules, update_last_notified, reset_all_limit_overrides,
)
from notifications import (
    notify_admins, notify_admin_error, send_to, edit_msg, delete_and_send, remove_from_others,
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

    text = f"{lead['title']}\n👤 <i>Черга: {manager_name}</i>"
    try:
        await send_to(manager_id, lead_id, text, build_keyboard(lead_id))
        q("UPDATE leads SET status='sent', manager_id=?, sent_at=? WHERE lead_id=?",
          (manager_id, datetime.now().timestamp(), lead_id))
        logger.info(f"Заявка {lead_id} → {manager_name} ({manager_id})")
    except Exception as e:
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
        logger.info(f"Заявка {lead_id}: перейшла в broadcast, чекає черги (активна: {active_broadcast['lead_id']})")
        return

    exclude = list(set(skipped + ([orig_manager] if orig_manager else [])))
    queue   = sorted_queue(exclude=exclude, **tick_ctx)

    if orig_manager:
        await edit_msg(orig_manager, lead_id, text, kb)

    for mid in queue:
        await delete_and_send(mid, lead_id, text, kb)

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
    kb    = build_keyboard(lead_id)
    queue = sorted_queue(exclude=get_skipped(lead_id), **tick_ctx)
    for mid in queue:
        await delete_and_send(mid, lead_id, warn, kb)
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
    kb    = build_keyboard(lead_id)
    queue = sorted_queue(exclude=get_skipped(lead_id), **tick_ctx)
    for mid in queue:
        await delete_and_send(mid, lead_id, sos, kb)
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
    kb    = build_keyboard(lead_id)
    queue = sorted_queue(exclude=get_skipped(lead_id), **tick_ctx)
    for mid in queue:
        await delete_and_send(mid, lead_id, msg, kb)
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
        await edit_msg(orig_manager, waiting['lead_id'], text, kb)

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
    """Надсилає нагадування менеджерам на початку робочого дня."""
    from datetime import timezone
    tz           = timezone(timedelta(hours=3))
    now          = datetime.now(tz)
    today        = now.strftime('%Y-%m-%d')
    weekday      = now.weekday()
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


async def scheduler_loop():
    last_cleanup = datetime.now().month
    last_day     = datetime.now().day
    last_sch_min = ''
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
        except Exception as e:
            logger.error(f"Scheduler помилка: {e}")
            await notify_admin_error("scheduler (фоновий планувальник)", e)
