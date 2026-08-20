import asyncio
import logging
from datetime import datetime, timedelta, timezone as _timezone
from typing import Optional

# Єдина timezone для всього модуля (Київ, UTC+3)
_TZ = _timezone(timedelta(hours=3))

from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.interval import IntervalTrigger
from telegram import InlineKeyboardButton, InlineKeyboardMarkup
from telegram.error import Forbidden

from config import (
    TIMEOUT_PERSONAL, TIMEOUT_WARN, TIMEOUT_SOS, TIMEOUT_REBROADCAST, SCHEDULER_TICK,
    AMO_PIPELINE_ID, AMO_HOT_STATUS_ID, AMO_DISTRIBUTED_PIPELINE_ID, AMO_DISTRIBUTED_STATUS_ID,
)
from db import (
    q, get_lead, get_all_taken, get_all_availability, get_all_max_leads_overrides,
    get_skipped, get_all_schedules, update_last_notified, reset_all_limit_overrides, get_msg_id,
    get_all_msgs, claim_lead_for_send, delete_msg, is_available, set_availability,
    get_all_managers, get_manager, get_exit_reason,
    add_distributed_lead, remove_distributed_lead, count_distributed_leads, get_distributed_lead,
    get_all_distributed_leads,
    transfer_taken, get_connected, get_managers_dict, get_all_exit_reasons, get_status_chats,
    get_all_held_leads, remove_held_lead, add_held_lead,
)
from phone_timezone import is_client_morning, resolve_client_timezone
from notifications import (
    notify_admins, notify_admin_error, send_to, edit_msg, delete_and_send, remove_from_others,
    cleanup_stale_messages, remove_buttons_for_manager, delete_messages_for_manager,
    send_long_to_chat, _deactivate_blocked, schedule_cleanup,
)
from sheets import fetch_managers_async

import state

logger = logging.getLogger(__name__)

# Захист від паралельного запуску _tick.
# Навмисно НЕ asyncio.Lock(): Lock створювався б на рівні модуля (в момент
# import, ще до uvicorn.run()) і прив'язувався б до "поточного" на той момент
# event loop, що на Python 3.9 часто виявляється ІНШИМ loop, ніж той, у якому
# реально виконується lifespan()/_tick() під uvicorn. Це й спричиняло
# "got Future <Future pending> attached to a different loop" у проді
# (11 разів за 7 днів — саме на job 'tick'). Тут же потрібен лише простий
# прапорець "тік вже виконується" в межах ОДНОГО loop, без реальної
# міжкорутинної конкуренції за ресурс — bool повністю покриває цей випадок
# і не залежить від того, який event loop був активний на момент імпорту.
_tick_running = False


def day_key() -> str:
    d = datetime.now(_TZ)
    return f"{d.year}-{d.month:02d}-{d.day:02d}"


def build_keyboard(lead_id: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Беру в роботу", callback_data=f"t:{lead_id}"),
        InlineKeyboardButton("❌ Не можу взяти", callback_data=f"s:{lead_id}"),
        InlineKeyboardButton("🔁 Дубль",         callback_data=f"d:{lead_id}"),
    ]])


def build_broadcast_keyboard(lead_id: str) -> InlineKeyboardMarkup:
    """Клавіатура для broadcast-повідомлень (відкрита черга) — без кнопки "Не можу взяти"."""
    return InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Беру в роботу", callback_data=f"t:{lead_id}"),
        InlineKeyboardButton("🔁 Дубль",         callback_data=f"d:{lead_id}"),
    ]])


async def _build_sent_map() -> dict:
    """Скільки заявок зараз персонально відправлено кожному менеджеру (status='sent')."""
    rows = await q(
        "SELECT manager_id, COUNT(*) as cnt FROM leads "
        "WHERE status = 'sent' AND manager_id IS NOT NULL "
        "GROUP BY manager_id",
        fetch='all',
    )
    return {r['manager_id']: r['cnt'] for r in rows} if rows else {}


async def _still_eligible(manager_id: str) -> bool:
    """Свіжа перевірка прямо перед фактичною відправкою.

    Списки черги (avail_map/sent_map) можуть бути знімком стану на початок тіку,
    а сам тік обробляє кілька заявок підряд з await-надсиланнями між ними — за цей
    час менеджер міг узяти іншу заявку в роботу (set_availability(False) з
    handlers/manager.py). Ця перевірка закриває те вікно гонки: якщо менеджер
    вже зайнятий/недоступний — не надсилаємо йому ще одну заявку.
    """
    if not await is_available(manager_id):
        return False
    pending = await q(
        "SELECT COUNT(*) as cnt FROM leads WHERE status='sent' AND manager_id=?",
        (manager_id,), fetch='one',
    )
    return not (pending and pending['cnt'] > 0)


async def build_manager_status_text(managers: dict) -> str:
    """
    Формує той самий текст, що і кнопка "👥 Статус менеджерів" в адмінці —
    винесено сюди, щоб використовувати і по кнопці, і в періодичній розсилці в чат.
    """
    month         = day_key()
    connected_ids = {r['manager_id'] for r in await get_connected()}
    avail_map     = await get_all_availability()
    overrides     = await get_all_max_leads_overrides()
    taken_map     = await get_all_taken(month)
    sent_map      = await _build_sent_map()
    exit_reasons  = await get_all_exit_reasons()

    lines = ["👥 <b>Статус менеджерів:</b>\n"]
    for name, tg_id in (await get_managers_dict()).items():
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
    chat_ids = await get_status_chats()
    if not chat_ids:
        return
    managers = await fetch_managers_async()
    text     = await build_manager_status_text(managers)
    for chat_id in chat_ids:
        try:
            await send_long_to_chat(chat_id, text)
        except Exception as e:
            logger.warning(f"broadcast_manager_status: не вдалось надіслати в чат {chat_id}: {e}")


async def sorted_queue(
    managers: dict,
    exclude: list[str] = None,
    taken_map: dict = None,
    avail_map: dict = None,
    overrides: dict = None,
    sent_map: dict = None,
) -> list[str]:
    """
    Повертає список tg_id менеджерів у порядку черги.
    managers — обов'язковий: fetch_managers() тепер async (gspread_asyncio),
    тож синхронно всередині цієї функції отримати його вже не можна;
    виклики нижче й так завжди передають managers явно.
    Прийняті ззовні taken_map/avail_map/overrides/sent_map дозволяють
    уникнути зайвих запитів до БД, якщо черга будується для багатьох лідів підряд.
    """
    if taken_map is None:
        taken_map = await get_all_taken(day_key())
    if avail_map is None:
        avail_map = await get_all_availability()
    if overrides is None:
        overrides = await get_all_max_leads_overrides()
    if sent_map is None:
        sent_map = await _build_sent_map()

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
        queue = await sorted_queue(exclude=exclude, managers=managers)
    except Exception as e:
        await notify_admin_error("assign_next (читання черги)", e)
        return

    if not queue:
        logger.warning(f"Заявка {lead_id}: немає вільних менеджерів")
        lead = await get_lead(lead_id)
        if lead and lead['status'] == 'queued':
            await q("UPDATE leads SET status='no_managers' WHERE lead_id=?", (lead_id,))
            await notify_admins(
                f"⚠️ <b>Немає вільних менеджерів!</b>\n\n"
                f"{lead['title']} не розподілена.\n"
                f"Перевірте таблицю — можливо не заповнено поточний місяць."
            )
        return

    manager_id   = queue[0]
    manager_name = managers.get(manager_id, {}).get('name', 'Менеджер')

    lead = await get_lead(lead_id)
    if not lead:
        return

    # Свіжа перевірка прямо перед бронюванням — за час між побудовою черги і сюди
    # менеджер міг узяти іншу заявку в роботу (закриваємо вікно гонки).
    # ВАЖЛИВО: цю перевірку треба робити ДО claim_lead_for_send, інакше вона
    # рахує щойно заброньовану цю саму заявку як "pending" і завжди повертає False.
    if not await _still_eligible(manager_id):
        logger.info(
            f"assign_next: {manager_name} ({manager_id}) став зайнятий за час побудови черги — "
            f"пропускаємо, заявка {lead_id} лишається в черзі"
        )
        return

    # Атомарно бронюємо заявку ПЕРЕД відправкою — захист від паралельних assign_next
    if not await claim_lead_for_send(lead_id, manager_id):
        logger.info(f"assign_next: заявка {lead_id} вже зайнята іншим менеджером — пропускаємо")
        return

    text = f"{lead['title']}\n👤 <i>Черга: {manager_name}</i>"
    try:
        await send_to(manager_id, lead_id, text, build_keyboard(lead_id))
        logger.info(f"Заявка {lead_id} → {manager_name} ({manager_id})")
    except Exception as e:
        # Повертаємо заявку в чергу якщо відправка не вдалась
        await q("UPDATE leads SET status='queued', manager_id=NULL, sent_at=NULL WHERE lead_id=?", (lead_id,))
        logger.error(f"assign_next відправка {lead_id} → {manager_id}: {e}")
        await notify_admin_error(f"assign_next (відправка заявки #{lead_id})", e, manager_id)


async def handle_manager_exit(manager_id: str):
    """
    При виході з черги (вручну або автоматично по розкладу) — видаляє всі активні
    повідомлення менеджера (особисті й broadcast) і, якщо серед них були особисті
    ("sent") заявки, повертає їх у чергу та передає іншому менеджеру —
    так само як це робить дія 'skip'.
    """
    rows = await q("""
        SELECT lead_id FROM leads
        WHERE manager_id = ? AND status = 'sent'
    """, (manager_id,), fetch='all')
    personal_leads = [r['lead_id'] for r in (rows or [])]

    await delete_messages_for_manager(manager_id)

    for lead_id in personal_leads:
        lead = await get_lead(lead_id)
        if lead and lead['status'] == 'sent' and lead['manager_id'] == manager_id:
            await q("UPDATE leads SET status='queued', manager_id=NULL, sent_at=NULL WHERE lead_id=?", (lead_id,))
            await assign_next(lead_id, exclude=await get_skipped(lead_id))


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
    avail_map = await get_all_availability()
    inactive  = [mid for mid, active in avail_map.items() if not active]

    cleaned = 0
    for manager_id in inactive:
        row = await q("""
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
    lead = await get_lead(lead_id)
    if not lead or lead['status'] in ('taken', 'duplicate', 'closed'):
        return

    orig_manager = lead['manager_id']
    skipped      = await get_skipped(lead_id)
    text         = f"{lead['title']}\n👤 <i>Відкрита черга</i>"
    kb           = build_broadcast_keyboard(lead_id)

    # Активна broadcast — та що вже реально надіслана всім (є sent_at)
    active_broadcast = await q(
        "SELECT lead_id FROM leads WHERE status='broadcast' AND sent_at IS NOT NULL AND lead_id != ? LIMIT 1",
        (lead_id,), fetch='one',
    )

    if active_broadcast:
        # sent_at=NULL щоб ескалація не починалась поки не надіслана реально
        await q("UPDATE leads SET status='broadcast', esc_level=1, sent_at=NULL WHERE lead_id=?", (lead_id,))
        if orig_manager:
            msg_id = await get_msg_id(lead_id, orig_manager)
            if msg_id:
                try:
                    await state._app.bot.delete_message(chat_id=orig_manager, message_id=msg_id)
                except Exception as e:
                    logger.debug(f"broadcast queue: не вдалось видалити повідомлення у {orig_manager}: {e}")
                # Видаляємо запис з messages незалежно від успіху видалення TG-повідомлення
                # (уникаємо "привидів" — записів без реального повідомлення в Telegram)
                await delete_msg(lead_id, orig_manager)
        logger.info(f"Заявка {lead_id}: перейшла в broadcast, чекає черги (активна: {active_broadcast['lead_id']})")
        return

    exclude = list(set(skipped + ([orig_manager] if orig_manager else [])))
    queue   = await sorted_queue(exclude=exclude, **tick_ctx)

    if orig_manager:
        await delete_and_send(orig_manager, lead_id, text, kb)

    for mid in queue:
        if not await _still_eligible(mid):
            continue
        await delete_and_send(mid, lead_id, text, kb)

    await q("UPDATE leads SET status='broadcast', esc_level=1, sent_at=? WHERE lead_id=?",
      (datetime.now().timestamp(), lead_id))
    logger.info(f"Заявка {lead_id} розіслана всім ({len(queue)} менеджерів)")


def _escalation_text(lead) -> str:
    """Текст ліда для повторного показу (відновлення кнопок) — залежить від esc_level."""
    lvl = lead['esc_level']
    if lvl <= 1:
        return f"{lead['title']}\n👤 <i>Відкрита черга</i>"
    elif lvl == 2:
        return f"⚠️⚠️⚠️ <b>ТЕРМІНОВО!</b>\nЗаявка без відповіді!\n\n{lead['title']}"
    else:
        return f"🆘🚨💀🔴 <b>SOS!!!</b>\n\n{lead['title']}"


async def restore_buttons_for_manager(manager_id: str):
    """Відновлює кнопки на активних лідах коли менеджер входить в чергу.
    Також надсилає broadcast заявки що вже активні але ще не приходили цьому менеджеру."""

    # 1. Відновлюємо кнопки на вже надісланих повідомленнях
    rows = await q("""
        SELECT l.* FROM leads l
        JOIN messages m ON m.lead_id = l.lead_id
        WHERE m.manager_id = ?
          AND l.status NOT IN ('taken', 'duplicate', 'closed')
    """, (manager_id,), fetch='all')

    for lead in (rows or []):
        text = _escalation_text(lead)
        await edit_msg(manager_id, lead['lead_id'], text, keyboard=build_keyboard(lead['lead_id']))

    # 2. Надсилаємо активні broadcast заявки що ще не приходили цьому менеджеру
    broadcast_leads = await q("""
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
        text = _escalation_text(lead)
        try:
            await send_to(manager_id, lead['lead_id'], text, build_broadcast_keyboard(lead['lead_id']))
            logger.info(f"restore: надіслано broadcast заявку {lead['lead_id']} → {manager_id}")
        except Exception as e:
            logger.error(f"restore: не вдалось надіслати {lead['lead_id']} → {manager_id}: {e}")


async def _update_offline(queue_set: set, lead_id: str, text: str):
    """Оновлює смс менеджерів поза чергою (без кнопок)."""
    for m in await get_all_msgs(lead_id):
        if m['manager_id'] not in queue_set:
            await edit_msg(m['manager_id'], lead_id, text)


async def _escalate(lead_id: str, text: str, sql: str, sql_params: tuple, log_msg: str, **tick_ctx):
    """Спільна механіка розсилки для escalate_warn/escalate_sos/rebroadcast_periodic:
    надіслати всій черзі, оновити офлайн-повідомлення без кнопок, записати новий стан."""
    lead = await get_lead(lead_id)
    if not lead or lead['status'] in ('taken', 'duplicate', 'closed'):
        return
    kb        = build_broadcast_keyboard(lead_id)
    queue     = await sorted_queue(exclude=await get_skipped(lead_id), **tick_ctx)
    queue_set = set(queue)
    for mid in queue:
        if not await _still_eligible(mid):
            continue
        await delete_and_send(mid, lead_id, text, kb)
    await _update_offline(queue_set, lead_id, text)
    await q(sql, sql_params)
    logger.info(log_msg)


async def escalate_warn(lead_id: str, title: str, **tick_ctx):
    text = (
        f"⚠️⚠️⚠️ <b>ТЕРМІНОВО!</b>\n"
        f"Заявка вже <b>5 хвилин</b> без відповіді!\n\n{title}"
    )
    await _escalate(
        lead_id, text,
        "UPDATE leads SET esc_level=2 WHERE lead_id=?", (lead_id,),
        f"Заявка {lead_id}: 5-хвилинне попередження",
        **tick_ctx,
    )


async def escalate_sos(lead_id: str, title: str, **tick_ctx):
    text = (
        f"🆘🚨💀🔴 <b>SOS!!! ЗАЯВКА 10 ХВИЛИН!!!</b> 🔴💀🚨🆘\n"
        f"😱🔥💥 ХТОСЬ ВІЗЬМІТЬ ВЖЕ! 💥🔥😱\n\n{title}"
    )
    await _escalate(
        lead_id, text,
        "UPDATE leads SET esc_level=3, last_rebroadcast_at=? WHERE lead_id=?",
        (datetime.now().timestamp(), lead_id),
        f"Заявка {lead_id}: SOS 10 хвилин",
        **tick_ctx,
    )


async def rebroadcast_periodic(lead_id: str, title: str, **tick_ctx):
    text = (
        f"🔄 <b>Заявка досі не взята!</b>\n"
        f"⏰ Повторна розсилка — будь ласка, візьміть в роботу!\n\n{title}"
    )
    await _escalate(
        lead_id, text,
        "UPDATE leads SET last_rebroadcast_at=? WHERE lead_id=?",
        (datetime.now().timestamp(), lead_id),
        f"Заявка {lead_id}: повторна розсилка (кожні 30 хв)",
        **tick_ctx,
    )


async def _send_next_queued_broadcast(**tick_ctx):
    """Надсилає наступну broadcast заявку що чекає своєї черги (якщо немає активної)."""
    active = await q(
        "SELECT 1 FROM leads WHERE status='broadcast' AND sent_at IS NOT NULL LIMIT 1",
        fetch='one',
    )
    if active:
        return

    # Затримка 10с після того як хтось взяв лід — щоб не засипати менеджера одразу
    recent = await q(
        "SELECT MAX(taken_at) as last FROM leads WHERE taken_at IS NOT NULL",
        fetch='one',
    )
    if recent and recent['last'] and datetime.now().timestamp() - recent['last'] < 10:
        return

    waiting = await q(
        "SELECT * FROM leads WHERE status='broadcast' AND sent_at IS NULL ORDER BY created_at DESC LIMIT 1",
        fetch='one',
    )
    if not waiting:
        return

    orig_manager = waiting['manager_id']
    skipped      = await get_skipped(waiting['lead_id'])
    text         = f"{waiting['title']}\n👤 <i>Відкрита черга</i>"
    kb           = build_broadcast_keyboard(waiting['lead_id'])
    exclude      = list(set(skipped + ([orig_manager] if orig_manager else [])))
    queue        = await sorted_queue(exclude=exclude, **tick_ctx)

    if orig_manager:
        await delete_and_send(orig_manager, waiting['lead_id'], text, kb)

    for mid in queue:
        await delete_and_send(mid, waiting['lead_id'], text, kb)

    await q("UPDATE leads SET sent_at=? WHERE lead_id=?",
      (datetime.now().timestamp(), waiting['lead_id']))
    logger.info(f"Заявка {waiting['lead_id']} надіслана всім з черги broadcast ({len(queue)} менеджерів)")


async def _tick():
    global _tick_running

    # Якщо попередній тік ще виконується — пропускаємо, не накопичуємо
    if _tick_running:
        logger.warning("_tick: попередній тік ще виконується, пропускаємо")
        return

    _tick_running = True
    try:
        now   = datetime.now().timestamp()
        leads = await q(
            "SELECT * FROM leads WHERE status NOT IN ('taken','duplicate','closed') ORDER BY created_at DESC",
            fetch='all',
        )
        if not leads:
            return

        managers  = await fetch_managers_async()
        taken_map = await get_all_taken(day_key())
        avail_map = await get_all_availability()
        overrides = await get_all_max_leads_overrides()
        sent_map  = await _build_sent_map()
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

            last_rb_str = f"{int(now - last_rb)}s ago" if last_rb else "none"
            logger.debug(
                f"_tick | lead={lid} status={lead['status']} esc={lvl} "
                f"sent={'yes' if sent_at else 'no'} last_rb={last_rb_str}"
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
    finally:
        _tick_running = False


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


def _shift_crosses_midnight(start: str, end: str) -> bool:
    return end <= start


def _in_shift(current_time: str, weekday: int, days: list[int], start: str, end: str) -> bool:
    """Чи потрапляє current_time/weekday у робочу зміну [start, end).
    Підтримує зміни, що переходять через північ (напр. 22:00–05:00)."""
    yesterday = (weekday - 1) % 7
    if _shift_crosses_midnight(start, end):
        return (current_time >= start and weekday in days) or \
               (current_time < end and yesterday in days)
    return weekday in days and start <= current_time < end


async def _check_schedules():
    """Надсилає нагадування на початку зміни та автоматично виводить з черги в кінці."""
    now          = datetime.now(_TZ)
    today        = now.strftime('%Y-%m-%d')
    weekday      = now.weekday()
    yesterday    = (weekday - 1) % 7
    current_time = now.strftime('%H:%M')

    schedules = await get_all_schedules()
    for manager_id, sch in schedules.items():
        if not sch.get('enabled', 1):
            continue

        days      = [int(d) for d in sch['days'].split(',') if d.strip()]
        start     = sch.get('start_time', '16:00')
        end       = sch.get('end_time', '23:00')
        crosses   = _shift_crosses_midnight(start, end)

        # ── Авто-деактивація в кінці зміни ──────────────────────────────────
        # Спрацьовує навіть якщо менеджер зараз поза чергою через 'has_distributed' —
        # інакше після закриття заявки on_lead_undistributed поверне його в чергу
        # вночі, вже після завершення зміни. Виконується лише один раз за хвилину
        # завдяки самообмеженню: одразу після виклику reason стає 'schedule',
        # тож умова нижче більше не виконується на наступних тіках.
        if current_time == end:
            working_day = yesterday if crosses else weekday
            if working_day in days:
                if await is_available(manager_id) or await get_exit_reason(manager_id) == 'has_distributed':
                    await set_availability(manager_id, False, reason='schedule')
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
        await update_last_notified(manager_id, today)

        # Якщо менеджер вже в черзі — мовчки відмічаємо як повідомлений
        if await is_available(manager_id):
            logger.info(f"Schedule: {name} вже в черзі — нагадування пропущено")
            continue

        asyncio.create_task(_send_shift_reminder(manager_id, name))


async def _reset_limit_overrides():
    await reset_all_limit_overrides()
    logger.info("Ручні ліміти скинуто (новий день)")


async def _cleanup_old_records():
    """Видаляє записи старші за 2 місяці."""
    now       = datetime.now()
    keep_from = (now.replace(day=1) - timedelta(days=1)).replace(day=1).strftime('%Y-%m')
    await q("DELETE FROM stats WHERE month < ?", (keep_from,))
    await q("DELETE FROM leads WHERE created_at < ? AND status IN ('taken','duplicate','closed')",
      (datetime.now().timestamp() - 60 * 24 * 3600,))
    await q("DELETE FROM messages WHERE lead_id NOT IN (SELECT lead_id FROM leads)")
    await q("DELETE FROM skipped  WHERE lead_id NOT IN (SELECT lead_id FROM leads)")
    logger.info(f"БД: очищено записи до {keep_from}")


async def deactivate_out_of_schedule():
    """При старті сервера виводить з черги менеджерів що зараз поза робочим часом."""
    now          = datetime.now(_TZ)
    weekday      = now.weekday()
    current_time = now.strftime('%H:%M')

    schedules = await get_all_schedules()
    for manager_id, sch in schedules.items():
        if not sch.get('enabled', 1):
            continue
        if not await is_available(manager_id) and await get_exit_reason(manager_id) != 'has_distributed':
            continue

        days    = [int(d) for d in sch['days'].split(',') if d.strip()]
        start   = sch.get('start_time', '16:00')
        end     = sch.get('end_time', '23:00')

        if not _in_shift(current_time, weekday, days, start, end):
            await set_availability(manager_id, False, reason='schedule')
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


async def resweep_active_leads_for_client_time() -> dict:
    """
    Проганяє ВСІ активні заявки (усе, крім taken/duplicate/closed — тобто
    no_managers/queued/broadcast/sent) через ту саму перевірку "чи настало
    9:00 у клієнта", що й webhook.py робить для нових заявок.

    Навіщо: перевірка "чи ранок у клієнта" в webhook.py рахується лише ОДИН
    раз — у момент приходу вебхука про нову заявку. Якщо на той момент у
    клієнта вже було ранку (is_client_morning), заявка лишається в живій
    черзі назавжди — навіть якщо потім настає її локальна північ і до
    ранку знову далеко. Ця функція — регулярний "дозавантаж": для кожної
    активної заявки перевіряє її ПОТОЧНИЙ стан і, якщо в клієнта зараз ще
    не 9:00, знімає заявку з активної черги/розсилки (як і при закритті —
    прибирає повідомлення в менеджерів, якщо вони є) і переносить у
    held_leads. Звідти її поверне назад в чергу _release_held_leads(),
    щойно в клієнта настане 9:00 — так само, як і звичайні "ранкові" ліди.

    Запускається автоматично щохвилини разом з іншими перевірками
    (build_scheduler() → 'minute_checks' → _run_minute_checks()). Це
    безпечно робити так часто, бо phone/timezone/is_reactivation для
    активних заявок КЕШУЮТЬСЯ в leads один раз — при створенні заявки
    (webhook.py) чи при звільненні з held_leads (_release_held_leads) —
    тому в типовому випадку тут немає жодного запиту в Kommo, лише читання
    з локальної БД. У Kommo лізем лише для "старих" рядків без кешу
    (створених до цієї міграції) — і одразу дозаповнюємо їх заднім числом.

    Раніше це доводилось робити вручну по кнопці "🌙 Звірити ранкові ліди";
    кнопка лишається як ручний тригер поза розкладом (напр. для позапланової
    перевірки).
    """
    from kommo import get_lead_phone, get_lead_info, is_lead_reactivation

    rows = await q(
        "SELECT lead_id, title, phone, timezone, is_reactivation FROM leads "
        "WHERE status NOT IN ('taken','duplicate','closed')",
        fetch='all',
    )
    checked = len(rows or [])
    held    = []

    for row in (rows or []):
        lead_id = row['lead_id']
        title   = row['title']

        # Джерело "Реактивация" — стара угода, повернута в роботу, ранкову
        # перевірку для неї не робимо (так само як для нових лідів у webhook.py).
        # Уже відомо з кешу — жодного запиту в Kommo не треба.
        if row['is_reactivation']:
            continue

        phone   = row['phone']
        tz_name = row['timezone']

        if tz_name is None:
            # Старий рядок без кешу (створений до цієї міграції) — рахуємо
            # так само, як webhook.py, і дозаповнюємо leads заднім числом,
            # щоб наступного разу піти "дешевим" шляхом без Kommo.
            try:
                info = await get_lead_info(lead_id)
            except Exception as e:
                logger.error(f"resweep_active_leads_for_client_time: get_lead_info {lead_id}: {e}")
                info = None
            if info and is_lead_reactivation(info):
                await q("UPDATE leads SET is_reactivation=1 WHERE lead_id=?", (lead_id,))
                continue

            try:
                phone = await get_lead_phone(lead_id)
            except Exception as e:
                logger.error(f"resweep_active_leads_for_client_time: телефон {lead_id}: {e}")
                phone = None

            tz_name = resolve_client_timezone(phone)
            await q("UPDATE leads SET phone=?, timezone=? WHERE lead_id=?", (phone, tz_name, lead_id))

        if is_client_morning(tz_name):
            continue

        try:
            await add_held_lead(lead_id, title, phone, tz_name)
        except Exception as e:
            logger.error(f"resweep_active_leads_for_client_time: held_leads запис {lead_id}: {e}")
            continue

        # Прибираємо з активної розсилки: повідомлення в менеджерів (якщо є) —
        # той самий шлях, що й при закритті заявки (remove_from_others +
        # schedule_cleanup), плюс видаляємо сам рядок з leads, щоб
        # _release_held_leads() пізніше могла вставити його заново.
        await remove_from_others(lead_id, note="🌙 Заявку тимчасово знято — у клієнта ще не настав ранок")
        schedule_cleanup(lead_id)
        await q("DELETE FROM leads WHERE lead_id=?", (lead_id,))

        logger.info(
            f"resweep_active_leads_for_client_time: заявка {lead_id} → held_leads ({tz_name})"
        )
        held.append((lead_id, tz_name))

    return {'checked': checked, 'held': held}


async def _release_held_leads():
    """
    Щохвилини перевіряє заявки, "заморожені" в held_leads (клієнту ще не
    настало 9:00 за його місцевим часом на момент приходу вебхука — див.
    webhook.py). Як тільки в клієнта настає 9:00 — заявка переводиться в
    звичайну чергу leads і одразу пропонується менеджерам.

    Також достроково звільняє заявки з джерелом "Реактивация" (перевіряє це
    тільки для тих, у кого ранок ще не настав, щоб не робити зайвих API-
    запитів). Це "запобіжник заднім числом": заявки, які встигли потрапити
    в held_leads ДО того, як реактивацію почали виключати з ранкової
    перевірки (ручним прогоном "🌙 Звірити ранкові ліди" чи через webhook),
    самі собою розморозяться в межах хвилини після деплою цього фіксу —
    ніяких ручних дій не треба.
    """
    from kommo import get_lead_info, is_lead_reactivation, lead_confirmed_missing

    held = await get_all_held_leads()
    for row in held:
        lead_id = row['lead_id']
        is_reactivation = False

        if not is_client_morning(row['timezone']):
            try:
                info = await get_lead_info(lead_id)
            except Exception as e:
                logger.error(f"_release_held_leads: get_lead_info {lead_id}: {e}")
                info = None
            if not (info and is_lead_reactivation(info)):
                continue
            is_reactivation = True
            logger.info(
                f"_release_held_leads: заявка {lead_id} — джерело 'Реактивация', "
                f"звільняємо достроково (ранок у клієнта ще не настав)"
            )
        else:
            # Настав ранок — перш ніж випустити заявку в живу чергу, перевіряємо,
            # що вона й досі існує в Kommo. Заявку могли видалити, поки вона
            # була заморожена (а delete-вебхук міг не долетіти чи прийти до
            # деплою фіксу в held_leads) — без цієї перевірки бот сліпо ставив
            # би в чергу й роздавав менеджеру заявку-привида (як сталось із
            # заявкою 26148047).
            #
            # ВАЖЛИВО: видаляємо з held_leads БЕЗ видачі в чергу тільки при
            # ПІДТВЕРДЖЕНІЙ фатальній помилці (204/404) — get_lead_info()
            # також повертає None при звичайному мережевому збої (напр.
            # "Server disconnected", в проді буває по кілька разів на день),
            # і прирівнювати "невідомо" до "видалено" означало б губити
            # реальні заявки через тимчасовий обрив з'єднання.
            try:
                confirmed_missing = await lead_confirmed_missing(lead_id)
            except Exception as e:
                logger.error(f"_release_held_leads: lead_confirmed_missing {lead_id}: {e}")
                confirmed_missing = False
            if confirmed_missing:
                await remove_held_lead(lead_id)
                logger.warning(
                    f"_release_held_leads: заявка {lead_id} не знайдена в Kommo "
                    f"(видалена?) — прибираємо з held_leads без видачі в чергу"
                )
                continue

        try:
            await q(
                "INSERT OR IGNORE INTO leads (lead_id, status, created_at, title, phone, timezone, is_reactivation) "
                "VALUES (?,?,?,?,?,?,?)",
                (lead_id, 'queued', datetime.now().timestamp(), row['title'],
                 row['phone'], row['timezone'], int(is_reactivation)),
            )
        except Exception as e:
            logger.error(f"_release_held_leads: не вдалось записати заявку {lead_id}: {e}")
            await notify_admin_error(f"_release_held_leads (запис заявки #{lead_id})", e)
            continue
        await remove_held_lead(lead_id)
        logger.info(f"_release_held_leads: заявка {lead_id} — у клієнта настало 9:00 ({row['timezone']}), видаємо")
        asyncio.create_task(assign_next(lead_id))


def _scheduler_job(name: str, fn):
    """Обгортає job-функцію try/except-ом, що логує і повідомляє адмінів —
    той самий except Exception, що раніше був спільним для всього scheduler_loop()."""
    async def _wrapped():
        try:
            await fn()
        except Exception as e:
            logger.error(f"Scheduler помилка ({name}): {e}")
            await notify_admin_error(f"scheduler ({name})", e)
    return _wrapped


async def _run_minute_checks():
    await _check_schedules()
    await _check_status_broadcast()
    await _release_held_leads()
    await resweep_active_leads_for_client_time()


async def _run_daily_reset():
    await _reset_limit_overrides()


async def _run_monthly_cleanup():
    await _cleanup_old_records()


def build_scheduler() -> AsyncIOScheduler:
    """AsyncIOScheduler-заміна для scheduler_loop(): той самий набір
    періодичних задач (тик, денний/місячний rollover, щохвилинні перевірки,
    чистка застарілих повідомлень), але через CronTrigger/IntervalTrigger
    замість ручного відстеження зміни day/month/minute в одному циклі.
    """
    scheduler = AsyncIOScheduler(timezone=_TZ)

    scheduler.add_job(
        _scheduler_job('tick', _tick),
        IntervalTrigger(seconds=SCHEDULER_TICK, timezone=_TZ),
        id='tick', max_instances=1,
    )
    scheduler.add_job(
        _scheduler_job('daily_reset', _run_daily_reset),
        CronTrigger(hour=0, minute=0, timezone=_TZ),
        id='daily_reset', max_instances=1,
    )
    scheduler.add_job(
        _scheduler_job('monthly_cleanup', _run_monthly_cleanup),
        CronTrigger(day=1, hour=0, minute=0, timezone=_TZ),
        id='monthly_cleanup', max_instances=1,
    )
    scheduler.add_job(
        _scheduler_job('minute_checks', _run_minute_checks),
        CronTrigger(second=0, timezone=_TZ),
        id='minute_checks', max_instances=1,
    )
    scheduler.add_job(
        _scheduler_job('stale_cleanup', cleanup_stale_messages),
        IntervalTrigger(seconds=300, timezone=_TZ),
        id='stale_cleanup', max_instances=1,
    )
    return scheduler


# ─── Розподілені заявки (Распределены) ───────────────────────────────────────

async def on_lead_distributed(lead_id: str):
    """
    Викликається коли заявка переходить в статус 'Распределены', а також коли
    в цьому статусі просто змінюють відповідального (Kommo шле ту саму
    вебхук-подію 'status' і на зміну responsible_user_id, без зміни стадії) —
    тому тут завжди перевіряємо ПОТОЧНОГО відповідального, а не тільки перший раз.
    """
    from kommo import get_lead_info, is_lead_reactivation

    info = await get_lead_info(lead_id)
    if not info:
        logger.warning(f"on_lead_distributed: не вдалось отримати дані заявки {lead_id}")
        return

    # Джерело "Реактивация" — рахуємо заявку і прив'язку до менеджера як завжди,
    # але з черги менеджера НЕ виводимо (він продовжує отримувати нові заявки).
    reactivation = is_lead_reactivation(info)

    responsible_kommo_id = info.get('responsible_user_id')
    if not responsible_kommo_id:
        logger.warning(f"on_lead_distributed: не вдалось отримати responsible для заявки {lead_id}")
        return

    all_managers = await get_all_managers(approved_only=True)
    mgr = next(
        (m for m in all_managers if m['kommo_id'] == responsible_kommo_id),
        None,
    )
    new_manager_id = mgr['tg_id'] if mgr else None

    # Лід вже був закріплений за кимось раніше — перевіряємо, чи це той самий
    existing = await get_distributed_lead(lead_id)
    if existing and existing['manager_id'] != new_manager_id:
        # Відповідального змінили, поки лід лишався в 'Распределены' —
        # звільняємо старого менеджера (і переносимо йому лічильник взятого)
        await _release_reassigned_manager(
            lead_id, existing['manager_id'], new_manager_id,
            kept_in_queue=bool(existing.get('kept_in_queue')),
        )

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

    await add_distributed_lead(lead_id, manager_id, kept_in_queue=reactivation)

    if reactivation:
        logger.info(
            f"on_lead_distributed: {name} ({manager_id}) — заявка {lead_id} "
            f"джерело 'Реактивация', з черги НЕ виводимо"
        )
        return

    # Виводимо з черги тільки якщо менеджер зараз активний —
    # якщо він вже вийшов вручну або за розкладом, не перетираємо його причину виходу
    if await is_available(manager_id):
        await set_availability(manager_id, False, reason='has_distributed')
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


async def _release_reassigned_manager(lead_id: str, old_manager_id: str, new_manager_id=None, kept_in_queue: bool = False):
    """
    Лід був закріплений за old_manager_id, але зараз в Kommo відповідальний
    інший (new_manager_id, або взагалі не наш менеджер — тоді None).
    Прибираємо стару прив'язку, переносимо лічильник «взятих» заявок і,
    якщо у старого менеджера більше немає інших розподілених лідів,
    повертаємо його в чергу (тільки якщо exit_reason саме 'has_distributed').

    kept_in_queue=True — старого менеджера з черги й не виводили (джерело було
    "Реактивация"), тому просто прибираємо прив'язку, без повернення в чергу/сповіщення.
    """
    await remove_distributed_lead(lead_id)
    await transfer_taken(old_manager_id, new_manager_id, day_key())

    remaining = await count_distributed_leads(old_manager_id)
    if remaining > 0:
        logger.info(
            f"on_lead_distributed (переприз.): {old_manager_id} ще має {remaining} заявок у 'Распределены'"
        )
        return

    if kept_in_queue:
        logger.info(
            f"on_lead_distributed (переприз.): {old_manager_id} — заявку {lead_id} передано іншому, "
            f"але з черги його й не виводили (реактивація) — нічого не змінюємо"
        )
        return

    mgr = await get_manager(old_manager_id)
    if not mgr:
        return
    name = mgr['sheet_name'] or mgr['tg_name'] or old_manager_id

    exit_reason = await get_exit_reason(old_manager_id)
    if exit_reason is not None and exit_reason != 'has_distributed':
        logger.info(
            f"on_lead_distributed (переприз.): {name} ({old_manager_id}) — не в черзі з іншої причини "
            f"({exit_reason}), не повертаємо"
        )
        return

    await set_availability(old_manager_id, True)
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
    row = await get_distributed_lead(lead_id)
    kept_in_queue = bool(row.get('kept_in_queue')) if row else False

    await remove_distributed_lead(lead_id)

    remaining = await count_distributed_leads(manager_id)
    if remaining > 0:
        logger.info(
            f"on_lead_undistributed: {manager_id} ще має {remaining} заявок у 'Распределены'"
        )
        return

    if kept_in_queue:
        logger.info(
            f"on_lead_undistributed: заявка {lead_id} — реактивація, "
            f"{manager_id} з черги й не виводили, нічого не змінюємо"
        )
        return

    mgr = await get_manager(manager_id)
    if not mgr:
        return

    name = mgr['sheet_name'] or mgr['tg_name'] or manager_id

    # Повертаємо в чергу тільки якщо причина виходу саме 'has_distributed' —
    # якщо менеджер вийшов вручну або за розкладом поки заявка була distributed, не чіпаємо
    exit_reason = await get_exit_reason(manager_id)
    if exit_reason is not None and exit_reason != 'has_distributed':
        logger.info(
            f"on_lead_undistributed: {name} ({manager_id}) — не в черзі з іншої причини "
            f"({exit_reason}), не повертаємо"
        )
        return

    await set_availability(manager_id, True)
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


async def _is_lead_still_distributed(lead_id: str, tracked_manager_id: str) -> bool:
    """
    Перепитує Kommo API напряму — чи заявка досі реально в статусі
    'Распределены' і закріплена за тим самим менеджером, якого ми трекаємо
    в distributed_leads. Той самий принцип перевірки, що й у webhook.py
    для події "покинула 'Распределены'", але викликається вручну (не з
    вебхука), тому тут немає pipeline_id/status_id з тіла події — довіряємо
    лише свіжим даним з get_lead_info().
    """
    from kommo import get_lead_info

    info = await get_lead_info(lead_id)
    if not info:
        # Заявку не вдалось отримати (видалена, чи Kommo недоступна після
        # ретраїв) — вважаємо, що вона більше не 'Распределены'.
        return False

    pipeline_id = str(info.get('pipeline_id'))
    status_id   = str(info.get('status_id'))

    if pipeline_id == AMO_DISTRIBUTED_PIPELINE_ID and status_id == AMO_DISTRIBUTED_STATUS_ID:
        return True

    if pipeline_id == AMO_PIPELINE_ID and status_id == AMO_HOT_STATUS_ID:
        # Могли повернути заявку в гарячу воронку, у ТОЙ САМИЙ статус
        # "Кваліфікація" — це наше власне відлуння (PATCH від "Беру в
        # роботу"), а не реальна передача. Звільняємо тільки якщо
        # відповідальний справді змінився.
        #
        # ВАЖЛИВО: перевіряти тут pipeline_id БЕЗ status_id — баг: системні
        # статуси "Закрито успішно"/"Закрито і не реалізовано" (142/143)
        # теж лежать у цій самій воронці (AMO_PIPELINE_ID). Якщо менеджер
        # закрив заявку як нереалізовану, відповідальний у Kommo зазвичай
        # НЕ змінюється — без перевірки status_id таке закриття помилково
        # розпізнавалось як "актуальна, у роботі", і менеджера не звільняли.
        tracked_mgr      = await get_manager(tracked_manager_id)
        tracked_kommo_id = tracked_mgr['kommo_id'] if tracked_mgr else None
        return info.get('responsible_user_id') == tracked_kommo_id

    return False


async def _release_to_shift_ended(lead_id: str, manager_id: str) -> Optional[str]:
    """
    Той самий cleanup, що й on_lead_undistributed(), АЛЕ замість повернення
    менеджера в активну чергу — переводить його у стан "зміна закінчилась"
    (exit_reason='schedule'), так само як при звичайному завершенні розкладу.

    Навіщо не повертати в чергу: якщо вебхук про закриття/передачу заявки
    не долетів, і менеджер завис заблокованим — це майже завжди означає, що
    його зміна вже й так закінчилась (типовий кейс: адмін бачить менеджера,
    який зараз не працює, але заблокований старою заявкою). Автоматично
    кидати такого менеджера назад в активну чергу небезпечно — тож ставимо
    його у той самий стан, що й звичайний кінець зміни, а не в чергу.

    Повертає ім'я менеджера, якщо когось реально перевели, інакше None.
    """
    row = await get_distributed_lead(lead_id)
    kept_in_queue = bool(row.get('kept_in_queue')) if row else False

    await remove_distributed_lead(lead_id)

    remaining = await count_distributed_leads(manager_id)
    if remaining > 0:
        logger.info(
            f"_release_to_shift_ended: {manager_id} ще має {remaining} заявок у 'Распределены'"
        )
        return None

    if kept_in_queue:
        logger.info(
            f"_release_to_shift_ended: заявка {lead_id} — реактивація, "
            f"{manager_id} з черги й не виводили, нічого не змінюємо"
        )
        return None

    mgr = await get_manager(manager_id)
    if not mgr:
        return None

    name = mgr['sheet_name'] or mgr['tg_name'] or manager_id

    # Не чіпаємо, якщо менеджер вже поза чергою з іншої причини (вручну/розклад) —
    # той самий принцип, що й в on_lead_undistributed().
    exit_reason = await get_exit_reason(manager_id)
    if exit_reason is not None and exit_reason != 'has_distributed':
        logger.info(
            f"_release_to_shift_ended: {name} ({manager_id}) — вже поза чергою з іншої "
            f"причини ({exit_reason}), не чіпаємо"
        )
        return None

    await set_availability(manager_id, False, reason='schedule')
    await handle_manager_exit(manager_id)
    logger.info(f"_release_to_shift_ended: {name} ({manager_id}) → переведено у 'зміна закінчилась'")

    try:
        await state._app.bot.send_message(
            chat_id=manager_id,
            text=(
                "🌙 <b>Твоя зміна закінчилась</b>\n\n"
                "Заявка «Распределены» вже закрита або передана іншому — "
                "тебе виведено з черги."
            ),
            parse_mode='HTML',
        )
    except Exception as e:
        logger.warning(f"_release_to_shift_ended: не вдалось повідомити {manager_id}: {e}")

    return name


async def reconcile_distributed_leads() -> dict:
    """
    Ручна звірка (адмін-кнопка "📡 Перевірити на зв'язку"): для кожного
    запису в distributed_leads напряму перепитує Kommo — чи заявка справді
    ще в 'Распределены'. Якщо ні — переводить менеджера у стан "зміна
    закінчилась" (див. _release_to_shift_ended) замість автоматичного
    повернення в чергу.

    Навіщо: on_lead_undistributed() спрацьовує лише за вебхуком від Kommo.
    Якщо вебхук не долетів (мережева проблема на боці Kommo, конфлікт
    подій тощо) — менеджер лишається заблокованим "на зв'язку з клієнтом"
    назавжди, хоча заявка вже давно закрита чи передана іншому. Ця функція —
    той самий фікс, просто ІНІЦІЙОВАНИЙ вручну, а не подією.
    """
    rows     = await get_all_distributed_leads()
    checked  = len(rows)
    released = []
    details  = []  # [(lead_id, manager_name, still_distributed: bool, note: str)] — для звіту адміну

    for row in rows:
        lead_id    = row['lead_id']
        manager_id = row['manager_id']
        mgr        = await get_manager(manager_id)
        name       = (mgr['sheet_name'] or mgr['tg_name'] or manager_id) if mgr else manager_id

        try:
            still = await _is_lead_still_distributed(lead_id, manager_id)
        except Exception as e:
            logger.error(f"reconcile_distributed_leads: заявка {lead_id}: {e}")
            details.append((lead_id, name, None, f"помилка перевірки: {e}"))
            continue

        if still:
            details.append((lead_id, name, True, "актуальна, у роботі"))
            continue

        logger.info(
            f"reconcile_distributed_leads: заявка {lead_id} ({manager_id}) — "
            f"вже не в 'Распределены', переводимо в 'зміна закінчилась'"
        )
        released_name = await _release_to_shift_ended(lead_id, manager_id)
        if released_name:
            released.append((lead_id, released_name))
            details.append((lead_id, name, False, "звільнено → 'зміна закінчилась'"))
        else:
            details.append((lead_id, name, False, "вже не 'Распределены', але звільнення не знадобилось"))

    return {'checked': checked, 'released': released, 'details': details}
