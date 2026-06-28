import asyncio
import logging

from telegram.error import Forbidden, RetryAfter, TimedOut, NetworkError

import state
from config import ADMIN_IDS
from db import q, get_msg_id, save_msg, get_all_msgs, set_availability

logger = logging.getLogger(__name__)


async def send_long(message, text: str, parse_mode: str = 'HTML'):
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
            await state._app.bot.send_message(chat_id=admin_id, text=text, parse_mode='HTML')
        except Exception as e:
            logger.warning(f"notify_admins: не вдалось надіслати адміну {admin_id}: {e}")


async def notify_admin_error(where: str, error: Exception, manager_id: str = None):
    if not ADMIN_IDS or not state._app:
        return
    mgr_part = ''
    if manager_id:
        name     = state.MANAGERS_BY_ID.get(manager_id) or manager_id
        mgr_part = f"\n👤 Менеджер: <b>{name}</b> (<code>{manager_id}</code>)"
    text = (
        f"🚨 <b>Помилка бота</b>\n"
        f"📍 Місце: <code>{where}</code>{mgr_part}\n"
        f"❗ Помилка: <code>{type(error).__name__}: {error}</code>"
    )
    await notify_admins(text)


async def _deactivate_blocked(manager_id: str):
    set_availability(manager_id, False, reason='bot_blocked')
    name = state.MANAGERS_BY_ID.get(manager_id, manager_id)
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


async def send_to(manager_id: str, lead_id: str, text: str, keyboard) -> int:
    try:
        msg = await _tg_retry(
            lambda: state._app.bot.send_message(
                chat_id=manager_id,
                text=text,
                reply_markup=keyboard,
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


async def edit_msg(manager_id: str, lead_id: str, text: str, keyboard=None):
    msg_id = get_msg_id(lead_id, manager_id)
    if not msg_id:
        return
    try:
        await state._app.bot.edit_message_text(
            chat_id=manager_id,
            message_id=msg_id,
            text=text,
            reply_markup=keyboard,
            parse_mode='HTML',
        )
    except Forbidden:
        await _deactivate_blocked(manager_id)
    except Exception as e:
        logger.debug(f"edit_msg {manager_id}: {e}")


async def delete_and_send(manager_id: str, lead_id: str, text: str, keyboard):
    msg_id = get_msg_id(lead_id, manager_id)
    if msg_id:
        try:
            await state._app.bot.delete_message(chat_id=manager_id, message_id=msg_id)
        except Exception as e:
            logger.debug(f"delete_and_send: не вдалось видалити {msg_id} для {manager_id}: {e}")
    await send_to(manager_id, lead_id, text, keyboard)


async def remove_buttons_for_manager(manager_id: str):
    """При виході з черги — прибирає кнопки з усіх активних повідомлень менеджера."""
    rows = q("""
        SELECT l.lead_id, l.title FROM leads l
        JOIN messages m ON m.lead_id = l.lead_id
        WHERE m.manager_id = ?
          AND l.status NOT IN ('taken', 'duplicate', 'closed')
    """, (manager_id,), fetch='all')
    for lead in (rows or []):
        await edit_msg(manager_id, lead['lead_id'], f"⏸ Ви вийшли з черги\n\n{lead['title']}")


async def remove_from_others(lead_id: str, except_id: str = None, note: str = "✅ Заявку вже взято в роботу"):
    for m in get_all_msgs(lead_id):
        if m['manager_id'] == except_id:
            continue
        await edit_msg(m['manager_id'], lead_id, note)


async def _delete_all_msgs(lead_id: str):
    for m in get_all_msgs(lead_id):
        try:
            await state._app.bot.delete_message(chat_id=m['manager_id'], message_id=m['msg_id'])
        except Exception:
            pass


def schedule_cleanup(lead_id: str, delay: int = 30):
    """Видаляє всі повідомлення по заявці через delay секунд."""
    async def _task():
        await asyncio.sleep(delay)
        await _delete_all_msgs(lead_id)
    asyncio.create_task(_task())


def schedule_delete_msg(manager_id: str, lead_id: str, delay: int = 30):
    """Видаляє смс конкретного менеджера через delay секунд."""
    async def _task():
        await asyncio.sleep(delay)
        msg_id = get_msg_id(lead_id, manager_id)
        if not msg_id:
            return
        try:
            await state._app.bot.delete_message(chat_id=manager_id, message_id=msg_id)
        except Exception:
            pass
    asyncio.create_task(_task())


async def cleanup_stale_messages() -> int:
    """
    Видаляє в Telegram всі повідомлення по лідах зі статусом taken/duplicate/closed.
    Повертає кількість видалених повідомлень.
    """
    rows = q("""
        SELECT m.manager_id, m.lead_id, m.msg_id
        FROM messages m
        JOIN leads l ON l.lead_id = m.lead_id
        WHERE l.status IN ('taken', 'duplicate', 'closed')
    """, fetch='all')

    if not rows:
        return 0

    deleted = 0
    for r in rows:
        try:
            await state._app.bot.delete_message(chat_id=r['manager_id'], message_id=r['msg_id'])
            deleted += 1
        except Exception:
            pass
        # Видаляємо запис незалежно від успіху — щоб не повторювати спробу наступного разу
        q("DELETE FROM messages WHERE lead_id=? AND manager_id=?", (r['manager_id'], r['lead_id']))

    if deleted:
        logger.info(f"cleanup_stale_messages: видалено {deleted} застарілих повідомлень")
    return deleted
