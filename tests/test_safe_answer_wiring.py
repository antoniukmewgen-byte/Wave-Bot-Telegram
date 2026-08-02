"""
Регресійні тести на те, що handlers/admin_callbacks.py та handlers/conversations.py
дійсно використовують notifications.safe_answer() замість "голого" query.answer().

Контекст: та сама вада, що й у handlers/manager.py (див. tests/test_manager_safe_answer.py) —
якщо query.answer() кидає BadRequest("query is too old"/"query id is invalid") бо колбек
протух або вже оброблений, виняток не повинен зупиняти решту обробника. У admin_callbacks.py
раніше не було взагалі жодного try/except навколо query.answer() — найгірший випадок,
бо після нього одразу йдуть критичні дії (approve_manager/delete_manager, сповіщення).
"""
from unittest.mock import AsyncMock, MagicMock

from telegram.error import BadRequest

import state
from handlers import admin_callbacks, conversations


def _fake_app():
    app = MagicMock()
    app.bot.send_message = AsyncMock(return_value=MagicMock(message_id=1))
    return app


def _fake_query(data: str, from_id: str, exc: Exception):
    query = MagicMock()
    query.data = data
    query.from_user.id = from_id
    query.from_user.full_name = "Тест Тестовий"
    query.answer = AsyncMock(side_effect=exc)
    query.edit_message_text = AsyncMock()
    return query


async def test_admin_callback_unauthorized_survives_stale_query(monkeypatch):
    """Неавторизований адмін + протухлий callback: answer() кидає BadRequest,
    але обробник не повинен впасти — просто виходить після попередження."""
    monkeypatch.setattr(admin_callbacks, 'ADMIN_IDS', ['777'])
    query = _fake_query(
        'mgr_approve:123',
        from_id='999',  # не в ADMIN_IDS
        exc=BadRequest("Query is too old and response timeout expired or query id is invalid"),
    )
    update = MagicMock(callback_query=query)

    await admin_callbacks.on_admin_callback(update, MagicMock())  # не повинно кинути виняток

    query.answer.assert_awaited_once()


async def test_admin_callback_authorized_survives_stale_query_and_continues(monkeypatch, temp_db):
    """Авторизований адмін, протухлий callback на bare query.answer(): виняток
    гаситься, і обробник йде далі — до пошуку менеджера в БД."""
    monkeypatch.setattr(admin_callbacks, 'ADMIN_IDS', ['777'])
    monkeypatch.setattr(state, '_app', _fake_app())

    query = _fake_query(
        'mgr_approve:not-in-db',
        from_id='777',
        exc=BadRequest("query id is invalid"),
    )
    update = MagicMock(callback_query=query)

    await admin_callbacks.on_admin_callback(update, MagicMock())  # не повинно кинути виняток

    query.answer.assert_awaited_once()
    # Дійшло до гілки "менеджера не знайдено" — тобто виконання продовжилось після answer()
    query.edit_message_text.assert_awaited_once()
    assert "не знайдено" in query.edit_message_text.await_args.args[0]


async def test_conversations_limits_select_survives_stale_query():
    """limits_select: скасування через протухлий callback не повинно кидати виняток."""
    query = _fake_query('setlim:cancel', from_id='777', exc=BadRequest("query is too old"))
    update = MagicMock(callback_query=query)

    result = await conversations.limits_select(update, MagicMock(user_data={}))

    query.answer.assert_awaited_once()
    query.edit_message_text.assert_awaited_once_with("❌ Скасовано")
    from telegram.ext import ConversationHandler
    assert result == ConversationHandler.END


async def test_conversations_schedules_select_survives_unexpected_exception():
    """schedules_select: неочікуваний виняток з answer() теж не повинен пробитись."""
    query = _fake_query('sched:cancel', from_id='777', exc=RuntimeError("щось геть несподіване"))
    update = MagicMock(callback_query=query)

    result = await conversations.schedules_select(update, MagicMock(user_data={}))

    query.answer.assert_awaited_once()
    query.edit_message_text.assert_awaited_once_with("❌ Скасовано")
    from telegram.ext import ConversationHandler
    assert result == ConversationHandler.END
