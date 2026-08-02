"""
Регресійний тест на handlers/manager.py::_safe_answer.

Контекст бага, який це закриває: у on_callback між критичними змінами стану
(take_lead/add_distributed_lead/mark_skipped тощо) і завершенням обробки
(підтвердження менеджеру, синк відповідального в Kommo, сповіщення інших
менеджерів/адмінів) стояв "голий" виклик query.answer(). Якщо Telegram-колбек
встигав протухнути (>10 хв) або вже був оброблений раніше, query.answer()
кидав BadRequest("query is too old"/"query id is invalid"), виняток летів у
зовнішній except і ВСЕ, що йшло після цього виклику, просто не виконувалось —
хоча БД вже була змінена. _safe_answer гарантує, що падіння саме на цьому
кроці ніколи не зупиняє решту ланцюжка.
"""
from unittest.mock import AsyncMock

from telegram.error import BadRequest

from handlers.manager import _safe_answer


class _FakeQuery:
    def __init__(self, exc: Exception | None = None):
        self.answer = AsyncMock(side_effect=exc)


async def test_safe_answer_swallows_query_too_old():
    query = _FakeQuery(BadRequest("Query is too old and response timeout expired or query id is invalid"))
    await _safe_answer(query)  # не повинно кинути виняток далі
    query.answer.assert_awaited_once()


async def test_safe_answer_swallows_other_bad_request():
    query = _FakeQuery(BadRequest("Message is not modified"))
    await _safe_answer(query, "text", show_alert=True)
    query.answer.assert_awaited_once_with("text", show_alert=True)


async def test_safe_answer_swallows_unexpected_exception():
    query = _FakeQuery(RuntimeError("щось геть несподіване"))
    await _safe_answer(query)  # теж не повинно проброситись — тільки залогуватись
    query.answer.assert_awaited_once()


async def test_safe_answer_passes_through_args_on_success():
    query = _FakeQuery()
    await _safe_answer(query, "✅ done", show_alert=True)
    query.answer.assert_awaited_once_with("✅ done", show_alert=True)
