import asyncio
from datetime import datetime
from unittest.mock import AsyncMock, MagicMock

import db
import queue_logic
import state
import webhook
from config import AMO_PIPELINE_ID, AMO_HOT_STATUS_ID


def _fake_app():
    app = MagicMock()
    app.bot.send_message = AsyncMock(return_value=MagicMock(message_id=555))
    return app


async def test_assign_next_assigns_to_least_loaded_manager(temp_db, monkeypatch):
    temp_db.q(
        "INSERT INTO leads (lead_id, status, created_at, title) VALUES (?,?,?,?)",
        ('L1', 'queued', datetime.now().timestamp(), 'Заявка L1'),
    )
    temp_db.set_availability('m1', True)

    managers = {'m1': {'name': 'M1', 'max_leads': None}}
    monkeypatch.setattr(queue_logic, 'fetch_managers_async', AsyncMock(return_value=managers))
    monkeypatch.setattr(state, '_app', _fake_app())

    await queue_logic.assign_next('L1')

    lead = temp_db.get_lead('L1')
    assert lead['status'] == 'sent'
    assert lead['manager_id'] == 'm1'


async def test_assign_next_no_managers_marks_no_managers(temp_db, monkeypatch):
    temp_db.q(
        "INSERT INTO leads (lead_id, status, created_at, title) VALUES (?,?,?,?)",
        ('L2', 'queued', datetime.now().timestamp(), 'Заявка L2'),
    )

    monkeypatch.setattr(queue_logic, 'fetch_managers_async', AsyncMock(return_value={}))
    monkeypatch.setattr(queue_logic, 'notify_admins', AsyncMock())

    await queue_logic.assign_next('L2')

    lead = temp_db.get_lead('L2')
    assert lead['status'] == 'no_managers'
    queue_logic.notify_admins.assert_awaited_once()


async def test_on_lead_undistributed_returns_manager_to_queue_when_no_remaining(temp_db, monkeypatch):
    temp_db.upsert_manager('m1', 'Manager One', sheet_name='M1')
    temp_db.approve_manager('m1')
    temp_db.add_distributed_lead('lead1', 'm1')
    temp_db.set_availability('m1', False, reason='has_distributed')

    monkeypatch.setattr(state, '_app', _fake_app())

    await queue_logic.on_lead_undistributed('lead1', 'm1')

    assert temp_db.is_available('m1') is True
    assert temp_db.get_distributed_lead('lead1') is None


async def test_on_lead_undistributed_keeps_manager_out_if_manual_exit(temp_db, monkeypatch):
    temp_db.upsert_manager('m1', 'Manager One', sheet_name='M1')
    temp_db.approve_manager('m1')
    temp_db.add_distributed_lead('lead1', 'm1')
    temp_db.set_availability('m1', False, reason='manual')

    monkeypatch.setattr(state, '_app', _fake_app())

    await queue_logic.on_lead_undistributed('lead1', 'm1')

    assert temp_db.is_available('m1') is False


async def test_handle_lead_event_inserts_new_hot_lead(temp_db, monkeypatch):
    monkeypatch.setattr(webhook, 'assign_next', AsyncMock())

    event = {
        'lead_id':     '424242',
        'status_id':   AMO_HOT_STATUS_ID,
        'pipeline_id': AMO_PIPELINE_ID,
        'is_delete':   False,
        'category':    'status',
    }

    await webhook._handle_lead_event(event)
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    lead = temp_db.get_lead('424242')
    assert lead is not None
    assert lead['status'] == 'queued'
    webhook.assign_next.assert_awaited_once_with('424242')


async def test_tick_assigns_stale_queued_lead_smoke(temp_db, monkeypatch):
    temp_db.q(
        "INSERT INTO leads (lead_id, status, created_at, title) VALUES (?,?,?,?)",
        ('L3', 'queued', datetime.now().timestamp() - 10, 'Заявка L3'),
    )
    temp_db.set_availability('m1', True)

    managers = {'m1': {'name': 'M1', 'max_leads': None}}
    monkeypatch.setattr(queue_logic, 'fetch_managers_async', AsyncMock(return_value=managers))
    monkeypatch.setattr(state, '_app', _fake_app())

    await queue_logic._tick()

    lead = temp_db.get_lead('L3')
    assert lead['status'] == 'sent'
    assert lead['manager_id'] == 'm1'
