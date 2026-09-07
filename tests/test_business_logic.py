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
    await temp_db.q(
        "INSERT INTO leads (lead_id, status, created_at, title) VALUES (?,?,?,?)",
        ('L1', 'queued', datetime.now().timestamp(), 'Заявка L1'),
    )
    await temp_db.set_availability('m1', True)

    managers = {'m1': {'name': 'M1', 'max_leads': None}}
    monkeypatch.setattr(queue_logic, 'fetch_managers_async', AsyncMock(return_value=managers))
    monkeypatch.setattr(state, '_app', _fake_app())

    await queue_logic.assign_next('L1')

    lead = await temp_db.get_lead('L1')
    assert lead['status'] == 'sent'
    assert lead['manager_id'] == 'm1'


async def test_assign_next_no_managers_marks_no_managers(temp_db, monkeypatch):
    await temp_db.q(
        "INSERT INTO leads (lead_id, status, created_at, title) VALUES (?,?,?,?)",
        ('L2', 'queued', datetime.now().timestamp(), 'Заявка L2'),
    )

    monkeypatch.setattr(queue_logic, 'fetch_managers_async', AsyncMock(return_value={}))
    monkeypatch.setattr(queue_logic, 'notify_admins', AsyncMock())

    await queue_logic.assign_next('L2')

    lead = await temp_db.get_lead('L2')
    assert lead['status'] == 'no_managers'
    queue_logic.notify_admins.assert_awaited_once()


async def test_on_lead_undistributed_returns_manager_to_queue_when_no_remaining(temp_db, monkeypatch):
    await temp_db.upsert_manager('m1', 'Manager One', sheet_name='M1')
    await temp_db.approve_manager('m1')
    await temp_db.add_distributed_lead('lead1', 'm1')
    await temp_db.set_availability('m1', False, reason='has_distributed')

    monkeypatch.setattr(state, '_app', _fake_app())

    await queue_logic.on_lead_undistributed('lead1', 'm1')

    assert await temp_db.is_available('m1') is True
    assert await temp_db.get_distributed_lead('lead1') is None


async def test_on_lead_undistributed_keeps_manager_out_if_manual_exit(temp_db, monkeypatch):
    await temp_db.upsert_manager('m1', 'Manager One', sheet_name='M1')
    await temp_db.approve_manager('m1')
    await temp_db.add_distributed_lead('lead1', 'm1')
    await temp_db.set_availability('m1', False, reason='manual')

    monkeypatch.setattr(state, '_app', _fake_app())

    await queue_logic.on_lead_undistributed('lead1', 'm1')

    assert await temp_db.is_available('m1') is False


async def test_handle_lead_event_inserts_new_hot_lead(temp_db, monkeypatch):
    monkeypatch.setattr(webhook, 'assign_next', AsyncMock())
    # Ізолюємось від Kommo (без цього тест реально стукається у справжній API
    # через .env-токен) і від поточного часу доби (без цього фолбек-пояс
    # America/New_York міг вважатись "ще не ранком" уночі/рано-вранці за NY,
    # і лід замість прямо в чергу пішов би в held_leads — тест мав би
    # перевіряти саме "ранковий" сценарій окремо, а не залежати від нього тут).
    monkeypatch.setattr(webhook, 'get_lead_info', AsyncMock(return_value=None))
    monkeypatch.setattr(webhook, 'get_lead_phone', AsyncMock(return_value=None))
    monkeypatch.setattr(webhook, 'is_client_morning', lambda tz_name: True)

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

    lead = await temp_db.get_lead('424242')
    assert lead is not None
    assert lead['status'] == 'queued'
    webhook.assign_next.assert_awaited_once_with('424242')


async def test_release_held_leads_preserves_original_created_at(temp_db, monkeypatch):
    orig_ts = datetime.now().timestamp() - 2 * 24 * 3600  # заявка "прийшла" позавчора
    await temp_db.add_held_lead('L4', 'Заявка L4', '+380501234567', 'Europe/Kyiv', orig_ts)

    monkeypatch.setattr(queue_logic, 'is_client_morning', lambda tz_name: True)
    monkeypatch.setattr(queue_logic, 'assign_next', AsyncMock())
    monkeypatch.setattr('kommo.lead_confirmed_missing', AsyncMock(return_value=False))
    monkeypatch.setattr(state, '_app', _fake_app())

    await queue_logic._release_held_leads()

    lead = await temp_db.get_lead('L4')
    assert lead is not None
    assert lead['created_at'] == orig_ts
    assert await temp_db.get_held_lead('L4') is None


async def test_resweep_stores_broadcast_state_in_held_leads(temp_db, monkeypatch):
    """Заявку, ескальовану до esc_level=3 (SOS), знімає resweep — і весь її стан
    (status/esc_level/manager_id/sent_at/last_rebroadcast_at) має потрапити в
    held_leads, а не загубитись (інакше після звільнення вона мовчки
    відкотилась би до esc_level=0/queued, як заново створена)."""
    now = datetime.now().timestamp()
    rb_at = now - 60
    await temp_db.q(
        "INSERT INTO leads (lead_id, status, created_at, title, phone, timezone, is_reactivation, "
        "esc_level, sent_at, manager_id, last_rebroadcast_at) "
        "VALUES (?,?,?,?,?,?,?,?,?,?,?)",
        ('L5', 'broadcast', now, 'Заявка L5', None, 'America/New_York', 0, 3, now, 'm1', rb_at),
    )

    monkeypatch.setattr(queue_logic, 'is_client_morning', lambda tz_name: False)
    monkeypatch.setattr(queue_logic, 'schedule_cleanup', lambda lead_id, delay=30: None)

    result = await queue_logic.resweep_active_leads_for_client_time()

    assert await temp_db.get_lead('L5') is None
    assert result['held'] == [('L5', 'America/New_York')]

    held = await temp_db.get_held_lead('L5')
    assert held is not None
    assert held['orig_status'] == 'broadcast'
    assert held['esc_level'] == 3
    assert held['orig_manager_id'] == 'm1'
    assert held['orig_sent_at'] == now
    assert held['orig_last_rebroadcast_at'] == rb_at


async def test_release_held_leads_restores_broadcast_esc_level(temp_db, monkeypatch):
    """Заявка, заморожена в esc_level=2 (broadcast), після звільнення має
    повернутись саме в broadcast з esc_level=2 — а не в queued/esc_level=0."""
    orig_ts = datetime.now().timestamp() - 3600
    await temp_db.add_held_lead('L6', 'Заявка L6', '+380501234567', 'Europe/Kyiv', orig_ts,
                                 orig_status='broadcast', esc_level=2)

    monkeypatch.setattr(queue_logic, 'is_client_morning', lambda tz_name: True)
    monkeypatch.setattr(queue_logic, 'assign_next', AsyncMock())
    monkeypatch.setattr('kommo.lead_confirmed_missing', AsyncMock(return_value=False))
    monkeypatch.setattr(state, '_app', _fake_app())

    await queue_logic._release_held_leads()

    lead = await temp_db.get_lead('L6')
    assert lead is not None
    assert lead['status'] == 'broadcast'
    assert lead['esc_level'] == 2
    assert lead['sent_at'] is None
    assert await temp_db.get_held_lead('L6') is None
    # broadcast-заявку далі підхоплює _send_next_queued_broadcast() на наступному
    # тіку, а не assign_next() (той — тільки для queued/no_managers/sent).
    queue_logic.assign_next.assert_not_awaited()


async def test_release_held_leads_restores_live_broadcast_when_no_conflict(temp_db, monkeypatch):
    """Якщо на момент звільнення жодна інша заявка не веде активний broadcast —
    відновлюємо sent_at/manager_id/last_rebroadcast_at "як є" (годинник
    очікування НЕ ставиться на паузу під час hold), а не гасимо їх у NULL."""
    orig_ts   = datetime.now().timestamp() - 3600
    orig_sent = datetime.now().timestamp() - 900  # 15 хв тому — вже за TIMEOUT_SOS
    orig_rb   = datetime.now().timestamp() - 120
    await temp_db.add_held_lead('L9', 'Заявка L9', '+380501234567', 'Europe/Kyiv', orig_ts,
                                 orig_status='broadcast', esc_level=3,
                                 orig_manager_id='m1', orig_sent_at=orig_sent,
                                 orig_last_rebroadcast_at=orig_rb)

    monkeypatch.setattr(queue_logic, 'is_client_morning', lambda tz_name: True)
    monkeypatch.setattr(queue_logic, 'assign_next', AsyncMock())
    monkeypatch.setattr('kommo.lead_confirmed_missing', AsyncMock(return_value=False))
    monkeypatch.setattr(state, '_app', _fake_app())

    await queue_logic._release_held_leads()

    lead = await temp_db.get_lead('L9')
    assert lead['status'] == 'broadcast'
    assert lead['esc_level'] == 3
    assert lead['manager_id'] == 'm1'
    assert lead['sent_at'] == orig_sent
    assert lead['last_rebroadcast_at'] == orig_rb
    queue_logic.assign_next.assert_not_awaited()


async def test_release_held_leads_keeps_waiting_when_another_broadcast_active(temp_db, monkeypatch):
    """Якщо просто зараз ВЖЕ активно розсилається ІНША заявка — звільнена
    заявка не повинна ставати другою "живою" (інакше менеджери отримають дві
    картки одночасно, порушивши інваріант "лише один active broadcast"). Вона
    йде в чергу очікування (sent_at=NULL), esc_level все одно зберігається."""
    now = datetime.now().timestamp()
    await temp_db.q(
        "INSERT INTO leads (lead_id, status, created_at, title, sent_at, esc_level) VALUES (?,?,?,?,?,?)",
        ('OTHER', 'broadcast', now, 'Інша активна заявка', now, 1),
    )

    orig_ts   = datetime.now().timestamp() - 3600
    orig_sent = datetime.now().timestamp() - 900
    await temp_db.add_held_lead('L10', 'Заявка L10', '+380501234567', 'Europe/Kyiv', orig_ts,
                                 orig_status='broadcast', esc_level=2,
                                 orig_manager_id='m2', orig_sent_at=orig_sent)

    monkeypatch.setattr(queue_logic, 'is_client_morning', lambda tz_name: True)
    monkeypatch.setattr(queue_logic, 'assign_next', AsyncMock())
    monkeypatch.setattr('kommo.lead_confirmed_missing', AsyncMock(return_value=False))
    monkeypatch.setattr(state, '_app', _fake_app())

    await queue_logic._release_held_leads()

    lead = await temp_db.get_lead('L10')
    assert lead['status'] == 'broadcast'
    assert lead['esc_level'] == 2
    assert lead['sent_at'] is None
    queue_logic.assign_next.assert_not_awaited()


async def test_release_held_leads_defaults_to_queued_without_orig_status(temp_db, monkeypatch):
    """Ліди без збереженого orig_status (напр. нові з webhook.py — там ще
    немає стану на момент заморозки) повертаються як і раніше — queued/0."""
    orig_ts = datetime.now().timestamp() - 3600
    await temp_db.add_held_lead('L8', 'Заявка L8', '+380501234567', 'Europe/Kyiv', orig_ts)

    monkeypatch.setattr(queue_logic, 'is_client_morning', lambda tz_name: True)
    monkeypatch.setattr(queue_logic, 'assign_next', AsyncMock())
    monkeypatch.setattr('kommo.lead_confirmed_missing', AsyncMock(return_value=False))
    monkeypatch.setattr(state, '_app', _fake_app())

    await queue_logic._release_held_leads()

    lead = await temp_db.get_lead('L8')
    assert lead['status'] == 'queued'
    assert lead['esc_level'] == 0
    queue_logic.assign_next.assert_awaited_once_with('L8')


async def test_send_next_queued_broadcast_uses_escalation_text(temp_db, monkeypatch):
    """Заявка, що чекає своєї черги в broadcast із esc_level=2 (напр. щойно
    відновлена зі стану hold), має піти з ескалаційним текстом ('ТЕРМІНОВО'),
    а не з дефолтним 'Відкрита черга' першого рівня."""
    now = datetime.now().timestamp()
    await temp_db.q(
        "INSERT INTO leads (lead_id, status, created_at, title, manager_id, esc_level, sent_at) "
        "VALUES (?,?,?,?,?,?,?)",
        ('L7', 'broadcast', now, 'Заявка L7', 'm1', 2, None),
    )

    monkeypatch.setattr(queue_logic, 'delete_and_send', AsyncMock())

    await queue_logic._send_next_queued_broadcast(
        managers={}, taken_map={}, avail_map={}, overrides={}, sent_map={},
    )

    queue_logic.delete_and_send.assert_awaited_once()
    manager_id, lead_id, text, _kb = queue_logic.delete_and_send.await_args.args
    assert manager_id == 'm1'
    assert lead_id == 'L7'
    assert 'ТЕРМІНОВО' in text
    assert 'Відкрита черга' not in text

    lead = await temp_db.get_lead('L7')
    assert lead['sent_at'] is not None


async def test_tick_assigns_stale_queued_lead_smoke(temp_db, monkeypatch):
    await temp_db.q(
        "INSERT INTO leads (lead_id, status, created_at, title) VALUES (?,?,?,?)",
        ('L3', 'queued', datetime.now().timestamp() - 10, 'Заявка L3'),
    )
    await temp_db.set_availability('m1', True)

    managers = {'m1': {'name': 'M1', 'max_leads': None}}
    monkeypatch.setattr(queue_logic, 'fetch_managers_async', AsyncMock(return_value=managers))
    monkeypatch.setattr(state, '_app', _fake_app())

    await queue_logic._tick()

    lead = await temp_db.get_lead('L3')
    assert lead['status'] == 'sent'
    assert lead['manager_id'] == 'm1'
