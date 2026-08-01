from datetime import datetime


async def test_claim_lead_for_send_is_atomic(temp_db):
    await temp_db.q(
        "INSERT INTO leads (lead_id, status, created_at) VALUES (?,?,?)",
        ('1', 'queued', datetime.now().timestamp()),
    )

    assert await temp_db.claim_lead_for_send('1', 'mgr_a') is True
    # Заявка вже 'sent' — повторне бронювання (тим самим чи іншим менеджером) має провалитись
    assert await temp_db.claim_lead_for_send('1', 'mgr_b') is False

    lead = await temp_db.get_lead('1')
    assert lead['status'] == 'sent'
    assert lead['manager_id'] == 'mgr_a'


async def test_take_lead_increments_stats_once(temp_db):
    await temp_db.q(
        "INSERT INTO leads (lead_id, status, created_at) VALUES (?,?,?)",
        ('2', 'sent', datetime.now().timestamp()),
    )
    month = '2026-07'

    assert await temp_db.take_lead('2', 'mgr_a', month) is True
    assert await temp_db.get_taken('mgr_a', month) == 1

    # Лід вже 'taken' — повторний виклик не повинен збільшити лічильник ще раз
    assert await temp_db.take_lead('2', 'mgr_a', month) is False
    assert await temp_db.get_taken('mgr_a', month) == 1


async def test_availability_roundtrip(temp_db):
    assert await temp_db.is_available('mgr_a') is False
    assert await temp_db.get_exit_reason('mgr_a') is None

    await temp_db.set_availability('mgr_a', True)
    assert await temp_db.is_available('mgr_a') is True
    assert await temp_db.get_exit_reason('mgr_a') is None

    await temp_db.set_availability('mgr_a', False, reason='schedule')
    assert await temp_db.is_available('mgr_a') is False
    assert await temp_db.get_exit_reason('mgr_a') == 'schedule'
