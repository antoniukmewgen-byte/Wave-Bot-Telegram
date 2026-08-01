"""
HTTP-рівневі тести для kommo.py: перевіряють ретраї, обробку 429/5xx та фатальних
помилок безпосередньо на рівні aiohttp-запитів (без реального звернення до Kommo API).
"""
import re

import pytest
from aioresponses import aioresponses

import kommo
from config import AMO_SUBDOMAIN

USERS_URL = f"https://{AMO_SUBDOMAIN}.kommo.com/api/v4/users"
LEADS_URL = f"https://{AMO_SUBDOMAIN}.kommo.com/api/v4/leads"

# sync_from_kommo() робить GET з query-параметрами (filter/limit/page), тому
# aioresponses не заматчить точний рядок LEADS_URL (він порівнює URL цілком,
# разом з querystring) — для GET-запитів використовуємо regex, що покриває
# LEADS_URL з будь-яким набором query-параметрів.
LEADS_URL_PATTERN = re.compile(rf"^{re.escape(LEADS_URL)}(\?.*)?$")


@pytest.fixture(autouse=True)
async def _kommo_test_env(monkeypatch):
    """Вмикає AMO_TOKEN (без нього всі функції одразу виходять) і чистить сесію
    aiohttp між тестами, щоб aioresponses завжди перехоплював свіжі запити."""
    monkeypatch.setattr(kommo, 'AMO_TOKEN', 'test-token')
    yield
    await kommo.close_session()


# ─── get_kommo_users ──────────────────────────────────────────────────────────

async def test_get_kommo_users_success():
    with aioresponses() as m:
        m.get(USERS_URL, payload={'_embedded': {'users': [{'id': 1, 'name': 'Alice', 'extra': 'x'}]}})
        users = await kommo.get_kommo_users()
    assert users == [{'id': 1, 'name': 'Alice'}]


async def test_get_kommo_users_http_error_returns_empty_list():
    with aioresponses() as m:
        m.get(USERS_URL, status=500)
        users = await kommo.get_kommo_users()
    assert users == []


# ─── set_kommo_responsible ────────────────────────────────────────────────────

async def test_set_kommo_responsible_success(temp_db):
    await temp_db.upsert_manager('m1', 'Manager One', sheet_name='M1', kommo_id=42)

    with aioresponses() as m:
        m.patch(LEADS_URL, status=200)
        ok = await kommo.set_kommo_responsible('1001', 'm1')
    assert ok is True


async def test_set_kommo_responsible_no_kommo_id_skips_request(temp_db):
    await temp_db.upsert_manager('m1', 'Manager One', sheet_name='M1', kommo_id=None)
    ok = await kommo.set_kommo_responsible('1001', 'm1')
    assert ok is False


async def test_set_kommo_responsible_http_error_returns_false(temp_db):
    await temp_db.upsert_manager('m1', 'Manager One', sheet_name='M1', kommo_id=42)

    with aioresponses() as m:
        m.patch(LEADS_URL, status=500)
        ok = await kommo.set_kommo_responsible('1001', 'm1')
    assert ok is False


# ─── sync_from_kommo: retry/429/5xx ───────────────────────────────────────────

async def test_sync_from_kommo_retries_on_429_then_succeeds(temp_db):
    with aioresponses() as m:
        m.get(LEADS_URL_PATTERN, status=429, headers={'Retry-After': '0'})
        m.get(LEADS_URL_PATTERN, payload={'_embedded': {'leads': [{'id': 111, 'created_at': 1000}]}})
        m.get(LEADS_URL_PATTERN, status=204)

        added, skipped, closed = await kommo.sync_from_kommo()

    assert added == 1
    assert skipped == 0
    lead = await temp_db.get_lead('111')
    assert lead is not None
    assert lead['status'] == 'queued'


async def test_sync_from_kommo_retries_on_5xx_then_succeeds(temp_db):
    with aioresponses() as m:
        m.get(LEADS_URL_PATTERN, status=502)
        m.get(LEADS_URL_PATTERN, payload={'_embedded': {'leads': [{'id': 222, 'created_at': 1000}]}})
        m.get(LEADS_URL_PATTERN, status=204)

        added, skipped, closed = await kommo.sync_from_kommo()

    assert added == 1
    lead = await temp_db.get_lead('222')
    assert lead is not None


async def test_sync_from_kommo_gives_up_after_3_failed_attempts(temp_db):
    with aioresponses() as m:
        # 3 послідовні 429 — усі спроби retry вичерпано, синхронізація зупиняється
        m.get(LEADS_URL_PATTERN, status=429, headers={'Retry-After': '0'})
        m.get(LEADS_URL_PATTERN, status=429, headers={'Retry-After': '0'})
        m.get(LEADS_URL_PATTERN, status=429, headers={'Retry-After': '0'})

        added, skipped, closed = await kommo.sync_from_kommo()

    assert (added, skipped, closed) == (0, 0, 0)


async def test_sync_from_kommo_fatal_error_stops_immediately(temp_db):
    with aioresponses() as m:
        m.get(LEADS_URL_PATTERN, status=403)

        added, skipped, closed = await kommo.sync_from_kommo()

    assert (added, skipped, closed) == (0, 0, 0)


async def test_sync_from_kommo_no_token_returns_zeros(monkeypatch, temp_db):
    monkeypatch.setattr(kommo, 'AMO_TOKEN', '')
    added, skipped, closed = await kommo.sync_from_kommo()
    assert (added, skipped, closed) == (0, 0, 0)


async def test_sync_from_kommo_closes_leads_missing_from_kommo(temp_db):
    from datetime import datetime
    await temp_db.q(
        "INSERT INTO leads (lead_id, status, created_at, title) VALUES (?,?,?,?)",
        ('999', 'queued', datetime.now().timestamp(), 'Заявка 999'),
    )

    with aioresponses() as m:
        m.get(LEADS_URL_PATTERN, status=204)  # Kommo не повертає жодного ліда

        added, skipped, closed = await kommo.sync_from_kommo()

    assert added == 0
    assert closed == 1
    lead = await temp_db.get_lead('999')
    assert lead['status'] == 'closed'
