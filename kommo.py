import asyncio
import logging
from datetime import datetime
from typing import Optional

import aiohttp
from tenacity import retry, retry_if_exception_type, stop_after_attempt

from config import AMO_SUBDOMAIN, AMO_TOKEN, AMO_PIPELINE_ID, AMO_HOT_STATUS_ID, HOT_STATUSES
from db import q, get_lead, get_manager
from notifications import remove_from_others

logger = logging.getLogger(__name__)

_session: Optional[aiohttp.ClientSession] = None
_session_lock = asyncio.Lock()


async def _get_session() -> aiohttp.ClientSession:
    """Повертає спільну ClientSession (лінива ініціалізація, потокобезпечно).

    Раніше кожен виклик до Kommo відкривав нову сесію/з'єднання — при масовому
    напливі запитів (див. шторм ~1700 запитів) це множило TCP-з'єднання без
    потреби. TCPConnector(limit=20) додатково обмежує паралелізм як
    захист-в-глибину, навіть якщо вище по стеку тротлінг колись знову проламається.
    """
    global _session
    if _session is None or _session.closed:
        async with _session_lock:
            if _session is None or _session.closed:
                _session = aiohttp.ClientSession(
                    connector=aiohttp.TCPConnector(limit=20)
                )
    return _session


async def close_session():
    global _session
    if _session is not None and not _session.closed:
        await _session.close()
    _session = None


def make_lead_title(status_id: str, lead_id: str) -> str:
    raw_label = HOT_STATUSES.get(str(status_id), 'Нова заявка')
    if 'Гаряча' in raw_label:
        header = '🔥 ГАРЯЧА ЗАЯВКА'
    elif 'Кваліфікована' in raw_label:
        header = '⭐ КВАЛІФІКОВАНА ЗАЯВКА'
    else:
        header = '📋 НОВА ЗАЯВКА'
    lead_url = f"https://{AMO_SUBDOMAIN}.kommo.com/leads/detail/{lead_id}"
    return f'{header}\n🔗 <a href="{lead_url}">Угода #{lead_id}</a>'


async def get_kommo_users() -> list:
    """Повертає список {id, name} з Kommo API для вибору при реєстрації."""
    if not AMO_TOKEN:
        return []
    url     = f"https://{AMO_SUBDOMAIN}.kommo.com/api/v4/users"
    headers = {"Authorization": f"Bearer {AMO_TOKEN}"}
    try:
        session = await _get_session()
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                logger.error(f"get_kommo_users: HTTP {resp.status}")
                return []
            data = await resp.json()
            return [
                {'id': u['id'], 'name': u['name']}
                for u in data.get('_embedded', {}).get('users', [])
            ]
    except Exception as e:
        logger.error(f"get_kommo_users: {e}")
        return []


async def get_lead_responsible(lead_id: str):
    """
    Повертає responsible_user_id заявки з Kommo API.
    Використовується щоб визначити менеджера при отриманні webhook 'Распределены'.
    """
    info = await get_lead_info(lead_id)
    return info.get('responsible_user_id') if info else None


async def get_lead_info(lead_id: str) -> Optional[dict]:
    """
    Повертає {responsible_user_id, status_id, pipeline_id} заявки напряму з Kommo API.

    Навмисно НЕ покладаємось на pipeline_id/status_id з тіла вебхука —
    для категорії 'responsible' Kommo може не присилати ці поля взагалі,
    через що перевірка "лід досі в 'Распределены'" мовчки провалювалась би.
    """
    if not AMO_TOKEN:
        return None
    url     = f"https://{AMO_SUBDOMAIN}.kommo.com/api/v4/leads/{lead_id}"
    headers = {"Authorization": f"Bearer {AMO_TOKEN}"}
    try:
        session = await _get_session()
        async with session.get(url, headers=headers) as resp:
            if resp.status != 200:
                logger.error(f"get_lead_info: HTTP {resp.status} для заявки {lead_id}")
                return None
            data = await resp.json()
            return {
                'responsible_user_id': data.get('responsible_user_id'),
                'status_id':           data.get('status_id'),
                'pipeline_id':         data.get('pipeline_id'),
            }
    except Exception as e:
        logger.error(f"get_lead_info: {e}")
        return None


async def set_kommo_responsible(lead_id: str, manager_id: str) -> bool:
    """Встановлює відповідального менеджера в Kommo. Повертає True якщо успішно."""
    mgr = await get_manager(manager_id)
    kommo_user_id = mgr['kommo_id'] if mgr else None
    if not kommo_user_id or not AMO_TOKEN:
        return False
    url     = f"https://{AMO_SUBDOMAIN}.kommo.com/api/v4/leads"
    payload = [{"id": int(lead_id), "responsible_user_id": kommo_user_id}]
    headers = {"Authorization": f"Bearer {AMO_TOKEN}", "Content-Type": "application/json"}
    try:
        session = await _get_session()
        async with session.patch(url, json=payload, headers=headers) as resp:
            if resp.status not in (200, 202, 204):
                body = await resp.text()
                logger.error(f"Kommo responsible: HTTP {resp.status} для заявки {lead_id} | {body[:200]}")
                return False
            logger.info(f"Kommo responsible: заявка {lead_id} → менеджер {kommo_user_id}")
            return True
    except Exception as e:
        logger.error(f"Kommo responsible: помилка для заявки {lead_id} | {e}")
        return False


class _KommoRateLimited(Exception):
    def __init__(self, retry_after: int):
        self.retry_after = retry_after


class _KommoServerError(Exception):
    def __init__(self, status: int):
        self.status = status


class _KommoFatalError(Exception):
    """Неретраюча помилка — синхронізацію треба зупинити негайно, без повторів."""
    def __init__(self, status: int):
        self.status = status


def _kommo_retry_wait(retry_state) -> float:
    exc = retry_state.outcome.exception()
    if isinstance(exc, _KommoRateLimited):
        return exc.retry_after
    return 2 ** (retry_state.attempt_number - 1)


def _kommo_before_sleep(retry_state):
    exc     = retry_state.outcome.exception()
    attempt = retry_state.attempt_number
    if isinstance(exc, _KommoRateLimited):
        logger.warning(f"Kommo sync: rate limit — чекаємо {exc.retry_after}с (спроба {attempt}/3)")
    else:
        logger.error(f"Kommo sync: server error HTTP {exc.status} (спроба {attempt}/3)")


@retry(
    retry=retry_if_exception_type((_KommoRateLimited, _KommoServerError)),
    wait=_kommo_retry_wait,
    stop=stop_after_attempt(3),
    before_sleep=_kommo_before_sleep,
    reraise=True,
)
async def _fetch_leads_page(session, url, headers, params) -> Optional[dict]:
    """Один запит сторінки лідів. None означає HTTP 204 (природний кінець пагінації)."""
    async with session.get(url, headers=headers, params=params) as resp:
        if resp.status == 204:
            return None
        if resp.status == 429:
            raise _KommoRateLimited(int(resp.headers.get('Retry-After', 5)))
        if resp.status >= 500:
            raise _KommoServerError(resp.status)
        if resp.status != 200:
            raise _KommoFatalError(resp.status)
        return await resp.json()


async def sync_from_kommo() -> tuple[int, int, int]:
    if not AMO_TOKEN:
        return 0, 0, 0

    url      = f"https://{AMO_SUBDOMAIN}.kommo.com/api/v4/leads"
    headers  = {"Authorization": f"Bearer {AMO_TOKEN}"}
    added    = 0
    skipped  = 0
    closed   = 0
    page     = 1
    kommo_ids: set[str] = set()

    session = await _get_session()
    while True:
        params = {
            "filter[statuses][0][pipeline_id]": AMO_PIPELINE_ID,
            "filter[statuses][0][status_id]":   AMO_HOT_STATUS_ID,
            "limit": 250,
            "page":  page,
        }
        try:
            data = await _fetch_leads_page(session, url, headers, params)
        except _KommoFatalError as e:
            logger.error(f"Kommo sync: HTTP {e.status} — зупиняємо синхронізацію")
            break
        except (_KommoRateLimited, _KommoServerError):
            logger.error(f"Kommo sync: сторінка {page} не завантажена після 3 спроб — зупиняємо")
            break

        if data is None:
            break  # HTTP 204 — природний кінець пагінації

        leads = data.get("_embedded", {}).get("leads", [])
        if not leads:
            break

        for lead in leads:
            lead_id = str(lead["id"])
            kommo_ids.add(lead_id)
            if await get_lead(lead_id):
                skipped += 1
                continue
            title   = make_lead_title(AMO_HOT_STATUS_ID, lead_id)
            created = lead.get("created_at") or datetime.now().timestamp()
            try:
                await q("INSERT INTO leads (lead_id, status, created_at, title) VALUES (?,?,?,?)",
                  (lead_id, "queued", created, title))
                added += 1
            except Exception as e:
                logger.error(f"Kommo sync: не вдалось додати {lead_id}: {e}")

        if len(leads) < 250:
            break
        page += 1

    active_rows = await q(
        "SELECT lead_id FROM leads WHERE status NOT IN ('taken','duplicate','closed')",
        fetch='all',
    )
    for row in (active_rows or []):
        if row['lead_id'] not in kommo_ids:
            await q("UPDATE leads SET status='closed' WHERE lead_id=?", (row['lead_id'],))
            await remove_from_others(row['lead_id'], note="📋 Заявку закрито в CRM")
            closed += 1
            logger.info(f"Sync: заявка {row['lead_id']} відсутня в Kommo → закрито")

    return added, skipped, closed
