import asyncio
import logging
from datetime import datetime

import aiohttp

from config import AMO_SUBDOMAIN, AMO_TOKEN, AMO_PIPELINE_ID, AMO_HOT_STATUS_ID, HOT_STATUSES, KOMMO_MANAGER_IDS
from db import q, get_lead
from notifications import remove_from_others

logger = logging.getLogger(__name__)


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


async def set_kommo_responsible(lead_id: str, manager_id: str) -> bool:
    """Встановлює відповідального менеджера в Kommo. Повертає True якщо успішно."""
    kommo_user_id = KOMMO_MANAGER_IDS.get(manager_id)
    if not kommo_user_id or not AMO_TOKEN:
        return False
    url     = f"https://{AMO_SUBDOMAIN}.kommo.com/api/v4/leads"
    payload = [{"id": int(lead_id), "responsible_user_id": kommo_user_id}]
    headers = {"Authorization": f"Bearer {AMO_TOKEN}", "Content-Type": "application/json"}
    try:
        async with aiohttp.ClientSession() as session:
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

    async with aiohttp.ClientSession() as session:
        while True:
            params = {
                "filter[statuses][0][pipeline_id]": AMO_PIPELINE_ID,
                "filter[statuses][0][status_id]":   AMO_HOT_STATUS_ID,
                "limit": 250,
                "page":  page,
            }
            retry_count = 0
            while retry_count < 3:
                async with session.get(url, headers=headers, params=params) as resp:
                    if resp.status == 204:
                        page = -1  # сигнал виходу з зовнішнього циклу
                        break
                    if resp.status == 429:
                        retry_after = int(resp.headers.get('Retry-After', 5))
                        logger.warning(f"Kommo sync: rate limit — чекаємо {retry_after}с (спроба {retry_count + 1}/3)")
                        await asyncio.sleep(retry_after)
                        retry_count += 1
                        continue
                    if resp.status >= 500:
                        logger.error(f"Kommo sync: server error HTTP {resp.status} (спроба {retry_count + 1}/3)")
                        await asyncio.sleep(2 ** retry_count)
                        retry_count += 1
                        continue
                    if resp.status != 200:
                        logger.error(f"Kommo sync: HTTP {resp.status} — зупиняємо синхронізацію")
                        page = -1
                        break
                    data  = await resp.json()
                    leads = data.get("_embedded", {}).get("leads", [])
                    if not leads:
                        page = -1
                        break

                    for lead in leads:
                        lead_id = str(lead["id"])
                        kommo_ids.add(lead_id)
                        if get_lead(lead_id):
                            skipped += 1
                            continue
                        title   = make_lead_title(AMO_HOT_STATUS_ID, lead_id)
                        created = lead.get("created_at") or datetime.now().timestamp()
                        try:
                            q("INSERT INTO leads (lead_id, status, created_at, title) VALUES (?,?,?,?)",
                              (lead_id, "queued", created, title))
                            added += 1
                        except Exception as e:
                            logger.error(f"Kommo sync: не вдалось додати {lead_id}: {e}")

                    if len(leads) < 250:
                        page = -1
                    else:
                        page += 1
                    break  # успішний запит — виходимо з retry циклу
            else:
                logger.error(f"Kommo sync: сторінка {page} не завантажена після 3 спроб — зупиняємо")
                break

            if page == -1:
                break

    active_rows = q(
        "SELECT lead_id FROM leads WHERE status NOT IN ('taken','duplicate','closed')",
        fetch='all',
    )
    for row in (active_rows or []):
        if row['lead_id'] not in kommo_ids:
            q("UPDATE leads SET status='closed' WHERE lead_id=?", (row['lead_id'],))
            await remove_from_others(row['lead_id'], note="📋 Заявку закрито в CRM")
            closed += 1
            logger.info(f"Sync: заявка {row['lead_id']} відсутня в Kommo → закрито")

    return added, skipped, closed
