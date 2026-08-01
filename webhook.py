import asyncio
import hmac
import logging
from datetime import datetime

from fastapi import APIRouter, HTTPException, Request
from tenacity import retry, stop_after_attempt, wait_exponential

from config import (
    WEBHOOK_PATH, WEBHOOK_SECRET,
    AMO_PIPELINE_ID, AMO_HOT_STATUS_ID,
    AMO_DISTRIBUTED_STATUS_ID, AMO_DISTRIBUTED_PIPELINE_ID,
)
from db import q, get_lead, get_distributed_lead
from kommo import make_lead_title, get_lead_info
from notifications import notify_admin_error, remove_from_others, schedule_cleanup
from queue_logic import assign_next, on_lead_distributed, on_lead_undistributed

logger = logging.getLogger(__name__)
router = APIRouter()

if not WEBHOOK_SECRET:
    logger.critical(
        f"WEBHOOK_SECRET не задано! Ендпоінт /webhook/{WEBHOOK_PATH} приймає запити "
        f"БЕЗ автентифікації — будь-хто, хто дізнається URL, може підробляти події Kommo. "
        f"Додайте WEBHOOK_SECRET в .env і допишіть ?secret=<значення> до URL вебхука "
        f"в налаштуваннях Kommo, щоб закрити цю діру."
    )


def _webhook_authorized(request: Request) -> bool:
    """Порівнює secret з query-параметра з WEBHOOK_SECRET (constant-time).

    Якщо WEBHOOK_SECRET не налаштовано — пропускає все (деградований режим
    для сумісності зі старими деплоями; дивись critical-попередження вище).
    """
    if not WEBHOOK_SECRET:
        return True
    provided = request.query_params.get('secret', '')
    return hmac.compare_digest(provided, WEBHOOK_SECRET)


def _insert_lead_after_attempt(retry_state):
    exc = retry_state.outcome.exception()
    if exc is not None:
        lead_id = retry_state.args[0]
        logger.warning(f"Webhook: INSERT заявки {lead_id} спроба {retry_state.attempt_number}/3: {exc}")


@retry(
    stop=stop_after_attempt(3),
    wait=wait_exponential(multiplier=0.5, min=0.5, max=2),
    after=_insert_lead_after_attempt,
    reraise=True,
)
async def _insert_lead_with_retry(lead_id: str, created_ts: float, title: str):
    await q("INSERT INTO leads (lead_id, status, created_at, title) VALUES (?,?,?,?)",
      (lead_id, 'queued', created_ts, title))


def _extract_lead_events(data) -> list[dict]:
    """
    Парсить форму вебхука Kommo. Kommo може пакувати кілька змін лідів
    в один HTTP-запит (leads[status][0], leads[status][1], ...) — тому
    треба зчитувати ВСІ індекси, а не тільки [0].
    """
    events = []
    for category in ('status', 'add', 'delete', 'responsible'):
        idx = 0
        while f'leads[{category}][{idx}][id]' in data:
            events.append({
                'lead_id':     data.get(f'leads[{category}][{idx}][id]'),
                'status_id':   data.get(f'leads[{category}][{idx}][status_id]'),
                'pipeline_id': data.get(f'leads[{category}][{idx}][pipeline_id]'),
                'is_delete':   category == 'delete',
                'category':    category,
            })
            idx += 1
    return events


@router.post(f'/webhook/{WEBHOOK_PATH}')
async def amocrm_webhook(request: Request):
    if not _webhook_authorized(request):
        client_host = request.client.host if request.client else '?'
        logger.warning(f"Webhook: відхилено запит з невірним/відсутнім secret від {client_host}")
        raise HTTPException(status_code=403, detail="forbidden")

    try:
        data = await request.form()
    except Exception:
        return {'ok': True}

    events = _extract_lead_events(data)
    logger.info(f"Webhook: {len(events)} подій, keys={list(data.keys())}")

    for event in events:
        await _handle_lead_event(event)

    return {'ok': True}


async def _handle_lead_event(event: dict):
    lead_id     = event['lead_id']
    status_id   = event['status_id']
    pipeline_id = event['pipeline_id']
    is_delete   = event['is_delete']

    logger.info(
        f"Webhook: lead_id={lead_id} status_id={status_id} "
        f"pipeline_id={pipeline_id} delete={is_delete}"
    )

    if not lead_id:
        return

    # Базова валідація: lead_id має бути числовим рядком
    if not str(lead_id).strip().isdigit():
        logger.warning(f"Webhook: невалідний lead_id={lead_id!r} — ігноруємо")
        return
    lead_id = str(lead_id).strip()

    # ── Змінили тільки відповідального (окрема категорія вебхука Kommo) ────
    # Не чіпаємо загальну логіку створення/закриття лідів — цікавить нас лише
    # випадок, коли лід лишається в 'Распределены', а власника переставили.
    #
    # ВАЖЛИВО: не довіряємо pipeline_id/status_id з тіла вебхука для цієї
    # категорії — Kommo для 'responsible' може взагалі не присилати ці поля,
    # через що перевірка мовчки провалювалась і distributed_leads лишався
    # "привидом" назавжди. Тому тут завжди перепитуємо актуальний стан ліда
    # напряму через API.
    if event.get('category') == 'responsible':
        # Швидкий фільтр ще ДО звернення до API: якщо Kommo таки прислала
        # pipeline_id у тілі вебхука і це точно не наша воронка (ні "гаряча",
        # ні "Распределены") — заявка нас не стосується, виходимо одразу.
        # Без цього фільтра масова зміна відповідального на чужих заявках
        # (в іншій воронці, стороння дія в CRM) змушувала бота робити зайвий
        # API-запит на КОЖНУ таку заявку без жодного тротлінгу — саме це
        # спричинило шторм із ~1700 запитів і rate-limit/бан з боку Kommo.
        # Якщо pipeline_id відсутній (Kommo для цієї категорії його іноді не
        # присилає) — лишаємо стару поведінку і перевіряємо напряму через API.
        if pipeline_id and str(pipeline_id) not in (AMO_PIPELINE_ID, AMO_DISTRIBUTED_PIPELINE_ID):
            return

        info = await get_lead_info(lead_id)
        actual_pipeline = str(info['pipeline_id']) if info else str(pipeline_id)
        actual_status   = str(info['status_id'])   if info else str(status_id)

        if (actual_pipeline == AMO_DISTRIBUTED_PIPELINE_ID
                and actual_status == AMO_DISTRIBUTED_STATUS_ID):
            logger.info(f"Webhook: заявка {lead_id} — змінили відповідального (лишається в 'Распределены')")
            asyncio.create_task(on_lead_distributed(lead_id))
            return

        # Лід більше не в 'Распределены' (або вже перейшов далі) — якщо у нас
        # все ще висить стара прив'язка в distributed_leads, звільняємо її,
        # інакше менеджер назавжди лишиться поза чергою через "привида".
        distributed_row = await get_distributed_lead(lead_id)
        if distributed_row:
            logger.info(
                f"Webhook: заявка {lead_id} — змінили відповідального, лід вже не в 'Распределены' "
                f"(звільняємо {distributed_row['manager_id']})"
            )
            asyncio.create_task(on_lead_undistributed(lead_id, distributed_row['manager_id']))
        return

    if is_delete:
        lead = await get_lead(lead_id)
        if lead and lead['status'] not in ('taken', 'duplicate', 'closed'):
            await q("UPDATE leads SET status='closed' WHERE lead_id=?", (lead_id,))
            await remove_from_others(lead_id, note="🗑 Заявку видалено в CRM")
            schedule_cleanup(lead_id)
            logger.info(f"Webhook: заявка {lead_id} видалена в CRM → закрито в боті")

        # Заявку могли видалити прямо зі статусу "Распределены" — цю гілку
        # webhook'а вище (is_delete) не перетинається з логікою нижче, тому
        # без цієї перевірки запис лишався б "привидом" у distributed_leads.
        distributed_row = await get_distributed_lead(lead_id)
        if distributed_row:
            logger.info(
                f"Webhook: заявка {lead_id} видалена в CRM з 'Распределены' "
                f"(менеджер {distributed_row['manager_id']})"
            )
            asyncio.create_task(on_lead_undistributed(lead_id, distributed_row['manager_id']))
        return

    # ── Заявка перейшла в статус "Распределены" ─────────────────────────────
    if (str(pipeline_id) == AMO_DISTRIBUTED_PIPELINE_ID
            and str(status_id) == AMO_DISTRIBUTED_STATUS_ID):
        logger.info(f"Webhook: заявка {lead_id} → 'Распределены'")
        asyncio.create_task(on_lead_distributed(lead_id))
        return

    # ── Заявка покинула статус "Распределены" ───────────────────────────────
    distributed_row = await get_distributed_lead(lead_id)
    if distributed_row:
        should_release = True

        if str(pipeline_id) == AMO_PIPELINE_ID:
            # Лід досі в нашій гарячій воронці — сюди потрапляє як "відлуння"
            # від PATCH, яким бот сам щойно виставив відповідального (менеджер
            # натиснув "Беру в роботу"), так і справжня ситуація, коли
            # відповідального руками змінили в CRM. pipeline/status в обох
            # випадках однакові, тому розрізняємо за ФАКТИЧНИМ відповідальним:
            # якщо в Kommo відповідальний — той самий менеджер, що вже
            # трекається як "розподілений", це наше власне відлуння і нічого
            # робити не треба; якщо відповідальний інший — заявку справді
            # передали комусь ще, повертаємо старого менеджера в чергу.
            from kommo import get_lead_responsible
            from db import get_manager

            current_kommo_id = await get_lead_responsible(lead_id)
            tracked_mgr      = await get_manager(distributed_row['manager_id'])
            tracked_kommo_id = tracked_mgr['kommo_id'] if tracked_mgr else None
            should_release   = current_kommo_id != tracked_kommo_id

        if should_release:
            logger.info(
                f"Webhook: заявка {lead_id} покинула 'Распределены' "
                f"(менеджер {distributed_row['manager_id']})"
            )
            asyncio.create_task(on_lead_undistributed(lead_id, distributed_row['manager_id']))
            # Не повертаємось — продовжуємо стандартну обробку (закриття в боті)

    if str(pipeline_id) != AMO_PIPELINE_ID:
        lead = await get_lead(lead_id)
        if lead and lead['status'] not in ('taken', 'duplicate', 'closed'):
            await q("UPDATE leads SET status='closed' WHERE lead_id=?", (lead_id,))
            await remove_from_others(lead_id, note="📋 Заявку переміщено в іншу воронку CRM")
            schedule_cleanup(lead_id)
            logger.info(f"Webhook: заявка {lead_id} пішла в іншу воронку → закрито в боті")
        else:
            logger.info(f"Webhook: ігноруємо pipeline_id={pipeline_id} (не наша воронка)")
        return

    if str(status_id) != AMO_HOT_STATUS_ID:
        lead = await get_lead(lead_id)
        if lead and lead['status'] not in ('taken', 'duplicate', 'closed'):
            await q("UPDATE leads SET status='closed' WHERE lead_id=?", (lead_id,))
            await remove_from_others(lead_id, note="📋 Заявку переміщено на інший етап в CRM")
            schedule_cleanup(lead_id)
            logger.info(f"Webhook: заявка {lead_id} змінила статус → закрито в боті")
        return

    if await get_lead(lead_id):
        return

    title = make_lead_title(status_id, lead_id)

    # Retry INSERT up to 3 times — a transient DB lock must not silently drop a lead
    try:
        await _insert_lead_with_retry(lead_id, datetime.now().timestamp(), title)
    except Exception as e:
        logger.error(f"Webhook: не вдалось записати заявку {lead_id} після 3 спроб: {e}")
        await notify_admin_error(f"webhook (запис заявки #{lead_id} в БД, 3 спроби)", e)
        return

    asyncio.create_task(assign_next(lead_id))
