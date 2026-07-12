import asyncio
import logging
from contextlib import asynccontextmanager
from datetime import datetime

from fastapi import FastAPI, Request
from telegram import BotCommand, MenuButtonCommands, Update
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ConversationHandler, MessageHandler, filters,
)

import state
from config import BOT_TOKEN, MANAGERS, KOMMO_MANAGER_IDS, WEBHOOK_PATH, AMO_PIPELINE_ID, AMO_HOT_STATUS_ID
from db import init_db, q, get_lead, init_default_schedules, migrate_managers_from_config, get_managers_dict
from kommo import make_lead_title
from notifications import notify_admin_error, remove_from_others, schedule_cleanup
from queue_logic import assign_next, scheduler_loop, deactivate_out_of_schedule
from sheets import warmup

from handlers.manager import on_start, on_work, on_work_button, on_callback
from handlers.admin import on_admin_button
from handlers.conversations import (
    LIMIT_SELECT, LIMIT_INPUT, limits_start, limits_select, limits_input, limits_cancel,
    SCHED_SELECT, SCHED_DAYS, SCHED_TIME, SCHED_END_TIME,
    schedules_start, schedules_select, schedules_days, schedules_time, schedules_end_time, schedules_cancel,
    REG_SELECT_SHEET, REG_SELECT_KOMMO,
    reg_start, reg_select_sheet, reg_select_kommo,
)
from handlers.admin_callbacks import on_admin_callback

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    level=logging.INFO,
)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(fastapi: FastAPI):
    init_db()
    migrate_managers_from_config(MANAGERS, KOMMO_MANAGER_IDS)
    state.reload_managers()
    init_default_schedules(get_managers_dict())

    state._app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(HTTPXRequest(
            read_timeout=15,
            write_timeout=15,
            connect_timeout=10,
            pool_timeout=10,
        ))
        .build()
    )
    app = state._app

    # ── Реєстрація менеджерів ────────────────────────────────────────────────
    _reg_entry = CallbackQueryHandler(reg_start, pattern=r'^reg:start$')
    app.add_handler(ConversationHandler(
        entry_points=[_reg_entry],
        states={
            REG_SELECT_SHEET: [CallbackQueryHandler(reg_select_sheet, pattern=r'^reg_sheet:')],
            REG_SELECT_KOMMO: [CallbackQueryHandler(reg_select_kommo, pattern=r'^reg_kommo:')],
        },
        fallbacks=[CommandHandler('start', on_start)],
        per_user=True,
        allow_reentry=True,
    ))

    # ── Адміністративні колбеки (схвалення менеджерів тощо) ─────────────────
    app.add_handler(CallbackQueryHandler(on_admin_callback, pattern=r'^mgr_(approve|reject):'))

    _lim_entry = MessageHandler(filters.TEXT & filters.Regex(r'^⚙️ Ліміти$'), limits_start)
    app.add_handler(ConversationHandler(
        entry_points=[_lim_entry],
        states={
            LIMIT_SELECT: [CallbackQueryHandler(limits_select, pattern=r'^setlim:'), _lim_entry],
            LIMIT_INPUT:  [_lim_entry, MessageHandler(filters.TEXT & ~filters.COMMAND, limits_input)],
        },
        fallbacks=[CommandHandler('cancel', limits_cancel)],
        per_user=True,
        allow_reentry=True,
    ))

    _sched_entry     = MessageHandler(filters.TEXT & filters.Regex(r'^⏰ Розклади$'), schedules_start)
    _sched_cancel_cb = CallbackQueryHandler(schedules_select, pattern=r'^sched:cancel$')
    app.add_handler(ConversationHandler(
        entry_points=[_sched_entry],
        states={
            SCHED_SELECT:   [CallbackQueryHandler(schedules_select, pattern=r'^sched:'), _sched_entry],
            SCHED_DAYS:     [_sched_cancel_cb, _sched_entry, MessageHandler(filters.TEXT & ~filters.COMMAND, schedules_days)],
            SCHED_TIME:     [_sched_cancel_cb, _sched_entry, MessageHandler(filters.TEXT & ~filters.COMMAND, schedules_time)],
            SCHED_END_TIME: [_sched_cancel_cb, _sched_entry, MessageHandler(filters.TEXT & ~filters.COMMAND, schedules_end_time)],
        },
        fallbacks=[CommandHandler('cancel', schedules_cancel)],
        per_user=True,
        allow_reentry=True,
    ))

    app.add_handler(CallbackQueryHandler(
        on_callback,
        pattern=r'^(?!reg:|reg_sheet:|reg_kommo:|mgr_approve:|mgr_reject:)',
    ))
    app.add_handler(CommandHandler('start', on_start))
    app.add_handler(CommandHandler('work', on_work))
    app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r'^(✅ Увійти в чергу|🚫 Вийти з черги)$'),
        on_work_button,
    ))
    app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(
            r'^(👥 Статус менеджерів|📊 Черга|🔌 Підключення|📋 Активні заявки'
            r'|📅 Статистика день|📆 Статистика місяць|🔄 Синхронізація'
            r'|⏰ Розклади|🔍 Діагностика|👤 Менеджери)$'
        ),
        on_admin_button,
    ))

    await app.initialize()
    await app.start()
    await app.bot.set_my_commands([BotCommand('start', '🔄 Головне меню / перезапуск')])
    await app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())

    loop = asyncio.get_event_loop()
    future = loop.run_in_executor(None, warmup)
    future.add_done_callback(
        lambda f: logger.error(f"Sheets warmup failed: {f.exception()}") if f.exception() else None
    )

    def _task_error_handler(task: asyncio.Task):
        if not task.cancelled() and task.exception():
            logger.error(f"Фонова задача '{task.get_name()}' впала: {task.exception()}")

    async def _safe_deactivate():
        try:
            await deactivate_out_of_schedule()
        except Exception as e:
            logger.error(f"deactivate_out_of_schedule: {e}")

    for coro, name in [
        (app.updater.start_polling(allowed_updates=Update.ALL_TYPES), 'polling'),
        (scheduler_loop(), 'scheduler'),
        (_safe_deactivate(), 'deactivate_on_start'),
    ]:
        t = asyncio.create_task(coro, name=name)
        t.add_done_callback(_task_error_handler)

    logger.info("Бот запущено")
    yield
    await app.updater.stop()
    await app.stop()
    await app.shutdown()
    logger.info("Бот зупинено")


fastapi_app = FastAPI(lifespan=lifespan)


@fastapi_app.post(f'/webhook/{WEBHOOK_PATH}')
async def amocrm_webhook(request: Request):
    try:
        data = await request.form()
    except Exception:
        return {'ok': True}

    lead_id     = (data.get('leads[status][0][id]')
                   or data.get('leads[add][0][id]')
                   or data.get('leads[delete][0][id]'))
    status_id   = (data.get('leads[status][0][status_id]')
                   or data.get('leads[add][0][status_id]'))
    pipeline_id = (data.get('leads[status][0][pipeline_id]')
                   or data.get('leads[add][0][pipeline_id]'))
    is_delete   = bool(data.get('leads[delete][0][id]'))

    logger.info(
        f"Webhook: lead_id={lead_id} status_id={status_id} "
        f"pipeline_id={pipeline_id} delete={is_delete} keys={list(data.keys())[:6]}"
    )

    if not lead_id:
        return {'ok': True}

    # Базова валідація: lead_id має бути числовим рядком
    if not str(lead_id).strip().isdigit():
        logger.warning(f"Webhook: невалідний lead_id={lead_id!r} — ігноруємо")
        return {'ok': True}
    lead_id = str(lead_id).strip()

    if is_delete:
        lead = get_lead(lead_id)
        if lead and lead['status'] not in ('taken', 'duplicate', 'closed'):
            q("UPDATE leads SET status='closed' WHERE lead_id=?", (lead_id,))
            await remove_from_others(lead_id, note="🗑 Заявку видалено в CRM")
            schedule_cleanup(lead_id)
            logger.info(f"Webhook: заявка {lead_id} видалена в CRM → закрито в боті")
        return {'ok': True}

    if str(pipeline_id) != AMO_PIPELINE_ID:
        lead = get_lead(lead_id)
        if lead and lead['status'] not in ('taken', 'duplicate', 'closed'):
            q("UPDATE leads SET status='closed' WHERE lead_id=?", (lead_id,))
            await remove_from_others(lead_id, note="📋 Заявку переміщено в іншу воронку CRM")
            schedule_cleanup(lead_id)
            logger.info(f"Webhook: заявка {lead_id} пішла в іншу воронку → закрито в боті")
        else:
            logger.info(f"Webhook: ігноруємо pipeline_id={pipeline_id} (не наша воронка)")
        return {'ok': True}

    if str(status_id) != AMO_HOT_STATUS_ID:
        lead = get_lead(lead_id)
        if lead and lead['status'] not in ('taken', 'duplicate', 'closed'):
            q("UPDATE leads SET status='closed' WHERE lead_id=?", (lead_id,))
            await remove_from_others(lead_id, note="📋 Заявку переміщено на інший етап в CRM")
            schedule_cleanup(lead_id)
            logger.info(f"Webhook: заявка {lead_id} змінила статус → закрито в боті")
        return {'ok': True}

    if get_lead(lead_id):
        return {'ok': True}

    title = make_lead_title(status_id, lead_id)

    # Retry INSERT up to 3 times — a transient DB lock must not silently drop a lead
    last_err: Exception = None
    for attempt in range(3):
        try:
            q("INSERT INTO leads (lead_id, status, created_at, title) VALUES (?,?,?,?)",
              (lead_id, 'queued', datetime.now().timestamp(), title))
            last_err = None
            break
        except Exception as e:
            last_err = e
            logger.warning(f"Webhook: INSERT заявки {lead_id} спроба {attempt + 1}/3: {e}")
            if attempt < 2:
                await asyncio.sleep(0.5 * (2 ** attempt))

    if last_err:
        logger.error(f"Webhook: не вдалось записати заявку {lead_id} після 3 спроб: {last_err}")
        await notify_admin_error(f"webhook (запис заявки #{lead_id} в БД, 3 спроби)", last_err)
        return {'ok': False}

    asyncio.create_task(assign_next(lead_id))
    return {'ok': True}


if __name__ == '__main__':
    import uvicorn
    uvicorn.run('main:fastapi_app', host='0.0.0.0', port=8080, reload=False)
