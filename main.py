import asyncio
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI
from telegram import BotCommand, BotCommandScopeAllGroupChats, MenuButtonCommands, Update
from telegram.request import HTTPXRequest
from telegram.ext import (
    Application, CallbackQueryHandler, CommandHandler,
    ConversationHandler, MessageHandler, PicklePersistence, filters,
)

import state
from config import BOT_TOKEN
from db import init_db, init_default_schedules, get_managers_dict, close_db
from kommo import close_session as close_kommo_session
from queue_logic import build_scheduler, deactivate_out_of_schedule
from sheets import warmup
from webhook import router as webhook_router

from handlers.manager import on_start, on_work, on_work_button, on_callback
from handlers.admin import on_admin_button, on_statuschat_toggle
from handlers.conversations import (
    LIMIT_SELECT, LIMIT_INPUT, limits_start, limits_select, limits_input, limits_cancel,
    SCHED_SELECT, SCHED_DAYS, SCHED_TIME, SCHED_END_TIME,
    schedules_start, schedules_select, schedules_days, schedules_time, schedules_end_time, schedules_cancel,
    REG_SELECT_SHEET, REG_SELECT_KOMMO,
    reg_start, reg_select_sheet, reg_select_kommo,
    FORCE_SELECT, FORCE_INPUT,
    force_start, force_select, force_input, force_cancel,
)
from handlers.admin_callbacks import on_admin_callback

logging.basicConfig(
    format='%(asctime)s %(levelname)s %(name)s: %(message)s',
    level=logging.INFO,
)
logging.getLogger('httpx').setLevel(logging.WARNING)
logger = logging.getLogger(__name__)


@asynccontextmanager
async def lifespan(fastapi: FastAPI):
    await init_db()
    await state.reload_managers()
    await init_default_schedules(await get_managers_dict())

    state._app = (
        Application.builder()
        .token(BOT_TOKEN)
        .request(HTTPXRequest(
            read_timeout=15,
            write_timeout=15,
            connect_timeout=10,
            pool_timeout=10,
        ))
        .persistence(PicklePersistence(filepath='bot_persistence.pkl'))
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
        persistent=True,
        name='registration_conversation',
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
        persistent=True,
        name='limits_conversation',
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
        persistent=True,
        name='schedules_conversation',
    ))

    _force_entry = MessageHandler(filters.TEXT & filters.Regex(r'^🔓 Заблоковані$'), force_start)
    app.add_handler(ConversationHandler(
        entry_points=[_force_entry],
        states={
            FORCE_SELECT: [CallbackQueryHandler(force_select, pattern=r'^forceq:'), _force_entry],
            FORCE_INPUT:  [_force_entry, MessageHandler(filters.TEXT & ~filters.COMMAND, force_input)],
        },
        fallbacks=[CommandHandler('cancel', force_cancel)],
        per_user=True,
        allow_reentry=True,
        persistent=True,
        name='force_conversation',
    ))

    app.add_handler(CallbackQueryHandler(
        on_callback,
        pattern=r'^(?!reg:|reg_sheet:|reg_kommo:|mgr_approve:|mgr_reject:)',
    ))
    app.add_handler(CommandHandler('start', on_start))
    app.add_handler(CommandHandler('work', on_work))
    app.add_handler(CommandHandler(['statuson', 'statusoff'], on_statuschat_toggle))
    app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(r'^(✅ Увійти в чергу|🚫 Вийти з черги)$'),
        on_work_button,
    ))
    app.add_handler(MessageHandler(
        filters.TEXT & filters.Regex(
            r'^(👥 Статус менеджерів|📊 Черга|🔌 Підключення|📋 Активні заявки'
            r'|📅 Статистика день|📆 Статистика місяць|🔄 Синхронізація'
            r'|⏰ Розклади|🔍 Діагностика|👤 Менеджери|🧹 Прибрати привиди'
            r"|📡 Перевірити на зв'язку|🌙 Звірити ранкові ліди)$"
        ),
        on_admin_button,
    ))

    await app.initialize()
    await app.start()
    await app.bot.set_my_commands([BotCommand('start', '🔄 Головне меню / перезапуск')])
    await app.bot.set_my_commands(
        [
            BotCommand('statuson', '✅ Увімкнути розсилку статусу менеджерів (17:00-22:00)'),
            BotCommand('statusoff', '🔕 Вимкнути розсилку статусу менеджерів'),
        ],
        scope=BotCommandScopeAllGroupChats(),
    )
    await app.bot.set_chat_menu_button(menu_button=MenuButtonCommands())

    def _task_error_handler(task: asyncio.Task):
        if not task.cancelled() and task.exception():
            logger.error(f"Фонова задача '{task.get_name()}' впала: {task.exception()}")

    async def _safe_deactivate():
        try:
            await deactivate_out_of_schedule()
        except Exception as e:
            logger.error(f"deactivate_out_of_schedule: {e}")

    async def _safe_warmup():
        try:
            await warmup()
        except Exception as e:
            logger.error(f"Sheets warmup failed: {e}")

    for coro, name in [
        (app.updater.start_polling(allowed_updates=Update.ALL_TYPES), 'polling'),
        (_safe_deactivate(), 'deactivate_on_start'),
        (_safe_warmup(), 'sheets_warmup'),
    ]:
        t = asyncio.create_task(coro, name=name)
        t.add_done_callback(_task_error_handler)

    scheduler = build_scheduler()
    scheduler.start()

    logger.info("Бот запущено")
    yield
    scheduler.shutdown(wait=False)
    await app.updater.stop()
    await app.stop()
    await app.shutdown()
    await close_kommo_session()
    await close_db()
    logger.info("Бот зупинено")


fastapi_app = FastAPI(lifespan=lifespan)
fastapi_app.include_router(webhook_router)


if __name__ == '__main__':
    import uvicorn
    uvicorn.run('main:fastapi_app', host='0.0.0.0', port=8080, reload=False)
