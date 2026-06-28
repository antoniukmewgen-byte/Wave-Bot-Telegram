import logging
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, ConversationHandler

import state
from config import ADMIN_IDS, MANAGERS
from db import get_all_max_leads_overrides, get_all_schedules, set_max_leads_override, set_schedule
from sheets import fetch_managers

logger = logging.getLogger(__name__)

LIMIT_SELECT, LIMIT_INPUT = range(2)

SCHED_SELECT, SCHED_DAYS, SCHED_TIME, SCHED_END_TIME = range(4)

DAYS_UA = {0: 'Пн', 1: 'Вт', 2: 'Ср', 3: 'Чт', 4: 'Пт', 5: 'Сб', 6: 'Нд'}


def _format_schedule(sch: dict) -> str:
    days   = [DAYS_UA[int(d)] for d in sch['days'].split(',') if d.strip()]
    status = '✅' if sch.get('enabled', 1) else '❌'
    end    = sch.get('end_time', '23:00')
    return f"{status} {', '.join(days)} {sch['start_time']}–{end}"


# ─── CONVERSATION: ЛІМІТИ ────────────────────────────────────────────────────

async def limits_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        return ConversationHandler.END

    managers  = fetch_managers()
    overrides = get_all_max_leads_overrides()

    buttons = []
    for tg_id, info in managers.items():
        name     = info['name']
        override = overrides.get(tg_id)
        if override is not None:
            lim_str = f"{override} ✏️"
        elif info['max_leads'] is None:
            lim_str = "∞"
        else:
            lim_str = str(info['max_leads'])
        buttons.append([InlineKeyboardButton(f"{name} — {lim_str}", callback_data=f"setlim:{tg_id}")])

    buttons.append([InlineKeyboardButton("❌ Скасувати", callback_data="setlim:cancel")])
    await update.message.reply_text(
        "⚙️ <b>Ліміти менеджерів</b>\n"
        "Виберіть менеджера для зміни ліміту:\n\n"
        "<i>✏️ = ручний ліміт | ∞ = без ліміту (з таблиці)</i>",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return LIMIT_SELECT


async def limits_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()

    if query.data == "setlim:cancel":
        await query.edit_message_text("❌ Скасовано")
        return ConversationHandler.END

    tg_id    = query.data.split(':', 1)[1]
    managers = fetch_managers()
    name     = managers.get(tg_id, {}).get('name', tg_id)

    context.user_data['limit_tg_id'] = tg_id
    context.user_data['limit_name']  = name

    await query.edit_message_text(
        f"⚙️ Менеджер: <b>{name}</b>\n\n"
        f"Введіть новий ліміт лідів на день:\n"
        f"• число (напр. <code>5</code>) — встановити ліміт\n"
        f"• <code>0</code> — без ліміту (∞, брати з таблиці)\n"
        f"• /cancel — скасувати",
        parse_mode='HTML',
    )
    return LIMIT_INPUT


async def limits_input(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        return ConversationHandler.END

    text = update.message.text.strip()
    if not text.isdigit():
        await update.message.reply_text("⚠️ Введіть ціле число або 0 для скидання ліміту")
        return LIMIT_INPUT

    value     = int(text)
    tg_id     = context.user_data.get('limit_tg_id')
    name      = context.user_data.get('limit_name', tg_id)
    max_leads = None if value == 0 else value

    set_max_leads_override(tg_id, max_leads)

    from handlers.admin import ADMIN_KB
    lim_str = "∞ (без ліміту, з таблиці)" if max_leads is None else str(max_leads)
    await update.message.reply_text(
        f"✅ Ліміт для <b>{name}</b> встановлено: <b>{lim_str}</b>",
        parse_mode='HTML',
        reply_markup=ADMIN_KB,
    )
    return ConversationHandler.END


async def limits_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from handlers.admin import ADMIN_KB
    await update.message.reply_text("❌ Скасовано", reply_markup=ADMIN_KB)
    return ConversationHandler.END


# ─── CONVERSATION: РОЗКЛАДИ ───────────────────────────────────────────────────

async def schedules_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        return ConversationHandler.END

    schedules = get_all_schedules()
    buttons   = []
    for name, tg_id in sorted(MANAGERS.items(), key=lambda x: x[0]):
        sch = schedules.get(tg_id)
        sch_text = _format_schedule(sch) if sch else '—'
        buttons.append([InlineKeyboardButton(
            f"{name} | {sch_text}", callback_data=f"sched:{tg_id}"
        )])

    buttons.append([InlineKeyboardButton("❌ Скасувати", callback_data="sched:cancel")])
    await update.message.reply_text(
        "⏰ <b>Розклади менеджерів</b>\nОберіть менеджера для редагування:",
        reply_markup=InlineKeyboardMarkup(buttons),
        parse_mode='HTML',
    )
    return SCHED_SELECT


async def schedules_select(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    tg_id = query.data.split(':', 1)[1]

    if tg_id == 'cancel':
        await query.edit_message_text("❌ Скасовано")
        return ConversationHandler.END

    context.user_data['sched_manager_id'] = tg_id

    name      = state.MANAGERS_BY_ID.get(tg_id, tg_id)
    schedules = get_all_schedules()
    sch       = schedules.get(tg_id)
    current   = _format_schedule(sch) if sch else 'не задано'

    cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Скасувати", callback_data="sched:cancel")]])
    await query.edit_message_text(
        f"⏰ <b>{name}</b>\nПоточний розклад: {current}\n\n"
        "Введіть робочі дні через кому:\n"
        "<code>0</code>=Пн <code>1</code>=Вт <code>2</code>=Ср <code>3</code>=Чт "
        "<code>4</code>=Пт <code>5</code>=Сб <code>6</code>=Нд\n\n"
        "Приклад: <code>0,1,2,3,4</code> (пн-пт)",
        parse_mode='HTML',
        reply_markup=cancel_kb,
    )
    return SCHED_DAYS


async def schedules_days(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        return ConversationHandler.END

    text = update.message.text.strip()
    try:
        days = [int(d.strip()) for d in text.split(',')]
        assert all(0 <= d <= 6 for d in days) and days
    except Exception:
        await update.message.reply_text(
            "❌ Невірний формат. Введіть цифри від 0 до 6 через кому.\nПриклад: <code>0,1,2,3,4</code>",
            parse_mode='HTML',
        )
        return SCHED_DAYS

    context.user_data['sched_days'] = ','.join(str(d) for d in sorted(set(days)))
    cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Скасувати", callback_data="sched:cancel")]])
    await update.message.reply_text(
        "Введіть час <b>початку</b> зміни у форматі <code>ГГ:ХХ</code>\nПриклад: <code>16:00</code>",
        parse_mode='HTML',
        reply_markup=cancel_kb,
    )
    return SCHED_TIME


async def schedules_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        return ConversationHandler.END

    text = update.message.text.strip()
    try:
        datetime.strptime(text, '%H:%M')
    except ValueError:
        await update.message.reply_text(
            "❌ Невірний формат. Введіть у форматі <code>ГГ:ХХ</code>\nПриклад: <code>16:00</code>",
            parse_mode='HTML',
        )
        return SCHED_TIME

    context.user_data['sched_start'] = text
    cancel_kb = InlineKeyboardMarkup([[InlineKeyboardButton("❌ Скасувати", callback_data="sched:cancel")]])
    await update.message.reply_text(
        "Введіть час <b>кінця</b> зміни у форматі <code>ГГ:ХХ</code>\n"
        "Приклад: <code>23:00</code>\n"
        "<i>Якщо зміна переходить через північ (напр. 22:00–05:00) — просто введіть 05:00</i>",
        parse_mode='HTML',
        reply_markup=cancel_kb,
    )
    return SCHED_END_TIME


async def schedules_end_time(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        return ConversationHandler.END

    text = update.message.text.strip()
    try:
        datetime.strptime(text, '%H:%M')
    except ValueError:
        await update.message.reply_text(
            "❌ Невірний формат. Введіть у форматі <code>ГГ:ХХ</code>\nПриклад: <code>23:00</code>",
            parse_mode='HTML',
        )
        return SCHED_END_TIME

    tg_id      = context.user_data['sched_manager_id']
    days       = context.user_data['sched_days']
    start_time = context.user_data['sched_start']
    end_time   = text
    set_schedule(tg_id, days, start_time, end_time)

    from handlers.admin import ADMIN_KB
    name     = state.MANAGERS_BY_ID.get(tg_id, tg_id)
    days_str = ', '.join(DAYS_UA[int(d)] for d in days.split(','))
    crosses  = end_time <= start_time
    note     = " (перехід через північ)" if crosses else ""
    await update.message.reply_text(
        f"✅ Розклад збережено!\n<b>{name}</b>: {days_str} {start_time}–{end_time}{note}",
        reply_markup=ADMIN_KB,
        parse_mode='HTML',
    )
    logger.info(f"Schedule: {name} ({tg_id}) → {days} {start_time}–{end_time}")
    return ConversationHandler.END


async def schedules_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    from handlers.admin import ADMIN_KB
    await update.message.reply_text("❌ Скасовано", reply_markup=ADMIN_KB)
    return ConversationHandler.END
