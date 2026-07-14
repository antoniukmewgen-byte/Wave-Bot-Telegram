import logging
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes, ConversationHandler

import state
from config import ADMIN_IDS
from db import (
    get_all_max_leads_overrides, get_all_schedules, set_max_leads_override, set_schedule,
    get_managers_dict, upsert_manager, get_manager, get_all_managers,
)
from sheets import fetch_managers_async

logger = logging.getLogger(__name__)

LIMIT_SELECT, LIMIT_INPUT = range(2)

SCHED_SELECT, SCHED_DAYS, SCHED_TIME, SCHED_END_TIME = range(4)

REG_SELECT_SHEET, REG_SELECT_KOMMO = range(2)

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
        await update.message.reply_text("❌ У вас немає доступу до цієї функції.")
        return ConversationHandler.END

    managers  = await fetch_managers_async()
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
    managers = await fetch_managers_async()
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
        await update.message.reply_text("❌ У вас немає доступу до цієї функції.")
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
    context.user_data.clear()

    from handlers.admin import ADMIN_KB
    lim_str = "∞ (без ліміту, з таблиці)" if max_leads is None else str(max_leads)
    await update.message.reply_text(
        f"✅ Ліміт для <b>{name}</b> встановлено: <b>{lim_str}</b>",
        parse_mode='HTML',
        reply_markup=ADMIN_KB,
    )
    return ConversationHandler.END


async def limits_cancel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    context.user_data.clear()
    from handlers.admin import ADMIN_KB
    await update.message.reply_text("❌ Скасовано", reply_markup=ADMIN_KB)
    return ConversationHandler.END


# ─── CONVERSATION: РОЗКЛАДИ ───────────────────────────────────────────────────

async def schedules_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    if user_id not in ADMIN_IDS:
        await update.message.reply_text("❌ У вас немає доступу до цієї функції.")
        return ConversationHandler.END

    schedules = get_all_schedules()
    buttons   = []
    for name, tg_id in sorted(get_managers_dict().items(), key=lambda x: x[0]):
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
        await update.message.reply_text("❌ У вас немає доступу до цієї функції.")
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
        await update.message.reply_text("❌ У вас немає доступу до цієї функції.")
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
        await update.message.reply_text("❌ У вас немає доступу до цієї функції.")
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
    context.user_data.clear()

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
    context.user_data.clear()
    from handlers.admin import ADMIN_KB
    await update.message.reply_text("❌ Скасовано", reply_markup=ADMIN_KB)
    return ConversationHandler.END


# ─── CONVERSATION: РЕЄСТРАЦІЯ МЕНЕДЖЕРА ──────────────────────────────────────

async def reg_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Крок 1 — вибір імені зі Sheets."""
    query = update.callback_query
    await query.answer()

    tg_id = str(query.from_user.id)

    # Якщо вже в системі
    existing = get_manager(tg_id)
    if existing:
        if existing['is_approved']:
            await query.edit_message_text("✅ Ви вже зареєстровані в системі.")
        else:
            await query.edit_message_text(
                "⏳ Ваша заявка вже відправлена та очікує схвалення адміністратором."
            )
        return ConversationHandler.END

    # Список імен з Google Sheets (тільки ті, кого немає в БД)
    try:
        sheet_data = await fetch_managers_async()
        registered_sheet_names = {
            r['sheet_name'] for r in get_all_managers(approved_only=False)
            if r['sheet_name']
        }
        available_names = [
            info['name'] for tg, info in sheet_data.items()
            if info['name'] not in registered_sheet_names
        ]
    except Exception as e:
        logger.error(f"reg_start: не вдалось отримати список з Sheets: {e}")
        await query.edit_message_text("❌ Помилка завантаження списку. Спробуйте пізніше.")
        return ConversationHandler.END

    if not available_names:
        await query.edit_message_text(
            "❌ Не знайдено вільних імен у таблиці.\n"
            "Зверніться до адміністратора."
        )
        return ConversationHandler.END

    buttons = [
        [InlineKeyboardButton(name, callback_data=f"reg_sheet:{name}")]
        for name in sorted(available_names)
    ]
    await query.edit_message_text(
        "📝 <b>Реєстрація</b>\n\nОберіть своє ім'я зі списку менеджерів:",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return REG_SELECT_SHEET


async def reg_select_sheet(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Крок 2 — вибір акаунту в Kommo."""
    query      = update.callback_query
    await query.answer()
    sheet_name = query.data.split(':', 1)[1]
    tg_id      = str(query.from_user.id)

    context.user_data['reg_sheet_name'] = sheet_name

    # Отримуємо список користувачів Kommo
    from kommo import get_kommo_users
    try:
        kommo_users = await get_kommo_users()
    except Exception as e:
        logger.error(f"reg_select_sheet: {e}")
        kommo_users = []

    if not kommo_users:
        # Якщо Kommo API недоступний — зберігаємо без kommo_id і відправляємо на схвалення
        await _submit_registration(query, tg_id, sheet_name, kommo_id=None)
        return ConversationHandler.END

    buttons = [
        [InlineKeyboardButton(u['name'], callback_data=f"reg_kommo:{u['id']}:{u['name']}")]
        for u in kommo_users
    ]
    buttons.append([InlineKeyboardButton("⏭ Пропустити", callback_data="reg_kommo:0:—")])

    await query.edit_message_text(
        f"📝 Ім'я в таблиці: <b>{sheet_name}</b>\n\n"
        "Оберіть свій акаунт у Kommo CRM:",
        parse_mode='HTML',
        reply_markup=InlineKeyboardMarkup(buttons),
    )
    return REG_SELECT_KOMMO


async def reg_select_kommo(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Крок 3 — збереження та відправка на схвалення."""
    query      = update.callback_query
    await query.answer()
    tg_id      = str(query.from_user.id)
    sheet_name = context.user_data.get('reg_sheet_name', '')

    parts     = query.data.split(':', 2)
    kommo_id  = int(parts[1]) if parts[1] != '0' else None
    kommo_name = parts[2] if len(parts) > 2 else '—'

    context.user_data['reg_kommo_id']   = kommo_id
    context.user_data['reg_kommo_name'] = kommo_name

    await _submit_registration(query, tg_id, sheet_name, kommo_id)
    context.user_data.clear()
    return ConversationHandler.END


async def _submit_registration(query, tg_id: str, sheet_name: str, kommo_id):
    """Зберігає заявку і повідомляє адмінів."""
    from notifications import notify_admins
    from telegram import InlineKeyboardButton, InlineKeyboardMarkup

    tg_name = query.from_user.full_name

    upsert_manager(tg_id, tg_name, sheet_name, kommo_id)

    await query.edit_message_text(
        f"✅ <b>Заявку відправлено!</b>\n\n"
        f"👤 Ім'я в таблиці: <b>{sheet_name}</b>\n"
        f"🔗 Kommo: <b>{kommo_id or '—'}</b>\n\n"
        f"Очікуйте схвалення від адміністратора. Ви отримаєте повідомлення.",
        parse_mode='HTML',
    )

    approval_kb = InlineKeyboardMarkup([[
        InlineKeyboardButton("✅ Схвалити", callback_data=f"mgr_approve:{tg_id}"),
        InlineKeyboardButton("❌ Відхилити", callback_data=f"mgr_reject:{tg_id}"),
    ]])
    await notify_admins(
        f"📝 <b>Новий запит на реєстрацію</b>\n\n"
        f"👤 Telegram: <b>{tg_name}</b> (<code>{tg_id}</code>)\n"
        f"📋 Sheets: <b>{sheet_name}</b>\n"
        f"🔗 Kommo ID: <b>{kommo_id or '—'}</b>",
    )
    # Відправляємо кнопки схвалення окремим повідомленням кожному адміну
    import state as _state
    from config import ADMIN_IDS
    for admin_id in ADMIN_IDS:
        try:
            await _state._app.bot.send_message(
                chat_id=admin_id,
                text=f"Схвалити реєстрацію <b>{tg_name}</b> ({sheet_name})?",
                reply_markup=approval_kb,
                parse_mode='HTML',
            )
        except Exception as e:
            logger.warning(f"_submit_registration: не вдалось надіслати адміну {admin_id}: {e}")
