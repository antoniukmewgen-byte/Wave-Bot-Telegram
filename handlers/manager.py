import logging
from datetime import datetime

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, KeyboardButton, ReplyKeyboardMarkup, Update
from telegram.error import BadRequest
from telegram.ext import ContextTypes

import state
from config import ADMIN_IDS
from db import (
    q, get_lead, get_taken, get_all_max_leads_overrides,
    is_available, set_availability, mark_connected, mark_skipped, get_skipped, take_lead,
    get_last_connected_ts, get_manager, clear_distributed_leads,
)
from kommo import set_kommo_responsible
from notifications import notify_admins, notify_admin_error, edit_msg, remove_from_others, schedule_cleanup, schedule_delete_msg, remove_buttons_for_manager
from queue_logic import assign_next, day_key, build_keyboard, restore_buttons_for_manager
from sheets import fetch_managers, get_block_reason

logger = logging.getLogger(__name__)

MANAGER_KB = ReplyKeyboardMarkup(
    [[KeyboardButton("✅ Увійти в чергу"), KeyboardButton("🚫 Вийти з черги")]],
    resize_keyboard=True,
    is_persistent=True,
)


def work_keyboard(is_active: bool) -> InlineKeyboardMarkup:
    label = "🚫 Вийти з черги" if is_active else "✅ Увійти в чергу"
    data  = "work:off"         if is_active else "work:on"
    return InlineKeyboardMarkup([[InlineKeyboardButton(label, callback_data=data)]])


async def on_start(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id   = str(update.effective_user.id)
    user_name = update.effective_user.full_name

    is_admin   = user_id in ADMIN_IDS
    mgr_row    = get_manager(user_id)
    is_manager = mgr_row is not None and bool(mgr_row.get('is_approved'))

    if not is_admin and not is_manager:
        if mgr_row and not mgr_row.get('is_approved'):
            # Заявка на реєстрацію ще очікує схвалення
            await update.message.reply_text(
                "⏳ Ваша заявка на реєстрацію очікує схвалення адміністратором.\n"
                "Ви отримаєте повідомлення після перевірки."
            )
        else:
            # Незнайомий користувач — пропонуємо реєстрацію
            from telegram import InlineKeyboardButton, InlineKeyboardMarkup
            kb = InlineKeyboardMarkup([[
                InlineKeyboardButton("📝 Зареєструватись", callback_data="reg:start")
            ]])
            await update.message.reply_text(
                "⛔ У вас немає доступу до цього бота.\n\n"
                "Якщо ви менеджер — зареєструйтесь:",
                reply_markup=kb,
            )
        return

    from handlers.admin import ADMIN_KB
    mgr_name = state.MANAGERS_BY_ID.get(user_id, user_name)

    # Запам'ятовуємо попередній час підключення ДО оновлення
    prev_connected_ts = get_last_connected_ts(user_id)
    mark_connected(user_id, mgr_name)
    # Оновлюємо runtime-словник щоб логи та повідомлення завжди мали актуальне ім'я
    state.MANAGERS_BY_ID[user_id] = mgr_name

    if is_admin:
        await update.message.reply_text("👋 Вітаю, адміне!\nОберіть дію:", reply_markup=ADMIN_KB)
    else:
        active = is_available(user_id)
        status = "✅ В черзі" if active else "🚫 Не в черзі"
        await update.message.reply_text(
            f"✅ Вітаю, {mgr_name}!\nПоточний статус: {status}",
            reply_markup=MANAGER_KB,
        )

    if not is_admin:
        # Throttle: не спамимо адмінам якщо менеджер перепідключився менше ніж 5 хв тому
        # (наприклад після рестарту сервера всі менеджери одразу тиснуть /start)
        just_reconnected = datetime.now().timestamp() - prev_connected_ts < 300
        if not just_reconnected:
            await notify_admins(f"✅ <b>{mgr_name}</b> підключив(ла) бота\n👤 ID: <code>{user_id}</code>")


async def on_work(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    name    = state.MANAGERS_BY_ID.get(user_id)
    if not name:
        await update.message.reply_text("⛔ Ця команда вам недоступна.")
        return
    active = is_available(user_id)
    status = "✅ В черзі" if active else "🚫 Не в черзі"
    await update.message.reply_text(
        f"👤 <b>{name}</b>\nПоточний статус: {status}\n\nОберіть дію:",
        reply_markup=work_keyboard(active),
        parse_mode='HTML',
    )


async def on_work_button(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user_id = str(update.effective_user.id)
    name    = state.MANAGERS_BY_ID.get(user_id)
    if not name:
        return

    active = update.message.text == "✅ Увійти в чергу"

    if active:
        managers = fetch_managers()
        if user_id not in managers:
            reason = get_block_reason(user_id) or "❌ Ви не можете увійти в чергу. Зверніться до керівника."
            await update.message.reply_text(reason, reply_markup=MANAGER_KB)
            set_availability(user_id, False, reason='blocked')
            return

    set_availability(user_id, active, reason=None if active else 'manual')
    clear_distributed_leads(user_id)
    status = "✅ Ви в черзі — заявки надходитимуть" if active else "🚫 Ви вийшли з черги — заявки не надходитимуть"
    await update.message.reply_text(status, reply_markup=MANAGER_KB)
    if active:
        await restore_buttons_for_manager(user_id)
    else:
        await remove_buttons_for_manager(user_id)
    logger.info(f"{name} {'увійшов в чергу' if active else 'вийшов з черги'}")


async def on_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query

    if query.data.startswith('setlim:'):
        await query.answer()
        return

    try:
        action, lead_id = query.data.split(':', 1)
    except ValueError:
        await query.answer()
        return

    manager_id = str(query.from_user.id)

    # ── work:on / work:off ────────────────────────────────────────────────────
    if action == 'work':
        try:
            name = state.MANAGERS_BY_ID.get(manager_id)
            if not name:
                await query.answer()
                return
            active = (lead_id == 'on')
            set_availability(manager_id, active, reason=None if active else 'manual')
            clear_distributed_leads(manager_id)
            await query.answer()
            status = "✅ Ви в черзі — заявки надходитимуть" if active else "🚫 Ви вийшли з черги — заявки не надходитимуть"
            await query.edit_message_text(
                f"👤 <b>{name}</b>\n\n{status}\n\nЩоб змінити — напишіть /work",
                reply_markup=work_keyboard(active),
                parse_mode='HTML',
            )
            if active:
                await restore_buttons_for_manager(manager_id)
            else:
                await remove_buttons_for_manager(manager_id)
            logger.info(f"{name} {'увійшов в чергу' if active else 'вийшов з черги'}")
        except Exception as e:
            logger.error(f"on_callback work: {e}")
            try:
                await query.answer()
            except Exception:
                pass
        return

    lead = get_lead(lead_id)
    if not lead:
        await query.answer("⚠️ Заявка не знайдена", show_alert=True)
        return

    if lead['status'] in ('taken', 'duplicate', 'closed'):
        await query.answer("❌ Цю заявку вже оброблено", show_alert=True)
        await edit_msg(manager_id, lead_id, "❌ Цю заявку вже оброблено")
        return

    managers = fetch_managers()
    mgr_name = managers.get(manager_id, {}).get('name', query.from_user.first_name or manager_id)

    try:
        if action in ('take', 't'):
            if not is_available(manager_id) or manager_id not in managers:
                await query.answer("⛔ Ви поза чергою — заявку взяти неможливо", show_alert=True)
                return

            mgr_info  = managers.get(manager_id, {})
            overrides = get_all_max_leads_overrides()
            max_leads = overrides[manager_id] if manager_id in overrides else mgr_info.get('max_leads')

            if max_leads is not None:
                taken_today = get_taken(manager_id, day_key())
                if taken_today >= max_leads:
                    await query.answer("⛔ Ви вже взяли максимальну кількість лідів на сьогодні", show_alert=True)
                    await edit_msg(manager_id, lead_id, f"⛔ Ліміт вичерпано ({taken_today}/{max_leads})\n\n{lead['title']}")
                    return

            if not take_lead(lead_id, manager_id, day_key()):
                await query.answer("❌ Заявку вже взяв інший менеджер", show_alert=True)
                await edit_msg(manager_id, lead_id, "❌ Заявку вже взяв інший менеджер")
                return

            await query.answer()
            await edit_msg(manager_id, lead_id, f"✅ Ви взяли заявку в роботу!\n\n{lead['title']}")

            kommo_ok = await set_kommo_responsible(lead_id, manager_id)
            if kommo_ok:
                await edit_msg(manager_id, lead_id, f"✅ Ви взяли заявку в роботу! | Відповідальний: {mgr_name}\n\n{lead['title']}")
            await remove_from_others(lead_id, except_id=manager_id,
                                     note=f"✅ Заявку взяв(ла) <b>{mgr_name}</b>")
            schedule_cleanup(lead_id)
            logger.info(f"Заявка {lead_id} взята {mgr_name} ({manager_id})")
            await notify_admins(f"✅ <b>{mgr_name}</b> взяв(ла) заявку в роботу\n\n{lead['title']}")

            if max_leads is not None:
                taken_today = get_taken(manager_id, day_key())
                if taken_today >= max_leads:
                    await state._app.bot.send_message(
                        chat_id=manager_id,
                        text=f"⛔ Ви взяли максимальну кількість лідів на сьогодні ({max_leads}). "
                             f"Нові заявки надходитимуть завтра.",
                    )

        elif action in ('skip', 's'):
            if not is_available(manager_id):
                await query.answer(
                    "⛔ Ви поза чергою. Щоб взаємодіяти із заявками — спочатку увійдіть у чергу (/work)",
                    show_alert=True,
                )
                return

            await query.answer()
            mark_skipped(lead_id, manager_id)
            await edit_msg(manager_id, lead_id, f"⏭ Ви відмовились від заявки\n\n{lead['title']}")
            schedule_delete_msg(manager_id, lead_id)

            lead = get_lead(lead_id)
            if lead and lead['status'] == 'sent':
                q("UPDATE leads SET status='queued', manager_id=NULL, sent_at=NULL WHERE lead_id=?", (lead_id,))
                await assign_next(lead_id, exclude=get_skipped(lead_id))
            logger.info(f"Заявка {lead_id} відхилена {mgr_name}")

        elif action in ('dup', 'd'):
            if not is_available(manager_id):
                await query.answer(
                    "⛔ Ви поза чергою. Щоб взаємодіяти із заявками — спочатку увійдіть у чергу (/work)",
                    show_alert=True,
                )
                return

            await query.answer()
            q("UPDATE leads SET status='duplicate' WHERE lead_id=?", (lead_id,))
            await edit_msg(manager_id, lead_id, "🔁 Ви позначили заявку як дубль")
            await remove_from_others(lead_id, except_id=manager_id, note="🔁 Заявка закрита як дубль")
            schedule_cleanup(lead_id)
            logger.info(f"Заявка {lead_id} — дубль ({mgr_name})")

        else:
            await query.answer()

    except BadRequest as e:
        # Callback query протух (>10 хв) або вже оброблений — не спамимо адміну
        if 'query is too old' in str(e).lower() or 'query id is invalid' in str(e).lower():
            logger.warning(f"on_callback {action} {lead_id}: callback протух — {e}")
        else:
            logger.error(f"on_callback {action} {lead_id}: {e}")
            await notify_admin_error(f"on_callback (дія: {action}, заявка: {lead_id})", e, manager_id)
    except Exception as e:
        logger.error(f"on_callback {action} {lead_id}: {e}")
        try:
            await query.answer()
        except Exception:
            pass
        await notify_admin_error(f"on_callback (дія: {action}, заявка: {lead_id})", e, manager_id)
