import logging

from telegram import InlineKeyboardButton, InlineKeyboardMarkup, Update
from telegram.ext import ContextTypes

import state
from config import ADMIN_IDS
from db import approve_manager, count_distributed_leads, delete_manager, get_manager, get_managers_dict
from notifications import notify_admins, safe_answer
from queue_logic import handle_manager_exit

logger = logging.getLogger(__name__)


async def on_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробляє колбеки адмін-панелі: схвалення/відхилення реєстрацій і видалення менеджерів."""
    query = update.callback_query
    admin_id = str(query.from_user.id)

    if admin_id not in ADMIN_IDS:
        await safe_answer(query, "⛔ Тільки для адміністраторів", show_alert=True)
        return

    await safe_answer(query)
    action, tg_id = query.data.split(':', 1)

    mgr = await get_manager(tg_id)
    if not mgr:
        await query.edit_message_text("⚠️ Менеджера не знайдено в БД (можливо вже оброблено)")
        return

    name = mgr['sheet_name'] or mgr['tg_name'] or tg_id

    if action == 'mgr_approve':
        await approve_manager(tg_id)
        # Ініціалізуємо розклад якщо немає
        from db import init_default_schedules
        await init_default_schedules({name: tg_id})
        # Оновлюємо runtime словник
        await state.reload_managers()

        await query.edit_message_text(
            f"✅ Менеджера <b>{name}</b> схвалено та додано до системи.",
            parse_mode='HTML',
        )
        logger.info(f"Менеджера {name} ({tg_id}) схвалено адміном {admin_id}")

        # Повідомляємо менеджера
        try:
            await state._app.bot.send_message(
                chat_id=tg_id,
                text=(
                    f"✅ <b>Вас схвалено!</b>\n\n"
                    f"Тепер ви можете увійти в чергу.\n"
                    f"Натисніть /start для початку роботи."
                ),
                parse_mode='HTML',
            )
        except Exception as e:
            logger.warning(f"on_admin_callback: не вдалось повідомити менеджера {tg_id}: {e}")

        await notify_admins(
            f"✅ Менеджер <b>{name}</b> (<code>{tg_id}</code>) схвалений адміном "
            f"<code>{admin_id}</code>"
        )

    elif action == 'mgr_reject':
        await delete_manager(tg_id)

        await query.edit_message_text(
            f"❌ Реєстрацію <b>{name}</b> відхилено та видалено.",
            parse_mode='HTML',
        )
        logger.info(f"Реєстрацію {name} ({tg_id}) відхилено адміном {admin_id}")

        try:
            await state._app.bot.send_message(
                chat_id=tg_id,
                text=(
                    "❌ <b>Вашу заявку на реєстрацію відхилено.</b>\n\n"
                    "Зверніться до адміністратора для уточнення причини."
                ),
                parse_mode='HTML',
            )
        except Exception as e:
            logger.warning(f"on_admin_callback: не вдалось повідомити відхиленого {tg_id}: {e}")

    elif action == 'mgr_del':
        active = await count_distributed_leads(tg_id)
        warn = (
            f"\n\n⚠️ У менеджера <b>{active}</b> заявок зараз у роботі — їх передадуть іншим менеджерам."
            if active else ""
        )
        await query.edit_message_text(
            f"🗑 Видалити менеджера <b>{name}</b> (<code>{tg_id}</code>) з системи?{warn}\n\n"
            f"Повернутись він зможе лише через повторну реєстрацію (/start).",
            parse_mode='HTML',
            reply_markup=InlineKeyboardMarkup([[
                InlineKeyboardButton("✅ Так, видалити", callback_data=f"mgr_delyes:{tg_id}"),
                InlineKeyboardButton("❌ Скасувати", callback_data=f"mgr_delno:{tg_id}"),
            ]]),
        )

    elif action == 'mgr_delyes':
        await handle_manager_exit(tg_id)
        await delete_manager(tg_id)
        state.MANAGERS_BY_ID.pop(tg_id, None)
        await state.reload_managers()

        await query.edit_message_text(
            f"✅ Менеджера <b>{name}</b> видалено з системи.",
            parse_mode='HTML',
        )
        logger.info(f"Менеджера {name} ({tg_id}) видалено адміном {admin_id}")

        try:
            await state._app.bot.send_message(
                chat_id=tg_id,
                text=(
                    "🔕 <b>Вас видалено з системи.</b>\n\n"
                    "Для повернення в роботу зверніться до адміністратора та пройдіть реєстрацію знову (/start)."
                ),
                parse_mode='HTML',
            )
        except Exception as e:
            logger.warning(f"on_admin_callback: не вдалось повідомити видаленого {tg_id}: {e}")

        await notify_admins(
            f"🗑 Менеджер <b>{name}</b> (<code>{tg_id}</code>) видалений адміном "
            f"<code>{admin_id}</code>"
        )

    elif action == 'mgr_delno':
        await query.edit_message_text(f"❌ Скасовано — <b>{name}</b> лишається в системі.", parse_mode='HTML')
