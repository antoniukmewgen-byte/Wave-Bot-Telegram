import logging

from telegram import Update
from telegram.ext import ContextTypes

import state
from config import ADMIN_IDS
from db import approve_manager, delete_manager, get_manager, get_managers_dict
from notifications import notify_admins

logger = logging.getLogger(__name__)


async def on_admin_callback(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Обробляє колбеки адмін-панелі: схвалення/відхилення реєстрацій менеджерів."""
    query = update.callback_query
    admin_id = str(query.from_user.id)

    if admin_id not in ADMIN_IDS:
        await query.answer("⛔ Тільки для адміністраторів", show_alert=True)
        return

    await query.answer()
    action, tg_id = query.data.split(':', 1)

    mgr = get_manager(tg_id)
    if not mgr:
        await query.edit_message_text("⚠️ Менеджера не знайдено в БД (можливо вже оброблено)")
        return

    name = mgr['sheet_name'] or mgr['tg_name'] or tg_id

    if action == 'mgr_approve':
        approve_manager(tg_id)
        # Ініціалізуємо розклад якщо немає
        from db import init_default_schedules
        init_default_schedules({name: tg_id})
        # Оновлюємо runtime словник
        state.reload_managers()

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
        delete_manager(tg_id)

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
