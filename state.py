from telegram.ext import Application

_app: Application = None
MANAGERS_BY_ID: dict = {}  # {tg_id: display_name} — завантажується з БД при старті


async def reload_managers():
    """Перезавантажує MANAGERS_BY_ID з таблиці managers у БД."""
    global MANAGERS_BY_ID
    from db import get_all_managers
    rows = await get_all_managers(approved_only=True)
    MANAGERS_BY_ID = {r['tg_id']: (r['sheet_name'] or r['tg_name'] or r['tg_id']) for r in rows}


async def reload_admins():
    """Довантажує в config.ADMIN_IDS адмінів, доданих через /addadmin (таблиця admins у БД).

    Мутує список config.ADMIN_IDS на місці (а не перестворює), бо всі хендлери
    зробили `from config import ADMIN_IDS` — їм дістався той самий об'єкт списку,
    і лише мутація in-place лишається видимою одразу в усіх модулях.
    """
    import config
    from db import get_all_admin_ids
    for tg_id in await get_all_admin_ids():
        if tg_id not in config.ADMIN_IDS:
            config.ADMIN_IDS.append(tg_id)
