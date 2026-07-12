from telegram.ext import Application

_app: Application = None
MANAGERS_BY_ID: dict = {}  # {tg_id: display_name} — завантажується з БД при старті


def reload_managers():
    """Перезавантажує MANAGERS_BY_ID з таблиці managers у БД."""
    global MANAGERS_BY_ID
    from db import get_all_managers
    rows = get_all_managers(approved_only=True)
    MANAGERS_BY_ID = {r['tg_id']: (r['sheet_name'] or r['tg_name'] or r['tg_id']) for r in rows}
