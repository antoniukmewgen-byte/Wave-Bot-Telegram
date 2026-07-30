import asyncio
import logging
import threading
from datetime import datetime
from typing import Dict, Optional

import gspread
from google.oauth2.service_account import Credentials

from config import (
    SHEETS_ID, SHEET_NAME, GOOGLE_CREDS,
    COL_MANAGER, COL_YEAR, COL_MONTH, COL_PLAN, COL_CONVERSION,
    COL_PAYMENTS, COL_HOT_TAKEN,
    CONV_UNLIMITED, CONV_MAX5_MIN, CONV_MAX2_MIN,
    LEADS_UNLIM_MAX, LEADS_MAX5_MAX, LEADS_MAX2_MAX,
    MAX_LEADS_5, MAX_LEADS_2,
    SHEETS_REFRESH,
)
from db import get_all_managers

logger = logging.getLogger(__name__)

MONTHS_UA = {
    1: 'Січень',  2: 'Лютий',    3: 'Березень', 4: 'Квітень',
    5: 'Травень', 6: 'Червень',  7: 'Липень',   8: 'Серпень',
    9: 'Вересень', 10: 'Жовтень', 11: 'Листопад', 12: 'Грудень',
}

# ─── Одне постійне підключення ───────────────────────────────────────────────
_gc: Optional[gspread.Client] = None
_ws: Optional[gspread.Worksheet] = None


_SCOPES = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
]


def _get_ws() -> gspread.Worksheet:
    """Повертає worksheet, перепідключається якщо сесія протухла."""
    global _gc, _ws
    if _ws is None:
        creds = Credentials.from_service_account_file(GOOGLE_CREDS, scopes=_SCOPES)
        _gc   = gspread.authorize(creds)
        _ws   = _gc.open_by_key(SHEETS_ID).worksheet(SHEET_NAME)
        logger.info("Sheets: підключення встановлено")
    return _ws


def _reconnect() -> gspread.Worksheet:
    """Примусове перепідключення (якщо токен протух)."""
    global _gc, _ws
    _gc = None
    _ws = None
    return _get_ws()


# ─── Кеш менеджерів ──────────────────────────────────────────────────────────
_cache: Dict[str, dict] = {}
_cache_ts: float = 0.0
_rows_cache: list = []
_lock = threading.Lock()

# Ключові слова, які очікуємо побачити в заголовку кожної колонки (нижній регістр).
# Джерело — коментарі біля COL_* у config.py. Використовується лише для
# діагностичного попередження в лог, якщо структура таблиці зміниться
# (зсув/перейменування колонок) — саму логіку парсингу не змінює.
_EXPECTED_HEADER_KEYWORDS = {
    COL_PLAN:       ('план', 'обіг'),
    COL_HOT_TAKEN:  ('гарячих', 'лідів'),
    COL_PAYMENTS:   ('проданих', 'консультацій'),
    COL_CONVERSION: ('конверсія',),
}
_header_warned: set = set()


def _validate_header_row(rows: list) -> None:
    """Звіряє текст заголовків з очікуваними ключовими словами.

    Лише пише warning у лог (раз на колонку за час роботи процесу) —
    не впливає на результат парсингу. Дозволяє помітити, що хтось змінив
    структуру Google Sheets, замість мовчазного неправильного розбору даних.
    """
    if not rows:
        return
    header = rows[0]
    for col_idx, keywords in _EXPECTED_HEADER_KEYWORDS.items():
        if col_idx in _header_warned:
            continue
        if col_idx >= len(header):
            logger.warning(f"Sheets: заголовок колонки {col_idx} відсутній (очікували ключові слова {keywords})")
            _header_warned.add(col_idx)
            continue
        header_text = header[col_idx].strip().lower()
        if not all(kw in header_text for kw in keywords):
            logger.warning(
                f"Sheets: заголовок колонки {col_idx} ('{header[col_idx]}') не містить "
                f"очікуваних слів {keywords} — можливо, стовпці таблиці змінились"
            )
            _header_warned.add(col_idx)


def _int_col(row: list, idx: int) -> int:
    if len(row) <= idx:
        return 0
    try:
        return int(float(row[idx].strip().replace(' ', '').replace('\xa0', '') or '0'))
    except (ValueError, TypeError):
        return 0


def _float_percent_col(row: list, idx: int) -> float:
    if len(row) <= idx:
        return 0.0
    raw = row[idx].strip().replace('%', '').replace(',', '.').replace(' ', '').replace('\xa0', '')
    try:
        return float(raw)
    except (ValueError, TypeError):
        return 0.0


def _has_plan(row: list) -> bool:
    if len(row) <= COL_PLAN:
        return False
    plan_raw = row[COL_PLAN].strip().replace(' ', '').replace('\xa0', '').replace('$', '').replace(',', '')
    return bool(plan_raw) and plan_raw != '0'


def _row_matches_period(row: list, year_str: str, month_str: str) -> bool:
    if len(row) <= COL_CONVERSION:
        return False
    return row[COL_YEAR].strip() == year_str and row[COL_MONTH].strip() == month_str


def _sheet_name_to_tg_id() -> Dict[str, str]:
    return {
        r['sheet_name']: r['tg_id']
        for r in get_all_managers(approved_only=True)
        if r['sheet_name']
    }


def _read_rows() -> list:
    """Читає всі рядки з кешованим підключенням."""
    global _rows_cache
    try:
        ws = _get_ws()
        _rows_cache = ws.get('A1:AK200', value_render_option='FORMATTED_VALUE') or []
    except Exception:
        ws = _reconnect()
        _rows_cache = ws.get('A1:AK200', value_render_option='FORMATTED_VALUE') or []
    _validate_header_row(_rows_cache)
    return _rows_cache


def fetch_managers() -> Dict[str, dict]:
    """
    Повертає {telegram_id: {name, conversion, payments, hot_taken, max_leads}}
    Кеш оновлюється раз на SHEETS_REFRESH секунд.
    """
    global _cache, _cache_ts

    now = datetime.now().timestamp()
    # Читаємо у локальні змінні — захист від зміни глобалів між перевіркою і поверненням
    cache_ts = _cache_ts
    cache    = _cache
    if now - cache_ts < SHEETS_REFRESH and cache:
        return cache

    with _lock:
        # Повторна перевірка після отримання lock (інший потік міг вже оновити кеш)
        now2 = datetime.now().timestamp()
        if now2 - _cache_ts < SHEETS_REFRESH and _cache:
            return _cache

        try:
            rows      = _read_rows()
            now_dt    = datetime.now()
            year_str  = str(now_dt.year)
            month_str = MONTHS_UA[now_dt.month]

            # Завантажуємо всіх менеджерів одним запитом → O(1) lookup у циклі
            sheet_name_to_id = _sheet_name_to_tg_id()

            result: Dict[str, dict] = {}
            for row in rows[1:]:
                if not _row_matches_period(row, year_str, month_str):
                    continue

                name  = row[COL_MANAGER].strip()
                tg_id = sheet_name_to_id.get(name)
                if not tg_id:
                    continue

                # Якщо колонка W (План обіг) порожня — менеджер поза чергою
                if not _has_plan(row):
                    continue

                conv      = _float_percent_col(row, COL_CONVERSION)
                payments  = _int_col(row, COL_PAYMENTS)
                hot_taken = _int_col(row, COL_HOT_TAKEN)

                if payments == 0:
                    # Гілка «0 оплат» — ліміт за к-тю взятих лідів
                    if hot_taken <= LEADS_UNLIM_MAX:
                        max_leads = None
                    elif hot_taken <= LEADS_MAX5_MAX:
                        max_leads = MAX_LEADS_5
                    elif hot_taken <= LEADS_MAX2_MAX:
                        max_leads = MAX_LEADS_2
                    else:
                        continue  # > 30 лідів без оплат → поза чергою
                else:
                    # Гілка «є оплати» — ліміт за конверсією
                    if conv >= CONV_UNLIMITED:
                        max_leads = None
                    elif conv >= CONV_MAX5_MIN:
                        max_leads = MAX_LEADS_5
                    elif conv >= CONV_MAX2_MIN:
                        max_leads = MAX_LEADS_2
                    else:
                        continue  # < 3.3% → поза чергою

                result[tg_id] = {
                    'name':       name,
                    'conversion': conv,
                    'payments':   payments,
                    'hot_taken':  hot_taken,
                    'max_leads':  max_leads,
                }

            _cache    = result
            _cache_ts = datetime.now().timestamp()
            if result:
                logger.info(f"Sheets: {len(result)} менеджерів у черзі")
            else:
                logger.warning("Sheets: жодного менеджера не знайдено за поточний місяць")

        except Exception as e:
            logger.error(f"Sheets помилка: {e}")

    return _cache


async def fetch_managers_async() -> Dict[str, dict]:
    """
    Async-безпечна обгортка над fetch_managers().

    fetch_managers() сама по собі синхронна: коли кеш (SHEETS_REFRESH) протух,
    вона виконує "живий" мережевий запит до Google Sheets і блокує потік до
    відповіді. Викликана напряму з async-коду (хендлери, _tick тощо) вона
    заморожує весь asyncio event loop бота — тобто взагалі всіх користувачів —
    на час цього запиту, приблизно раз на хвилину.

    Ця функція виносить виклик в окремий потік через asyncio.to_thread, тому
    event loop лишається вільним. Кешування, локи й логіка fetch_managers()
    лишаються без змін — тут просто інший спосіб її викликати з async-коду.
    """
    return await asyncio.to_thread(fetch_managers)


def get_block_reason(tg_id: str) -> Optional[str]:
    """Повертає причину, чому менеджер не може потрапити в чергу, або None якщо все ок."""
    try:
        rows     = _read_rows()
        now_dt   = datetime.now()
        year_str = str(now_dt.year)
        month_str = MONTHS_UA[now_dt.month]

        sheet_to_id = _sheet_name_to_tg_id()

        for row in rows[1:]:
            if not _row_matches_period(row, year_str, month_str):
                continue

            name = row[COL_MANAGER].strip()
            if sheet_to_id.get(name) != tg_id:
                continue

            if not _has_plan(row):
                return "❌ Вам не встановлено план обігу (колонка W порожня). Зверніться до керівника."

            conv      = _float_percent_col(row, COL_CONVERSION)
            payments  = _int_col(row, COL_PAYMENTS)
            hot_taken = _int_col(row, COL_HOT_TAKEN)

            if payments == 0:
                if hot_taken > LEADS_MAX2_MAX:
                    return f"❌ Ви взяли {hot_taken} лідів без жодної оплати. Потрібно закрити ліди перед поверненням в чергу."
            else:
                if conv < CONV_MAX2_MIN:
                    return f"❌ Ваша конверсія {conv}% занадто низька (мінімум {CONV_MAX2_MIN}%). Зверніться до керівника."

            return None  # знайдено рядок і всі перевірки пройдені

    except Exception as e:
        logger.error(f"get_block_reason помилка: {e}")

    return None


def warmup():
    """Прогрів кешу при старті — викликати один раз."""
    logger.info("Sheets: прогрів кешу...")
    fetch_managers()
    logger.info("Sheets: кеш готовий")
