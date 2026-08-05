import asyncio
import logging
from datetime import datetime
from typing import Dict, Optional

import gspread_asyncio
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
# gspread_asyncio сам відповідає за поновлення токена (reauth_interval) —
# ручний _reconnect() тут більше не потрібен для авторизації, лишається
# тільки для перепідключення worksheet, якщо саме він став невалідним.
_agcm: Optional[gspread_asyncio.AsyncioGspreadClientManager] = None
_ws: Optional[gspread_asyncio.AsyncioGspreadWorksheet] = None
# Лінива ініціалізація (див. аналогічний коментар/фікс у kommo.py
# _session_lock) — уникаємо прив'язки Lock-а до "чужого" event loop, що
# існував на момент import модуля, а не того, в якому реально працює uvicorn.
_ws_lock: Optional[asyncio.Lock] = None


_SCOPES = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
]


def _get_credentials() -> Credentials:
    return Credentials.from_service_account_file(GOOGLE_CREDS, scopes=_SCOPES)


def _get_agcm() -> gspread_asyncio.AsyncioGspreadClientManager:
    global _agcm
    if _agcm is None:
        _agcm = gspread_asyncio.AsyncioGspreadClientManager(_get_credentials)
    return _agcm


async def _get_ws() -> gspread_asyncio.AsyncioGspreadWorksheet:
    """Повертає worksheet, перепідключається якщо сесія протухла."""
    global _ws, _ws_lock
    if _ws is None:
        if _ws_lock is None:
            _ws_lock = asyncio.Lock()
        async with _ws_lock:
            if _ws is None:
                agc = await _get_agcm().authorize()
                ss  = await agc.open_by_key(SHEETS_ID)
                _ws = await ss.worksheet(SHEET_NAME)
                logger.info("Sheets: підключення встановлено")
    return _ws


async def _reconnect() -> gspread_asyncio.AsyncioGspreadWorksheet:
    """Примусове перепідключення (якщо worksheet протух)."""
    global _ws
    _ws = None
    return await _get_ws()


# ─── Кеш менеджерів ──────────────────────────────────────────────────────────
_cache: Dict[str, dict] = {}
_cache_ts: float = 0.0
_rows_cache: list = []
# Лінива ініціалізація — той самий фікс, що й вище для _ws_lock/_session_lock.
_lock: Optional[asyncio.Lock] = None

# Ключові слова, які очікуємо побачити в заголовку кожної колонки (нижній регістр).
# Джерело — коментарі біля COL_* у config.py. Використовується лише для
# діагностичного попередження в лог, якщо структура таблиці зміниться
# (зсув/перейменування колонок) — саму логіку парсингу не змінює.
_EXPECTED_HEADER_KEYWORDS = {
    COL_PLAN:       ('план', 'обіг'),
    COL_HOT_TAKEN:  ('гарячих', 'лідів'),
    COL_PAYMENTS:   ('проданих', 'конс'),
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


async def _sheet_name_to_tg_id() -> Dict[str, str]:
    return {
        r['sheet_name']: r['tg_id']
        for r in await get_all_managers(approved_only=True)
        if r['sheet_name']
    }


async def _read_rows() -> list:
    """Читає всі рядки з кешованим підключенням."""
    global _rows_cache
    try:
        ws = await _get_ws()
        _rows_cache = await ws.get('A1:AK200', value_render_option='FORMATTED_VALUE') or []
    except Exception:
        ws = await _reconnect()
        _rows_cache = await ws.get('A1:AK200', value_render_option='FORMATTED_VALUE') or []
    _validate_header_row(_rows_cache)
    return _rows_cache


async def fetch_managers() -> Dict[str, dict]:
    """
    Повертає {telegram_id: {name, conversion, payments, hot_taken, max_leads}}
    Кеш оновлюється раз на SHEETS_REFRESH секунд.
    """
    global _cache, _cache_ts, _lock

    now = datetime.now().timestamp()
    # Читаємо у локальні змінні — захист від зміни глобалів між перевіркою і поверненням
    cache_ts = _cache_ts
    cache    = _cache
    if now - cache_ts < SHEETS_REFRESH and cache:
        return cache

    if _lock is None:
        _lock = asyncio.Lock()
    async with _lock:
        # Повторна перевірка після отримання lock (інший таск міг вже оновити кеш)
        now2 = datetime.now().timestamp()
        if now2 - _cache_ts < SHEETS_REFRESH and _cache:
            return _cache

        try:
            rows      = await _read_rows()
            now_dt    = datetime.now()
            year_str  = str(now_dt.year)
            month_str = MONTHS_UA[now_dt.month]

            # Завантажуємо всіх менеджерів одним запитом → O(1) lookup у циклі
            sheet_name_to_id = await _sheet_name_to_tg_id()

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


async def get_all_sheet_names() -> list:
    """
    Повертає унікальні непорожні імена з колонки A таблиці (COL_MANAGER) за
    ПОТОЧНИЙ рік/місяць (колонки C/D) — але, на відміну від fetch_managers(),
    без вимоги заповненого плану обігу чи проходження лімітів по конверсії/оплатах.

    Потрібно для реєстрації: людина ще не в системі бота, тож вимагати від неї
    вже мати виставлений план за поточний місяць, щоб просто зареєструватись —
    нелогічне обмеження, яке раніше могло показувати "немає вільних імен"
    навіть коли ім'я в таблиці є. Фільтр по періоду лишаємо — щоб не пропонувати
    для реєстрації імена з рядків за минулі місяці, яких уже немає в поточному.
    """
    try:
        rows = await _read_rows()
    except Exception as e:
        logger.error(f"get_all_sheet_names: {e}")
        return []
    now_dt    = datetime.now()
    year_str  = str(now_dt.year)
    month_str = MONTHS_UA[now_dt.month]
    names = {
        row[COL_MANAGER].strip()
        for row in rows[1:]
        if len(row) > COL_MANAGER and row[COL_MANAGER].strip()
        and _row_matches_period(row, year_str, month_str)
    }
    return sorted(names)


# Історичний псевдонім: раніше fetch_managers() була синхронною і викликалась
# через asyncio.to_thread(), а fetch_managers_async() — обгорткою над нею.
# Тепер fetch_managers() сама по собі async (gspread_asyncio), тож псевдонім
# лишається лише щоб не займатись пере-роботою кожного виклику по всьому коду.
fetch_managers_async = fetch_managers


async def get_block_reason(tg_id: str) -> Optional[str]:
    """Повертає причину, чому менеджер не може потрапити в чергу, або None якщо все ок."""
    try:
        rows     = await _read_rows()
        now_dt   = datetime.now()
        year_str = str(now_dt.year)
        month_str = MONTHS_UA[now_dt.month]

        sheet_to_id = await _sheet_name_to_tg_id()

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


async def warmup():
    """Прогрів кешу при старті — викликати один раз."""
    logger.info("Sheets: прогрів кешу...")
    await fetch_managers()
    logger.info("Sheets: кеш готовий")
