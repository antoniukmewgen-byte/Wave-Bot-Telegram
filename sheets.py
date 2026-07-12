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


def _read_rows() -> list:
    """Читає всі рядки з кешованим підключенням."""
    global _rows_cache
    try:
        ws = _get_ws()
        _rows_cache = ws.get('A1:AK200', value_render_option='FORMATTED_VALUE') or []
    except Exception:
        ws = _reconnect()
        _rows_cache = ws.get('A1:AK200', value_render_option='FORMATTED_VALUE') or []
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

            def _int_col(row: list, idx: int) -> int:
                if len(row) <= idx:
                    return 0
                try:
                    return int(float(row[idx].strip().replace(' ', '').replace('\xa0', '') or '0'))
                except (ValueError, TypeError):
                    return 0

            # Завантажуємо всіх менеджерів одним запитом → O(1) lookup у циклі
            sheet_name_to_id: Dict[str, str] = {
                r['sheet_name']: r['tg_id']
                for r in get_all_managers(approved_only=True)
                if r['sheet_name']
            }

            result: Dict[str, dict] = {}
            for row in rows[1:]:
                if len(row) <= COL_CONVERSION:
                    continue
                if row[COL_YEAR].strip() != year_str:
                    continue
                if row[COL_MONTH].strip() != month_str:
                    continue

                name  = row[COL_MANAGER].strip()
                tg_id = sheet_name_to_id.get(name)
                if not tg_id:
                    continue

                raw = (row[COL_CONVERSION].strip()
                       .replace('%', '').replace(',', '.').replace(' ', '').replace('\xa0', ''))
                try:
                    conv = float(raw)
                except (ValueError, TypeError):
                    conv = 0.0

                payments  = _int_col(row, COL_PAYMENTS)
                hot_taken = _int_col(row, COL_HOT_TAKEN)

                # Якщо колонка W (План обіг) порожня — менеджер поза чергою
                plan_raw = row[COL_PLAN].strip().replace(' ', '').replace('\xa0', '').replace('$', '').replace(',', '') if len(row) > COL_PLAN else ''
                if not plan_raw or plan_raw == '0':
                    continue

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


def get_block_reason(tg_id: str) -> Optional[str]:
    """Повертає причину, чому менеджер не може потрапити в чергу, або None якщо все ок."""
    try:
        rows     = _read_rows()
        now_dt   = datetime.now()
        year_str = str(now_dt.year)
        month_str = MONTHS_UA[now_dt.month]

        sheet_to_id: Dict[str, str] = {
            r['sheet_name']: r['tg_id']
            for r in get_all_managers(approved_only=True)
            if r['sheet_name']
        }

        for row in rows[1:]:
            if len(row) <= COL_CONVERSION:
                continue
            if row[COL_YEAR].strip() != year_str:
                continue
            if row[COL_MONTH].strip() != month_str:
                continue

            name = row[COL_MANAGER].strip()
            if sheet_to_id.get(name) != tg_id:
                continue

            plan_raw = row[COL_PLAN].strip().replace(' ', '').replace('\xa0', '').replace('$', '').replace(',', '') if len(row) > COL_PLAN else ''
            if not plan_raw or plan_raw == '0':
                return "❌ Вам не встановлено план обігу (колонка W порожня). Зверніться до керівника."

            raw = row[COL_CONVERSION].strip().replace('%', '').replace(',', '.').replace(' ', '').replace('\xa0', '')
            try:
                conv = float(raw)
            except (ValueError, TypeError):
                conv = 0.0

            try:
                payments = int(float(row[COL_PAYMENTS].strip().replace(' ', '').replace('\xa0', '') or '0')) if len(row) > COL_PAYMENTS else 0
            except (ValueError, TypeError):
                payments = 0

            try:
                hot_taken = int(float(row[COL_HOT_TAKEN].strip().replace(' ', '').replace('\xa0', '') or '0')) if len(row) > COL_HOT_TAKEN else 0
            except (ValueError, TypeError):
                hot_taken = 0

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
