import os
from dotenv import load_dotenv

load_dotenv()


def _require(key: str) -> str:
    val = os.environ.get(key, '').strip()
    if not val:
        raise RuntimeError(
            f"Відсутня обов'язкова змінна середовища: {key}\n"
            f"Переконайтесь що файл .env існує і містить {key}=..."
        )
    return val


BOT_TOKEN      = _require('BOT_TOKEN')
AMO_SUBDOMAIN     = os.environ.get('AMO_SUBDOMAIN', 'movenation')
AMO_TOKEN         = os.environ.get('AMO_TOKEN', '')
AMO_PIPELINE_ID   = os.environ.get('AMO_PIPELINE_ID', '10815171')
AMO_HOT_STATUS_ID = os.environ.get('AMO_HOT_STATUS_ID', '85731907')
WEBHOOK_PATH   = os.environ.get('WEBHOOK_PATH', 'movenation')
SHEETS_ID      = _require('SHEETS_ID')
SHEET_NAME     = os.environ.get('SHEET_NAME', 'План|Факт|Мотивація Мдж')
GOOGLE_CREDS   = os.environ.get('GOOGLE_CREDS', 'google_creds.json')
ADMIN_IDS      = [i.strip() for i in os.environ.get('ADMIN_IDS', '').split(',') if i.strip()]

COL_MANAGER    = 0
COL_YEAR       = 2
COL_MONTH      = 3
COL_PLAN       = 22   # W  — план обіг
COL_HOT_TAKEN  = 24   # Y  — взято гарячих лідів
COL_PAYMENTS   = 35   # AJ — к-ть проданих консультацій
COL_CONVERSION = 36   # AK — конверсія %

MANAGERS = {
    'Тимур Мартиросян':     '882157285',
    'Денис Брюхарєв':       '8356737322',
    'Олексій Тихоненко':    '7083918297',
    'Ярослав Глуховецький': '7398315975',
    'Олександр Флоряк':     '7820509171',
    'Денис Местоян':        '8880314477',
    'Антон Нечипорук':      '8625011946',
    'Федір Козулін':        '8762578305',
    'Данііл Коренков':      '6897495788',
    'Семен Оленіч':         '8789635065',
    'Владислав Смирнов':    '8679654304',
    'Єгор Рубцов':          '8742796502',
    'Олександр Каулько':    '442293112',
}

# Telegram ID → Kommo user ID (для виставлення відповідального в CRM)
KOMMO_MANAGER_IDS = {
    '882157285':   13887159,   # Тимур Мартиросян
    '8356737322':  14680252,   # Денис Брюхарєв
    '7083918297':  14887016,   # Олексій Тихоненко
    '7398315975':  15064776,   # Ярослав Глуховецький
    '7820509171':  15064956,   # Олександр Флоряк
    '8880314477':  15248748,   # Денис Местоян
    '8625011946':  11335127,   # Антон Нечипорук
    '8762578305':  15375644,   # Федір Козулін
    '6897495788':   9469203,   # Данііл Коренков
    '8789635065':  14996852,   # Семен Оленіч
    '8679654304':  15248724,   # Владислав Смирнов
    '8742796502':   8265878,   # Єгор Рубцов
    '442293112':   15406088,   # Олександр Каулько
}

TIMEOUT_PERSONAL     = 120    # 2 хв  → перша особиста розсилка
TIMEOUT_WARN         = 300    # 5 хв  → попередження «ТЕРМІНОВО»
TIMEOUT_SOS          = 600    # 10 хв → SOS
TIMEOUT_REBROADCAST  = 1800   # 30 хв → повторна розсилка (до взяття)
SCHEDULER_TICK       = 10
SHEETS_REFRESH       = 60

# Пороги конверсії (якщо є оплати, AJ > 0)
CONV_UNLIMITED   = 10.0  # ≥ 10%          → необмежено
CONV_MAX5_MIN    =  5.0  # 5.0% – 9.9%   → max 5
CONV_MAX2_MIN    =  3.3  # 3.3% – 4.9%   → max 2
                         # < 3.3%         → поза чергою

# Пороги по к-ті взятих лідів (якщо оплат = 0, AJ = 0)
LEADS_UNLIM_MAX  = 10    # ≤ 10           → необмежено
LEADS_MAX5_MAX   = 20    # 11 – 20        → max 5
LEADS_MAX2_MAX   = 30    # 21 – 30        → max 2
                         # > 30           → поза чергою

MAX_LEADS_5      = 10
MAX_LEADS_2      = 5

# backward-compat aliases
CONV_LIMITED_MIN = CONV_MAX5_MIN
MAX_LEADS_MID    = MAX_LEADS_5

HOT_STATUSES = {
    '85731907':  'Гаряча заявка 🔥',
    '104159672': 'Кваліфікована заявка ⭐',
}
