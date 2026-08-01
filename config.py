from pydantic import Field, ValidationError, field_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class _Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file='.env', env_file_encoding='utf-8', extra='ignore')

    bot_token: str = Field(min_length=1)
    sheets_id: str = Field(min_length=1)

    amo_subdomain: str = 'movenation'
    amo_token: str = ''
    amo_pipeline_id: str = '10815171'
    amo_hot_status_id: str = '85731907'
    amo_distributed_status_id: str = '98056412'
    amo_distributed_pipeline_id: str = '12703972'

    webhook_path: str = 'movenation'
    webhook_secret: str = ''

    sheet_name: str = 'План|Факт|Мотивація Мдж'
    google_creds: str = 'google_creds.json'

    admin_ids: str = ''

    @field_validator('webhook_secret', mode='before')
    @classmethod
    def _strip_secret(cls, v):
        return v.strip() if isinstance(v, str) else v


try:
    _settings = _Settings()
except ValidationError as e:
    missing = ', '.join(err['loc'][0].upper() for err in e.errors() if err['type'] == 'missing')
    if missing:
        raise RuntimeError(
            f"Відсутні обов'язкові змінні середовища: {missing}\n"
            f"Переконайтесь що файл .env існує і містить їх значення."
        ) from e
    raise


BOT_TOKEN                   = _settings.bot_token
AMO_SUBDOMAIN                = _settings.amo_subdomain
AMO_TOKEN                    = _settings.amo_token
AMO_PIPELINE_ID              = _settings.amo_pipeline_id
AMO_HOT_STATUS_ID            = _settings.amo_hot_status_id
AMO_DISTRIBUTED_STATUS_ID    = _settings.amo_distributed_status_id
AMO_DISTRIBUTED_PIPELINE_ID  = _settings.amo_distributed_pipeline_id
WEBHOOK_PATH   = _settings.webhook_path
# Спільний секрет для перевірки вхідних вебхуків Kommo (?secret=... в URL).
# Без нього /webhook/{WEBHOOK_PATH} приймає запити від БУДЬ-КОГО, хто дізнається шлях —
# див. попередження при старті бота, якщо змінна не задана.
WEBHOOK_SECRET = _settings.webhook_secret
SHEETS_ID      = _settings.sheets_id
SHEET_NAME     = _settings.sheet_name
GOOGLE_CREDS   = _settings.google_creds
ADMIN_IDS      = [i.strip() for i in _settings.admin_ids.split(',') if i.strip()]

COL_MANAGER    = 0
COL_YEAR       = 2
COL_MONTH      = 3
COL_PLAN       = 22   # W  — план обіг
COL_HOT_TAKEN  = 24   # Y  — взято гарячих лідів
COL_PAYMENTS   = 35   # AJ — к-ть проданих консультацій
COL_CONVERSION = 36   # AK — конверсія %


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

HOT_STATUSES = {
    '85731907':  'Гаряча заявка 🔥',
    '104159672': 'Кваліфікована заявка ⭐',
}
