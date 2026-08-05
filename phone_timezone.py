"""
Визначення часового поясу клієнта за номером телефону — для фічі
"ранкові ліди" (не видаємо заявку менеджеру, поки клієнту не настало 9:00
за його місцевим часом).

Правило (за вимогою): реальний часовий пояс визначаємо тільки для
українських і американських номерів. Для всіх інших країн примусово
підставляємо "America/New_York" (незалежно від фактичного поясу клієнта).
"""
import logging
from datetime import datetime
from typing import Optional
from zoneinfo import ZoneInfo

import phonenumbers
from phonenumbers import timezone as phonenumbers_timezone

logger = logging.getLogger(__name__)

FALLBACK_TZ = "America/New_York"

_REAL_TZ_REGIONS = {"UA", "US"}

# Кандидати регіонів для номерів БЕЗ "+" (Kommo/менеджери часто вводять
# телефон у "місцевому" форматі — з нулем на початку або взагалі без нього,
# напр. "0955322390" чи "955322390" замість "+380955322390"). phonenumbers
# не вміє визначити країну сам без "+", тому пробуємо розпарсити його як
# "національний" номер для кожного з наших двох реальних регіонів по черзі
# і беремо перший, що вийде валідним — той самий принцип .FirstOrDefault(),
# що й для вибору часового поясу нижче.
_DEFAULT_REGIONS = ("UA", "US")


def _parse_valid(phone: str, region: Optional[str]):
    """Повертає розпарсений валідний PhoneNumber або None (без винятків)."""
    try:
        parsed = phonenumbers.parse(phone, region)
    except phonenumbers.NumberParseException:
        return None
    return parsed if phonenumbers.is_valid_number(parsed) else None


def resolve_client_timezone(phone: Optional[str]) -> str:
    """
    Повертає IANA-ідентифікатор часового поясу клієнта.

    Для UA/US номерів — реальний часовий пояс (перший кандидат зі списку,
    аналог C# .FirstOrDefault()). Для решти країн (і якщо номер взагалі
    не вдалось розпарсити) — примусово FALLBACK_TZ.

    phonenumbers без "+" сам не вміє визначити країну. Тому пробуємо
    в такому порядку:
      1) як є (працює, якщо "+" вже є);
      2) підставляємо "+" спереду, якщо його немає — покриває номери, де
         код країни вже присутній, просто без "+" (напр. "380955322390"
         чи "19144460788");
      3) якщо й це не дало валідного номера (коду країни в рядку взагалі
         нема — напр. "955322390" чи "0955322390") — перебираємо UA/US як
         "припущений" регіон і беремо перший валідний варіант.
    """
    if not phone:
        return FALLBACK_TZ

    phone = phone.strip()

    parsed = _parse_valid(phone, None)

    if parsed is None and not phone.startswith('+'):
        # Обережно: сліпе "+" може випадково влучити в код зовсім іншої країни
        # (напр. "955322390" з "+" валідний як М'янма, хоча малось на увазі
        # "0955322390" без ведучого нуля). Тому приймаємо цей варіант тільки
        # якщо він влучає саме в UA/US — інші "випадкові" збіги нам не цікаві,
        # для них однаково буде FALLBACK_TZ, а UA/US номер такий збіг не
        # повинен перебивати.
        candidate = _parse_valid('+' + phone, None)
        if candidate is not None and phonenumbers.region_code_for_number(candidate) in _REAL_TZ_REGIONS:
            parsed = candidate

    if parsed is None:
        for region in _DEFAULT_REGIONS:
            parsed = _parse_valid(phone, region)
            if parsed is not None:
                break

    if parsed is None:
        logger.warning(f"resolve_client_timezone: не вдалось розпарсити номер '{phone}'")
        return FALLBACK_TZ

    region = phonenumbers.region_code_for_number(parsed)
    if region not in _REAL_TZ_REGIONS:
        return FALLBACK_TZ

    zones = phonenumbers_timezone.time_zones_for_number(parsed)
    if not zones:
        return FALLBACK_TZ

    return zones[0]


def is_client_morning(tz_name: str) -> bool:
    """True, якщо у клієнта зараз >= 9:00 за його місцевим часом."""
    try:
        now_local = datetime.now(ZoneInfo(tz_name))
    except Exception as e:
        logger.error(f"is_client_morning: невалідна таймзона '{tz_name}': {e}")
        now_local = datetime.now(ZoneInfo(FALLBACK_TZ))
    return now_local.hour >= 9
