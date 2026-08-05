"""
Разова ручна перевірка (нічого не змінює, тільки друкує звіт): проганяє
ВСІ заявки з гарячого статусу Kommo (той самий фільтр, що й sync_from_kommo/
"🔄 Синхронізація") і для кожної показує, чи джерело "Реактивация".

На відміну від check_held_reactivation.py — перевіряє не тільки "заморожені"
(held_leads), а взагалі ВСІ заявки, які Kommo зараз повертає по гарячому
статусу (те саме число, що показує кнопка "🔄 Синхронізація" як
"Вже були в системі" + "Додано нових").

Запуск на сервері (з кореня проєкту, з тим самим .env що й бот):
    python3 check_all_reactivation.py
"""
import asyncio

from kommo import (
    AMO_SUBDOMAIN, AMO_TOKEN, AMO_PIPELINE_ID, AMO_HOT_STATUS_ID,
    _get_session, _fetch_leads_page, _KommoFatalError, _KommoRateLimited, _KommoServerError,
    is_lead_reactivation, close_session,
)


async def main():
    if not AMO_TOKEN:
        print("AMO_TOKEN не задано — нема чим перевіряти")
        return

    url     = f"https://{AMO_SUBDOMAIN}.kommo.com/api/v4/leads"
    headers = {"Authorization": f"Bearer {AMO_TOKEN}"}
    session = await _get_session()

    page  = 1
    total = 0
    reactivation_found = []

    while True:
        params = {
            "filter[statuses][0][pipeline_id]": AMO_PIPELINE_ID,
            "filter[statuses][0][status_id]":   AMO_HOT_STATUS_ID,
            "limit": 250,
            "page":  page,
        }
        try:
            data = await _fetch_leads_page(session, url, headers, params)
        except _KommoFatalError as e:
            print(f"HTTP {e.status} — зупиняємось")
            break
        except (_KommoRateLimited, _KommoServerError):
            print(f"сторінка {page} не завантажена після 3 спроб — зупиняємось")
            break

        if data is None:
            break  # HTTP 204 — кінець пагінації

        leads = data.get("_embedded", {}).get("leads", [])
        if not leads:
            break

        for lead in leads:
            total += 1
            lead_id  = str(lead["id"])
            is_react = is_lead_reactivation(lead)  # список вже містить custom_fields_values
            mark = "🔴 РЕАКТИВАЦІЯ" if is_react else "⚪ звичайна"
            print(f"  {mark} — {lead_id}")
            if is_react:
                reactivation_found.append(lead_id)

        if len(leads) < 250:
            break
        page += 1

    print(f"\nУсього заявок перевірено: {total}")
    print(f"Знайдено реактивацій: {len(reactivation_found)}")
    if reactivation_found:
        print("lead_id:", ", ".join(reactivation_found))

    await close_session()


if __name__ == '__main__':
    asyncio.run(main())
