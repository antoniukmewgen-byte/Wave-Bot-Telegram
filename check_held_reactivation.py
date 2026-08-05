"""
Разова ручна перевірка (нічого не змінює, тільки друкує звіт): чи є серед
заявок, які зараз "заморожені" в held_leads (чекають ранку клієнта), такі,
де джерело "Источник" = "Реактивация" — тобто мали б пропустити ранкову
перевірку і йти в чергу одразу, а не чекати 9:00.

Працює на функціях, які вже були в проєкті ДО фіксу з реактивацією
(get_lead_info, is_lead_reactivation), тому запускати можна навіть до
деплою нового коду — просто щоб побачити, чи взагалі є що виправляти.

Запуск на сервері (з кореня проєкту, з тим самим .env що й бот):
    python3 check_held_reactivation.py
"""
import asyncio

from db import init_db, get_all_held_leads, close_db
from kommo import get_lead_info, is_lead_reactivation, close_session


async def main():
    await init_db()
    held = await get_all_held_leads()
    print(f"Усього заявок у held_leads: {len(held)}\n")

    reactivation_found = []
    for row in held:
        lead_id = row['lead_id']
        tz_name = row['timezone']
        info = await get_lead_info(lead_id)
        if info is None:
            print(f"  ⚠️  {lead_id} ({tz_name}) — не вдалось отримати дані з Kommo")
            continue
        is_react = is_lead_reactivation(info)
        mark = "🔴 РЕАКТИВАЦІЯ" if is_react else "⚪ звичайна"
        print(f"  {mark} — {lead_id} ({tz_name})")
        if is_react:
            reactivation_found.append(lead_id)

    print(f"\nЗнайдено реактивацій серед заморожених: {len(reactivation_found)}")
    if reactivation_found:
        print("lead_id:", ", ".join(reactivation_found))

    await close_session()
    await close_db()


if __name__ == '__main__':
    asyncio.run(main())
