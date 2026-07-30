"""
Швидкий тест: симулює webhook від AmoCRM.
Запуск: python test_send.py
Бот має бути запущений (python main.py)
"""
import os

import requests
from dotenv import load_dotenv

load_dotenv()

# Міняйте ці значення для тесту
LEAD_ID   = '99999001'
STATUS_ID = '85731907'   # Горяча заявка

WEBHOOK_SECRET = os.environ.get('WEBHOOK_SECRET', '').strip()
WEBHOOK_PATH   = os.environ.get('WEBHOOK_PATH', 'movenation')

url = f'http://localhost:8080/webhook/{WEBHOOK_PATH}'
if WEBHOOK_SECRET:
    url += f'?secret={WEBHOOK_SECRET}'

resp = requests.post(
    url,
    data={
        'leads[status][0][id]':        LEAD_ID,
        'leads[status][0][status_id]': STATUS_ID,
    }
)
print(f"Статус: {resp.status_code} | Відповідь: {resp.json()}")
