"""
Швидкий тест: симулює webhook від AmoCRM.
Запуск: python test_send.py
Бот має бути запущений (python main.py)
"""
import requests

# Міняйте ці значення для тесту
LEAD_ID   = '99999001'
STATUS_ID = '85731907'   # Горяча заявка

resp = requests.post(
    'http://localhost:8080/webhook/movenation',
    data={
        'leads[status][0][id]':        LEAD_ID,
        'leads[status][0][status_id]': STATUS_ID,
    }
)
print(f"Статус: {resp.status_code} | Відповідь: {resp.json()}")
