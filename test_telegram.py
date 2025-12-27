import os
import requests

BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")
API_URL = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"

response = requests.post(API_URL, data={
    "chat_id": CHAT_ID,
    "text": "✅ Telegram bot test message",
    "parse_mode": "Markdown"
})
print(response.json())
