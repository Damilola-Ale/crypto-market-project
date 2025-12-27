# execution/notifier.py
import requests
import os
from typing import Optional

TELEGRAM_BOT_TOKEN = os.getenv("TELEGRAM_BOT_TOKEN")
TELEGRAM_CHAT_ID = os.getenv("TELEGRAM_CHAT_ID")     # or import from config/settings.py
TELEGRAM_API_URL = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"

class TelegramNotifier:
    def __init__(self, bot_token: Optional[str] = None, chat_id: Optional[str] = None):
        self.bot_token = bot_token or TELEGRAM_BOT_TOKEN
        self.chat_id = chat_id or TELEGRAM_CHAT_ID
        self.api_url = f"https://api.telegram.org/bot{self.bot_token}/sendMessage"
        if not self.bot_token or not self.chat_id:
            raise ValueError("Telegram bot token and chat ID must be set.")

    def send_signal(
        self,
        symbol: str,
        direction: int,
        timestamp,
        price: Optional[float] = None,
        stop_loss: Optional[float] = None,
        trade_quality: Optional[float] = None
    ):
        """
        Sends a formatted trade signal to Telegram.
        """
        if direction == 1:
            dir_text = "LONG 📈"
        elif direction == -1:
            dir_text = "SHORT 📉"
        else:
            dir_text = "FLAT ⬜"

        msg = f"*{symbol}* → *{dir_text}*\n"
        msg += f"Time (UTC): `{timestamp}`\n"
        if price is not None:
            msg += f"Entry Price: `{price}`\n"
        if stop_loss is not None:
            msg += f"ATR Stop: `{stop_loss}`\n"
        if trade_quality is not None:
            msg += f"Trade Quality: `{trade_quality:.2f}`\n"

        payload = {
            "chat_id": self.chat_id,
            "text": msg,
            "parse_mode": "Markdown"
        }

        try:
            response = requests.post(self.api_url, data=payload, timeout=10)
            response.raise_for_status()
        except Exception as e:
            print(f"[TelegramNotifier] Failed to send message for {symbol}: {e}")
