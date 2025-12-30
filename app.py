from flask import Flask, request, abort
from execution.notifier import TelegramNotifier
from execution.hourly_runner import run_hourly
import os

app = Flask(__name__)

@app.route("/health")
def health():
    return {"status": "alive"}, 200

@app.route("/run")
def run():
    if request.args.get("key") != os.getenv("RUN_KEY", "local"):
        abort(403)

    run_hourly()
    return {"status": "executed"}, 200

@app.route("/test-telegram")
def test_telegram():
    notifier = TelegramNotifier()
    notifier.send_signal("✅ Telegram connected to Render successfully")
    return {"status": "telegram_test_sent"}, 200

import os

@app.route("/debug-env")
def debug_env():
    return {
        "BOT": bool(os.getenv("TELEGRAM_BOT_TOKEN")),
        "CHAT": bool(os.getenv("TELEGRAM_CHAT_ID"))
    }


if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
