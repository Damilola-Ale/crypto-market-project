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
    notifier.send_text("✅ Telegram connected to Render successfully")
    return {"status": "telegram_test_sent"}, 200

@app.route("/debug-env")
def debug_env():
    return {
        "BOT": bool(os.getenv("TELEGRAM_BOT_TOKEN")),
        "CHAT": bool(os.getenv("TELEGRAM_CHAT_ID"))
    }

@app.route("/debug-candles")
def debug_candles():
    from data_pipeline.updater import update_symbol
    summary = {}
    for symbol in SYMBOLS:
        try:
            df = update_symbol(symbol)
            summary[symbol] = {
                "candles": len(df),
                "first": str(df.index[0]),
                "last": str(df.index[-1])
            }
        except Exception as e:
            summary[symbol] = {"error": str(e)}
    return summary, 200

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
