from flask import Flask, request, abort
import os
import json

from execution.notifier import TelegramNotifier
from execution.hourly_runner import run_hourly, SYMBOLS

app = Flask(__name__)

# ==================================================
# CORE ENDPOINTS
# ==================================================

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


# ==================================================
# DEBUG / OBSERVABILITY ENDPOINTS
# ==================================================

@app.route("/debug/env")
def debug_env():
    return {
        "BOT_TOKEN_SET": bool(os.getenv("TELEGRAM_BOT_TOKEN")),
        "CHAT_ID_SET": bool(os.getenv("TELEGRAM_CHAT_ID")),
        "RUN_KEY_SET": bool(os.getenv("RUN_KEY")),
    }


@app.route("/debug/data")
def debug_data_health():
    """
    Confirms that market data is being fetched correctly
    """
    from data_pipeline.updater import update_symbol

    summary = {}
    for symbol in SYMBOLS:
        try:
            df = update_symbol(symbol)
            summary[symbol] = {
                "candles": len(df),
                "first": str(df.index[0]),
                "last": str(df.index[-1]),
            }
        except Exception as e:
            summary[symbol] = {"error": str(e)}

    return summary, 200


@app.route("/debug/run")
def debug_last_run():
    """
    GUARANTEE #1 — proves /run executed
    """
    path = "data/last_run.json"
    if not os.path.exists(path):
        return {"exists": False}

    with open(path, "r") as f:
        return {"exists": True, "run": json.load(f)}


@app.route("/debug/candles")
def debug_candle_state():
    """
    GUARANTEE #2 — shows candle gating state
    """
    path = "data/last_candles.json"
    if not os.path.exists(path):
        return {"exists": False, "candles": {}}

    with open(path, "r") as f:
        return {"exists": True, "candles": json.load(f)}


@app.route("/debug/gate")
def debug_gate_log():
    """
    Shows why symbols were allowed or skipped
    """
    path = "data/candle_gate.json"
    if not os.path.exists(path):
        return {"exists": False}

    with open(path, "r") as f:
        return {"exists": True, "gate": json.load(f)}


@app.route("/debug/positions")
def debug_positions():
    """
    Shows open positions known to the system
    """
    path = "data/positions/open_positions.json"
    if not os.path.exists(path):
        return {"exists": False, "positions": {}}

    with open(path, "r") as f:
        return {"exists": True, "positions": json.load(f)}


# ==================================================
# ENTRYPOINT
# ==================================================

if __name__ == "__main__":
    port = int(os.environ.get("PORT", 5000))
    app.run(host="0.0.0.0", port=port)
