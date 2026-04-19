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
    if request.args.get("key") != os.getenv("RUN_KEY", "local"):
        abort(403)

    from data_pipeline.updater import update_symbol

    summary = {}

    for symbol in SYMBOLS:

        try:
            path_ltf  = f"data/cache/{symbol}_1h.parquet"
            path_htf  = f"data/cache/{symbol}_4h.parquet"
            path_lltf = f"data/cache/{symbol}_5m.parquet"

            def _read(path):
                if not os.path.exists(path):
                    return None
                df = pd.read_parquet(path)
                df.index = pd.to_datetime(df.index, utc=True)
                return df

            df      = _read(path_ltf)
            htf_df  = _read(path_htf)
            lltf_df = _read(path_lltf)

            summary[symbol] = {
                "ltf_candles":  len(df)      if df      is not None else "missing",
                "htf_candles":  len(htf_df)  if htf_df  is not None else "missing",
                "lltf_candles": len(lltf_df) if lltf_df is not None else "missing",
                "ltf_first":  str(df.index[0])      if df      is not None else None,
                "ltf_last":   str(df.index[-1])     if df      is not None else None,
                "htf_first":  str(htf_df.index[0])  if htf_df  is not None else None,
                "htf_last":   str(htf_df.index[-1]) if htf_df  is not None else None,
                "lltf_first": str(lltf_df.index[0]) if lltf_df is not None else None,
                "lltf_last":  str(lltf_df.index[-1])if lltf_df is not None else None,
            }

        except Exception as e:

            summary[symbol] = {"error": str(e)}

    return summary, 200


@app.route("/debug/signals")
def debug_signals():

    path = "data/signals.json"

    if not os.path.exists(path):
        return {"exists": False}

    with open(path) as f:
        return {"exists": True, "signals": json.load(f)}


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
    
@app.route("/test-pipeline")
def test_pipeline():
    if request.args.get("key") != os.getenv("RUN_KEY", "local"):
        abort(403)

    import threading

    def run_test():
        from execution.notifier import TelegramNotifier
        from data_pipeline.fetcher import fetch_ohlcv
        from datetime import datetime, timezone, timedelta
        import pandas as pd
        import os

        notifier = TelegramNotifier()
        symbol = "LDOUSDT"
        os.makedirs("data/cache", exist_ok=True)

        notifier.send_text(
            f"🧪 *PIPELINE TEST STARTED*\n"
            f"Symbol: `{symbol}`\n"
            f"Plan: download 800×1h, equivalent 4h+5m, then increment 200×1h"
        )

        try:
            now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)

            # ── PHASE 1: Download 800 1h candles ──────────────────────
            warmup_end   = now - timedelta(hours=200)
            warmup_start = warmup_end - timedelta(hours=800)

            notifier.send_text(
                f"📥 *PHASE 1: Downloading 800×1h warmup*\n"
                f"from `{warmup_start}`\n"
                f"to   `{warmup_end}`"
            )

            df_1h = fetch_ohlcv(symbol, start=warmup_start, end=warmup_end, interval="1h", limit=1000, verbose=False)

            notifier.send_text(
                f"✅ *1H WARMUP DONE*\n"
                f"rows=`{len(df_1h)}`\n"
                f"first=`{df_1h.index[0]}`\n"
                f"last=`{df_1h.index[-1]}`"
            )

            # ── 4h equivalent ─────────────────────────────────────────
            notifier.send_text(f"📥 *Downloading 4h warmup*")
            df_4h = fetch_ohlcv(symbol, start=warmup_start, end=warmup_end, interval="4h", limit=1000, verbose=False)
            notifier.send_text(f"✅ *4H WARMUP DONE* rows=`{len(df_4h)}`")

            # ── 5m equivalent ─────────────────────────────────────────
            notifier.send_text(f"📥 *Downloading 5m warmup* (this is the big one)")
            df_5m = fetch_ohlcv(symbol, start=warmup_start, end=warmup_end, interval="5m", limit=1000, verbose=False)
            notifier.send_text(f"✅ *5M WARMUP DONE* rows=`{len(df_5m)}`")

            # ── Save warmup parquets ───────────────────────────────────
            df_1h.to_parquet("data/cache/LDOUSDT_1h.parquet")
            df_4h.to_parquet("data/cache/LDOUSDT_4h.parquet")
            df_5m.to_parquet("data/cache/LDOUSDT_5m.parquet")

            notifier.send_text(
                f"💾 *WARMUP PARQUETS SAVED*\n"
                f"1h=`{len(df_1h)}` 4h=`{len(df_4h)}` 5m=`{len(df_5m)}`"
            )

            # ── PHASE 2: Increment 200×1h candles one at a time ───────
            notifier.send_text(
                f"🔄 *PHASE 2: Incrementing 200×1h candles*\n"
                f"Simulating live hourly updates"
            )

            increment_start = warmup_end
            increment_end   = now

            # fetch all 200 at once so we can iterate
            df_increment_1h = fetch_ohlcv(symbol, start=increment_start, end=increment_end, interval="1h", limit=1000, verbose=False)
            df_increment_4h = fetch_ohlcv(symbol, start=increment_start, end=increment_end, interval="4h", limit=1000, verbose=False)
            df_increment_5m = fetch_ohlcv(symbol, start=increment_start, end=increment_end, interval="5m", limit=1000, verbose=False)

            notifier.send_text(
                f"📦 *INCREMENT DATA FETCHED*\n"
                f"1h=`{len(df_increment_1h)}` 4h=`{len(df_increment_4h)}` 5m=`{len(df_increment_5m)}`"
            )

            # reload current parquets
            base_1h = pd.read_parquet("data/cache/LDOUSDT_1h.parquet")
            base_4h = pd.read_parquet("data/cache/LDOUSDT_4h.parquet")
            base_5m = pd.read_parquet("data/cache/LDOUSDT_5m.parquet")
            base_1h.index = pd.to_datetime(base_1h.index, utc=True)
            base_4h.index = pd.to_datetime(base_4h.index, utc=True)
            base_5m.index = pd.to_datetime(base_5m.index, utc=True)

            for i, (ts, row) in enumerate(df_increment_1h.iterrows()):

                # append this 1h candle
                new_1h = pd.DataFrame([row], index=[ts])
                base_1h = pd.concat([base_1h, new_1h])
                base_1h = base_1h[~base_1h.index.duplicated(keep="last")]

                # append all 4h candles up to this timestamp
                new_4h = df_increment_4h[df_increment_4h.index <= ts]
                base_4h = pd.concat([base_4h, new_4h])
                base_4h = base_4h[~base_4h.index.duplicated(keep="last")]

                # append all 5m candles within this 1h window
                if i == 0:
                    prev_ts = increment_start
                else:
                    prev_ts = df_increment_1h.index[i - 1]
                new_5m = df_increment_5m[
                    (df_increment_5m.index > prev_ts) &
                    (df_increment_5m.index <= ts)
                ]
                base_5m = pd.concat([base_5m, new_5m])
                base_5m = base_5m[~base_5m.index.duplicated(keep="last")]

                # save parquets
                base_1h.to_parquet("data/cache/LDOUSDT_1h.parquet")
                base_4h.to_parquet("data/cache/LDOUSDT_4h.parquet")
                base_5m.to_parquet("data/cache/LDOUSDT_5m.parquet")

                notifier.send_text(
                    f"📊 *CANDLE {i+1}/~200*\n"
                    f"1h ts=`{ts}`\n"
                    f"base_1h=`{len(base_1h)}` base_4h=`{len(base_4h)}` base_5m=`{len(base_5m)}`\n"
                    f"new_5m bars this hour=`{len(new_5m)}`"
                )

            notifier.send_text(
                f"✅ *PIPELINE TEST COMPLETE*\n"
                f"Symbol: `{symbol}`\n"
                f"Final 1h=`{len(base_1h)}` 4h=`{len(base_4h)}` 5m=`{len(base_5m)}`\n"
                f"Parquets saved and ready"
            )

        except Exception as e:
            import traceback
            notifier.send_text(
                f"💥 *PIPELINE TEST FAILED*\n"
                f"error=`{str(e)[:300]}`"
            )
            traceback.print_exc()

    thread = threading.Thread(target=run_test)
    thread.daemon = True
    thread.start()

    return {"status": "pipeline_test_started"}, 200
    
@app.route("/replay")
def replay():
    if request.args.get("key") != os.getenv("RUN_KEY", "local"):
        abort(403)

    from_ts = request.args.get("from")
    to_ts   = request.args.get("to")

    import threading
    from execution.replay_engine import fast_replay_all

    thread = threading.Thread(target=fast_replay_all, kwargs={
        "from_ts": from_ts,
        "to_ts": to_ts,
        "notify_trades": True
    })
    thread.daemon = True
    thread.start()

    return {"status": "replay_started"}, 200

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

    entries = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                try:
                    entries.append(json.loads(line))
                except json.JSONDecodeError:
                    pass

    return {"exists": True, "gate": entries}


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
