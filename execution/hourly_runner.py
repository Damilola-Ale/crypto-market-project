# execution/hourly_runner.py

import os
import json
from datetime import datetime, timezone

from data_pipeline.updater import update_symbol
from indicators.indicators import generate_signal
from strategy.lifecycle import PositionManager
from execution.candle_gate import CandleGate

SYMBOLS = [
    "BTCUSDT",
    "ETHUSDT",
    "XRPUSDT",
    "ADAUSDT",
    "BNBUSDT",
    "LINKUSDT",
    "SOLUSDT",
    "AVAXUSDT",
    "ETCUSDT",
    "VETUSDT",
]

TIMEFRAME = "1h"  # informational only


def run_hourly():
    print("=== CRYPTO MARKET PROJECT :: HOURLY EXECUTION ===")

    # --------------------------------------------------
    # 🔒 GUARANTEE #1 — RUN HEARTBEAT
    # --------------------------------------------------
    os.makedirs("data", exist_ok=True)
    with open("data/last_run.json", "w") as f:
        json.dump(
            {
                "ran_at": datetime.now(timezone.utc).isoformat(),
                "symbols": SYMBOLS,
            },
            f,
            indent=2,
        )

    pm = PositionManager()
    gate = CandleGate()

    for symbol in SYMBOLS:
        print(f"\n--- Processing {symbol} ---")

        try:
            # --------------------------------------------------
            # Fetch authoritative history
            # --------------------------------------------------
            df = update_symbol(symbol)
            last_ts = df.index[-1]

            # --------------------------------------------------
            # Candle gate
            # --------------------------------------------------
            allowed, reason = gate.allow(symbol, last_ts)

            if not allowed:
                print(f"[{symbol}] ⏭️ Skipped → {reason}")
                continue

            # --------------------------------------------------
            # Generate indicators & signal
            # --------------------------------------------------
            df = generate_signal(df)

            # --------------------------------------------------
            # Lifecycle decision
            # --------------------------------------------------
            event = pm.update(df, symbol)

            gate.mark_candle(symbol, last_ts)

            if event:
                print(f"[{symbol}] 📌 EVENT → {event}")
            else:
                print(f"[{symbol}] ⚪ No action")

            print(f"[{symbol}] ✅ OK | Candles: {len(df)}")

        except Exception as e:
            print(f"[{symbol}] ❌ FAILED → {e}")

    print("\n=== EXECUTION CYCLE COMPLETE ===")
