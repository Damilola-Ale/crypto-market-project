# execution/hourly_runner.py

from strategy.account_state import account_state
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

    pm = PositionManager()
    gate = CandleGate()

    for symbol in SYMBOLS:
        print(f"\n--- Processing {symbol} ---")

        try:
            # --------------------------------------------------
            # Fetch & maintain LONG-HISTORY data (authoritative)
            # --------------------------------------------------
            df = update_symbol(symbol)

            # --------------------------------------------------
            # Candle gate (single authority)
            # --------------------------------------------------
            last_ts = df.index[-1]

            if gate.is_same_candle(symbol, last_ts):
                print(f"[{symbol}] ⏭️ Same candle — skipped")
                continue

            gate.mark_candle(symbol, last_ts)

            # --------------------------------------------------
            # Generate signal (indicators expect deep history)
            # --------------------------------------------------
            # --------------------------------------------------
            # Generate indicators + signals INTO df
            # --------------------------------------------------
            df = generate_signal(df)

            # --------------------------------------------------
            # Lifecycle decision (single authority)
            # --------------------------------------------------
            event = pm.update(df, symbol)

            if event:
                print(f"[{symbol}] 📌 EVENT → {event['state']}")
            else:
                print(f"[{symbol}] ⚪ No action")

            print(f"[{symbol}] ✅ OK | Candles: {len(df)}")

        except Exception as e:
            print(f"[{symbol}] ❌ FAILED → {e}")

    print("\n=== EXECUTION CYCLE COMPLETE ===")
