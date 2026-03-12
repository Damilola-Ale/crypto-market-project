# execution/hourly_runner.py

import os
import json
from datetime import datetime, timezone

from data_pipeline.updater import update_symbol
from indicators.indicators import generate_signal
from strategy.lifecycle import PositionManager
from execution.candle_gate import CandleGate
from execution.notifier import TelegramNotifier

SYMBOLS = [
    "FETUSDT", "QNTUSDT", "EOSUSDT", "DOTUSDT", "SUIUSDT",
    "ADAUSDT", "LDOUSDT", "AVAXUSDT", "BANDUSDT", "BTCUSDT"
]

SIGNAL_STORE = "data/signals.json"


def run_hourly():

    print("\n==============================")
    print("CRYPTO MARKET PROJECT EXECUTION")
    print("==============================\n")

    os.makedirs("data", exist_ok=True)

    # --------------------------------
    # HEARTBEAT
    # --------------------------------
    with open("data/last_run.json", "w") as f:
        json.dump(
            {
                "ran_at": datetime.now(timezone.utc).isoformat(),
                "symbols": SYMBOLS
            },
            f,
            indent=2
        )

    pm = PositionManager()
    gate = CandleGate()
    notifier = TelegramNotifier()

    signals = []

    for symbol in SYMBOLS:

        print(f"\n======= {symbol} =======")

        try:

            # --------------------------------
            # FETCH DATA
            # --------------------------------
            df, htf_df = update_symbol(symbol)

            last_ts = df.index[-1]

            print(f"[DATA] LTF candles: {len(df)}")
            print(f"[DATA] HTF candles: {len(htf_df)}")

            # --------------------------------
            # CANDLE GATE
            # --------------------------------
            allowed, reason = gate.allow(symbol, last_ts)

            if not allowed:
                print(f"[GATE] skipped → {reason}")
                continue

            print("[GATE] new candle detected")

            # --------------------------------
            # SIGNAL GENERATION
            # --------------------------------
            df = generate_signal(df, htf_df)

            latest = df.iloc[-1]

            signal = int(latest.get("final_signal", 0))
            price = float(latest["close"])

            print(f"[SIGNAL] final_signal={signal}")

            # --------------------------------
            # STORE SIGNAL
            # --------------------------------
            signal_event = {
                "symbol": symbol,
                "timestamp": last_ts.isoformat(),
                "price": price,
                "signal": signal
            }

            signals.append(signal_event)

            # --------------------------------
            # TELEGRAM ALERT (signal detection)
            # --------------------------------
            if signal != 0:

                notifier.send_signal(
                    symbol=symbol,
                    direction=signal,
                    timestamp=last_ts.isoformat(),
                    price=price,
                    stop_loss=latest.get("stop_loss"),
                    trade_quality=latest.get("ASYM_SCORE")
                )

                print("[TELEGRAM] signal alert sent")

            # --------------------------------
            # POSITION LIFECYCLE
            # --------------------------------
            event = pm.update(df, symbol)

            if event:
                print(f"[POSITION] event → {event}")
            else:
                print("[POSITION] no change")

            # --------------------------------
            # MARK CANDLE
            # --------------------------------
            gate.mark_candle(symbol, last_ts)

            print(f"[{symbol}] COMPLETE")

        except Exception as e:

            print(f"[ERROR] {symbol} → {e}")

    # --------------------------------
    # STORE SIGNAL HISTORY
    # --------------------------------
    with open(SIGNAL_STORE, "w") as f:
        json.dump(signals, f, indent=2)

    print("\nSignals stored:", len(signals))

    print("\n=== EXECUTION COMPLETE ===\n")