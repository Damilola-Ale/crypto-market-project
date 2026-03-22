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
    "LINKUSDT", "AAVEUSDT", "BTCUSDT", "TRXUSDT", "FXSUSDT", "FILUSDT", "RUNEUSDT", "ARBUSDT", "OPUSDT", 
    "UNIUSDT", "XTZUSDT", "ADAUSDT", "SUIUSDT", "CRVUSDT", "MKRUSDT", "XLMUSDT", "BIOUSDT", "JTOUSDT"
]

SIGNAL_STORE = "data/signals.json"
SIGNAL_MEMORY_FILE = "data/last_signal_processed.json"


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

    # --------------------------------
    # LOAD SIGNAL MEMORY
    # --------------------------------
    if os.path.exists(SIGNAL_MEMORY_FILE):
        with open(SIGNAL_MEMORY_FILE, "r") as f:
            last_processed = json.load(f)
    else:
        last_processed = {}

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
            # SIGNAL GENERATION
            # --------------------------------
            df = generate_signal(df, htf_df)

            # --------------------------------
            # SIGNAL DETECTION (FIXED)
            # --------------------------------
            recent = df.iloc[-3:]  # lookback window (2–5 is fine)

            signals_recent = recent[recent['final_signal'] != 0]

            if not signals_recent.empty:
                latest_signal = signals_recent.iloc[-1]

                signal = int(latest_signal['final_signal'])
                price = float(latest_signal['close'])
                signal_ts = latest_signal.name

                print(f"[SIGNAL] FOUND recent signal={signal} at {signal_ts}")
            else:
                signal = 0
                price = float(df.iloc[-1]["close"])
                signal_ts = df.index[-1]

                print(f"[SIGNAL] no recent signal")

            # --------------------------------
            # SIGNAL DEDUPLICATION (CRITICAL FIX)
            # --------------------------------
            if signal != 0:
                key = f"{symbol}_{signal_ts.isoformat()}"

                if last_processed.get(symbol) == key:
                    print("[SIGNAL] already processed → skipping")
                    signal = 0  # prevent reuse
                else:
                    last_processed[symbol] = key
            
            # --------------------------------
            # CANDLE GATE (FIXED)
            # --------------------------------
            gate_ts = signal_ts if signal != 0 else last_ts

            allowed, reason = gate.allow(symbol, gate_ts)

            if not allowed:
                print(f"[GATE] skipped → {reason}")
                continue

            print(f"[GATE] allowed using {'signal_ts' if signal != 0 else 'last_ts'}")

            # --------------------------------
            # STORE SIGNAL
            # --------------------------------
            signal_event = {
                "symbol": symbol,
                "timestamp": signal_ts.isoformat(),
                "price": price,
                "signal": signal
            }

            signals.append(signal_event)

            # --------------------------------
            # POSITION LIFECYCLE
            # --------------------------------
            event = pm.update(df, symbol, external_signal=signal, external_row=latest_signal if signal != 0 else df.iloc[-1])

            # --------------------------------
            # TELEGRAM ALERT (signal detection AFTER PM DECISION)
            # --------------------------------
            if signal != 0 and event and event.get("state") != "BLOCKED":
                notifier.send_signal(
                    symbol=symbol,
                    direction=signal,
                    timestamp=signal_ts.isoformat(),
                    price=price,
                    stop_loss=latest_signal.get("stop_loss") if signal != 0 else None,
                    trade_quality=latest_signal.get("ASYM_SCORE") if signal != 0 else None
                )
                print("[TELEGRAM] signal alert sent (post-PM check)")

            if event:
                print(f"[POSITION] event → {event}")

                # 🔥 CRITICAL VISIBILITY FIX
                if isinstance(event, dict) and event.get("state") == "BLOCKED":
                    print(f"[BLOCKED] reason={event.get('reason')}")

            else:
                print("[POSITION] no change")

            # --------------------------------
            # MARK CANDLE (only if something meaningful happened)
            # --------------------------------
            if signal != 0 or event:
                gate.mark_candle(symbol, gate_ts)
                print(f"[GATE] candle marked → signal={signal}, event={event is not None}")
            else:
                print(f"[GATE] candle NOT marked → no signal, no position change")

            print(f"[{symbol}] COMPLETE")

        except Exception as e:

            print(f"[ERROR] {symbol} → {e}")

    # --------------------------------
    # STORE SIGNAL HISTORY
    # --------------------------------
    with open(SIGNAL_STORE, "w") as f:
        json.dump(signals, f, indent=2)

    print("\nSignals stored:", len(signals))

    # --------------------------------
    # SAVE SIGNAL MEMORY
    # --------------------------------
    with open(SIGNAL_MEMORY_FILE + ".tmp", "w") as f:
        json.dump(last_processed, f, indent=2)
    os.replace(SIGNAL_MEMORY_FILE + ".tmp", SIGNAL_MEMORY_FILE)

    print("\n=== EXECUTION COMPLETE ===\n")