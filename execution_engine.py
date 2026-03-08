# execution_engine.py
import os
import time
import pandas as pd

from data_pipeline.updater import update_symbol
from data_pipeline.validators import validate_ohlcv

from indicators.indicators import generate_signal

from execution.notifier import TelegramNotifier

from diagnostics import trade_diagnostics
from strategy.lifecycle import PositionManager
from strategy._risk import compute_risk

# ===================================================
# CONFIG ZEN, EGLD, BAND, AVAX, TRX, BTC, LINK, PAXG, BCH, LDO
# ===================================================
COINS = [
    "ZENUSDT", "EGLDUSDT", "BANDUSDT", "AVAXUSDT", "TRXUSDT",
    "BTCUSDT", "LINKUSDT", "PAXGUSDT", "BCHUSDT", "LDOUSDT"
]

INTERVAL = "1h"
CACHE_DIR = "data/cache"

MAX_RETRIES = 3
RETRY_DELAY = 5  # seconds


# ===================================================
# UTILS
# ===================================================
def _cache_path(symbol: str) -> str:
    return os.path.join(CACHE_DIR, f"{symbol}_{INTERVAL}.parquet")


# ===================================================
# ENGINE
# ===================================================
def run():
    print("=== CRYPTO MARKET PROJECT :: HOURLY EXECUTION ===")

    notifier = TelegramNotifier()
    position_manager = PositionManager()

    for symbol in COINS:
        print(f"\n--- Processing {symbol} ---")

        try:
            # -------------------------------------------------
            # 1. Fetch / update data (with retries)
            # -------------------------------------------------
            df = None
            for attempt in range(1, MAX_RETRIES + 1):
                try:
                    df = update_symbol(symbol)
                    if df is None or df.empty:
                        raise ValueError("Empty DataFrame returned")
                    break
                except Exception as e:
                    if attempt == MAX_RETRIES:
                        raise RuntimeError(
                            f"Fetch failed after {MAX_RETRIES} attempts: {e}"
                        )
                    time.sleep(RETRY_DELAY)

            # -------------------------------------------------
            # 2. Index normalization
            # -------------------------------------------------
            df.index = pd.to_datetime(df.index, utc=True)
            df = df.sort_index().asfreq(INTERVAL)

            # -------------------------------------------------
            # 3. Validate OHLCV
            # -------------------------------------------------
            validate_ohlcv(df, symbol)

            # -------------------------------------------------
            # 4. Candle gate (CRITICAL)
            # -------------------------------------------------
            latest_ts = df.index[-1]
            # if not is_new_candle(symbol, latest_ts):
            #     print(f"[{symbol}] ⏭️ Same candle — skipped")
            #     continue
            
            # -------------------------------------------------
            # 6. Signal generation
            # -------------------------------------------------
            df = generate_signal(df)

            latest = df.iloc[-1]
            signal = int(latest.get("final_signal", 0))

            # -------------------------------------------------
            # 7. Risk computation (ENTRY ONLY)
            # -------------------------------------------------
            if signal != 0:
                risk = compute_risk(df)
                for k, v in risk.items():
                    df.loc[df.index[-1], k] = v

            # -------------------------------------------------
            # 8. Persist dataset
            # -------------------------------------------------
            os.makedirs(CACHE_DIR, exist_ok=True)
            tmp = _cache_path(symbol) + ".tmp"
            df.to_parquet(tmp)
            os.replace(tmp, _cache_path(symbol))

            # -------------------------------------------------
            # 9. Lifecycle update (SINGLE AUTHORITY)
            # -------------------------------------------------
            event = position_manager.update(df, symbol)

            if event:
                state = event["state"]

                # ===============================
                # OPEN
                # ===============================
                if state == "OPEN":
                    notifier.send_signal(
                        symbol=symbol,
                        direction=event["direction"],
                        timestamp=event["entry_time"],
                        price=event["entry_price"],
                        stop_loss=event["stop_loss"],
                        trade_quality=event["quality"]["trade_quality"],
                    )

                    print(f"[{symbol}] 🟢 POSITION OPENED")

                # ===============================
                # CLOSED
                # ===============================
                elif state == "CLOSED":
                    notifier.send_signal(
                        symbol=symbol,
                        direction=0,
                        timestamp=event["exit"]["exit_time"],
                        price=event["exit"]["exit_price"],
                        stop_loss=None,
                        trade_quality=None,
                    )

                    print(
                        f"[{symbol}] 🔴 POSITION CLOSED | "
                        f"Reason: {event['exit']['exit_reason']}"
                    )

                # ===============================
                # BLOCKED
                # ===============================
                elif state == "BLOCKED":
                    trade_diagnostics.record(
                        df=df,
                        symbol=symbol,
                        blocked_reason=event["reason"],
                        cooldown_active=False,
                    )

                    print(
                        f"[{symbol}] ⛔ ENTRY BLOCKED | "
                        f"Reason: {event['reason']}"
                    )

            # -------------------------------------------------
            # 10. Diagnostics (ALWAYS)
            # -------------------------------------------------
            trade_diagnostics.record(
                df=df,
                symbol=symbol,
                blocked_reason=None,
                cooldown_active=False,
            )

            # -------------------------------------------------
            # 11. Summary
            # -------------------------------------------------
            print(f"[{symbol}] ✅ OK | Candles: {len(df)}")

        except Exception as e:
            print(f"[{symbol}] ❌ FAILED → {e}")

    print("\n=== EXECUTION CYCLE COMPLETE ===")


# ===================================================
# ENTRYPOINT
# ===================================================
if __name__ == "__main__":
    run()
