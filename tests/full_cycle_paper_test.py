import os
import sys

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if ROOT not in sys.path:
    sys.path.insert(0, ROOT)

import shutil
import pandas as pd
from datetime import datetime, timedelta

from strategy.account_state import account_state
from strategy.lifecycle import PositionManager
from strategy.risk import compute_risk
from execution.candle_gate import is_new_candle

# ==========================================================
# TEST CONFIG
# ==========================================================
SYMBOL = "TESTUSDT"
START_PRICE = 100.0
CANDLES = 72  # 3 days of hourly candles


# ==========================================================
# HELPERS
# ==========================================================
def reset_runtime():
    """Hard reset all runtime state."""
    for path in [
        "data/positions",
        "data/runtime",
    ]:
        if os.path.exists(path):
            shutil.rmtree(path)


def build_test_df():
    """
    Build a deterministic OHLCV DataFrame with:
    - one LONG entry
    - one exit via opposite signal
    """
    rows = []
    ts = datetime.utcnow()

    price = START_PRICE

    for i in range(CANDLES):
        # Simple price walk
        price += 1 if i < 24 else -1

        rows.append({
            "timestamp": ts,
            "open": price - 0.5,
            "high": price + 1,
            "low": price - 1,
            "close": price,
            "volume": 1000,

            # Signals
            "final_signal": (
                1 if i == 5 else     # open LONG
                -1 if i == 40 else   # exit LONG
                0
            ),

            # Risk fields
            "stop_loss": price - 5,
            "trade_quality": 0.7,
            "confidence": 0.8,

            # Regime
            "Regime_norm": 0.8,
            "EMA_Ribbon_Score": 0.6,
            "HTF_Score": 0.5,
        })

        ts += timedelta(hours=1)

    df = pd.DataFrame(rows)
    df["timestamp"] = pd.to_datetime(df["timestamp"], utc=True)
    df = df.set_index("timestamp")

    return df


# ==========================================================
# TEST
# ==========================================================
def test_full_cycle():
    print("\n=== FULL CYCLE PAPER TEST ===")

    reset_runtime()

    pm = PositionManager()
    df = build_test_df()

    opened = False
    closed = False

    for i in range(len(df)):
        sub = df.iloc[: i + 1]
        ts = sub.index[-1]

        # Candle gate
        if not is_new_candle(SYMBOL, ts):
            continue

        # Inject risk only on signal
        if int(sub.iloc[-1]["final_signal"]) != 0:
            risk = compute_risk(
                sub,
                account_equity=10_000,
                realized_pnl_today=account_state.realized_pnl_today
                )
            for k, v in risk.items():
                sub.loc[sub.index[-1], k] = v

        event = pm.update(sub, SYMBOL)

        if event:
            if event["state"] == "OPEN":
                opened = True
                print(f"🟢 OPEN @ {event['entry_price']}")

            elif event["state"] == "CLOSED":
                closed = True
                print(
                    f"🔴 CLOSE @ {event['exit']['exit_price']} | "
                    f"Reason: {event['exit']['exit_reason']}"
                )

    # ======================================================
    # ASSERTIONS
    # ======================================================
    assert opened, "Position was never opened"
    assert closed, "Position was never closed"
    assert SYMBOL not in pm.positions, "Position still open at end"

    print("✅ FULL CYCLE TEST PASSED")


if __name__ == "__main__":
    test_full_cycle()
