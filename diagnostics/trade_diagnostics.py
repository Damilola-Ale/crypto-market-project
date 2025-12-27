import os
import json
import pandas as pd
from datetime import datetime
from typing import Optional

# ==========================================================
# CONFIG
# ==========================================================
DIAGNOSTICS_DIR = "diagnostics/logs"
DIAGNOSTICS_FILE = os.path.join(DIAGNOSTICS_DIR, "trade_diagnostics.jsonl")

# Columns we attempt to capture (safe if missing)
BOOL_COLUMNS = [
    "EMA_Expansion",
    "HTF_Polarized",
    "ATR_Squeeze_OK",
    "commitment_ok",
    "EARLY_ENTRY_LONG",
    "EARLY_ENTRY_SHORT",
]

SCORE_COLUMNS = [
    "EMA_Ribbon_Score",
    "HTF_Score",
    "Regime_norm",
]

VOL_COLUMNS = [
    "ATR",
    "ATR_Expansion",
]

# ==========================================================
# PUBLIC API
# ==========================================================
def record(
    df: pd.DataFrame,
    symbol: str,
    blocked_reason: Optional[str] = None,
    cooldown_active: bool = False,
):
    """
    Record diagnostics for the latest candle.
    This function is READ-ONLY and NEVER raises.
    """
    try:
        if df is None or df.empty:
            return

        latest = df.iloc[-1]
        timestamp = latest.name

        if not isinstance(timestamp, pd.Timestamp):
            return

        final_signal = int(latest.get("final_signal", 0))

        direction = (
            "LONG" if final_signal == 1 else
            "SHORT" if final_signal == -1 else
            "FLAT"
        )

        record = {
            "timestamp": timestamp.isoformat(),
            "symbol": symbol,
            "final_signal": final_signal,
            "direction": direction,
            "blocked": final_signal == 0,
            "block_reason": blocked_reason,
            "cooldown_active": cooldown_active,
            "conditions": _extract_columns(latest, BOOL_COLUMNS),
            "scores": _extract_columns(latest, SCORE_COLUMNS),
            "volatility": _extract_columns(latest, VOL_COLUMNS),
        }

        _append_record(record)

    except Exception:
        # Diagnostics must NEVER interfere with execution
        return


# ==========================================================
# INTERNALS
# ==========================================================
def _extract_columns(row: pd.Series, columns):
    data = {}
    for col in columns:
        val = row.get(col)
        if pd.isna(val):
            data[col] = None
        elif isinstance(val, (bool, int, float)):
            data[col] = bool(val) if isinstance(val, bool) else float(val)
        else:
            data[col] = None
    return data


def _append_record(record: dict):
    os.makedirs(DIAGNOSTICS_DIR, exist_ok=True)
    with open(DIAGNOSTICS_FILE, "a", encoding="utf-8") as f:
        f.write(json.dumps(record) + "\n")
