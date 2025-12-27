# strategy/risk.py
import numpy as np
import pandas as pd
from typing import Dict, Optional

# ==========================================================
# CONFIG
# ==========================================================
DEFAULT_RISK_PCT = 0.01
MAX_LEVERAGE = 5.0
MIN_STOP_DISTANCE = 1e-6
DAILY_LOSS_CAP_PCT = 0.03  # <<< NEW

# ==========================================================
# PUBLIC API
# ==========================================================
def compute_risk(
    df: pd.DataFrame,
    account_equity: float,
    realized_pnl_today: float,
    risk_pct: float = DEFAULT_RISK_PCT,
) -> Dict[str, Optional[float]]:

    try:
        # -------- DAILY LOSS CAP --------
        if realized_pnl_today <= -account_equity * DAILY_LOSS_CAP_PCT:
            return _blocked("daily_loss_cap_reached")

        if df is None or df.empty:
            return _blocked("empty_dataframe")

        row = df.iloc[-1]

        final_signal = int(row.get("final_signal", 0))
        if final_signal == 0:
            return _blocked("no_signal")

        entry_price = float(row.get("close"))
        stop_loss = row.get("stop_loss")

        if stop_loss is None or np.isnan(stop_loss):
            return _blocked("missing_stop_loss")

        stop_distance = abs(entry_price - stop_loss)
        if stop_distance <= MIN_STOP_DISTANCE:
            return _blocked("invalid_stop_distance")

        risk_amount = account_equity * risk_pct
        position_size = risk_amount / stop_distance
        notional = position_size * entry_price
        leverage = notional / account_equity

        if leverage > MAX_LEVERAGE:
            return {
                "allowed": False,
                "reason": "leverage_exceeds_cap",
                "leverage": leverage,
                "max_leverage": MAX_LEVERAGE,
            }

        return {
            "allowed": True,
            "direction": final_signal,
            "entry_price": entry_price,
            "stop_loss": float(stop_loss),
            "stop_distance": stop_distance,
            "risk_amount": risk_amount,
            "position_size": position_size,
            "notional": notional,
            "leverage": leverage,
            "trade_quality": _safe_float(row.get("trade_quality")),
            "confidence": _safe_float(row.get("confidence")),
        }

    except Exception as e:
        return _blocked(f"risk_exception: {e}")

# ==========================================================
# INTERNALS
# ==========================================================
def _blocked(reason: str) -> Dict[str, Optional[float]]:
    return {
        "allowed": False,
        "reason": reason,
    }

def _safe_float(val) -> Optional[float]:
    try:
        if val is None or np.isnan(val):
            return None
        return float(val)
    except Exception:
        return None
