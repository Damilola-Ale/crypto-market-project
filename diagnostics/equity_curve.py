import os
import json
from datetime import datetime
from typing import List

# ==========================================================
# CONFIG
# ==========================================================
EQUITY_DIR = "diagnostics/equity"
EQUITY_FILE = os.path.join(EQUITY_DIR, "equity_curve.jsonl")

# ==========================================================
# DATA MODEL (JSONL per update)
# ==========================================================
# {
#   timestamp: ISO
#   equity: float
#   open_positions: int
#   realized_pnl: float
# }

# ==========================================================
# PUBLIC API
# ==========================================================
class EquityCurveLogger:
    def __init__(self):
        os.makedirs(EQUITY_DIR, exist_ok=True)

    def record(
        self,
        equity: float,
        open_positions: int,
        realized_pnl: float,
        timestamp: datetime,
    ):
        """
        Append a single equity snapshot.
        Safe, append-only, never raises.
        """
        try:
            record = {
                "timestamp": timestamp.isoformat(),
                "equity": float(equity),
                "open_positions": int(open_positions),
                "realized_pnl": float(realized_pnl),
            }

            with open(EQUITY_FILE, "a", encoding="utf-8") as f:
                f.write(json.dumps(record) + "\n")
        except Exception:
            return

    def load(self) -> List[dict]:
        """
        Load full equity curve (for plotting / analysis).
        """
        if not os.path.exists(EQUITY_FILE):
            return []
        records = []
        with open(EQUITY_FILE, "r") as f:
            for line in f:
                try:
                    records.append(json.loads(line))
                except Exception:
                    continue
        return records
