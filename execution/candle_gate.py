# execution/candle_gate.py

import os
import json
from datetime import datetime, timezone

CANDLE_FILE = "data/last_candles.json"
GATE_LOG = "data/candle_gate.json"


class CandleGate:
    def __init__(self):
        os.makedirs("data", exist_ok=True)
        self.last = {}

        if os.path.exists(CANDLE_FILE):
            with open(CANDLE_FILE, "r") as f:
                self.last = json.load(f)

    # --------------------------------------------------
    def allow(self, symbol, timestamp):
        ts = timestamp.isoformat()
        previous = self.last.get(symbol)

        if previous == ts:
            self._log(symbol, ts, False, "SAME_CANDLE")
            return False, "SAME_CANDLE"

        self._log(symbol, ts, True, "NEW_CANDLE")
        return True, "NEW_CANDLE"

    # --------------------------------------------------
    def mark_candle(self, symbol, timestamp):
        self.last[symbol] = timestamp.isoformat()
        self._save()

    # --------------------------------------------------
    def _save(self):
        with open(CANDLE_FILE + ".tmp", "w") as f:
            json.dump(self.last, f, indent=2)
        os.replace(CANDLE_FILE + ".tmp", CANDLE_FILE)

    # --------------------------------------------------
    def _log(self, symbol, ts, allowed, reason):
        with open(GATE_LOG, "w") as f:
            json.dump(
                {
                    "symbol": symbol,
                    "timestamp": ts,
                    "allowed": allowed,
                    "reason": reason,
                    "logged_at": datetime.now(timezone.utc).isoformat(),
                },
                f,
                indent=2,
            )
