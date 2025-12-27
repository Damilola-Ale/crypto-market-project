# execution/candle_gate.py

import os
import json

CANDLE_FILE = "data/last_candles.json"


class CandleGate:
    def __init__(self):
        os.makedirs("data", exist_ok=True)
        self.last = {}
        if os.path.exists(CANDLE_FILE):
            with open(CANDLE_FILE, "r") as f:
                self.last = json.load(f)

    def is_same_candle(self, symbol, timestamp):
        ts = timestamp.isoformat()
        return self.last.get(symbol) == ts

    def mark_candle(self, symbol, timestamp):
        self.last[symbol] = timestamp.isoformat()
        self._save()

    def _save(self):
        with open(CANDLE_FILE + ".tmp", "w") as f:
            json.dump(self.last, f, indent=2)
        os.replace(CANDLE_FILE + ".tmp", CANDLE_FILE)
