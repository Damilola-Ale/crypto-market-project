# strategy/lifecycle.py

import os
import json
from datetime import datetime
from typing import Optional
import pandas as pd

from execution.notifier import TelegramNotifier
from strategy.account_state import account_state

POSITIONS_DIR = "data/positions"
POSITIONS_FILE = os.path.join(POSITIONS_DIR, "open_positions.json")
POSITION_EXPIRY_CANDLES = 48


class PositionManager:
    def __init__(self):
        self.positions = {}
        self.notifier = TelegramNotifier()  # 🔔 notifier initialized once
        os.makedirs(POSITIONS_DIR, exist_ok=True)
        self._load()

    # --------------------------------------------------
    def update(self, df: pd.DataFrame, symbol: str) -> Optional[dict]:
        latest = df.iloc[-1]
        timestamp = latest.name
        price = float(latest["close"])
        final_signal = int(latest.get("final_signal", 0))

        position = self.positions.get(symbol)

        # ================= ENTRY =================
        if position is None:
            if final_signal == 0:
                return None

            allowed, reason = account_state.can_open()
            if not allowed:
                return {
                    "state": "BLOCKED",
                    "reason": reason,
                    "symbol": symbol,
                    "timestamp": timestamp.isoformat(),
                }

            return self._open(symbol, final_signal, price, timestamp, latest)

        # ================= EXIT =================
        exit_reason = self._check_exit(position, latest, price)
        if exit_reason:
            return self._close(symbol, price, timestamp, exit_reason)

        return None

    # --------------------------------------------------
    def _open(self, symbol, direction, price, ts, row):
        position = {
            "symbol": symbol,
            "direction": direction,
            "entry_price": price,
            "entry_time": ts.isoformat(),
            "stop_loss": row.get("stop_loss"),
            "state": "OPEN",
            "exit": {},
        }

        self.positions[symbol] = position
        account_state.on_position_open()
        self._save()

        # 🔔 TELEGRAM OPEN ALERT
        self.notifier.send_text(
            f"🚀 OPEN {symbol}\n"
            f"Direction: {'LONG' if direction == 1 else 'SHORT'}\n"
            f"Entry: {price}\n"
            f"Time: {ts.isoformat()}"
        )

        return position

    # --------------------------------------------------
    def _close(self, symbol, price, ts, reason):
        position = self.positions.pop(symbol)

        direction = position["direction"]
        entry = position["entry_price"]
        pnl = (price - entry) * direction

        position["state"] = "CLOSED"
        position["exit"] = {
            "exit_price": price,
            "exit_time": ts.isoformat(),
            "exit_reason": reason,
            "pnl": pnl,
        }

        account_state.on_position_close(pnl)
        self._save()

        # 🔔 TELEGRAM CLOSE ALERT
        self.notifier.send_text(
            f"❌ CLOSE {symbol}\n"
            f"Reason: {reason}\n"
            f"Exit: {price}\n"
            f"PnL: {pnl:.2f}\n"
            f"Time: {ts.isoformat()}"
        )

        return position

    # --------------------------------------------------
    def _check_exit(self, pos, row, price):
        stop = pos.get("stop_loss")
        direction = pos["direction"]

        if stop:
            if direction == 1 and price <= stop:
                return "STOP"
            if direction == -1 and price >= stop:
                return "STOP"

        if int(row.get("final_signal", 0)) == -direction:
            return "OPPOSITE_SIGNAL"

        age = (
            row.name - datetime.fromisoformat(pos["entry_time"])
        ).total_seconds() / 3600

        if age >= POSITION_EXPIRY_CANDLES:
            return "TIME_EXPIRY"

        return None

    # --------------------------------------------------
    def _load(self):
        if os.path.exists(POSITIONS_FILE):
            with open(POSITIONS_FILE, "r") as f:
                self.positions = json.load(f)

    def _save(self):
        with open(POSITIONS_FILE + ".tmp", "w") as f:
            json.dump(self.positions, f, indent=2)
        os.replace(POSITIONS_FILE + ".tmp", POSITIONS_FILE)
