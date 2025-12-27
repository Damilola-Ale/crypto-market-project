# strategy/account_state.py

import os
import json
from datetime import datetime, timezone

STATE_FILE = "data/account/account_state.json"

MAX_CONCURRENT_POSITIONS = 3
DAILY_LOSS_CAP = -0.03  # -3% of equity


class AccountState:
    def __init__(self):
        os.makedirs(os.path.dirname(STATE_FILE), exist_ok=True)
        self._load()

    # ==================================================
    # GATES
    # ==================================================
    def can_open(self) -> tuple[bool, str | None]:
        self._reset_if_new_day()

        if self.open_positions >= MAX_CONCURRENT_POSITIONS:
            return False, "max_concurrent_positions"

        if self.realized_pnl_today <= DAILY_LOSS_CAP * self.equity:
            return False, "daily_loss_cap"

        return True, None

    # ==================================================
    # EVENTS
    # ==================================================
    def on_position_open(self):
        self.open_positions += 1
        self._save()

    def on_position_close(self, pnl: float):
        """
        pnl = realized PnL in account currency
        """
        self._reset_if_new_day()

        self.open_positions = max(0, self.open_positions - 1)

        self.realized_pnl_today += pnl
        self.today_pnl = self.realized_pnl_today  # alias
        self.total_pnl += pnl
        self.equity += pnl

        self._save()

    # ==================================================
    # DAY RESET
    # ==================================================
    def _reset_if_new_day(self):
        today = datetime.now(timezone.utc).date().isoformat()

        if self.day != today:
            self.day = today
            self.realized_pnl_today = 0.0
            self.today_pnl = 0.0

    # ==================================================
    # PERSISTENCE
    # ==================================================
    def _load(self):
        if not os.path.exists(STATE_FILE):
            self.day = datetime.now(timezone.utc).date().isoformat()
            self.realized_pnl_today = 0.0
            self.today_pnl = 0.0
            self.total_pnl = 0.0
            self.open_positions = 0
            self.equity = 10_000.0  # default paper equity
            self._save()
            return

        with open(STATE_FILE, "r") as f:
            d = json.load(f)

        self.day = d["day"]
        self.realized_pnl_today = d.get("realized_pnl_today", d.get("today_pnl", 0.0))
        self.today_pnl = self.realized_pnl_today
        self.total_pnl = d["total_pnl"]
        self.open_positions = d["open_positions"]
        self.equity = d.get("equity", 10_000.0)

        self._reset_if_new_day()

    def _save(self):
        with open(STATE_FILE + ".tmp", "w") as f:
            json.dump(
                {
                    "day": self.day,
                    "realized_pnl_today": self.realized_pnl_today,
                    "today_pnl": self.today_pnl,
                    "total_pnl": self.total_pnl,
                    "open_positions": self.open_positions,
                    "equity": self.equity,
                },
                f,
                indent=2,
            )
        os.replace(STATE_FILE + ".tmp", STATE_FILE)


# ==================================================
# GLOBAL SINGLETON
# ==================================================
account_state = AccountState()
