# execution/signal_store.py

import json
import os
from datetime import datetime, timedelta, timezone
from typing import Optional, Dict

# ===================================================
# CONFIG
# ===================================================
CACHE_DIR = "data/cache"
SIGNAL_FILE = os.path.join(CACHE_DIR, "signals.json")

DEFAULT_COOLDOWN_HOURS = 6


# ===================================================
# SIGNAL STORE
# ===================================================
class SignalStore:
    """
    Persistent signal state manager.

    Responsibilities:
    - Prevent duplicate signal emission
    - Enforce cooldown windows
    - Prevent rapid directional flipping
    - Persist state across runs
    """

    def __init__(self, cooldown_hours: int = DEFAULT_COOLDOWN_HOURS):
        self.cooldown = timedelta(hours=cooldown_hours)
        self.signals: Dict[str, dict] = {}
        os.makedirs(CACHE_DIR, exist_ok=True)
        self._load()

    # -------------------------------------------------
    # Persistence
    # -------------------------------------------------
    def _load(self):
        if not os.path.exists(SIGNAL_FILE):
            return

        try:
            with open(SIGNAL_FILE, "r") as f:
                raw = json.load(f)

            for symbol, info in raw.items():
                self.signals[symbol] = {
                    "timestamp": self._parse_ts(info["timestamp"]),
                    "direction": int(info["direction"]),
                    "cooldown_until": self._parse_ts(info["cooldown_until"]),
                    "meta": info.get("meta", {}),
                }

        except Exception as e:
            print(f"[SignalStore] Warning: failed to load state ({e}), starting fresh.")
            self.signals = {}

    def _save(self):
        payload = {}

        for symbol, info in self.signals.items():
            payload[symbol] = {
                "timestamp": info["timestamp"].isoformat(),
                "direction": info["direction"],
                "cooldown_until": info["cooldown_until"].isoformat(),
                "meta": info.get("meta", {}),
            }

        tmp_path = SIGNAL_FILE + ".tmp"
        with open(tmp_path, "w") as f:
            json.dump(payload, f, indent=2)

        os.replace(tmp_path, SIGNAL_FILE)

    # -------------------------------------------------
    # Core logic
    # -------------------------------------------------
    def should_emit(
        self,
        symbol: str,
        direction: int,
        timestamp: datetime,
        meta: Optional[dict] = None
    ) -> bool:
        """
        Decide whether a signal should be emitted.

        Rules enforced:
        - direction != 0
        - no duplicate signals
        - cooldown window respected
        - directional flip locked during cooldown
        """

        if direction == 0:
            return False

        ts = self._normalize_ts(timestamp)
        state = self.signals.get(symbol)

        # ---------------------------------------------
        # First-ever signal for this symbol
        # ---------------------------------------------
        if state is None:
            self._store(symbol, direction, ts, meta)
            return True

        # ---------------------------------------------
        # Cooldown enforcement
        # ---------------------------------------------
        if ts <= state["cooldown_until"]:
            # Block duplicates AND directional flips
            return False

        # ---------------------------------------------
        # Passed all checks → emit
        # ---------------------------------------------
        self._store(symbol, direction, ts, meta)
        return True

    # -------------------------------------------------
    # Helpers
    # -------------------------------------------------
    def _store(
        self,
        symbol: str,
        direction: int,
        timestamp: datetime,
        meta: Optional[dict]
    ):
        self.signals[symbol] = {
            "timestamp": timestamp,
            "direction": direction,
            "cooldown_until": timestamp + self.cooldown,
            "meta": meta or {},
        }
        self._save()

    @staticmethod
    def _normalize_ts(ts: datetime) -> datetime:
        if ts.tzinfo is None:
            return ts.replace(tzinfo=timezone.utc)
        return ts.astimezone(timezone.utc)

    @staticmethod
    def _parse_ts(value: str) -> datetime:
        ts = datetime.fromisoformat(value)
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        return ts
