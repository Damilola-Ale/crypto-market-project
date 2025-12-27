import json
import os
from typing import Dict, Optional

# ==========================================================
# CONFIG
# ==========================================================
STATS_DIR = "diagnostics/stats"
STATS_FILE = os.path.join(STATS_DIR, "performance_stats.json")

# ==========================================================
# CORE CLASS
# ==========================================================
class PerformanceStats:
    def __init__(self):
        os.makedirs(STATS_DIR, exist_ok=True)
        self.stats: Dict[str, dict] = {}
        self._load()

    # ------------------------------------------------------
    # Persistence
    # ------------------------------------------------------
    def _load(self):
        if not os.path.exists(STATS_FILE):
            return
        try:
            with open(STATS_FILE, "r") as f:
                self.stats = json.load(f)
        except Exception:
            print("[PerformanceStats] Failed to load stats, starting fresh")
            self.stats = {}

    def _save(self):
        tmp = STATS_FILE + ".tmp"
        with open(tmp, "w") as f:
            json.dump(self.stats, f, indent=2)
        os.replace(tmp, STATS_FILE)

    # ------------------------------------------------------
    # CLOSE INGESTION (PRIMARY ENTRY)
    # ------------------------------------------------------
    def record_close(
        self,
        symbol: str,
        realized_pnl: float,
        risk_amount: float,
        equity_after: float,
    ):
        """
        Called when broker confirms a position CLOSE.

        realized_pnl : float   -> absolute PnL
        risk_amount  : float   -> capital at risk for trade
        equity_after : float   -> account equity after close
        """

        try:
            if risk_amount <= 0:
                return  # invalid risk, ignore safely

            r_multiple = realized_pnl / risk_amount
            self.record_trade(symbol, r_multiple, equity_after)

        except Exception:
            # Stats must NEVER interfere with execution
            return

    # ------------------------------------------------------
    # CORE UPDATE LOGIC
    # ------------------------------------------------------
    def record_trade(
        self,
        symbol: str,
        r_multiple: float,
        equity_after: float,
    ):
        """
        Internal normalized stats update.
        r_multiple MUST already be computed.
        """

        s = self.stats.setdefault(symbol, self._empty_stats())

        s["trades"] += 1
        s["total_r"] += r_multiple

        if r_multiple > 0:
            s["wins"] += 1
        elif r_multiple < 0:
            s["losses"] += 1
        else:
            s["breakeven"] += 1

        # --------------------------------------------------
        # Equity & drawdown tracking
        # --------------------------------------------------
        if s["equity_peak"] == 0.0:
            s["equity_peak"] = equity_after

        s["equity_peak"] = max(s["equity_peak"], equity_after)

        if s["equity_peak"] > 0:
            drawdown = (s["equity_peak"] - equity_after) / s["equity_peak"]
            s["max_drawdown"] = max(s["max_drawdown"], drawdown)

        # --------------------------------------------------
        # Derived metrics
        # --------------------------------------------------
        s["avg_r"] = s["total_r"] / s["trades"]
        s["win_rate"] = s["wins"] / s["trades"]
        s["expectancy"] = s["avg_r"]

        self._save()

    # ------------------------------------------------------
    # Read-only access
    # ------------------------------------------------------
    def snapshot(self, symbol: Optional[str] = None) -> dict:
        if symbol:
            return self.stats.get(symbol, self._empty_stats())
        return self.stats

    # ------------------------------------------------------
    # Internals
    # ------------------------------------------------------
    @staticmethod
    def _empty_stats():
        return {
            "trades": 0,
            "wins": 0,
            "losses": 0,
            "breakeven": 0,
            "total_r": 0.0,
            "avg_r": 0.0,
            "win_rate": 0.0,
            "expectancy": 0.0,
            "max_drawdown": 0.0,
            "equity_peak": 0.0,
        }
