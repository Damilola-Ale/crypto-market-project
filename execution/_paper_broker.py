# execution/paper_broker.py
from typing import Dict, Any
from datetime import datetime

from execution.broker import Broker


class PaperBroker(Broker):
    """
    Simulated broker with instant fills.
    Acts as the execution truth for backtests & dry-runs.
    """

    def __init__(self, starting_equity: float):
        self.starting_equity = float(starting_equity)
        self.equity = float(starting_equity)

        self.positions: Dict[str, Dict[str, Any]] = {}
        self.trade_log = []

    # --------------------------------------------------
    # ORDER ENTRY
    # --------------------------------------------------
    def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        position_id = order["position_id"]

        if position_id in self.positions:
            return {
                "status": "REJECTED",
                "reason": "position_already_exists",
            }

        entry_price = float(order["price"])
        size = float(order["size"])
        direction = int(order["direction"])

        position = {
            "position_id": position_id,
            "symbol": order["symbol"],
            "direction": direction,

            "entry_time": datetime.utcnow().isoformat(),
            "entry_price": entry_price,
            "size": size,
            "stop_loss": order.get("stop_loss"),

            "state": "OPEN",
        }

        self.positions[position_id] = position

        return {
            "status": "FILLED",
            "position_id": position_id,
            "fill_price": entry_price,
            "timestamp": position["entry_time"],
        }

    # --------------------------------------------------
    # POSITION EXIT
    # --------------------------------------------------
    def close_position(self, position_id: str, price: float) -> Dict[str, Any]:
        position = self.positions.pop(position_id, None)

        if position is None:
            return {
                "status": "REJECTED",
                "reason": "position_not_found",
            }

        exit_price = float(price)
        direction = position["direction"]
        entry_price = position["entry_price"]
        size = position["size"]

        pnl = direction * (exit_price - entry_price) * size
        self.equity += pnl

        trade = {
            "position_id": position_id,
            "symbol": position["symbol"],
            "direction": direction,

            "entry_price": entry_price,
            "exit_price": exit_price,
            "size": size,

            "pnl": pnl,
            "exit_time": datetime.utcnow().isoformat(),
        }

        self.trade_log.append(trade)

        return {
            "status": "CLOSED",
            "position_id": position_id,
            "exit_price": exit_price,
            "realized_pnl": pnl,
            "equity": self.equity,
        }

    # --------------------------------------------------
    # STATE QUERIES
    # --------------------------------------------------
    def get_open_positions(self) -> Dict[str, Dict[str, Any]]:
        return dict(self.positions)

    def get_account_info(self) -> Dict[str, Any]:
        return {
            "starting_equity": self.starting_equity,
            "equity": self.equity,
            "open_positions": len(self.positions),
            "realized_trades": len(self.trade_log),
        }
