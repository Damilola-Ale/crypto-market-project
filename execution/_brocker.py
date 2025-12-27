# execution/broker.py
from typing import Dict, Any


class Broker:
    """
    Abstract broker interface.
    Execution logic lives in implementations (paper / real).
    """

    def place_order(self, order: Dict[str, Any]) -> Dict[str, Any]:
        """
        Place a new order.

        Expected order fields (minimum):
        {
            "position_id": str,
            "symbol": str,
            "direction": int,   # 1 = long, -1 = short
            "price": float,
            "size": float,
            "stop_loss": float | None
        }
        """
        raise NotImplementedError

    def close_position(self, position_id: str, price: float) -> Dict[str, Any]:
        """
        Close an existing position at given price.
        """
        raise NotImplementedError

    def get_open_positions(self) -> Dict[str, Dict[str, Any]]:
        """
        Return all open broker-side positions.
        """
        raise NotImplementedError

    def get_account_info(self) -> Dict[str, Any]:
        """
        Return account-level info (equity, margin, exposure).
        """
        raise NotImplementedError
