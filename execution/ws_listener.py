# execution/ws_listener.py
"""
Binance User Data Stream websocket listener.

Connects to the User Data Stream on startup and maintains a local
state file that hourly_runner.py reads instead of polling
/fapi/v2/positionRisk and /fapi/v2/account directly.

Handles:
  - ACCOUNT_UPDATE  → updates equity in data/ws_account.json
  - ORDER_TRADE_UPDATE → updates positions in data/ws_positions.json

listenKey is refreshed every 29 minutes (Binance expires it at 60m).
Reconnects automatically on drop with exponential backoff.

Usage (called once on Flask startup in app.py):
    from execution.ws_listener import start_ws_listener
    start_ws_listener()
"""

import json
import os
import threading
import time
from datetime import datetime, timezone

import requests
import websocket  # websocket-client

_TESTNET = os.getenv("BINANCE_TESTNET", "0") == "1"

_REST_BASE = (
    "https://demo-fapi.binance.com"
    if _TESTNET
    else "https://fapi.binance.com"
)
_WS_BASE = (
    "wss://stream.binancefuture.com"
    if _TESTNET
    else "wss://fstream.binance.com"
)

_API_KEY    = os.getenv("BINANCE_API_KEY", "")
_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

WS_ACCOUNT_FILE   = "data/ws_account.json"
WS_POSITIONS_FILE = "data/ws_positions.json"

# ── module-level state ────────────────────────────────────────────
_listen_key: str = ""
_ws_app: websocket.WebSocketApp = None
_running = False
_lock = threading.Lock()


def _headers() -> dict:
    return {"X-MBX-APIKEY": _API_KEY}


def _get_listen_key() -> str:
    r = requests.post(
        f"{_REST_BASE}/fapi/v1/listenKey",
        headers=_headers(),
        timeout=10,
    )
    r.raise_for_status()
    return r.json()["listenKey"]


def _keepalive_listen_key(key: str) -> None:
    requests.put(
        f"{_REST_BASE}/fapi/v1/listenKey",
        headers=_headers(),
        params={"listenKey": key},
        timeout=10,
    )


def _delete_listen_key(key: str) -> None:
    try:
        requests.delete(
            f"{_REST_BASE}/fapi/v1/listenKey",
            headers=_headers(),
            params={"listenKey": key},
            timeout=10,
        )
    except Exception:
        pass


def _atomic_write(path: str, data: dict) -> None:
    os.makedirs(os.path.dirname(path) if os.path.dirname(path) else ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w") as f:
        json.dump(data, f, indent=2)
    os.replace(tmp, path)


def _on_message(ws, raw: str) -> None:
    try:
        msg = json.loads(raw)
    except Exception:
        return

    event = msg.get("e")

    # ── balance / equity ──────────────────────────────────────────
    if event == "ACCOUNT_UPDATE":
        balances = msg.get("a", {}).get("B", [])
        for b in balances:
            if b.get("a") == "USDT":
                equity = float(b.get("wb", 0))   # wallet balance
                _atomic_write(WS_ACCOUNT_FILE, {
                    "equity": equity,
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                })
                print(f"[WS] ACCOUNT_UPDATE equity=${equity:.2f}")
                break

    # ── order fills / stop hits ───────────────────────────────────
    elif event == "ORDER_TRADE_UPDATE":
        order = msg.get("o", {})
        symbol    = order.get("s")
        status    = order.get("X")   # order status
        side      = order.get("S")   # BUY / SELL
        order_type = order.get("o")  # MARKET, STOP_MARKET, etc.
        qty        = float(order.get("q", 0))
        fill_price = float(order.get("ap", 0) or order.get("sp", 0) or 0)
        reduce     = order.get("R", False)

        print(
            f"[WS] ORDER_TRADE_UPDATE {symbol} "
            f"type={order_type} status={status} "
            f"side={side} qty={qty} fill={fill_price} reduce={reduce}"
        )

        if status in ("FILLED", "PARTIALLY_FILLED"):
            # Load current ws positions
            positions = {}
            if os.path.exists(WS_POSITIONS_FILE):
                try:
                    with open(WS_POSITIONS_FILE) as f:
                        positions = json.load(f)
                except Exception:
                    positions = {}

            if reduce or order_type in ("STOP_MARKET", "TAKE_PROFIT_MARKET"):
                # Position closed externally (stop hit or manual close)
                if symbol in positions:
                    positions.pop(symbol)
                    print(f"[WS] position closed externally: {symbol}")
            else:
                # New position opened externally or size changed
                existing = positions.get(symbol, {})
                direction = 1 if side == "BUY" else -1
                positions[symbol] = {
                    "side":        direction,
                    "qty":         qty,
                    "entry_price": fill_price or existing.get("entry_price", 0),
                    "updated_at":  datetime.now(timezone.utc).isoformat(),
                }

            _atomic_write(WS_POSITIONS_FILE, positions)


def _on_error(ws, error) -> None:
    print(f"[WS] error: {error}")


def _on_close(ws, close_status_code, close_msg) -> None:
    print(f"[WS] closed: {close_status_code} {close_msg}")


def _on_open(ws) -> None:
    print(f"[WS] connected to User Data Stream")


def _keepalive_loop(interval: int = 29 * 60) -> None:
    """Refresh listenKey every 29 minutes forever."""
    global _listen_key
    while _running:
        time.sleep(interval)
        if not _running:
            break
        try:
            _keepalive_listen_key(_listen_key)
            print(f"[WS] listenKey refreshed")
        except Exception as e:
            print(f"[WS] keepalive failed: {e}")


def _connect_loop() -> None:
    """Connect and reconnect with exponential backoff."""
    global _listen_key, _ws_app, _running

    backoff = 5
    while _running:
        try:
            _listen_key = _get_listen_key()
            url = f"{_WS_BASE}/ws/{_listen_key}"
            print(f"[WS] connecting to {url[:60]}...")

            _ws_app = websocket.WebSocketApp(
                url,
                on_open=_on_open,
                on_message=_on_message,
                on_error=_on_error,
                on_close=_on_close,
            )
            # run_forever blocks until disconnected
            _ws_app.run_forever(ping_interval=30, ping_timeout=10)

            backoff = 5  # reset on clean disconnect

        except Exception as e:
            print(f"[WS] connect failed: {e}")

        if not _running:
            break

        print(f"[WS] reconnecting in {backoff}s...")
        time.sleep(backoff)
        backoff = min(backoff * 2, 120)


def start_ws_listener() -> None:
    """
    Start the websocket listener in background threads.
    Safe to call multiple times — only starts once.
    Call this once from app.py on startup.
    """
    global _running

    if not _API_KEY or not _API_SECRET:
        print("[WS] no API keys set — websocket listener not started")
        return

    with _lock:
        if _running:
            print("[WS] already running")
            return
        _running = True

    t_connect = threading.Thread(target=_connect_loop, daemon=True, name="ws-connect")
    t_keepalive = threading.Thread(target=_keepalive_loop, daemon=True, name="ws-keepalive")
    t_connect.start()
    t_keepalive.start()
    print("[WS] listener threads started")


def stop_ws_listener() -> None:
    global _running, _ws_app, _listen_key
    _running = False
    if _ws_app:
        _ws_app.close()
    if _listen_key:
        _delete_listen_key(_listen_key)
    print("[WS] listener stopped")


def read_ws_equity() -> float | None:
    """Read last known equity from websocket state. Returns None if no data yet."""
    if not os.path.exists(WS_ACCOUNT_FILE):
        return None
    try:
        with open(WS_ACCOUNT_FILE) as f:
            return float(json.load(f).get("equity", 0)) or None
    except Exception:
        return None


def read_ws_positions() -> dict:
    """Read last known positions from websocket state. Returns {} if no data yet."""
    if not os.path.exists(WS_POSITIONS_FILE):
        return {}
    try:
        with open(WS_POSITIONS_FILE) as f:
            return json.load(f)
    except Exception:
        return {}