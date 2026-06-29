# execution/binance_client.py
"""
Binance USDT-M Futures execution client.

Responsibilities:
  - Place MARKET entry orders (LONG = BUY, SHORT = SELL)
  - Place STOP_MARKET stop-loss orders immediately after entry
  - Cancel + replace stop-loss orders when the trailing stop moves
  - Close positions with a MARKET order (cancel stop first)
  - Sync open positions from Binance to detect external closes

Environment variables required:
  BINANCE_API_KEY      — Futures API key
  BINANCE_API_SECRET   — Futures API secret
  BINANCE_TESTNET      — set to "1" to use testnet (optional)

All methods are safe to call from a daemon thread.
All errors are caught and re-raised as BinanceExecutionError so the
caller (lifecycle.py) can handle them without crashing the whole run.
"""

import hashlib
import hmac
import json
import os
import time
from urllib.parse import urlencode

import requests

# ──────────────────────────────────────────────────────────────────
# CONFIG
# ──────────────────────────────────────────────────────────────────

_TESTNET = os.getenv("BINANCE_TESTNET", "0") == "1"

BASE_URL = (
    "https://demo-fapi.binance.com"
    if _TESTNET
    else "https://fapi.binance.com"
)

_API_KEY    = os.getenv("BINANCE_API_KEY", "")
_API_SECRET = os.getenv("BINANCE_API_SECRET", "")

RECV_WINDOW = 5000   # ms
DEFAULT_LEVERAGE = int(os.getenv("BINANCE_LEVERAGE", "1"))

print(
    f"[BINANCE CLIENT] mode={'TESTNET' if _TESTNET else 'LIVE'} "
    f"base={BASE_URL} leverage={DEFAULT_LEVERAGE}"
)

# ──────────────────────────────────────────────────────────────────
# TIME SYNC
# ──────────────────────────────────────────────────────────────────

_time_offset_ms: int = 0

def _sync_binance_time() -> None:
    global _time_offset_ms
    try:
        _proxy_url = os.getenv("PROXY_URL")
        _proxies = {"http": _proxy_url, "https": _proxy_url} if _proxy_url else None
        r = requests.get(f"{BASE_URL}/fapi/v1/time", timeout=5, proxies=_proxies)
        server_time = r.json()["serverTime"]
        local_time  = int(time.time() * 1000)
        _time_offset_ms = server_time - local_time
        print(f"[TIME SYNC] offset={_time_offset_ms}ms")
    except Exception as e:
        print(f"[TIME SYNC FAILED] {e} — using local time")
        _time_offset_ms = 0

def _get_binance_time() -> int:
    return int(time.time() * 1000) + _time_offset_ms

_sync_binance_time()


# ──────────────────────────────────────────────────────────────────
# EXCEPTIONS
# ──────────────────────────────────────────────────────────────────

class BinanceExecutionError(Exception):
    """Raised for any Binance API or network error during execution."""


# ──────────────────────────────────────────────────────────────────
# INTERNAL HELPERS
# ──────────────────────────────────────────────────────────────────

def _sign(params: dict) -> str:
    query = urlencode(params)
    return hmac.new(
        _API_SECRET.encode("utf-8"),
        query.encode("utf-8"),
        digestmod=hashlib.sha256,
    ).hexdigest()


def _headers() -> dict:
    return {
        "X-MBX-APIKEY": _API_KEY,
        "Content-Type":  "application/x-www-form-urlencoded",
    }


def _request(method: str, path: str, params: dict = None, signed: bool = True) -> dict:
    """
    Low-level HTTP call. Adds timestamp + signature for signed endpoints.
    Raises BinanceExecutionError on any non-200 or Binance error code.
    """
    from data_pipeline.rate_limiter import rate_limiter

    # Block if currently banned or rate-limited
    try:
        rate_limiter.check()
    except RuntimeError as e:
        raise BinanceExecutionError(f"Rate limiter blocked request: {e}") from e

    params = params or {}
    if signed:
        params["timestamp"]  = _get_binance_time()
        params["recvWindow"] = RECV_WINDOW
        params["signature"]  = _sign(params)

    url = BASE_URL + path

    _proxy_url = os.getenv("PROXY_URL")
    _proxies = {"http": _proxy_url, "https": _proxy_url} if _proxy_url else None

    try:
        if method == "GET":
            r = requests.get(url, params=params, headers=_headers(), timeout=10, proxies=_proxies)
        elif method == "POST":
            r = requests.post(url, data=params, headers=_headers(), timeout=10, proxies=_proxies)
        elif method == "DELETE":
            r = requests.delete(url, params=params, headers=_headers(), timeout=10, proxies=_proxies)
        else:
            raise BinanceExecutionError(f"Unknown HTTP method: {method}")
    except requests.exceptions.RequestException as e:
        raise BinanceExecutionError(f"Network error [{method} {path}]: {e}") from e

    # Always update weight tracker from response headers
    used_weight_raw = r.headers.get("X-MBX-USED-WEIGHT-1M", "0")
    used_weight = int(used_weight_raw) if used_weight_raw.isdigit() else 0
    if used_weight > 0:
        rate_limiter.on_response(used_weight)

    retry_after_raw = r.headers.get("Retry-After")
    retry_after_int = int(retry_after_raw) if retry_after_raw else None

    if r.status_code == 429:
        rate_limiter.on_429(retry_after_int)
        raise BinanceExecutionError(
            f"Binance HTTP 429 [{method} {path}]: rate limited, retry_after={retry_after_int}"
        )
    if r.status_code == 418:
        rate_limiter.on_418(retry_after_int)
        raise BinanceExecutionError(
            f"Binance HTTP 418 [{method} {path}]: IP banned, retry_after={retry_after_int}"
        )

    try:
        body = r.json()
    except Exception:
        body = r.text

    if r.status_code != 200:
        if isinstance(body, dict) and body.get("code") == -1021:
            _sync_binance_time()
            # Retry once with corrected timestamp
            params["timestamp"] = _get_binance_time()
            params["signature"] = _sign(params)
            try:
                if method == "GET":
                    r = requests.get(url, params=params, headers=_headers(), timeout=10, proxies=_proxies)
                elif method == "POST":
                    r = requests.post(url, data=params, headers=_headers(), timeout=10, proxies=_proxies)
                elif method == "DELETE":
                    r = requests.delete(url, params=params, headers=_headers(), timeout=10, proxies=_proxies)
                body = r.json()
                if r.status_code == 200:
                    return body
            except Exception:
                pass
        raise BinanceExecutionError(
            f"Binance HTTP {r.status_code} [{method} {path}]: {body}"
        )

    if isinstance(body, dict) and "code" in body:
        try:
            _code = int(body["code"])
        except (TypeError, ValueError):
            _code = body["code"]  # non-numeric — fall through to comparison below
        if _code != 200:
            raise BinanceExecutionError(
                f"Binance API error [{method} {path}]: code={body['code']} msg={body.get('msg')}"
            )

    return body


# ──────────────────────────────────────────────────────────────────
# PRECISION HELPERS
# ──────────────────────────────────────────────────────────────────

_exchange_info_cache: dict = {}

def _get_symbol_info(symbol: str) -> dict:
    """Fetch and cache exchange info for a symbol."""
    global _exchange_info_cache
    if symbol not in _exchange_info_cache:
        info = _request("GET", "/fapi/v1/exchangeInfo", signed=False)
        for s in info.get("symbols", []):
            _exchange_info_cache[s["symbol"]] = s
    if symbol not in _exchange_info_cache:
        raise BinanceExecutionError(f"Symbol {symbol} not found in exchange info")
    return _exchange_info_cache[symbol]


def _qty_precision(symbol: str) -> int:
    """Return the quantity decimal places for a symbol."""
    info = _get_symbol_info(symbol)
    for f in info.get("filters", []):
        if f["filterType"] == "LOT_SIZE":
            step = f["stepSize"].rstrip("0")
            if "." in step:
                return len(step.split(".")[1])
            return 0
    return 3  # safe fallback


def _price_precision(symbol: str) -> int:
    """Return the price decimal places for a symbol."""
    info = _get_symbol_info(symbol)
    for f in info.get("filters", []):
        if f["filterType"] == "PRICE_FILTER":
            tick = f["tickSize"].rstrip("0")
            if "." in tick:
                return len(tick.split(".")[1])
            return 0
    return 4  # safe fallback


def _max_qty(symbol: str) -> float:
    """Return the maximum order quantity allowed for a symbol."""
    info = _get_symbol_info(symbol)
    for f in info.get("filters", []):
        if f["filterType"] == "LOT_SIZE":
            return float(f["maxQty"])
    return float("inf")  # no cap found — don't block


def _fmt_qty(symbol: str, qty: float) -> str:
    prec = _qty_precision(symbol)
    return f"{qty:.{prec}f}"


def _fmt_price(symbol: str, price: float) -> str:
    prec = _price_precision(symbol)
    return f"{price:.{prec}f}"


# ──────────────────────────────────────────────────────────────────
# LEVERAGE
# ──────────────────────────────────────────────────────────────────

def set_leverage(symbol: str, leverage: int = DEFAULT_LEVERAGE) -> dict:
    """Set leverage for a symbol. Call once before first trade."""
    return _request("POST", "/fapi/v1/leverage", {
        "symbol":   symbol,
        "leverage": leverage,
    })


# ──────────────────────────────────────────────────────────────────
# ORDER HELPERS
# ──────────────────────────────────────────────────────────────────

def _place_market_order(
    symbol: str,
    side: str,           # "BUY" or "SELL"
    quantity: float,
    reduce_only: bool = False,
    client_order_id: str = None,
) -> dict:
    params = {
        "symbol":   symbol,
        "side":     side,
        "type":     "MARKET",
        "quantity": _fmt_qty(symbol, quantity),
    }
    if reduce_only:
        params["reduceOnly"] = "true"
    if client_order_id:
        params["newClientOrderId"] = client_order_id[:36]  # Binance max 36 chars

    return _request("POST", "/fapi/v1/order", params)


def _place_stop_market_order(
    symbol: str,
    side: str,           # "SELL" for long stop, "BUY" for short stop
    stop_price: float,
    quantity: float,
    reduce_only: bool = True,
    client_order_id: str = None,
) -> dict:
    # Binance migrated all conditional orders (STOP_MARKET, TAKE_PROFIT_MARKET,
    # etc.) to the Algo Order API effective 2025-12-09. The old /fapi/v1/order
    # endpoint now rejects these with -4120 STOP_ORDER_SWITCH_ALGO.
    # New endpoint: POST /fapi/v1/algoOrder
    # Field renamed: stopPrice -> triggerPrice
    # Response field renamed: orderId -> algoId (handled by callers)
    params = {
        "algoType":    "CONDITIONAL",
        "symbol":      symbol,
        "side":        side,
        "type":        "STOP_MARKET",
        "triggerPrice": _fmt_price(symbol, stop_price),
        "quantity":    _fmt_qty(symbol, quantity),
        "workingType": "CONTRACT_PRICE",
    }
    if reduce_only:
        params["reduceOnly"] = "true"
    if client_order_id:
        params["clientAlgoId"] = client_order_id[:36]

    return _request("POST", "/fapi/v1/algoOrder", params)


def _cancel_order(symbol: str, order_id: int) -> dict:
    """Cancel a single order via standard USDT-M Futures endpoint."""
    return _request("DELETE", "/fapi/v1/order", {
        "symbol":  symbol,
        "orderId": order_id,
    })


def _cancel_algo_order(algo_id: int) -> dict:
    """Cancel a conditional (algo) order — e.g. a STOP_MARKET stop-loss,
    placed via the new Algo Order API (mandatory since 2025-12-09)."""
    return _request("DELETE", "/fapi/v1/algoOrder", {
        "algoId": algo_id,
    })


def _cancel_all_open_orders(symbol: str) -> dict:
    """Cancel ALL open orders for a symbol (nuclear option for cleanup)."""
    return _request("DELETE", "/fapi/v1/allOpenOrders", {"symbol": symbol})


# ──────────────────────────────────────────────────────────────────
# PUBLIC EXECUTION API
# ──────────────────────────────────────────────────────────────────

def open_position(
    symbol: str,
    direction: int,      # 1 = LONG, -1 = SHORT
    quantity: float,
    stop_price: float,
    trade_id: str = None,
) -> dict:
    """
    1. Set leverage (idempotent — Binance ignores if already set).
    2. Place MARKET entry order.
    3. Place STOP_MARKET stop-loss order.

    Returns:
        {
            "entry_order":  <Binance order dict>,
            "stop_order":   <Binance order dict>,
            "fill_price":   <float | None>,   # best-effort from avgPrice
        }
    """
    if not _API_KEY or not _API_SECRET:
        raise BinanceExecutionError("BINANCE_API_KEY / BINANCE_API_SECRET not set")

    try:
        lev_result = set_leverage(symbol)
        print(f"[BINANCE] leverage confirmed: {symbol} → {lev_result}")
    except BinanceExecutionError as e:
        raise BinanceExecutionError(f"[BINANCE] set_leverage failed for {symbol}, aborting entry: {e}")

    entry_side = "BUY" if direction == 1 else "SELL"
    stop_side  = "SELL" if direction == 1 else "BUY"

    entry_cid = f"entry_{trade_id}"[:36] if trade_id else None
    stop_cid  = f"stop_{trade_id}"[:36]  if trade_id else None

    # ── Entry ──────────────────────────────────────────────────────
    entry_order = _place_market_order(
        symbol=symbol,
        side=entry_side,
        quantity=quantity,
        client_order_id=entry_cid,
    )

    fill_price = None
    try:
        avg = entry_order.get("avgPrice") or entry_order.get("price")
        if avg:
            fill_price = float(avg)
    except Exception:
        pass

    print(
        f"[BINANCE ENTRY] {symbol} {entry_side} qty={quantity} "
        f"fill={fill_price} orderId={entry_order.get('orderId')}"
    )

    # ── Stop-loss ──────────────────────────────────────────────────
    stop_order = {}
    try:
        stop_order = _place_stop_market_order(
            symbol=symbol,
            side=stop_side,
            stop_price=stop_price,
            quantity=quantity,
            reduce_only=True,
            client_order_id=stop_cid,
        )
        # Algo Order API returns "algoId", not "orderId"
        print(
            f"[BINANCE STOP] {symbol} {stop_side} stop={stop_price} "
            f"qty={quantity} algoId={stop_order.get('algoId')}"
        )
    except BinanceExecutionError as e:
        # Stop failed — the position is now naked on Binance. Don't leave
        # it there silently waiting for the next reconcile cycle to notice:
        # immediately flatten the entry we just filled.
        print(f"[BINANCE STOP FAILED — FLATTENING ENTRY] {symbol}: {e}")
        try:
            _place_market_order(
                symbol=symbol,
                side=stop_side,        # opposite of entry side = flattening order
                quantity=quantity,
                reduce_only=True,
                client_order_id=f"emerg_{trade_id}"[:36] if trade_id else None,
            )
            print(f"[BINANCE EMERGENCY FLATTEN OK] {symbol}")
        except BinanceExecutionError as flatten_err:
            # Flattening also failed — worst case: naked, unprotected
            # position with no automatic recovery. Raise loudly.
            raise BinanceExecutionError(
                f"STOP FAILED AND EMERGENCY FLATTEN ALSO FAILED for {symbol}. "
                f"Stop error: {e}. Flatten error: {flatten_err}. "
                f"MANUAL INTERVENTION REQUIRED — position is naked on Binance."
            ) from flatten_err
        raise  # re-raise original stop error so lifecycle.py's existing handling still fires

    return {
        "entry_order": entry_order,
        "stop_order":  stop_order,
        "fill_price":  fill_price,
    }


def amend_stop(
    symbol: str,
    direction: int,
    quantity: float,
    new_stop_price: float,
    existing_stop_order_id: int = None,
    trade_id: str = None,
) -> dict:
    """
    Place the NEW stop first, confirm it, THEN cancel the old one.
    Binance Futures does not support in-place stop amendment, so this
    is implemented as place-then-cancel rather than cancel-then-place —
    the latter leaves the position completely unprotected on the
    exchange for the duration of the round trip between the two calls.
    Worst case here is briefly having two live stops (harmless — the
    first one to trigger closes the position via reduce-only, making
    the other a no-op once quantity is gone), never zero stops.

    Stops are Algo Orders (mandatory since 2025-12-09) — cancel via
    /fapi/v1/algoOrder using algoId, not the legacy orderId-based cancel.

    Returns the new stop order dict.
    """
    stop_side = "SELL" if direction == 1 else "BUY"
    stop_cid  = f"stop_{trade_id}_{int(time.time() * 1000)}"[:36] if trade_id else None

    # ── Place new stop FIRST ─────────────────────────────────────
    try:
        new_stop = _place_stop_market_order(
            symbol=symbol,
            side=stop_side,
            stop_price=new_stop_price,
            quantity=quantity,
            reduce_only=True,
            client_order_id=stop_cid,
        )
    except BinanceExecutionError as e:
        # New stop failed to place — the OLD stop is still live and still
        # protecting the position. Do NOT cancel it. Raise so the caller
        # knows the trail did not actually move and must not update its
        # local stop_loss state.
        raise BinanceExecutionError(
            f"[BINANCE AMEND STOP] new stop placement failed for {symbol}, "
            f"OLD stop (algoId={existing_stop_order_id}) left intact: {e}"
        ) from e

    print(
        f"[BINANCE AMEND STOP] {symbol} new stop={new_stop_price} "
        f"algoId={new_stop.get('algoId')}"
    )

    # ── Cancel old stop SECOND, only after new one is confirmed live ──
    if existing_stop_order_id:
        try:
            _cancel_algo_order(existing_stop_order_id)
            print(f"[BINANCE AMEND STOP] cancelled old stop algoId={existing_stop_order_id}")
        except BinanceExecutionError as e:
            # Old stop cancel failed — most likely it already triggered.
            # Not dangerous: worst case is two reduce-only stop orders
            # briefly coexisting; whichever fills first closes the
            # quantity and makes the other a harmless no-op.
            print(
                f"[BINANCE AMEND STOP] old stop cancel failed (likely already "
                f"filled/gone) — old and new may briefly coexist, this is safe: {e}"
            )

    return new_stop


def close_position(
    symbol: str,
    direction: int,
    quantity: float,
    stop_order_id: int = None,
    trade_id: str = None,
) -> dict:
    """
    1. Cancel the standing stop-loss order (if any).
    2. Place a MARKET reduce-only order to close the position.

    Returns the close order dict.
    """
    # Cancel stop first to avoid double-close.
    # The stop is now an Algo Order — cancel via algoId, not the legacy order cancel.
    if stop_order_id:
        try:
            _cancel_algo_order(stop_order_id)
            print(f"[BINANCE CLOSE] cancelled stop algoId={stop_order_id}")
        except BinanceExecutionError as e:
            print(f"[BINANCE CLOSE] stop cancel failed (may be already hit): {e}")

    close_side = "SELL" if direction == 1 else "BUY"
    close_cid  = f"close_{trade_id}"[:36] if trade_id else None

    close_order = _place_market_order(
        symbol=symbol,
        side=close_side,
        quantity=quantity,
        reduce_only=True,
        client_order_id=close_cid,
    )

    fill_price = None
    try:
        avg = close_order.get("avgPrice") or close_order.get("price")
        if avg:
            fill_price = float(avg)
    except Exception:
        pass

    print(
        f"[BINANCE CLOSE] {symbol} {close_side} qty={quantity} "
        f"fill={fill_price} orderId={close_order.get('orderId')}"
    )

    return close_order


def get_open_positions() -> dict[str, dict]:
    """
    Fetch all open Binance Futures positions.

    Returns:
        { "ETHUSDT": {"side": 1, "qty": 0.01, "entry_price": 3000.0}, ... }
        Only includes positions with non-zero quantity.
    """
    raw = _request("GET", "/fapi/v2/positionRisk")
    result = {}
    for p in raw:
        amt = float(p.get("positionAmt", 0))
        if amt == 0:
            continue
        symbol = p["symbol"]
        result[symbol] = {
            "side":         1 if amt > 0 else -1,
            "qty":          abs(amt),
            "entry_price":  float(p.get("entryPrice", 0)),
            "unrealized_pnl": float(p.get("unRealizedProfit", 0)),
            "leverage":     int(p.get("leverage", 1)),
        }
    return result


def get_open_orders(symbol: str) -> list[dict]:
    """Return all open orders for a symbol."""
    return _request("GET", "/fapi/v1/openOrders", {"symbol": symbol})


def get_open_algo_orders(symbol: str) -> list[dict]:
    """Return all open algo (conditional) orders for a symbol — this is
    where STOP_MARKET/TAKE_PROFIT_MARKET orders now live, since Binance's
    2025-12-09 migration moved them off the regular open-orders endpoint
    (GET /fapi/v1/openOrders no longer returns them)."""
    return _request("GET", "/fapi/v1/openAlgoOrders", {"symbol": symbol})


def get_account_balance() -> dict:
    """
    Returns available USDT balance.
    { "total": float, "available": float }
    """
    info = _request("GET", "/fapi/v2/account")
    for asset in info.get("assets", []):
        if asset["asset"] == "USDT":
            return {
                "total":     float(asset["walletBalance"]),
                "available": float(asset["availableBalance"]),
            }
    return {"total": 0.0, "available": 0.0}

def reconcile_positions(local_positions: dict, live_positions: dict = None) -> list[str]:
    """
    Compare local open_positions.json against Binance's actual state.
    Returns a list of warning strings for any divergence found.
    Pass live_positions if already fetched to avoid a second API call.
    """
    warnings = []
    try:
        live = live_positions if live_positions is not None else get_open_positions()
    except BinanceExecutionError as e:
        return [f"reconcile: could not fetch Binance positions — {e}"]

    for symbol, pos in local_positions.items():
        if symbol not in live:
            warnings.append(
                f"GHOST POSITION: {symbol} exists locally "
                f"(dir={pos.get('direction')} entry={pos.get('entry_price')}) "
                f"but NOT on Binance — may need manual cleanup"
            )
        else:
            live_side = live[symbol]["side"]
            local_side = pos.get("direction")
            if live_side != local_side:
                warnings.append(
                    f"SIDE MISMATCH: {symbol} local={local_side} binance={live_side}"
                )

    for symbol in live:
        if symbol not in local_positions:
            warnings.append(
                f"UNKNOWN POSITION: {symbol} is open on Binance "
                f"but not tracked locally — orphaned position"
            )

    return warnings