import requests
import pandas as pd
from datetime import datetime, timezone

BASE_URL = "https://api.binance.com/api/v3/klines"

def _to_ms(dt: datetime) -> int:
    """
    Convert a datetime to Binance API timestamp (milliseconds).
    Binance expects ms since Unix epoch (UTC).
    """
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)

def fetch_ohlcv(
    symbol: str,
    start: datetime = None,
    end: datetime = None,
    interval: str = "1h",
    limit: int = 1000
) -> pd.DataFrame:
    """
    Fetch candlestick (OHLCV) data from Binance public API.

    Args:
        symbol: Trading pair, e.g., "BTCUSDT" (NOT with hyphen).
        start: Datetime of earliest candle to fetch (UTC).
        end: Datetime of latest candle to fetch (UTC).
        interval: Candle interval (1h, 4h, etc.).
        limit: Max rows per call (max 1000).

    Returns:
        pd.DataFrame with index = UTC timestamp, columns:
        ["open","high","low","close","volume"]
    """

    # Binance expects uppercase without punctuation
    symbol = symbol.replace("-", "").upper()

    params = {
        "symbol": symbol,
        "interval": interval,
        "limit": limit
    }
    if start:
        params["startTime"] = _to_ms(start)
    if end:
        params["endTime"] = _to_ms(end)

    # make the request
    response = requests.get(BASE_URL, params=params, timeout=10)
    response.raise_for_status()

    data = response.json()
    if not data:
        return pd.DataFrame()

    # Parse into DataFrame
    df = pd.DataFrame(data, columns=[
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_vol",
        "num_trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore"
    ])

    # Convert and clean
    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.set_index("timestamp")

    df = df[["open", "high", "low", "close", "volume"]].astype(float)

    return df
