import requests
import pandas as pd
from datetime import datetime, timezone
import time

BASE_URL = "https://api.binance.com/api/v3/klines"


def _to_ms(dt):
    """
    Convert a string, pandas Timestamp, or datetime to milliseconds since epoch UTC.
    """
    # Convert string to pandas Timestamp
    if isinstance(dt, str):
        dt = pd.Timestamp(dt)

    # Convert pandas Timestamp to datetime
    if isinstance(dt, pd.Timestamp):
        dt = dt.to_pydatetime()

    # Ensure datetime has UTC tzinfo
    if isinstance(dt, datetime):
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
    else:
        raise TypeError(f"_to_ms() expected str, pd.Timestamp, or datetime, got {type(dt)}")

    return int(dt.timestamp() * 1000)

def fetch_ohlcv(
    symbol: str,
    start: datetime = None,
    end: datetime = None,
    interval: str = "1h",
    limit: int = 1000,
    retries: int = 3,
    verbose: bool = True,       # ← NEW: suppress during pagination
) -> pd.DataFrame:

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

    for attempt in range(retries):

        try:
            if verbose:
                print(f"[FETCH] {symbol} | {interval}")
                print(f"[FETCH] start={start} end={end}")

            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            if not data:
                if verbose:
                    print("[FETCH] No candles returned")
                return pd.DataFrame()

            df = pd.DataFrame(data, columns=[
                "open_time", "open", "high", "low", "close", "volume",
                "close_time", "quote_asset_vol", "num_trades",
                "taker_buy_base", "taker_buy_quote", "ignore"
            ])

            df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            df = df.set_index("timestamp")
            df = df[["open", "high", "low", "close", "volume"]].astype(float)

            if verbose:
                print(
                    f"[FETCH] returned {len(df)} candles | "
                    f"{df.index.min()} → {df.index.max()}"
                )

            return df

        except Exception as e:
            print(f"[FETCH ERROR] attempt {attempt+1} : {e}")
            time.sleep(1)

    raise RuntimeError(f"Failed to fetch data for {symbol}")