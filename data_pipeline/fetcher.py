import requests
import pandas as pd
from datetime import datetime, timezone
import time

BASE_URL = "https://api.binance.com/api/v3/klines"


def _to_ms(dt: datetime) -> int:
    return int(dt.replace(tzinfo=timezone.utc).timestamp() * 1000)


def fetch_ohlcv(
    symbol: str,
    start: datetime = None,
    end: datetime = None,
    interval: str = "1h",
    limit: int = 1000,
    retries: int = 3
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

            print(f"[FETCH] {symbol} | {interval}")
            print(f"[FETCH] start={start} end={end}")

            response = requests.get(BASE_URL, params=params, timeout=10)
            response.raise_for_status()

            data = response.json()

            if not data:
                print("[FETCH] No candles returned")
                return pd.DataFrame()

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

            df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
            df = df.set_index("timestamp")

            df = df[["open", "high", "low", "close", "volume"]].astype(float)

            print(
                f"[FETCH] returned {len(df)} candles | "
                f"{df.index.min()} → {df.index.max()}"
            )

            return df

        except Exception as e:

            print(f"[FETCH ERROR] attempt {attempt+1} : {e}")
            time.sleep(1)

    raise RuntimeError(f"Failed to fetch data for {symbol}")