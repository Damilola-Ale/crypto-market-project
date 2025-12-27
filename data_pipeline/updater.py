# updater.py
import os
import pandas as pd
from datetime import datetime, timezone, timedelta

from data_pipeline.fetcher import fetch_ohlcv
from data_pipeline.validators import validate_ohlcv

# ==========================================================
# CONFIG
# ==========================================================
CACHE_DIR = "data/cache"
HOURS_LOOKBACK = 800  # maintain exact rolling window
INTERVAL = "1h"


# ==========================================================
# Utilities
# ==========================================================
def _now_utc_hour():
    """Return current UTC hour, floored."""
    return datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_path(symbol: str):
    return os.path.join(CACHE_DIR, f"{symbol}_{INTERVAL}.parquet")


# ==========================================================
# Core updater
# ==========================================================
def update_symbol(symbol: str) -> pd.DataFrame:
    """
    Maintain a clean, gap-free rolling OHLCV dataset
    with exactly HOURS_LOOKBACK candles.
    """
    _ensure_cache_dir()
    path = _cache_path(symbol)

    now = _now_utc_hour()
    start_required = now - timedelta(hours=HOURS_LOOKBACK)

    # ------------------------------------------------------
    # 1. Load cache (if exists)
    # ------------------------------------------------------
    df = None
    last_ts = None

    if os.path.exists(path):
        df = pd.read_parquet(path)

        df = df.copy()
        df.index = pd.to_datetime(df.index, utc=True)
        df = df[~df.index.duplicated(keep="last")]
        df = df.sort_index()

        if not df.empty:
            last_ts = df.index[-1]

    # ------------------------------------------------------
    # 2. Determine fetch window
    # ------------------------------------------------------
    fetch_start = (
        start_required
        if df is None or df.empty
        else last_ts + timedelta(hours=1)
    )
    fetch_end = now

    # ------------------------------------------------------
    # 3. Fetch missing candles
    # ------------------------------------------------------
    if fetch_start <= fetch_end:
        new_data = fetch_ohlcv(
            symbol=symbol,
            interval=INTERVAL,
            start=fetch_start,
            end=fetch_end,
        )

        if new_data is not None and not new_data.empty:
            new_data.index = pd.to_datetime(new_data.index, utc=True)
            new_data = new_data[~new_data.index.duplicated(keep="last")]

            df = pd.concat([df, new_data]) if df is not None else new_data

    # ------------------------------------------------------
    # 4. Final sanitation
    # ------------------------------------------------------
    if df is None or df.empty:
        raise RuntimeError(f"[{symbol}] No data available after fetch")

    df = df.sort_index()
    df = df[df.index >= start_required]

    # Enforce EXACT rolling window size
    df = df.iloc[-HOURS_LOOKBACK:]

    # ------------------------------------------------------
    # 5. Gap check (hard failure)
    # ------------------------------------------------------
    expected_index = pd.date_range(
        start=df.index[0],
        periods=len(df),
        freq=INTERVAL,
        tz="UTC",
    )

    if not df.index.equals(expected_index):
        diff = df.index.symmetric_difference(expected_index)
        raise RuntimeError(
            f"[{symbol}] Data gap detected. Sample timestamps: {diff[:5].tolist()}"
        )

    # ------------------------------------------------------
    # 6. Validate structure & content
    # ------------------------------------------------------
    validate_ohlcv(df, symbol)

    # ------------------------------------------------------
    # 7. Final NaN safety
    # ------------------------------------------------------
    df = df.fillna(0)

    # ------------------------------------------------------
    # 8. Atomic save
    # ------------------------------------------------------
    tmp_path = path + ".tmp"
    df.to_parquet(tmp_path)
    os.replace(tmp_path, path)

    print(
        f"[{symbol}] ✅ Data OK | "
        f"{len(df)} candles | "
        f"{df.index[0]} → {df.index[-1]}"
    )

    return df
