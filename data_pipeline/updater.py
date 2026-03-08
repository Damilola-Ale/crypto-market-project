import os
import pandas as pd
from datetime import datetime, timezone, timedelta

from data_pipeline.fetcher import fetch_ohlcv
from data_pipeline.validators import validate_ohlcv
from data_pipeline.timeframe_builder import build_htf


CACHE_DIR = "data/cache"

HOURS_LOOKBACK = 800

LTF_INTERVAL = "1h"
HTF_INTERVAL = "4h"


def _now_utc_hour():
    return datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_path(symbol: str, tf: str):
    return os.path.join(CACHE_DIR, f"{symbol}_{tf}.parquet")


def update_symbol(symbol: str):

    print(f"\n========== UPDATE {symbol} ==========")

    _ensure_cache_dir()

    path_ltf = _cache_path(symbol, LTF_INTERVAL)
    path_htf = _cache_path(symbol, HTF_INTERVAL)

    now = _now_utc_hour()
    start_required = now - timedelta(hours=HOURS_LOOKBACK)

    df = None
    last_ts = None

    # --------------------------------------------------
    # LOAD CACHE
    # --------------------------------------------------

    if os.path.exists(path_ltf):

        print("[CACHE] Loading LTF cache")

        df = pd.read_parquet(path_ltf)

        df.index = pd.to_datetime(df.index, utc=True)
        df = df.sort_index()

        if not df.empty:
            last_ts = df.index[-1]

    # --------------------------------------------------
    # DETERMINE FETCH WINDOW
    # --------------------------------------------------

    fetch_start = start_required if df is None else last_ts + timedelta(hours=1)
    fetch_end = now

    print("[FETCH WINDOW]")
    print("start:", fetch_start)
    print("end:", fetch_end)

    # --------------------------------------------------
    # FETCH NEW DATA
    # --------------------------------------------------

    if fetch_start <= fetch_end:

        new_data = fetch_ohlcv(
            symbol=symbol,
            interval=LTF_INTERVAL,
            start=fetch_start,
            end=fetch_end,
        )

        if not new_data.empty:

            print("[MERGE] merging new candles")

            df = pd.concat([df, new_data]) if df is not None else new_data

    # --------------------------------------------------
    # FINAL CLEAN
    # --------------------------------------------------

    df = df.sort_index()
    df = df[df.index >= start_required]
    df = df.iloc[-HOURS_LOOKBACK:]

    print("[DATA] final LTF candles:", len(df))

    # --------------------------------------------------
    # GAP CHECK
    # --------------------------------------------------

    expected = pd.date_range(
        start=df.index[0],
        periods=len(df),
        freq=LTF_INTERVAL,
        tz="UTC"
    )

    if not df.index.equals(expected):

        diff = df.index.symmetric_difference(expected)

        raise RuntimeError(
            f"[{symbol}] LTF GAP DETECTED {diff[:5]}"
        )

    # --------------------------------------------------
    # VALIDATE LTF
    # --------------------------------------------------

    validate_ohlcv(df, symbol, freq=LTF_INTERVAL)

    # --------------------------------------------------
    # BUILD HTF
    # --------------------------------------------------

    df_htf = build_htf(df, HTF_INTERVAL)

    validate_ohlcv(df_htf, symbol, freq=HTF_INTERVAL)

    print("[HTF] candles:", len(df_htf))

    # --------------------------------------------------
    # SAVE ATOMIC
    # --------------------------------------------------

    tmp_ltf = path_ltf + ".tmp"
    tmp_htf = path_htf + ".tmp"

    df.to_parquet(tmp_ltf)
    df_htf.to_parquet(tmp_htf)

    os.replace(tmp_ltf, path_ltf)
    os.replace(tmp_htf, path_htf)

    print("[SAVE] LTF + HTF cache updated")

    return df, df_htf