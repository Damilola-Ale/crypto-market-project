import sys
import os
from math import ceil
from datetime import datetime, timezone
import pandas as pd
import time
import requests

sys.path.append(os.path.dirname(os.path.dirname(__file__)))
from data_pipeline.fetcher import fetch_ohlcv

# --------------------------------------------------
# CONFIG
# --------------------------------------------------
SYMBOLS = ["BCHUSDT", "BANDUSDT", "TIAUSDT", "XLMUSDT", "SUIUSDT"]

LLTF_INTERVAL = "5m"
LTF_INTERVAL  = "1h"
HTF_INTERVAL  = "4h"

LLTF_LIMIT  = 24000
LTF_CANDLES = 2000
HTF_CANDLES = 500

CACHE_DIR = "data/cache"
os.makedirs(CACHE_DIR, exist_ok=True)

NOW_UTC = datetime.now(timezone.utc)
END = pd.Timestamp(NOW_UTC.replace(minute=0, second=0, microsecond=0))

# --------------------------------------------------
# HELPER
# --------------------------------------------------
def fetch_all(symbol, interval, total_limit, end_time):
    all_data = []
    remaining = total_limit
    current_end = int(end_time.timestamp() * 1000)

    while remaining > 0:
        limit = min(1000, remaining)
        df = fetch_ohlcv(symbol=symbol, interval=interval, limit=limit, end=pd.Timestamp(current_end, unit='ms'), verbose=False)  # ← suppress inner prints
        if df.empty:
            break

        all_data.insert(0, df)
        remaining -= len(df)
        current_end = int(df.index[0].timestamp() * 1000) - 1

        if len(df) < limit:
            break

        time.sleep(0.25)

    if all_data:
        result = pd.concat(all_data)
        return result
    return pd.DataFrame()

# --------------------------------------------------
# DOWNLOAD LOOP
# --------------------------------------------------
for symbol in SYMBOLS:
    print(f"\n=== Downloading {symbol} ===")

    # ---------------- 1H (fetch first — others derive their limits from it) ----------------
    df_1h = fetch_all(symbol, LTF_INTERVAL, LTF_CANDLES, END)
    df_1h.to_parquet(f"{CACHE_DIR}/{symbol}_1h.parquet")
    print(f"1h candles: {len(df_1h)} | {df_1h.index[0]} → {df_1h.index[-1]}")

    # ---------------- 4H (derived from 1H length) ----------------
    htf_needed = ceil(len(df_1h) / 4) + 1
    df_4h = fetch_all(symbol, HTF_INTERVAL, htf_needed, END)
    df_4h.to_parquet(f"{CACHE_DIR}/{symbol}_4h.parquet")
    print(f"4h candles: {len(df_4h)} | {df_4h.index[0]} → {df_4h.index[-1]}")

    # ---------------- 5M (derived from 1H length — 12 five-minute bars per hour) ----------------
    lltf_needed = len(df_1h) * 12
    df_5m = fetch_all(symbol, LLTF_INTERVAL, lltf_needed, END)
    df_5m.to_parquet(f"{CACHE_DIR}/{symbol}_5m.parquet")
    print(f"5m candles: {len(df_5m)} | {df_5m.index[0]} → {df_5m.index[-1]}")

print("\nALL CANDLES DOWNLOADED ✅")