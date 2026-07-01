"""
Local candle dump — mirrors the /debug/candle-dump Flask route, but reads
straight off disk on your local machine instead of over HTTP.

Default source is data/backtest_cache (what main.py / the backtest writes to).
Pass --source live to point it at data/cache instead (only useful if you've
pulled a copy of the Render cache down locally).

Output JSON shape matches the live route exactly, so you can diff the two
directly — e.g. save one to bt.json and one to live.json and `diff` them,
or load both into pandas and compare cell-by-cell.

USAGE:
    python candle_dump_local.py --symbol NFPUSDT
    python candle_dump_local.py --symbol NFPUSDT --hours-back 12
    python candle_dump_local.py --symbol NFPUSDT --start "2026-07-01 04:00:00" --end "2026-07-01 04:30:00"
    python candle_dump_local.py --symbol NFPUSDT --source live
    python candle_dump_local.py --symbol NFPUSDT --out nfp_backtest_dump.json
"""

import argparse
import json
import os
from datetime import datetime, timezone, timedelta

import pandas as pd


def dump_candles(symbol, source="backtest", hours_back=4, start=None, end=None):
    symbol = symbol.upper()
    cache_dir = "data/backtest_cache" if source == "backtest" else "data/cache"

    now_utc = datetime.now(timezone.utc)
    end_ts = pd.Timestamp(end, tz="UTC") if end else pd.Timestamp(now_utc)
    start_ts = pd.Timestamp(start, tz="UTC") if start else end_ts - timedelta(hours=hours_back)

    result = {
        "symbol": symbol,
        "source": source,
        "cache_dir": cache_dir,
        "server_time_utc": now_utc.isoformat(),
        "window_start": str(start_ts),
        "window_end": str(end_ts),
        "timeframes": {},
    }

    for tf in ("5m", "1h", "4h"):
        path = os.path.join(cache_dir, f"{symbol}_{tf}.parquet")

        if not os.path.exists(path):
            result["timeframes"][tf] = {"exists": False, "path": path}
            continue

        try:
            df = pd.read_parquet(path)
            df.index = pd.to_datetime(df.index, utc=True)
            window = df.loc[start_ts:end_ts, ["open", "high", "low", "close", "volume"]]

            result["timeframes"][tf] = {
                "exists": True,
                "cache_first": str(df.index[0]),
                "cache_last": str(df.index[-1]),
                "rows_in_window": len(window),
                "data": {
                    str(ts): row.to_dict() for ts, row in window.iterrows()
                },
            }
        except Exception as e:
            result["timeframes"][tf] = {"exists": True, "error": str(e)}

    return result


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Dump local candle cache (mirrors /debug/candle-dump)")
    parser.add_argument("--symbol", required=True, help="e.g. NFPUSDT")
    parser.add_argument("--source", default="backtest", choices=["backtest", "live"],
                         help="which local cache dir to read (default: backtest)")
    parser.add_argument("--hours-back", type=float, default=4,
                         help="window size in hours, ending now (default: 4)")
    parser.add_argument("--start", default=None, help="e.g. '2026-07-01 04:00:00' (UTC)")
    parser.add_argument("--end", default=None, help="e.g. '2026-07-01 04:30:00' (UTC)")
    parser.add_argument("--out", default=None, help="optional path to save JSON output")

    args = parser.parse_args()

    result = dump_candles(
        symbol=args.symbol,
        source=args.source,
        hours_back=args.hours_back,
        start=args.start,
        end=args.end,
    )

    output = json.dumps(result, indent=2)
    print(output)

    if args.out:
        with open(args.out, "w") as f:
            f.write(output)
        print(f"\n[SAVED] {args.out}")