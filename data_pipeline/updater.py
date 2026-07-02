import os
import pandas as pd
import time
from datetime import datetime, timezone, timedelta

from data_pipeline.fetcher import fetch_ohlcv
from data_pipeline.validators import validate_ohlcv
from execution.notifier import TelegramNotifier


CACHE_DIR = "data/cache"

HOURS_LOOKBACK = 900

LLTF_INTERVAL = "5m"
LTF_INTERVAL = "1h"
HTF_INTERVAL = "4h"


def _now_utc_hour():
    return datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)


def _ensure_cache_dir():
    os.makedirs(CACHE_DIR, exist_ok=True)


def _cache_path(symbol: str, tf: str):
    return os.path.join(CACHE_DIR, f"{symbol}_{tf}.parquet")

def _estimate_pages(start: datetime, end: datetime, interval: str) -> int:
    """Estimate worst-case number of 1000-bar pages needed to cover [start, end]."""
    interval_seconds = {"5m": 5 * 60, "1h": 60 * 60, "4h": 4 * 60 * 60}
    bar_sec = interval_seconds.get(interval, 60 * 60)
    bars    = max(0, (end - start).total_seconds()) / bar_sec
    return max(1, int(bars / 1000) + 1)

def _fetch_all(symbol: str, interval: str, start: datetime, end: datetime) -> pd.DataFrame:
    """
    Paginating fetch — works backwards from end until start is covered.
    Handles any window size, bypassing Binance's 1000-bar per request limit.
    """
    all_chunks = []
    current_end = end

    while True:
        df = fetch_ohlcv(
            symbol   = symbol,
            interval = interval,
            start    = start,
            end      = current_end,
            limit    = 1000,
            verbose  = False,
        )

        if df.empty:
            break

        all_chunks.insert(0, df)

        # If we've reached or passed the start, we're done
        if df.index[0] <= start:
            break

        # Step back: next fetch ends just before the earliest bar we got
        current_end = df.index[0] - pd.Timedelta(milliseconds=1)

    if not all_chunks:
        return pd.DataFrame()

    result = pd.concat(all_chunks)
    result = result[~result.index.duplicated(keep="last")]
    result = result.sort_index()
    return result


def continuity_fix_5m(symbol: str, df_lltf: pd.DataFrame, start_required: datetime) -> pd.DataFrame:
    """
    Two independent passes to catch two different staleness failure modes:

    1. CONTINUITY SCAN — a bar's open doesn't match the prior bar's close.
       Catches candles cached mid-formation (the original bug: 16:45/20:10
       style corruption where the WHOLE bar was wrong).

    2. BLIND TRAILING REVALIDATION — unconditionally refetches every bar
       in the last REVALIDATE_WINDOW_HOURS, regardless of continuity.
       Catches a second failure mode: Binance keeps revising a bar's
       high/low/close/volume for HOURS after it closes, while `open`
       stays untouched. Since open never changes, the continuity check
       is structurally blind to this — a stale bar with a correct open
       but wrong close/volume passes the continuity check every time.
       This is why LLTF_REVALIDATE_BARS's 30-minute window wasn't enough:
       Binance settlement can lag well past 30 minutes on volatile bars.
    """
    if df_lltf is None or df_lltf.empty or len(df_lltf) < 3:
        return df_lltf

    from data_pipeline.rate_limiter import rate_limiter

    # ── PASS 1: CONTINUITY SCAN (open vs prev close) ──────────────────
    CONTINUITY_TOLERANCE_PCT = 0.001  # 0.1% — real ticks never gap more than this on 5m closes
    MAX_CONTINUITY_REFETCHES_PER_RUN = 20  # safety cap — avoid runaway refetch storms

    df_sorted = df_lltf.sort_index()
    prev_close = df_sorted["close"].shift(1)
    prev_ts    = df_sorted.index.to_series().shift(1)
    gap_pct = (df_sorted["open"] - prev_close).abs() / prev_close.replace(0, pd.NA)

    is_adjacent = (df_sorted.index.to_series() - prev_ts) == pd.Timedelta(minutes=5)
    suspicious_mask = (gap_pct > CONTINUITY_TOLERANCE_PCT) & is_adjacent
    suspicious_ts = df_sorted.index[suspicious_mask.fillna(False)]
    suspicious_ts = suspicious_ts[suspicious_ts >= start_required]

    if len(suspicious_ts) > MAX_CONTINUITY_REFETCHES_PER_RUN:
        print(
            f"[CONTINUITY SCAN] {symbol} — {len(suspicious_ts)} suspicious bars found, "
            f"capping refetch to oldest {MAX_CONTINUITY_REFETCHES_PER_RUN} this run"
        )
        suspicious_ts = suspicious_ts[:MAX_CONTINUITY_REFETCHES_PER_RUN]

    for ts in suspicious_ts:
        print(f"[CONTINUITY GAP] {symbol} {ts} — open vs prev close diverged >{CONTINUITY_TOLERANCE_PCT*100:.2f}%, refetching")
        win_start = ts - timedelta(minutes=5)
        win_end   = ts + timedelta(minutes=5)
        rate_limiter.wait_if_needed_for_symbol(
            symbol       = f"{symbol}/5m_continuity",
            n_timeframes = 1,
            pages_per_tf = 1,
        )
        revalidated = _fetch_all(symbol, LLTF_INTERVAL, win_start, win_end)
        if not revalidated.empty:
            before = df_lltf.loc[df_lltf.index.isin(revalidated.index)]
            for rts in revalidated.index:
                if rts in before.index:
                    old_close = before.loc[rts, "close"]
                    new_close = revalidated.loc[rts, "close"]
                    if abs(old_close - new_close) > 1e-12:
                        print(f"[CONTINUITY FIX] {symbol} {rts} close corrected {old_close} → {new_close}")
            df_lltf = pd.concat([df_lltf, revalidated])
            df_lltf = df_lltf[~df_lltf.index.duplicated(keep="last")]

    # ── PASS 2: BLIND TRAILING REVALIDATION (open-invisible drift) ────
    REVALIDATE_WINDOW_HOURS = 3  # Binance settlement can lag well past 30 min
    now_ts = pd.Timestamp.now(tz="UTC")
    reval_start = now_ts - timedelta(hours=REVALIDATE_WINDOW_HOURS)

    df_sorted = df_lltf.sort_index()
    reval_start = max(reval_start, start_required)
    window_df = df_sorted[df_sorted.index >= reval_start]

    if not window_df.empty:
        rate_limiter.wait_if_needed_for_symbol(
            symbol       = f"{symbol}/5m_blind_revalidate",
            n_timeframes = 1,
            pages_per_tf = _estimate_pages(reval_start, now_ts, LLTF_INTERVAL),
        )
        blind_revalidated = _fetch_all(symbol, LLTF_INTERVAL, reval_start, now_ts)
        if not blind_revalidated.empty:
            before = df_lltf.loc[df_lltf.index.isin(blind_revalidated.index)]
            changed_count = 0
            for rts in blind_revalidated.index:
                if rts in before.index:
                    old_close = before.loc[rts, "close"]
                    new_close = blind_revalidated.loc[rts, "close"]
                    old_vol   = before.loc[rts, "volume"]
                    new_vol   = blind_revalidated.loc[rts, "volume"]
                    if abs(old_close - new_close) > 1e-12 or abs(old_vol - new_vol) > 1e-6:
                        changed_count += 1
            if changed_count:
                print(
                    f"[BLIND REVALIDATE] {symbol} — {changed_count} bar(s) in trailing "
                    f"{REVALIDATE_WINDOW_HOURS}h had stale close/volume (open-invisible drift), corrected"
                )
            df_lltf = pd.concat([df_lltf, blind_revalidated])
            df_lltf = df_lltf[~df_lltf.index.duplicated(keep="last")]

    return df_lltf.sort_index()

def update_symbol(symbol: str):

    print(f"\n========== UPDATE {symbol} ==========")

    _ensure_cache_dir()

    # --------------------------------------------------
    # ONE-TIME PURGE — wipe stale caches built before the
    # revalidation fix existed. Runs once per symbol via sentinel.
    # --------------------------------------------------
    _purge_sentinel = os.path.join(CACHE_DIR, f"{symbol}_revalidate_purge.done")
    if not os.path.exists(_purge_sentinel):
        for _tf in (LTF_INTERVAL, HTF_INTERVAL, LLTF_INTERVAL, "htf_scores"):
            _p = _cache_path(symbol, _tf)
            if os.path.exists(_p):
                os.remove(_p)
                print(f"[ONE-TIME PURGE] {symbol} — removed {_p}")
        with open(_purge_sentinel, "w") as f:
            f.write("done")
        print(f"[ONE-TIME PURGE] {symbol} — complete, will rebuild from scratch")

    # --------------------------------------------------
    # MIGRATE — delete old last_close meta format so the
    # new checksum format takes over cleanly on first run
    # --------------------------------------------------
    _old_meta_path = _cache_path(symbol, "htf_scores_meta")
    if os.path.exists(_old_meta_path):
        try:
            with open(_old_meta_path) as _f:
                _old_meta = _json.load(_f)
            if "last_close" in _old_meta and "checksum" not in _old_meta:
                os.remove(_old_meta_path)
                print(f"[MIGRATE] {symbol} — deleted old htf_scores_meta (last_close format)")
        except Exception:
            pass

    path_ltf  = _cache_path(symbol, LTF_INTERVAL)
    path_htf  = _cache_path(symbol, HTF_INTERVAL)
    path_lltf = _cache_path(symbol, LLTF_INTERVAL)

    now = datetime.now(timezone.utc)  # full timestamp for boundary check
    now_hour = now.replace(minute=0, second=0, microsecond=0)
    start_required = now_hour - timedelta(hours=HOURS_LOOKBACK)

    # Compute 4H boundary once — used in both fast-exit and slow paths
    hours_into_cycle = now_hour.hour % 4
    current_4h_open = now_hour - timedelta(hours=hours_into_cycle)

    # --------------------------------------------------
    # FAST EARLY-EXIT — nothing new to fetch
    # --------------------------------------------------
    if os.path.exists(path_lltf) and os.path.getsize(path_lltf) > 0:
        try:
            df_check = pd.read_parquet(path_lltf, columns=["close"])
            df_check.index = pd.to_datetime(df_check.index, utc=True)
            last_5m_ts = df_check.index[-1]

            minutes_floored = (now.minute // 5) * 5
            current_5m_boundary = now.replace(minute=minutes_floored, second=0, microsecond=0)
            candle_age_seconds = (now - current_5m_boundary).total_seconds()

            # wait at least 10 seconds after candle close before processing
            # prevents acting on unclosed or not-yet-propagated candles
            if last_5m_ts >= current_5m_boundary:
                if candle_age_seconds >= 10:
                    print(f"[SKIP] {symbol} — cache is current (last={last_5m_ts}, boundary={current_5m_boundary})")
                else:
                    print(f"[CANDLE FRESH] {symbol} — {candle_age_seconds:.0f}s since close, waiting for propagation")

                # check if 1H cache has the latest closed candle
                ltf_check = pd.read_parquet(path_ltf)
                ltf_check.index = pd.to_datetime(ltf_check.index, utc=True)
                if ltf_check.index[-1] < now_hour - timedelta(hours=1):
                    print(f"[SKIP BYPASSED] {symbol} — 1H cache behind ({ltf_check.index[-1]} < {now_hour - timedelta(hours=1)}), fetching")
                    raise Exception("1H cache stale — force full fetch")

                # also check 5m cache isn't stale by more than 2 bars
                expected_5m = current_5m_boundary
                actual_5m = last_5m_ts
                if (expected_5m - actual_5m).total_seconds() > 600:  # more than 2 bars behind
                    print(f"[SKIP BYPASSED] {symbol} — 5m cache stale by {(expected_5m - actual_5m).total_seconds()/60:.0f}m, fetching")
                    raise Exception("5m cache stale — force full fetch")

                df_lltf = pd.read_parquet(path_lltf)
                df_lltf.index = pd.to_datetime(df_lltf.index, utc=True)

                # Continuity scan runs on EVERY fast-exit tick, not just
                # full rebuilds — this path serves ~99% of ticks, so this
                # is where corrupted mid-formation bars actually get caught.
                _fix_start_required = now_hour - timedelta(hours=HOURS_LOOKBACK)
                df_lltf = continuity_fix_5m(symbol, df_lltf, _fix_start_required)
                tmp_fix = path_lltf + ".tmp"
                df_lltf.to_parquet(tmp_fix)
                os.replace(tmp_fix, path_lltf)

                df = ltf_check
                df_htf  = pd.read_parquet(path_htf)
                df_htf.index  = pd.to_datetime(df_htf.index,  utc=True)
                df = df[df.index <= now_hour - timedelta(hours=1)]
                hours_into_cycle = now_hour.hour % 4
                _last_closed_4h = now_hour - timedelta(hours=hours_into_cycle) - timedelta(hours=4)
                _current_4h_open = _last_closed_4h + timedelta(hours=4)
                df_htf = df_htf[df_htf.index < _current_4h_open]

                # Load HTF scores cache for fast-exit path
                _htf_scores = None
                _path_htf_scores = _cache_path(symbol, "htf_scores")
                _scores_meta_path = _cache_path(symbol, "htf_scores_meta")
                if os.path.exists(_path_htf_scores):
                    try:
                        _htf_scores = pd.read_parquet(_path_htf_scores)
                        _htf_scores.index = pd.to_datetime(_htf_scores.index, utc=True)

                        # validate checksum — stale scores from a different htf_df must not be served
                        import hashlib, json as _json
                        _htf_checksum = hashlib.md5(
                            df_htf['close'].round(8).values.tobytes()
                        ).hexdigest()
                        _scores_meta = {}
                        if os.path.exists(_scores_meta_path):
                            try:
                                with open(_scores_meta_path) as _f:
                                    _scores_meta = _json.load(_f)
                            except Exception:
                                pass
                        if _scores_meta.get("checksum") != _htf_checksum:
                            print(f"[FAST EXIT] {symbol} — htf_scores checksum mismatch, forcing full fetch")
                            raise Exception("htf_scores stale — force full fetch")

                    except Exception:
                        _htf_scores = None

                return df, df_htf, df_lltf, _htf_scores
        except Exception as e:
            import pyarrow.lib as _pal
            # ArrowInvalid inherits from ValueError — must check isinstance
            # before any isinstance(e, ValueError) branch or it gets swallowed.
            _is_file_error = isinstance(
                e, (OSError, PermissionError, MemoryError,
                    _pal.ArrowInvalid, _pal.ArrowIOError)
            )
            if _is_file_error:
                print(f"[CACHE CORRUPT] {symbol} — {type(e).__name__}: {e}")
                for _p in [path_lltf, path_ltf]:
                    try:
                        if os.path.exists(_p):
                            os.remove(_p)
                            print(f"[CACHE CORRUPT] deleted {_p}")
                    except Exception:
                        pass
                try:
                    TelegramNotifier().send_text(
                        f"⚠️ *CACHE FILE ERROR*\n"
                        f"`{symbol}` `{type(e).__name__}`\n"
                        f"`{str(e)[:200]}`"
                    )
                except Exception:
                    pass
            elif isinstance(e, (ValueError, KeyError, IndexError)):
                print(f"[SKIP CHECK BYPASSED] {symbol} — {e}, fetching")
            else:
                print(f"[SKIP FAILED] {symbol} — {type(e).__name__}: {e}")
            # always fall through to full fetch

    now_full = now   # preserve full-precision timestamp for 5m fetch
    now = now_hour   # 1H and 4H fetches use top-of-hour only

    df = None
    last_ts = None

    # --------------------------------------------------
    # LOAD CACHE
    # --------------------------------------------------

    if os.path.exists(path_ltf):
        if os.path.getsize(path_ltf) == 0:
            print(f"[WARN] LTF cache is 0 bytes, discarding: {path_ltf}")
            os.remove(path_ltf)
        else:
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
    fetch_end = now_hour  # fetch up to current hour boundary, trim after

    print("[FETCH WINDOW]")
    print("start:", fetch_start)
    print("end:", fetch_end)

    # --------------------------------------------------
    # FETCH NEW DATA
    # --------------------------------------------------

    if fetch_start <= fetch_end:
        from data_pipeline.rate_limiter import rate_limiter
        rate_limiter.wait_if_needed_for_symbol(
            symbol       = f"{symbol}/1h",
            n_timeframes = 1,
            pages_per_tf = _estimate_pages(fetch_start, fetch_end, LTF_INTERVAL),
        )
        new_data = _fetch_all(symbol, LTF_INTERVAL, fetch_start, fetch_end)

        if not new_data.empty:

            print("[MERGE] merging new candles")

            df = pd.concat([df, new_data]) if df is not None else new_data
            df = df[~df.index.duplicated(keep="last")]

    if df is None or df.empty:
        raise RuntimeError(f"[{symbol}] No LTF data available after fetch")

    # --------------------------------------------------
    # REVALIDATE LAST CLOSED BARS — Binance can revise/finalize
    # a bar's close/volume shortly after it closes, and a previous
    # cron tick may have cached it mid-formation. Always refetch
    # the last few closed bars and overwrite the cache copies.
    # --------------------------------------------------
    REVALIDATE_BARS = 3
    revalidate_start = (now_hour - timedelta(hours=1)) - timedelta(hours=REVALIDATE_BARS - 1)
    revalidate_end   = now_hour - timedelta(hours=1)

    if revalidate_start in df.index or revalidate_end in df.index:
        from data_pipeline.rate_limiter import rate_limiter
        rate_limiter.wait_if_needed_for_symbol(
            symbol       = f"{symbol}/1h_revalidate",
            n_timeframes = 1,
            pages_per_tf = 1,
        )
        revalidated = _fetch_all(symbol, LTF_INTERVAL, revalidate_start, revalidate_end)
        if not revalidated.empty:
            before = df.loc[df.index.isin(revalidated.index)]
            changed = []
            for ts in revalidated.index:
                if ts in before.index:
                    old_close = before.loc[ts, "close"]
                    new_close = revalidated.loc[ts, "close"]
                    if abs(old_close - new_close) > 1e-12:
                        changed.append((ts, old_close, new_close))
            df = pd.concat([df, revalidated])
            df = df[~df.index.duplicated(keep="last")]
            if changed:
                for ts, old_c, new_c in changed:
                    print(f"[REVALIDATE] {symbol} {ts} close corrected {old_c} → {new_c}")

    # --------------------------------------------------
    # FINAL CLEAN
    # --------------------------------------------------

    df = df.sort_index()
    df = df[df.index >= start_required]
    df = df[df.index <= now_hour - timedelta(hours=1)]  # only closed bars before gap check and save
    df = df.iloc[-HOURS_LOOKBACK:]

    print("[DATA] final LTF candles:", len(df))

    # --------------------------------------------------
    # GAP CHECK
    # --------------------------------------------------

    # Floor to the interval frequency before comparison.
    # Binance occasionally returns candles with sub-millisecond timestamp
    # offsets (13:00:00.001 instead of 13:00:00.000). Without flooring,
    # symmetric_difference sees the offset bar as both missing and extra,
    # raises RuntimeError, and the symbol is dead until cache rebuilds.
    df.index = df.index.floor(LTF_INTERVAL)
    df = df[~df.index.duplicated(keep="last")]

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
    # BUILD HTF (incremental, cache-aware)
    # --------------------------------------------------

    df_htf = None
    last_htf_ts = None

    # Load HTF cache if exists
    if os.path.exists(path_htf):
        if os.path.getsize(path_htf) == 0:
            print(f"[WARN] HTF cache is 0 bytes, discarding: {path_htf}")
            os.remove(path_htf)
        else:
            print("[CACHE] Loading HTF cache")
            df_htf = pd.read_parquet(path_htf)
            df_htf.index = pd.to_datetime(df_htf.index, utc=True)
            df_htf = df_htf.sort_index()
            if not df_htf.empty:
                last_htf_ts = df_htf.index[-1]

    # Determine fetch window
    htf_fetch_start = start_required if df_htf is None else last_htf_ts + timedelta(hours=4)
    htf_fetch_end = current_4h_open  # fetch up to but not including the open bar

    print("[FETCH HTF WINDOW]")
    print("start:", htf_fetch_start)
    print("end:", htf_fetch_end)

    # Fetch only missing HTF candles
    if htf_fetch_start <= htf_fetch_end:
        from data_pipeline.rate_limiter import rate_limiter
        rate_limiter.wait_if_needed_for_symbol(
            symbol       = f"{symbol}/4h",
            n_timeframes = 1,
            pages_per_tf = _estimate_pages(htf_fetch_start, htf_fetch_end, HTF_INTERVAL),
        )
        new_htf = _fetch_all(symbol, HTF_INTERVAL, htf_fetch_start, htf_fetch_end)

        if not new_htf.empty:
            print("[MERGE HTF] merging new candles")
            df_htf = pd.concat([df_htf, new_htf]) if df_htf is not None else new_htf
            df_htf = df_htf[~df_htf.index.duplicated(keep="last")]
    
    if df_htf is None or df_htf.empty:
        raise RuntimeError(f"[{symbol}] No HTF data available after fetch")

    # Only keep closed 4H bars.
    # A 4H bar that opened at T is closed when now >= T + 4h.
    # last_closed_4h = the open timestamp of the most recent fully closed 4H bar.
    # Example: now_hour=21:00 → 21%4=1 → last_closed_4h = 21:00 - 1h - 4h = 16:00 ✓
    # Example: now_hour=20:00 → 20%4=0 → last_closed_4h = 20:00 - 0h - 4h = 16:00 ✓
    # The 16:00 bar closes exactly at 20:00 — we exclude it at the boundary to be safe.

    df_htf = df_htf.sort_index()
    df_htf = df_htf[df_htf.index >= start_required]
    df_htf = df_htf[df_htf.index < current_4h_open]  # exclude open bar — prevents HTF_QUALITY drift
    df_htf = df_htf.iloc[-HOURS_LOOKBACK:]
    validate_ohlcv(df_htf, symbol, freq=HTF_INTERVAL)

    print(f"[DEBUG] live htf_df last={df_htf.index[-1]} len={len(df_htf)} current_4h_open={current_4h_open}")
    # TEMP DIAGNOSTIC
    for _ts, _row in df_htf.tail(3).iterrows():
        print(f"[DEBUG HTF BAR] {_ts} | open={_row['open']:.4f} close={_row['close']:.4f} volume={_row['volume']:.2f}")

    print("[HTF] candles:", len(df_htf))

    # --------------------------------------------------
    # HTF SCORES CACHE (compute once per 4H close)
    # --------------------------------------------------
    from indicators.indicators import compute_htf_scores

    path_htf_scores = _cache_path(symbol, "htf_scores")
    htf_scores = None
    last_scores_ts = None

    if os.path.exists(path_htf_scores):
        try:
            htf_scores = pd.read_parquet(path_htf_scores)
            htf_scores.index = pd.to_datetime(htf_scores.index, utc=True)
            last_scores_ts = htf_scores.index[-1]
        except Exception as e:
            print(f"[HTF SCORES] cache load failed: {e}, recomputing")
            htf_scores = None
            last_scores_ts = None

    import json as _json
    import hashlib

    htf_last_ts = df_htf.index[-1]

    _htf_checksum = hashlib.md5(
        df_htf['close'].round(8).values.tobytes()
    ).hexdigest()

    _scores_meta_path = _cache_path(symbol, "htf_scores_meta")
    _scores_meta = {}
    if os.path.exists(_scores_meta_path):
        try:
            with open(_scores_meta_path) as f:
                _scores_meta = _json.load(f)
        except Exception:
            pass

    _cached_checksum = _scores_meta.get("checksum")
    _ts_changed      = last_scores_ts is None or str(last_scores_ts) != str(htf_last_ts)
    _data_changed    = _cached_checksum != _htf_checksum

    if _ts_changed or _data_changed:
        print(
            f"[HTF SCORES] recomputing — ts_changed={_ts_changed} "
            f"data_changed={_data_changed} checksum={_htf_checksum[:8]}"
        )
        htf_scores = compute_htf_scores(df_htf)

        tmp_scores = path_htf_scores + ".tmp"
        htf_scores.to_parquet(tmp_scores)
        os.replace(tmp_scores, path_htf_scores)

        with open(_scores_meta_path + ".tmp", "w") as f:
            _json.dump({
                "last_ts":  str(htf_last_ts),
                "checksum": _htf_checksum,
            }, f)
        os.replace(_scores_meta_path + ".tmp", _scores_meta_path)

        print(f"[HTF SCORES] saved — {len(htf_scores)} bars, last={htf_scores.index[-1]}")
    else:
        print(f"[HTF SCORES] cache current — last={last_scores_ts} checksum={_htf_checksum[:8]}")

    # --------------------------------------------------
    # SAVE ATOMIC
    # --------------------------------------------------
    os.makedirs(CACHE_DIR, exist_ok=True)

    tmp_ltf = path_ltf + ".tmp"
    tmp_htf = path_htf + ".tmp"

    df.to_parquet(tmp_ltf)
    df_htf.to_parquet(tmp_htf)

    os.makedirs(os.path.dirname(path_ltf), exist_ok=True)
    os.replace(tmp_ltf, path_ltf)
    os.makedirs(os.path.dirname(path_htf), exist_ok=True)
    os.replace(tmp_htf, path_htf)

    print("[SAVE] LTF + HTF cache updated")

    # --------------------------------------------------
    # BUILD LLTF (5M) — same pattern as HTF
    # --------------------------------------------------
    path_lltf    = _cache_path(symbol, LLTF_INTERVAL)
    df_lltf      = None
    last_lltf_ts = None

    if os.path.exists(path_lltf):
        if os.path.getsize(path_lltf) == 0:
            print(f"[WARN] LLTF cache is 0 bytes, discarding: {path_lltf}")
            os.remove(path_lltf)
        else:
            print("[CACHE] Loading LLTF cache")
            df_lltf = pd.read_parquet(path_lltf)
            df_lltf.index = pd.to_datetime(df_lltf.index, utc=True)
            df_lltf = df_lltf.sort_index()
            if not df_lltf.empty:
                last_lltf_ts = df_lltf.index[-1]

    lltf_fetch_start = start_required if df_lltf is None else last_lltf_ts + timedelta(minutes=5)

    # Only ever fetch/cache CLOSED 5m candles. Using unfloored `now_full`
    # here let Binance return the still-forming candle (openTime <= endTime
    # is always true for the current bar), which then got written into the
    # persistent parquet cache as if it were final — with a partial
    # high/low/volume that undercounts the real bar. Once current_5m_boundary
    # advanced past it, the FAST EARLY-EXIT path above saw
    # last_5m_ts >= current_5m_boundary and returned early on every
    # subsequent tick, skipping the revalidation block below entirely — so
    # the corrupted partial bar was never corrected.
    _minutes_floored_now = (now_full.minute // 5) * 5
    _current_5m_boundary_full = now_full.replace(minute=_minutes_floored_now, second=0, microsecond=0)
    lltf_fetch_end = _current_5m_boundary_full - timedelta(milliseconds=1)

    print("[FETCH LLTF WINDOW]")
    print("start:", lltf_fetch_start)
    print("end:  ", lltf_fetch_end)

    if lltf_fetch_start <= lltf_fetch_end:
        from data_pipeline.rate_limiter import rate_limiter
        rate_limiter.wait_if_needed_for_symbol(
            symbol       = f"{symbol}/5m",
            n_timeframes = 1,
            pages_per_tf = _estimate_pages(lltf_fetch_start, lltf_fetch_end, LLTF_INTERVAL),
        )
        new_lltf = _fetch_all(symbol, LLTF_INTERVAL, lltf_fetch_start, lltf_fetch_end)
        
        if not new_lltf.empty:
            print("[MERGE LLTF] merging new candles")
            df_lltf = pd.concat([df_lltf, new_lltf]) if df_lltf is not None else new_lltf
            df_lltf = df_lltf[~df_lltf.index.duplicated(keep="last")]

    if df_lltf is None or df_lltf.empty:
        raise RuntimeError(f"[{symbol}] No LLTF data available after fetch")

    # --------------------------------------------------
    # REVALIDATE LAST CLOSED 5M BARS — same reasoning as the 1H block:
    # Binance can revise a bar's high/low/close/volume for a short window
    # after it closes (or even while it's still forming, if fetched too
    # early). Without this, a bar fetched moments after opening gets
    # permanently frozen at that near-empty snapshot.
    # --------------------------------------------------
    LLTF_REVALIDATE_BARS = 6  # 30 minutes — normal recent-bar settle window
    lltf_revalidate_end   = now_full - timedelta(minutes=5)
    lltf_revalidate_start = lltf_revalidate_end - timedelta(minutes=5 * (LLTF_REVALIDATE_BARS - 1))

    # ── CONTINUITY SCAN — catch bars cached mid-formation, no matter how
    # old they are. A real candle's open must equal the previous candle's
    # close (within float tolerance). Any bar that breaks continuity was
    # almost certainly frozen at a partial snapshot and never corrected,
    # because the fixed 30-minute window above only looks at recent bars.
    # This also serves as a one-time (and ongoing) cleanup for cache
    # files that already contain corrupted bars from before this fix —
    # no manual intervention needed, it self-heals on the next run.
    CONTINUITY_TOLERANCE_PCT = 0.001  # 0.1% — real ticks never gap more than this on 5m closes
    MAX_CONTINUITY_REFETCHES_PER_RUN = 20  # safety cap — avoid runaway refetch storms

    df_lltf_sorted = df_lltf.sort_index()
    prev_close = df_lltf_sorted["close"].shift(1)
    prev_ts    = df_lltf_sorted.index.to_series().shift(1)
    gap_pct = (df_lltf_sorted["open"] - prev_close).abs() / prev_close.replace(0, pd.NA)

    is_adjacent = (df_lltf_sorted.index.to_series() - prev_ts) == pd.Timedelta(minutes=5)
    suspicious_mask = (gap_pct > CONTINUITY_TOLERANCE_PCT) & is_adjacent
    suspicious_ts = df_lltf_sorted.index[suspicious_mask.fillna(False)]

    if len(suspicious_ts) > MAX_CONTINUITY_REFETCHES_PER_RUN:
        print(
            f"[CONTINUITY SCAN] {symbol} — {len(suspicious_ts)} suspicious bars found, "
            f"capping refetch to oldest {MAX_CONTINUITY_REFETCHES_PER_RUN} this run "
            f"(remainder will be caught on subsequent runs)"
        )
        suspicious_ts = suspicious_ts[:MAX_CONTINUITY_REFETCHES_PER_RUN]

    # Trailing revalidation window — mirrors continuity_fix_5m's Pass 2.
    # 30 minutes was proven insufficient (see continuity_fix_5m comments):
    # Binance can revise close/high/low/volume for hours after a bar closes.
    # Reuse the same 3-hour blind-revalidation logic here so the slow path
    # (restarts, new-hour boundaries, cache gaps) gets the same protection
    # as the fast-exit path, instead of a separate, shorter window.
    df_lltf = continuity_fix_5m(symbol, df_lltf, start_required)
    revalidate_targets = set()

    for ts in suspicious_ts:
        win_start = ts - timedelta(minutes=5)
        win_end   = ts + timedelta(minutes=5)
        revalidate_targets.add((win_start, win_end))
        print(f"[CONTINUITY GAP] {symbol} {ts} — open vs prev close diverged >{CONTINUITY_TOLERANCE_PCT*100:.2f}%, flagging for refetch")

    if revalidate_targets:
        from data_pipeline.rate_limiter import rate_limiter
        for r_start, r_end in revalidate_targets:
            rate_limiter.wait_if_needed_for_symbol(
                symbol       = f"{symbol}/5m_revalidate",
                n_timeframes = 1,
                pages_per_tf = 1,
            )
            lltf_revalidated = _fetch_all(symbol, LLTF_INTERVAL, r_start, r_end)
            if not lltf_revalidated.empty:
                before = df_lltf.loc[df_lltf.index.isin(lltf_revalidated.index)]
                for ts in lltf_revalidated.index:
                    if ts in before.index:
                        old_close = before.loc[ts, "close"]
                        new_close = lltf_revalidated.loc[ts, "close"]
                        if abs(old_close - new_close) > 1e-12:
                            print(f"[REVALIDATE LLTF] {symbol} {ts} close corrected {old_close} → {new_close}")
                df_lltf = pd.concat([df_lltf, lltf_revalidated])
                df_lltf = df_lltf[~df_lltf.index.duplicated(keep="last")]

        df_lltf = df_lltf.sort_index()

    df_lltf = df_lltf.sort_index()
    df_lltf = df_lltf[df_lltf.index >= start_required]
    df_lltf = df_lltf.iloc[-(HOURS_LOOKBACK * 12):]
    try:
        validate_ohlcv(df_lltf, symbol, freq=LLTF_INTERVAL)
    except RuntimeError as e:
        print(f"[WARN] LLTF validation failed for {symbol} (non-fatal): {e}")

    os.makedirs(CACHE_DIR, exist_ok=True)

    tmp_lltf = path_lltf + ".tmp"
    df_lltf.to_parquet(tmp_lltf)
    os.makedirs(os.path.dirname(path_lltf), exist_ok=True)
    os.replace(tmp_lltf, path_lltf)

    print("[SAVE] LLTF cache updated | candles:", len(df_lltf))

    return df, df_htf, df_lltf, htf_scores