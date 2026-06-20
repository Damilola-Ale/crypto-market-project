import pandas as pd
import numpy as np
import requests
import matplotlib.pyplot as plt
import time

from indicators.indicators import generate_signal
from backtest import SignalBacktester
from trade_diagnostics import diagnose_trades
from diagnostics import plot_asymmetry


# ==========================================================
# BINANCE DATA FETCHER
# ==========================================================

BINANCE_URL = "https://api.binance.com/api/v3/klines"

def fetch_binance(symbol, interval, limit):

    all_data = []
    end_time = None

    for attempt in range(3):
        try:
            all_data = []
            end_time = None

            while len(all_data) < limit:

                params = {
                    "symbol": symbol,
                    "interval": interval,
                    "limit": min(1000, limit - len(all_data))
                }

                if end_time:
                    params["endTime"] = end_time

                response = requests.get(BINANCE_URL, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()

                if isinstance(data, dict):
                    print(f"[FETCH] {symbol} {interval} — Binance error: {data}")
                    raise RuntimeError(f"Binance error: {data}")

                if not isinstance(data, list) or not data:
                    break

                all_data = data + all_data

                first_open_time = data[0][0]
                end_time = first_open_time - 1
                time.sleep(0.25)

                if len(data) < 1000:
                    break

            break  # success

        except Exception as e:
            print(f"[FETCH] {symbol} {interval} — attempt {attempt+1} failed: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                raise RuntimeError(f"[FETCH] {symbol} {interval} — all retries failed: {e}")

    if not all_data:
        raise RuntimeError(f"[FETCH] {symbol} {interval} — no data returned after retries")

    df = pd.DataFrame(all_data, columns=[
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "num_trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore"
    ])

    df = df[["open_time", "open", "high", "low", "close", "volume", "taker_buy_base"]]

    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.drop(columns=["open_time"])
    df = df.set_index("timestamp")
    df = df.astype(float)

    return df

# ==========================================================
# CONFIG 
# ==========================================================
# SYMBOLS = [
#     "SOLUSDT", "ICXUSDT", "RUNEUSDT", "ZILUSDT", "OPUSDT", "LDOUSDT", "SUIUSDT",
#     "ADAUSDT", "APTUSDT", "LINKUSDT", "AAVEUSDT", "GMXUSDT", "LSKUSDT", "TONUSDT",
#     "AXLUSDT", "SANDUSDT", "VETUSDT", "ORDIUSDT", "TRBUSDT", "LTCUSDT", "LUMIAUSDT",
#     "IDUSDT", "ETHUSDT", "ZECUSDT", "DEXEUSDT", "RPLUSDT", "GRTUSDT",
#     "RENDERUSDT", "PAXGUSDT", "IOSTUSDT", "KNCUSDT", "KAVAUSDT", "RLCUSDT", "BELUSDT"
# ] IOST KNC KAVA RLC | KSM BEL
SYMBOL = "KNCUSDT"

LLTF_INTERVAL = "5m"
LTF_INTERVAL = "1h"
HTF_INTERVAL = "4h"

LEVERAGE = 1

# LLTF_LIMIT = 630720
# LTF_LIMIT = 52560   # ~30 days of 1h candles
# HTF_LIMIT = 13140   # ~120 days of 4h candles

# LTF_LIMIT = 43800   # ~30 days of 1h candles
# HTF_LIMIT = 10950   # ~120 days of 4h candles

# LLTF_LIMIT = 420480
# LTF_LIMIT = 35040   # ~30 days of 1h candles
# HTF_LIMIT = 8760   # ~120 days of 4h candles

# LTF_LIMIT = 26280   # ~30 days of 1h candles
# HTF_LIMIT = 6570   # ~120 days of 4h candles

LLTF_LIMIT = 210240
LTF_LIMIT = 17520   # ~30 days of 1h candles
HTF_LIMIT = 4380   # ~120 days of 4h candles

# LTF_LIMIT = 8760   # ~30 days of 1h candles
# HTF_LIMIT = 2190   # ~120 days of 4h candles

# LLTF_LIMIT = 52560
# LTF_LIMIT = 4380   # ~30 days of 1h candles
# HTF_LIMIT = 1095   # ~120 days of 4h candles

# LLTF_LIMIT = 24000
# LTF_LIMIT = 2000   # ~30 days of 1h candles
# HTF_LIMIT = 500   # ~120 days of 4h candles

# LLTF_LIMIT = 12000
# LTF_LIMIT = 1000   # ~30 days of 1h candles
# HTF_LIMIT = 250   # ~120 days of 4h candles

# LLTF_LIMIT = 9600
# LTF_LIMIT = 800   # ~30 days of 1h candles
# HTF_LIMIT = 200   # ~120 days of 4h candles

# ==========================================================
# FETCH DATA
# ==========================================================

now_utc = pd.Timestamp.now(tz="UTC")

import os

CACHE_DIR = "data/backtest_cache"
os.makedirs(CACHE_DIR, exist_ok=True)

def load_or_fetch(symbol, interval, limit, now_utc):
    path = os.path.join(CACHE_DIR, f"{symbol}_{interval}.parquet")
    sentinel_path = os.path.join(CACHE_DIR, f"{symbol}_{interval}.oldest")  # ← new

    INTERVAL_SECONDS = {"5m": 300, "1h": 3600, "4h": 14400}
    interval_td = pd.Timedelta(seconds=INTERVAL_SECONDS.get(interval, 3600))

    if os.path.exists(path):
        cached = pd.read_parquet(path)
        cached.index = pd.to_datetime(cached.index, utc=True)
        cached = cached.sort_index()

        required_start = now_utc - limit * interval_td
        cache_start    = cached.index[0]

        # ── STEP 1: backward extension ──
        # Skip if we already know we've hit the listing wall
        already_at_oldest = os.path.exists(sentinel_path)

        if not already_at_oldest and cache_start > required_start + interval_td:
            print(f"[CACHE] {symbol} {interval} — cache starts at {cache_start}, need {required_start}, fetching older bars...")
            old_data = fetch_binance_range(symbol, interval, required_start, cache_start - interval_td)
            print(f"[CACHE] {symbol} {interval} — backward fetch returned {len(old_data)} bars")

            if not old_data.empty:
                cached = pd.concat([old_data, cached])
                cached = cached[~cached.index.duplicated(keep="last")]
                cached = cached.sort_index()
                print(f"[CACHE] {symbol} {interval} — extended backward, now {len(cached)} bars")
            else:
                # Got nothing — we've hit the listing wall, record it
                print(f"[CACHE] {symbol} {interval} — hit listing wall at {cache_start}, saving sentinel")
                with open(sentinel_path, "w") as f:
                    f.write(str(cache_start))
        elif already_at_oldest:
            print(f"[CACHE] {symbol} {interval} — listing wall known, skipping backward fetch")

        # ── STEP 2: extend FORWARD if cache is behind current time ──
        last_ts = cached.index[-1]

        if interval == "1h":
            fetch_start = last_ts + interval_td
            fetch_end   = now_utc.floor("h")
        elif interval == "4h":
            fetch_start = last_ts + interval_td
            fetch_end   = now_utc
        elif interval == "5m":
            fetch_start = last_ts + interval_td
            fetch_end   = now_utc
        else:
            fetch_start = last_ts + interval_td
            fetch_end   = now_utc

        if fetch_start <= fetch_end:
            print(f"[CACHE] {symbol} {interval} — fetching new bars from {fetch_start} to {fetch_end}")
            new_data = fetch_binance_range(symbol, interval, fetch_start, fetch_end)
            print(f"[CACHE] {symbol} {interval} — forward fetch returned {len(new_data)} bars")
            if not new_data.empty:
                cached = pd.concat([cached, new_data])
                cached = cached[~cached.index.duplicated(keep="last")]
                cached = cached.sort_index()
        else:
            print(f"[CACHE] {symbol} {interval} — cache current at {last_ts}")

        # ── STEP 2.5: REVALIDATE last few closed bars ──
        # Binance can finalize a bar's close/volume after it closes, and a
        # previous run may have cached it mid-formation. Refetch the last
        # few bars and overwrite with current values.
        REVALIDATE_BARS = 3
        revalidate_end   = last_ts
        revalidate_start = revalidate_end - (REVALIDATE_BARS - 1) * interval_td

        revalidated = fetch_binance_range(symbol, interval, revalidate_start, revalidate_end)
        if not revalidated.empty:
            before = cached.loc[cached.index.isin(revalidated.index)]
            for ts in revalidated.index:
                if ts in before.index:
                    old_close = before.loc[ts, "close"]
                    new_close = revalidated.loc[ts, "close"]
                    if abs(old_close - new_close) > 1e-12:
                        print(f"[REVALIDATE] {symbol} {interval} {ts} close corrected {old_close} → {new_close}")
            cached = pd.concat([cached, revalidated])
            cached = cached[~cached.index.duplicated(keep="last")]
            cached = cached.sort_index()

        # ── STEP 3: save the FULL cache (never trim to limit) ──
        # Trimming to limit is what caused the 2-month test to destroy
        # the 2-year cache. Save everything, slice on return only.
        cached.to_parquet(path)
        print(f"[CACHE] {symbol} {interval} — saved {len(cached)} bars total")

        # ── STEP 4: return only the requested window ──
        return cached.iloc[-limit:].copy()

    else:
        print(f"[CACHE] {symbol} {interval} — no cache, downloading {limit} bars...")
        df = fetch_binance(symbol, interval, limit)
        df.to_parquet(path)
        print(f"[CACHE] {symbol} {interval} — saved {len(df)} bars")
        return df

def fetch_binance_range(symbol, interval, start, end):
    start_ms = int(start.timestamp() * 1000)
    end_ms   = int(end.timestamp() * 1000)

    if start_ms >= end_ms:
        print(f"[FETCH RANGE] {symbol} {interval} — start >= end, nothing to fetch")
        return pd.DataFrame()

    all_data = []
    current_end_ms = end_ms

    for attempt in range(3):
        try:
            while True:
                params = {
                    "symbol":   symbol.replace("-", "").upper(),
                    "interval": interval,
                    "limit":    1000,
                    "endTime":  current_end_ms,
                    # ← NO startTime here; we walk backward and break manually
                }
                response = requests.get(BINANCE_URL, params=params, timeout=10)
                response.raise_for_status()
                data = response.json()

                if isinstance(data, dict):
                    print(f"[FETCH RANGE] {symbol} {interval} — Binance error: {data}")
                    raise RuntimeError(f"Binance error: {data}")

                if not isinstance(data, list) or len(data) == 0:
                    break

                all_data = data + all_data
                oldest = data[0][0]

                # Stop if we've reached or passed our desired start
                if oldest <= start_ms:
                    break

                # Stop if Binance returned a partial page (no more history)
                if len(data) < 1000:
                    break

                current_end_ms = oldest - 1
                time.sleep(0.25)

            break  # success

        except Exception as e:
            print(f"[FETCH RANGE] {symbol} {interval} — attempt {attempt+1} failed: {e}")
            if attempt < 2:
                time.sleep(2 ** attempt)
            else:
                print(f"[FETCH RANGE] {symbol} {interval} — all retries failed, cache will remain stale")
                return pd.DataFrame()

    if not all_data:
        return pd.DataFrame()

    df = pd.DataFrame(all_data, columns=[
        "open_time", "open", "high", "low", "close", "volume",
        "close_time", "quote_asset_volume", "num_trades",
        "taker_buy_base", "taker_buy_quote", "ignore"
    ])
    df = df[["open_time", "open", "high", "low", "close", "volume", "taker_buy_base"]]
    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms", utc=True)
    df = df.drop(columns=["open_time"]).set_index("timestamp").astype(float)
    df = df[~df.index.duplicated(keep="last")].sort_index()

    # Trim to requested window AFTER fetching
    df = df[df.index >= pd.to_datetime(start_ms, unit="ms", utc=True)]
    df = df[df.index <= pd.to_datetime(end_ms,   unit="ms", utc=True)]

    return df

print("Loading LLTF data (5m)...")
lltf_df = load_or_fetch(SYMBOL, LLTF_INTERVAL, LLTF_LIMIT, now_utc)
print(f"lltf_df last bar: {lltf_df.index[-1]}")

print("Loading LTF data (1h)...")
ltf_df = load_or_fetch(SYMBOL, LTF_INTERVAL, LTF_LIMIT, now_utc)

print("Loading HTF data (4h)...")
htf_df = load_or_fetch(SYMBOL, HTF_INTERVAL, HTF_LIMIT, now_utc)

# Drop current incomplete 1H bar
current_1h_boundary = now_utc.floor("h")
ltf_df = ltf_df[ltf_df.index < current_1h_boundary].copy()

current_4h_open = now_utc.floor("4h")
htf_df = htf_df[htf_df.index < current_4h_open].copy()
print(f"[DEBUG] now_utc={now_utc.strftime('%Y-%m-%d %H:%M UTC')} | current_4h_open={current_4h_open} | last_closed_4h={htf_df.index[-1]} len={len(htf_df)}")

# lltf_df.index = pd.to_datetime(lltf_df.index, utc=True)
ltf_df.index = pd.to_datetime(ltf_df.index, utc=True)
htf_df.index = pd.to_datetime(htf_df.index, utc=True)

# ==========================================================
# SIGNAL GENERATION
# ==========================================================

# Inject a placeholder row at current_1h_boundary so align_htf_scores
# has a landing spot for any 4H score that closed at or before this
# timestamp. The placeholder copies OHLC from the last real bar so
# indicator math doesn't crash. After generate_signal runs, the HTF
# columns are copied back onto the real last bar and the placeholder
# is discarded — it never reaches the backtest or position manager.
_placeholder = pd.DataFrame(
    [ltf_df.iloc[-1].values],
    columns=ltf_df.columns,
    index=[current_1h_boundary]
)
_ltf_with_placeholder = pd.concat([ltf_df, _placeholder])
_ltf_with_placeholder = generate_signal(_ltf_with_placeholder, htf_df)

# ← move here, and use _ltf_with_placeholder not ltf_df
print(_ltf_with_placeholder[["FLOW_STRENGTH", "COMPRESSION_OK", "EARLY_EXPANSION", "signal"]].tail(50))
print(f"[DEBUG] _ltf_with_placeholder shape: {_ltf_with_placeholder.shape}")
print(f"[DEBUG] ltf_df shape going in: {len(ltf_df)} rows, first={ltf_df.index[0]}, last={ltf_df.index[-1]}")

# Copy HTF columns from placeholder back onto every real bar via bfill.
# The placeholder is the last row, so bfill pulls its values backward
# onto all preceding rows that still carry the stale score.
_htf_cols = ["HTF_DIRECTION", "HTF_QUALITY"]
for _col in _htf_cols:
    if _col in _ltf_with_placeholder.columns:
        # Only backfill if the placeholder actually received a new score
        _placeholder_val = _ltf_with_placeholder.loc[current_1h_boundary, _col]
        _ltf_with_placeholder[_col] = _ltf_with_placeholder[_col].where(
            _ltf_with_placeholder.index != current_1h_boundary,
            _placeholder_val
        )
        # Propagate the new score backward to all bars in the same 4H period
        _ltf_with_placeholder[_col] = _ltf_with_placeholder[_col].bfill()

# Discard the placeholder
ltf_df = _ltf_with_placeholder[_ltf_with_placeholder.index < current_1h_boundary].copy()

# ==========================================================
# BACKTEST
# ==========================================================

backtester = SignalBacktester(ltf_df, htf_df=htf_df, lltf_df=lltf_df, leverage=LEVERAGE)

backtest_output = backtester.run()

trade_log = backtest_output["trades"]
equity_curve = backtest_output["equity_curve"]
results = backtest_output["summary"]

print(results)

print("=== TRADE LOG ===")
print(trade_log.head(10))

print("\nColumns:", trade_log.columns)
print("\nNumber of trades:", len(trade_log))
print("LTF candles (1h):", len(ltf_df))
print("HTF candles (4h):", len(htf_df))

# ==========================================================
# DIAGNOSTICS
# ==========================================================

diagnostics_df = diagnose_trades(trade_log)

# ==========================================================
# EDGE DECAY ANALYSIS
# ==========================================================
def edge_decay_analysis(trades_df, lltf_df):
    checkpoints = [3, 6, 12, 18, 24, 36, 48]
    results = []

    for _, trade in trades_df.iterrows():
        entry_idx   = int(trade['entry_idx'])
        entry_price = trade['entry_price']
        side        = trade['side']
        atr         = trade['ATR']
        final_mfe   = trade['MFE']
        R           = abs(entry_price - trade['initial_stop'])
        if R <= 0:
            R = atr * 1.5

        row = {
            'side': side,
            'final_mfe_r': trade['mfe_r'],
            'final_pnl_r': trade['pnl_r'],
            'exit_reason': trade.get('exit_reason', ''),
        }

        for bars in checkpoints:
            end_idx = min(entry_idx + bars, len(lltf_df) - 1)
            window  = lltf_df.iloc[entry_idx:end_idx + 1]

            if window.empty:
                row[f'mfe_r_{bars}b'] = np.nan
                row[f'pnl_r_{bars}b'] = np.nan
                continue

            if side == 1:
                mfe_price = window['high'].max()
                pnl_price = window['close'].iloc[-1]
                mfe_r = (mfe_price - entry_price) / R
                pnl_r = (pnl_price - entry_price) / R
            else:
                mfe_price = window['low'].min()
                pnl_price = window['close'].iloc[-1]
                mfe_r = (entry_price - mfe_price) / R
                pnl_r = (entry_price - pnl_price) / R

            row[f'mfe_r_{bars}b'] = max(mfe_r, 0.0)
            row[f'pnl_r_{bars}b'] = pnl_r

        # what fraction of final MFE was present at each checkpoint?
        for bars in checkpoints:
            if final_mfe > 0 and not np.isnan(row.get(f'mfe_r_{bars}b', np.nan)):
                row[f'mfe_pct_{bars}b'] = row[f'mfe_r_{bars}b'] / trade['mfe_r'] if trade['mfe_r'] > 0 else np.nan
            else:
                row[f'mfe_pct_{bars}b'] = np.nan

        results.append(row)

    result_df = pd.DataFrame(results)

    print("\n=== EDGE DECAY PROFILE ===")
    print(f"{'Bars':>5} {'Time':>6} {'MFE%':>8} {'AvgPnL R':>10} {'WinMFE%':>10} {'LosMFE%':>10} {'AvgMFE R':>10}")
    print("-" * 65)

    winners = result_df[result_df['final_mfe_r'] >= 0.5]
    losers  = result_df[result_df['final_mfe_r'] <  0.5]

    for bars in checkpoints:
        mfe_pct_col = f'mfe_pct_{bars}b'
        pnl_col     = f'pnl_r_{bars}b'
        mfe_r_col   = f'mfe_r_{bars}b'

        avg_mfe_pct = result_df[mfe_pct_col].mean()
        avg_pnl     = result_df[pnl_col].mean()
        win_mfe_pct = winners[mfe_pct_col].mean() if not winners.empty else np.nan
        los_mfe_pct = losers[mfe_pct_col].mean()  if not losers.empty  else np.nan
        avg_mfe_r   = result_df[mfe_r_col].mean()

        print(
            f"{bars:>5} "
            f"{bars*5:>5}m "
            f"{avg_mfe_pct:>8.1%} "
            f"{avg_pnl:>10.3f} "
            f"{win_mfe_pct:>10.1%} "
            f"{los_mfe_pct:>10.1%} "
            f"{avg_mfe_r:>10.3f}"
        )

    # when does average PnL peak?
    pnl_by_time = [result_df[f'pnl_r_{b}b'].mean() for b in checkpoints]
    peak_idx    = int(np.nanargmax(pnl_by_time))
    peak_bar    = checkpoints[peak_idx]
    print(f"\nAvg PnL peaks at bar {peak_bar} ({peak_bar * 5} min) — holding longer costs edge on average")

    # exit reason breakdown
    print("\n--- Exit reason vs avg R ---")
    for reason, grp in result_df.groupby('exit_reason'):
        print(f"  {reason:<25} n={len(grp):>3}  avg_final_pnl_r={grp['final_pnl_r'].mean():>6.3f}")

    # front-load score: what % of final MFE is captured in first 30 min?
    fl = result_df['mfe_pct_6b'].mean()
    print(f"\nFront-load score (MFE% at 30 min): {fl:.1%}")
    if fl >= 0.70:
        print("  → FRONT-LOADED. Most edge is gone by bar 6. Prioritize fast profit lock.")
    elif fl >= 0.45:
        print("  → GRADUAL BUILD. Edge persists past 30 min. Tighter trail is the fix.")
    else:
        print("  → SLOW STARTER. Edge develops late. Check if entries are early enough.")

    return result_df

decay_df = edge_decay_analysis(trade_log, backtester.lltf_df)

# # ==========================================================
# # EXIT COUNTERFACTUAL
# # ==========================================================
# def exit_counterfactual(trades_df, lltf_df):
#     """
#     For each soft exit (OIE or stall), shows what would have happened
#     if the trade held to the hard stop instead.
#     Key question: are these exits saving losses or cutting winners?
#     """
#     soft_exits = trades_df[
#         trades_df['exit_reason'].isin(['opposite_impulse', 'stall_exit'])
#     ].copy()

#     if soft_exits.empty:
#         print("\n=== EXIT COUNTERFACTUAL ===")
#         print("No OIE or stall exits to analyse.")
#         return pd.DataFrame()

#     results = []
#     for _, trade in soft_exits.iterrows():
#         entry       = trade['entry_price']
#         stop        = trade['initial_stop']
#         side        = trade['side']
#         exit_idx    = int(trade['exit_idx'])
#         R           = abs(entry - stop)
#         if R <= 0:
#             continue

#         # bars AFTER the soft exit
#         future = lltf_df.iloc[exit_idx + 1 : exit_idx + 49]
#         if future.empty:
#             continue

#         if side == 1:
#             stop_hit_after  = (future['low'] <= stop).any()
#             max_future_r    = (future['high'].max() - entry) / R
#         else:
#             stop_hit_after  = (future['high'] >= stop).any()
#             max_future_r    = (entry - future['low'].min()) / R

#         max_future_r_clipped = max(max_future_r, 0.0)

#         results.append({
#             'exit_reason':    trade['exit_reason'],
#             'actual_pnl_r':   trade['pnl_r'],
#             'mfe_r':          trade['mfe_r'],
#             'bars_held':      trade.get('bars_held', 0),
#             'stop_hit_after': stop_hit_after,
#             'max_future_r':   max_future_r_clipped,
#             'cost_of_exit':   max_future_r_clipped - trade['pnl_r'],
#         })

#     if not results:
#         print("\n=== EXIT COUNTERFACTUAL ===")
#         print("No valid counterfactuals computable.")
#         return pd.DataFrame()

#     df = pd.DataFrame(results)

#     print("\n=== EXIT COUNTERFACTUAL ===")
#     print("stop_hit_after = % of exits where stop would have been hit anyway")
#     print("max_future_r   = avg best-case R if trade had been held")
#     print("cost_of_exit   = avg R left on table by exiting early")
#     print()
#     summary = df.groupby('exit_reason').agg(
#         n               = ('actual_pnl_r', 'count'),
#         actual_pnl_r    = ('actual_pnl_r', 'mean'),
#         stop_hit_after  = ('stop_hit_after', 'mean'),
#         max_future_r    = ('max_future_r', 'mean'),
#         cost_of_exit    = ('cost_of_exit', 'mean'),
#     ).round(3)
#     print(summary.to_string())

#     print("\n--- Interpretation ---")
#     for reason, row in summary.iterrows():
#         pct_would_stop = row['stop_hit_after'] * 100
#         cost           = row['cost_of_exit']
#         print(f"\n  {reason}:")
#         if row['stop_hit_after'] > 0.6:
#             print(f"    {pct_would_stop:.0f}% of these exits: stop would have been hit anyway → exit is CORRECT")
#         else:
#             print(f"    Only {pct_would_stop:.0f}% would have stopped out → exit is cutting winners early")
#         if cost > 0.1:
#             print(f"    Avg {cost:.3f}R left on table per exit — consider loosening this exit")
#         else:
#             print(f"    Minimal R left on table — exit timing is reasonable")

#     return df

# counterfactual_df = exit_counterfactual(trade_log, backtester.lltf_df)

# # ==========================================================
# # OIE FIRING ANALYSIS
# # ==========================================================
# def oie_firing_analysis(trades_df):
#     """
#     Shows exactly when and in what state OIE fired.
#     Identifies whether it fires too early (low mfe_r) or appropriately.
#     """
#     oie = trades_df[trades_df['exit_reason'] == 'opposite_impulse'].copy()

#     print("\n=== OIE FIRING ANALYSIS ===")

#     if oie.empty:
#         print("No OIE exits found.")
#         return

#     print(f"Total OIE exits : {len(oie)}")
#     print(f"Avg bars held   : {oie['bars_held'].mean():.1f}")
#     print(f"Avg mfe_r       : {oie['mfe_r'].mean():.3f}R")
#     print(f"Avg pnl_r       : {oie['pnl_r'].mean():.3f}R")

#     in_profit = oie[oie['pnl_r'] > 0]
#     in_loss   = oie[oie['pnl_r'] <= 0]
#     print(f"\nFired while in profit : {len(in_profit)}  avg R={in_profit['pnl_r'].mean():.3f}" if not in_profit.empty else "\nFired while in profit : 0")
#     print(f"Fired while in loss   : {len(in_loss)}  avg R={in_loss['pnl_r'].mean():.3f}"   if not in_loss.empty   else "Fired while in loss   : 0")

#     early = oie[oie['bars_held'] <= 6]
#     late  = oie[oie['bars_held'] >  6]
#     print(f"\nEarly OIE (≤6 bars)  : {len(early)}  avg R={early['pnl_r'].mean():.3f}" if not early.empty else "\nEarly OIE (≤6 bars)  : 0")
#     print(f"Late  OIE (>6 bars)  : {len(late)}   avg R={late['pnl_r'].mean():.3f}"  if not late.empty  else "Late  OIE (>6 bars)  : 0")

#     # mfe_r buckets at time of exit
#     print("\n--- mfe_r at time of OIE exit ---")
#     buckets = [(0.0, 0.3, "0–0.3R (no progress)"),
#                (0.3, 0.7, "0.3–0.7R (small gain)"),
#                (0.7, 1.5, "0.7–1.5R (solid gain)"),
#                (1.5, 99,  ">1.5R (large winner)")]
#     for lo, hi, label in buckets:
#         subset = oie[(oie['mfe_r'] >= lo) & (oie['mfe_r'] < hi)]
#         if not subset.empty:
#             print(f"  {label:30s} : n={len(subset):>3}  avg_pnl_r={subset['pnl_r'].mean():>6.3f}")

#     print("\n--- Verdict ---")
#     early_loss_rate = (early['pnl_r'] < 0).mean() if not early.empty else 0
#     if early_loss_rate > 0.5:
#         print("  OIE is firing early AND mostly on losers — it may be reacting to noise.")
#         print("  Consider raising the body size threshold (atr * 1.2 → atr * 1.5).")
#     elif oie['mfe_r'].mean() > 0.5:
#         print("  OIE is firing on trades that had real MFE — it's cutting winners.")
#         print("  Consider adding a minimum mfe_r guard (e.g. only fire OIE if mfe_r < 0.5).")
#     else:
#         print("  OIE appears to be firing appropriately — exits are on low-MFE trades.")

# oie_firing_analysis(trade_log)

# # ==========================================================
# # HTF QUALITY DIAGNOSTIC — last 30 hours
# # ==========================================================
print("\n=== HTF QUALITY (last 30 bars) ===")
print(f"{'timestamp':>25} {'HTF_DIR':>8} {'HTF_QUAL':>10} {'signal':>8} {'final_sig':>10}")
print("-" * 65)

diag_cols = ["HTF_DIRECTION", "HTF_QUALITY", "signal", "final_signal"]
available = [c for c in diag_cols if c in ltf_df.columns]

now_utc = pd.Timestamp.now(tz="UTC")
last_closed_1h = now_utc.floor("h") - pd.Timedelta(hours=1)
hours_into_4h = last_closed_1h.hour % 4
last_closed_4h_boundary = last_closed_1h - pd.Timedelta(hours=hours_into_4h)
diag = ltf_df[available][ltf_df.index <= last_closed_1h].tail(30)

for ts, row in diag.iterrows():
    htf_dir  = int(row["HTF_DIRECTION"])  if "HTF_DIRECTION"  in row.index else "N/A"
    htf_qual = f"{row['HTF_QUALITY']:.4f}" if "HTF_QUALITY"    in row.index else "N/A"
    sig      = int(row["signal"])          if "signal"          in row.index else "N/A"
    fsig     = int(row["final_signal"])    if "final_signal"    in row.index else "N/A"

    import pytz
    WAT = pytz.timezone("Africa/Lagos")
    ts_wat = ts.tz_convert(WAT).strftime("%Y-%m-%d %H:%M WAT")

    blocked = " ← NO HTF DATA" if (htf_qual == "nan" or htf_qual == "N/A") else (" ← BLOCKED" if float(htf_qual) <= 0.45 else "")
    print(f"{ts_wat:>25} {str(htf_dir):>8} {htf_qual:>10} {str(sig):>8} {str(fsig):>10}{blocked}")

print(f"\nHTF threshold: 0.45")
print(f"Last HTF_DIRECTION : {int(ltf_df['HTF_DIRECTION'].iloc[-1])}")
print(f"Last HTF_QUALITY   : {ltf_df['HTF_QUALITY'].iloc[-1]:.4f}")
print(f"Last final_signal  : {int(ltf_df['final_signal'].iloc[-1])}")

# Add this to your backtest script after computing scores
from indicators.indicators import compute_htf_scores
scores = compute_htf_scores(htf_df)
print("Backtest HTF last 5 bars:")
print(scores.tail(5))
print("HTF_QUALITY last value:", scores['HTF_QUALITY'].iloc[-1])

print(f"[DEBUG] backtest htf_df last={htf_df.index[-1]} len={len(htf_df)}")
for _ts, _row in htf_df.tail(3).iterrows():
    print(f"[DEBUG HTF BAR] {_ts} | open={_row['open']:.4f} close={_row['close']:.4f} volume={_row['volume']:.2f}")

# # # ── 5m candle dump per trade ────────────────────────────────
# # print("\n=== 5M CANDLES PER TRADE ===")
# # for _, t in trade_log.iterrows():
# #     entry_time = backtester.lltf_df.index[int(t["entry_idx"])]
# #     exit_time  = backtester.lltf_df.index[int(t["exit_idx"])]
# #     window     = backtester.lltf_df.loc[entry_time:exit_time].copy()
# #     R          = abs(t["entry_price"] - t["stop_loss"])
# #     print(f"\n{'='*60}")
# #     print(f"{t['direction']} | entry={t['entry_price']:.4f} stop={t['stop_loss']:.4f} R={R:.4f}")
# #     print(f"{'time':>8} {'open':>8} {'high':>8} {'low':>8} {'close':>8} {'vol':>12} {'body/atr':>9} {'stop_r':>7} {'pnl_r':>7}")
# #     for ts, row in window.iterrows():
# #         body    = abs(row["close"] - row["open"])
# #         atr_5m  = row.get("ATR_5M", float("nan"))
# #         if pd.isna(atr_5m) or atr_5m <= 0:
# #             atr_5m = row.get("ATR", float("nan")) * 0.20
# #         body_atr = body / atr_5m if atr_5m and not pd.isna(atr_5m) and atr_5m > 0 else float("nan")
# #         if t["side"] == 1:
# #             stop_r = (row["close"] - t["stop_loss"]) / R if R > 0 else float("nan")
# #             pnl_r  = (row["close"] - t["entry_price"]) / R if R > 0 else float("nan")
# #         else:
# #             stop_r = (t["stop_loss"] - row["close"]) / R if R > 0 else float("nan")
# #             pnl_r  = (t["entry_price"] - row["close"]) / R if R > 0 else float("nan")
# #         wat_ts = (ts + pd.Timedelta(hours=1)).strftime("%H:%M")
# #         print(f"{wat_ts:>8} {row['open']:>8.4f} {row['high']:>8.4f} {row['low']:>8.4f} {row['close']:>8.4f} {row['volume']:>12.2f} {body_atr:>9.2f} {stop_r:>7.2f} {pnl_r:>7.2f}")