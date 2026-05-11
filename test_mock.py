import pandas as pd
import os
import sys
import inspect

# ==========================================================
# PRE-FLIGHT CHECK 6 — VOL_SCORE fix actually in file
# ==========================================================
import indicators.indicators as ind

src = inspect.getsource(ind.htf_structural_stack)

print("=" * 60)
print("CHECK 6 — htf_vol_lookback fix in htf_structural_stack")
print("=" * 60)
if "htf_vol_lookback" in src:
    print("PASS — htf_vol_lookback found in htf_structural_stack")
else:
    print("FAIL — htf_vol_lookback NOT found. VOL_SCORE cliff still present.")
    print("       Add this fix before deploying:")
    print("       htf_vol_lookback = min(vol_lookback, max(20, len(htf) // 3))")
    print("       htf['HTF_VOL_PCTL'] = htf['HTF_ATR'].rolling(htf_vol_lookback).rank(pct=True)")

if "min_periods=min_periods" in src or "min_periods" in inspect.getsource(ind.hybrid_zscore):
    print("PASS — hybrid_zscore min_periods clamp present")
else:
    print("FAIL — hybrid_zscore min_periods clamp missing")

print()

# ==========================================================
# LOAD CACHED DATA (mirrors exactly what Render has)
# ==========================================================
print("=" * 60)
print("LOADING RENDER-EQUIVALENT DATA (1000 LTF, 250 HTF bars)")
print("=" * 60)

df_1h  = pd.read_parquet("data/cache/ETCUSDT_1h.parquet")
df_4h  = pd.read_parquet("data/cache/ETCUSDT_4h.parquet")
df_5m  = pd.read_parquet("data/cache/ETCUSDT_5m.parquet")

for d in (df_1h, df_4h, df_5m):
    d.index = pd.to_datetime(d.index, utc=True)

print(f"LTF bars:  {len(df_1h)} | first={df_1h.index[0]} | last={df_1h.index[-1]}")
print(f"HTF bars:  {len(df_4h)} | first={df_4h.index[0]} | last={df_4h.index[-1]}")
print(f"LLTF bars: {len(df_5m)} | first={df_5m.index[0]} | last={df_5m.index[-1]}")
print()

# ==========================================================
# CHECK 3 — signal stable at exactly 1000 LTF / 250 HTF bars
# ==========================================================
print("=" * 60)
print("CHECK 3 — signal at Render's exact bar counts (1000/250)")
print("=" * 60)

# Clip to the signal bar exactly as Render would
signal_bar_ts  = pd.Timestamp("2026-05-10 16:00:00", tz="UTC")
df_clipped     = df_1h[df_1h.index <= signal_bar_ts].copy()
htf_clipped    = df_4h[df_4h.index <= signal_bar_ts].copy()

print(f"Clipped LTF bars: {len(df_clipped)}")
print(f"Clipped HTF bars: {len(htf_clipped)}")

sig = ind.generate_signal(df_clipped.copy(), htf_clipped.copy(), live=True)
row = sig.loc[signal_bar_ts]

print(f"signal         = {int(row['signal'])}")
print(f"final_signal   = {int(row['final_signal'])}")
print(f"HTF_QUALITY    = {float(row['HTF_QUALITY']):.4f}  (need > 0.45)")
print(f"HTF_DIRECTION  = {int(row['HTF_DIRECTION'])}")
print(f"VALID_BREAK_LONG = {bool(row['VALID_BREAK_LONG'])}")
print(f"ENTRY_LONG     = {bool(row['ENTRY_LONG'])}")
print(f"signals_total  = {(sig['final_signal'] != 0).sum()}")

if int(row['signal']) == 1:
    print("PASS — signal=1 at Render bar counts")
else:
    print("FAIL — signal=0 at Render bar counts, fix not effective for this slice")
print()

# ==========================================================
# SIMULATE RUN 1 — 17:00:59 UTC (cursor was 15:55 UTC)
# This is the run where the candle guard excluded the signal bar
# ==========================================================
print("=" * 60)
print("SIMULATING RUN 1 — now=17:00:59 UTC | cursor=15:55 UTC")
print("=" * 60)

from execution.hourly_runner import map_ltf_to_htf
from indicators.indicators import generate_signal, atr_ema
from strategy.lifecycle import PositionManager

def simulate_run(label, now_utc, cursor, df_1h, df_4h, df_5m, pm):
    print(f"\n--- {label} ---")

    now_hour = now_utc.replace(minute=0, second=0, microsecond=0)

    # Mirror run_hourly_for_symbol clipping
    df_sim  = df_1h[df_1h.index <= now_hour - pd.Timedelta(hours=1)].copy()
    htf_sim = df_4h[df_4h.index <= now_hour - pd.Timedelta(hours=1)].copy()

    # Candle guard
    minutes_floored     = (now_utc.minute // 5) * 5
    current_5m_boundary = now_utc.replace(minute=minutes_floored, second=0, microsecond=0)
    lltf_sim = df_5m[df_5m.index < current_5m_boundary].copy()

    print(f"df_sim last:   {df_sim.index[-1]}")
    print(f"lltf last:     {lltf_sim.index[-1]}")
    print(f"5m boundary:   {current_5m_boundary}")
    print(f"LTF bars:      {len(df_sim)}")
    print(f"HTF bars:      {len(htf_sim)}")

    df_sig = generate_signal(df_sim.copy(), htf_sim.copy(), live=True)

    print(f"HTF_QUALITY:   {df_sig['HTF_QUALITY'].iloc[-1]:.4f}")
    print(f"signal[-1]:    {int(df_sig['signal'].iloc[-1])}")
    print(f"signals_total: {(df_sig['final_signal'] != 0).sum()}")

    lltf_sim = lltf_sim[lltf_sim.index >= df_sig.index[0]].copy()
    lltf_sim = map_ltf_to_htf(lltf_sim, df_sig)
    lltf_sim["final_signal"] = df_sig["final_signal"].reindex(lltf_sim.index, method="ffill")

    # Zero out signal bar window
    signal_bar_ts  = df_sig.index[-1]
    signal_bar_end = signal_bar_ts + pd.Timedelta(hours=1)
    within = (lltf_sim.index >= signal_bar_ts) & (lltf_sim.index < signal_bar_end)
    lltf_sim.loc[within, "final_signal"] = 0

    lltf_sim["ATR"]    = df_sig["ATR"].reindex(lltf_sim.index, method="ffill")
    lltf_sim["ATR_5M"] = atr_ema(lltf_sim, period=14)

    lltf_frozen = lltf_sim.dropna(subset=["ltf_index"]).copy()
    lltf_frozen["ltf_index"] = lltf_frozen["ltf_index"].astype(int)

    # new_bars using cursor
    new_bars = lltf_frozen if cursor is None else lltf_frozen[lltf_frozen.index > cursor]

    non_zero = (new_bars["final_signal"] != 0).sum() if not new_bars.empty else 0
    print(f"cursor:        {cursor}")
    print(f"new_bars:      {len(new_bars)}")
    print(f"non_zero:      {non_zero}")

    # Show the critical window around signal bar
    window_start = pd.Timestamp("2026-05-10 16:50:00", tz="UTC")
    window_end   = pd.Timestamp("2026-05-10 17:15:00", tz="UTC")
    window = lltf_frozen[
        (lltf_frozen.index >= window_start) &
        (lltf_frozen.index <= window_end)
    ]
    if not window.empty:
        print(f"\n5m bars 16:50–17:15 UTC:")
        print(window[["final_signal", "ltf_index"]].to_string())

    # ==========================================================
    # CHECK 1 — signal expiry for each non-zero bar
    # ==========================================================
    if non_zero > 0:
        print(f"\nCHECK 1 — signal expiry for non-zero bars:")
        SIGNAL_EXPIRY_BARS_LIVE = 12
        for ts, row_5m in new_bars[new_bars["final_signal"] != 0].iterrows():
            bar_signal     = int(row_5m["final_signal"])
            ltf_row        = df_sig.iloc[int(row_5m["ltf_index"])]
            signal_bar_end_expiry = ltf_row.name + pd.Timedelta(hours=1)
            signal_age_bars = len(
                lltf_frozen[
                    (lltf_frozen.index >= signal_bar_end_expiry) &
                    (lltf_frozen.index <= ts)
                ]
            )
            expired = signal_age_bars > SIGNAL_EXPIRY_BARS_LIVE
            print(
                f"  ts={ts} | signal={bar_signal} | "
                f"signal_bar_end={signal_bar_end_expiry} | "
                f"age_bars={signal_age_bars} | "
                f"expiry_limit={SIGNAL_EXPIRY_BARS_LIVE} | "
                f"expired={'*** YES — BLOCKED ***' if expired else 'no — OK'}"
            )

    # ==========================================================
    # CHECK 2 — actually call pm.update for non-zero bars
    # ==========================================================
    if non_zero > 0:
        print(f"\nCHECK 2 — pm.update results for non-zero bars:")
        for ts, row_5m in new_bars[new_bars["final_signal"] != 0].iterrows():
            bar_signal = int(row_5m["final_signal"])
            ltf_row    = df_sig.iloc[int(row_5m["ltf_index"])]
            result = pm.update(
                df=df_sig,
                symbol="ETCUSDT",
                lltf_df=lltf_frozen,
                external_signal=bar_signal,
                external_row=ltf_row,
                current_5m_row=row_5m
            )
            state = result.get("state") if isinstance(result, dict) else result
            print(
                f"  ts={ts} | signal={bar_signal} | "
                f"reentry_lock={pm._reentry_lock.get('ETCUSDT')} | "
                f"has_position={'ETCUSDT' in pm.positions} | "
                f"state={state}"
            )
            if state == "OPEN":
                print(f"  *** ENTRY FIRED — position opened ***")

    # Advance cursor
    new_cursor = new_bars.index[-1] if not new_bars.empty else cursor
    print(f"\ncursor after this run: {new_cursor}")
    return new_cursor


pm = PositionManager(persist=False, notify=False)

cursor_run1 = pd.Timestamp("2026-05-10 15:55:00", tz="UTC")

cursor_run2 = simulate_run(
    label   = "RUN 1 — 17:00:59 UTC",
    now_utc = pd.Timestamp("2026-05-10 17:00:59", tz="UTC"),
    cursor  = cursor_run1,
    df_1h=df_1h, df_4h=df_4h, df_5m=df_5m,
    pm=pm
)

cursor_run3 = simulate_run(
    label   = "RUN 2 — 17:06:17 UTC",
    now_utc = pd.Timestamp("2026-05-10 17:06:17", tz="UTC"),
    cursor  = cursor_run2,
    df_1h=df_1h, df_4h=df_4h, df_5m=df_5m,
    pm=pm
)

print()
print("=" * 60)
print("FINAL VERDICT")
print("=" * 60)
if "ETCUSDT" in pm.positions:
    print("PASS — position is open after both runs")
elif pm._reentry_lock.get("ETCUSDT"):
    print("FAIL — reentry lock blocked entry")
else:
    print("FAIL — no position opened and no lock. Signal expired or new_bars was empty.")