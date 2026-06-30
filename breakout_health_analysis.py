"""
breakout_health_analysis.py

Standalone, additive analysis — does NOT touch live signal generation
or the backtester's exit logic. Run this AFTER your normal backtest
script (run_backtest.py or whatever you call it) has produced
`ltf_df_bt` (the 1H df with all indicator columns, post generate_signal)
and `lltf_df_bt` (5m df) — or just re-import and recompute if you'd
rather run this as a fully separate script.

PURPOSE
-------
Your current entry rule inside a 6-bar VALID_BREAK window is:
    "first bar where BULL_CONT / BEAR_CONT fires"
i.e. first bullish continuation candle.

This script tests a different philosophy:
    "first bar where the breakout is PROVING ITSELF (getting stronger)"

using four components you already compute in indicators.py:
    1. FLOW_STRENGTH increasing  (participation improving)
    2. VER increasing            (volatility expansion improving)
    3. DISPLACEMENT_SCORE stable-or-improving  (energy not fading)
    4. close still accepting above/below the breakout level

For every VALID_BREAK_LONG / VALID_BREAK_SHORT event, it looks at the
6-bar window that follows (matching BREAKOUT_WINDOW_LONG/SHORT) and
scores each bar. It then compares 4 candidate entry-bar selection
rules by forward MFE/MAE over the next N bars (pure price, no
trail/exit logic — that's a separate, slower test you can run later
if a rule wins here).

USAGE
-----
    from breakout_health_analysis import run_breakout_health_analysis
    run_breakout_health_analysis(ltf_df_bt, forward_bars=12)

`ltf_df_bt` must be the 1H dataframe AFTER generate_signal() has run
(so VALID_BREAK_LONG, VALID_BREAK_SHORT, BREAKOUT_WINDOW_LONG/SHORT,
FLOW_STRENGTH, VER, DISPLACEMENT_SCORE, BULL_CONT, BEAR_CONT, RESISTANCE,
SUPPORT all exist).

This operates on the 1H timeframe (same timeframe VALID_BREAK_LONG /
BULL_CONT already live on) — not the 5m execution timeframe. If you
want this re-run on 5m granularity later, the same functions work,
just pass the 5m df with equivalent columns aligned onto it first.
"""

import pandas as pd
import numpy as np


# ==========================================================
# STEP 1 — find breakout events and their 6-bar windows
# ==========================================================
def _find_breakout_events(df, side):
    """
    side: 'long' or 'short'
    Returns list of dicts: {event_idx, window_start_idx, window_end_idx}
    window = the 6 bars strictly AFTER the VALID_BREAK bar, matching
    post_breakout_event_window(window=6) semantics in indicators.py.
    """
    col = "VALID_BREAK_LONG" if side == "long" else "VALID_BREAK_SHORT"
    if col not in df.columns:
        raise KeyError(f"{col} not found — did you pass df after generate_signal()?")

    events = []
    flags = df[col].values
    n = len(df)
    for i in range(n):
        if flags[i]:
            w_start = i + 1
            w_end = min(i + 6, n - 1)  # inclusive, 6-bar window after event
            if w_start > w_end:
                continue
            events.append({
                "event_idx": i,
                "event_time": df.index[i],
                "window_start_idx": w_start,
                "window_end_idx": w_end,
            })
    return events


# ==========================================================
# STEP 2 — score each bar in the window on the 4 components
# ==========================================================
def _score_window(df, ev, side):
    """
    Returns a list of per-bar dicts with raw deltas and a 0-4 score,
    for every bar in the breakout window.
    """
    rows = []
    breakout_level = (
        df["RESISTANCE"].iloc[ev["event_idx"]] if side == "long"
        else df["SUPPORT"].iloc[ev["event_idx"]]
    )

    prev_flow = df["FLOW_STRENGTH"].iloc[ev["event_idx"]]
    prev_ver = df["VER"].iloc[ev["event_idx"]]
    prev_disp = df["DISPLACEMENT_SCORE"].iloc[ev["event_idx"]]

    for idx in range(ev["window_start_idx"], ev["window_end_idx"] + 1):
        flow = df["FLOW_STRENGTH"].iloc[idx]
        ver = df["VER"].iloc[idx]
        disp = df["DISPLACEMENT_SCORE"].iloc[idx]
        close = df["close"].iloc[idx]

        flow_improving = flow > prev_flow
        ver_improving = ver > prev_ver
        disp_ok = disp >= prev_disp  # stable or improving

        if side == "long":
            accepting = close > breakout_level
            cont_fired = bool(df["BULL_CONT"].iloc[idx]) if "BULL_CONT" in df.columns else False
        else:
            accepting = close < breakout_level
            cont_fired = bool(df["BEAR_CONT"].iloc[idx]) if "BEAR_CONT" in df.columns else False

        score = int(flow_improving) + int(ver_improving) + int(disp_ok) + int(accepting)

        rows.append({
            "idx": idx,
            "time": df.index[idx],
            "bars_into_window": idx - ev["window_start_idx"] + 1,
            "flow_improving": flow_improving,
            "ver_improving": ver_improving,
            "disp_ok": disp_ok,
            "accepting": accepting,
            "score": score,
            "cont_fired": cont_fired,
            "close": close,
        })

        prev_flow, prev_ver, prev_disp = flow, ver, disp

    return rows


# ==========================================================
# STEP 3 — forward MFE/MAE from a candidate entry bar
# ==========================================================
def _forward_mfe_mae(df, entry_idx, side, forward_bars):
    entry_price = df["close"].iloc[entry_idx]
    end_idx = min(entry_idx + forward_bars, len(df) - 1)
    if end_idx <= entry_idx:
        return None

    future = df.iloc[entry_idx + 1: end_idx + 1]
    if future.empty:
        return None

    if side == "long":
        mfe = future["high"].max() - entry_price
        mae = entry_price - future["low"].min()
    else:
        mfe = entry_price - future["low"].min()
        mae = future["high"].max() - entry_price

    return {
        "entry_idx": entry_idx,
        "entry_price": entry_price,
        "mfe": max(mfe, 0.0),
        "mae": max(mae, 0.0),
        "bars_forward": end_idx - entry_idx,
    }


# ==========================================================
# STEP 4 — apply the 4 candidate selection rules to one event
# ==========================================================
def _select_entries_for_event(scored_rows, score_threshold=3):
    """
    Returns dict of rule_name -> chosen row (or None if no bar qualifies)
    """
    chosen = {"A_first_cont": None, "B_health_score": None,
              "C_strict_all4": None, "D_oracle": None}

    # A — current rule: first BULL_CONT/BEAR_CONT bar
    for r in scored_rows:
        if r["cont_fired"]:
            chosen["A_first_cont"] = r
            break

    # B — first bar where score >= threshold
    for r in scored_rows:
        if r["score"] >= score_threshold:
            chosen["B_health_score"] = r
            break

    # C — first bar where ALL 4 components are true simultaneously
    for r in scored_rows:
        if r["score"] == 4:
            chosen["C_strict_all4"] = r
            break

    # D — oracle is resolved later (needs forward MFE/MAE for every bar)
    # placeholder, filled in by caller
    return chosen


# ==========================================================
# MAIN ENTRY POINT
# ==========================================================
def run_breakout_health_analysis(df, forward_bars=12, score_threshold=3, sides=("long", "short"),
                                  symbol="?", verbose=True):
    """
    df: 1H dataframe after generate_signal() has run.
    forward_bars: how many bars forward to measure MFE/MAE from the
                  chosen entry bar (12 bars = 12 hours on 1H).
    score_threshold: cutoff for rule B (health score >= this fires).
    symbol: label only, stamped onto every result row so multi-symbol
            runs (see run_multi_symbol_analysis) can be grouped/compared.
    verbose: if False, suppresses the per-call summary print — used by
             the multi-symbol runner, which prints its own aggregate
             summary instead of one block per symbol.

    Prints a comparison table (unless verbose=False) and returns a
    DataFrame of per-event results for further slicing if you want it.
    """
    required_cols = [
        "VALID_BREAK_LONG", "VALID_BREAK_SHORT", "FLOW_STRENGTH", "VER",
        "DISPLACEMENT_SCORE", "RESISTANCE", "SUPPORT", "close", "high", "low",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"df is missing required columns: {missing}. "
                        f"Pass the df AFTER generate_signal() has run.")

    all_results = []

    for side in sides:
        events = _find_breakout_events(df, side)
        if verbose:
            print(f"\n[BREAKOUT HEALTH] {symbol} {side.upper()} — {len(events)} VALID_BREAK events found")

        for ev in events:
            scored_rows = _score_window(df, ev, side)
            if not scored_rows:
                continue

            chosen = _select_entries_for_event(scored_rows, score_threshold)

            # D — oracle: evaluate forward MFE for every bar in window,
            # pick the bar with the best forward MFE - MAE (net favorable).
            best_net = -np.inf
            best_row = None
            for r in scored_rows:
                fm = _forward_mfe_mae(df, r["idx"], side, forward_bars)
                if fm is None:
                    continue
                net = fm["mfe"] - fm["mae"]
                if net > best_net:
                    best_net = net
                    best_row = r
            chosen["D_oracle"] = best_row

            event_result = {
                "symbol": symbol,
                "event_time": ev["event_time"],
                "side": side,
                "window_bars": len(scored_rows),
            }

            for rule_name, row in chosen.items():
                if row is None:
                    event_result[f"{rule_name}_fired"] = False
                    event_result[f"{rule_name}_bars_into_window"] = np.nan
                    event_result[f"{rule_name}_mfe"] = np.nan
                    event_result[f"{rule_name}_mae"] = np.nan
                    event_result[f"{rule_name}_net"] = np.nan
                    continue

                fm = _forward_mfe_mae(df, row["idx"], side, forward_bars)
                event_result[f"{rule_name}_fired"] = True
                event_result[f"{rule_name}_bars_into_window"] = row["bars_into_window"]
                if fm is not None:
                    event_result[f"{rule_name}_mfe"] = fm["mfe"]
                    event_result[f"{rule_name}_mae"] = fm["mae"]
                    event_result[f"{rule_name}_net"] = fm["mfe"] - fm["mae"]
                else:
                    event_result[f"{rule_name}_mfe"] = np.nan
                    event_result[f"{rule_name}_mae"] = np.nan
                    event_result[f"{rule_name}_net"] = np.nan

            all_results.append(event_result)

    results_df = pd.DataFrame(all_results)
    if results_df.empty:
        if verbose:
            print(f"\n[BREAKOUT HEALTH] {symbol} — No events found — nothing to analyze.")
        return results_df

    if verbose:
        _print_summary(results_df, forward_bars, score_threshold)
        _print_symbol_side_table(results_df, score_threshold)
    return results_df


def _print_summary(results_df, forward_bars, score_threshold):
    rules = ["A_first_cont", "B_health_score", "C_strict_all4", "D_oracle"]
    labels = {
        "A_first_cont": "A: first BULL/BEAR_CONT (current)",
        "B_health_score": f"B: health score >= {score_threshold}/4",
        "C_strict_all4": "C: all 4 components simultaneously",
        "D_oracle": "D: best-in-window oracle (upper bound)",
    }

    print(f"\n=== BREAKOUT HEALTH RULE COMPARISON (forward {forward_bars} bars) ===")
    print(f"{'Rule':45} {'fired%':>8} {'avg_mfe':>9} {'avg_mae':>9} {'avg_net':>9} "
          f"{'win_rate(net>0)':>16} {'avg_bars_in':>12}")
    print("-" * 112)

    for rule in rules:
        fired_col = f"{rule}_fired"
        mfe_col = f"{rule}_mfe"
        mae_col = f"{rule}_mae"
        net_col = f"{rule}_net"
        bars_col = f"{rule}_bars_into_window"

        n_total = len(results_df)
        fired_pct = results_df[fired_col].mean() * 100 if n_total else 0.0

        sub = results_df[results_df[fired_col] == True]
        if sub.empty:
            print(f"{labels[rule]:45} {fired_pct:>7.1f}% {'--':>9} {'--':>9} {'--':>9} {'--':>16} {'--':>12}")
            continue

        avg_mfe = sub[mfe_col].mean()
        avg_mae = sub[mae_col].mean()
        avg_net = sub[net_col].mean()
        win_rate = (sub[net_col] > 0).mean() * 100
        avg_bars_in = sub[bars_col].mean()

        print(f"{labels[rule]:45} {fired_pct:>7.1f}% {avg_mfe:>9.4f} {avg_mae:>9.4f} "
              f"{avg_net:>9.4f} {win_rate:>15.1f}% {avg_bars_in:>12.2f}")

    print("\n--- How to read this ---")
    print("avg_net = avg_mfe - avg_mae over the forward window from the chosen entry bar.")
    print("Higher avg_net = better entries on a pure price basis (no exit logic applied).")
    print("D (oracle) is an upper bound — it cheats by knowing the future. The gap between")
    print("your current rule (A) and B/C tells you how much of that ceiling each rule recovers.")
    print("If B or C's avg_net beats A's by a meaningful margin with similar fired%, that's")
    print("a real signal the health-score philosophy is worth wiring into ENTRY_LONG/SHORT.")

    # Breakdown by side
    print("\n--- By side ---")
    for side, grp in results_df.groupby("side"):
        print(f"\n  {side.upper()} ({len(grp)} events)")
        for rule in rules:
            sub = grp[grp[f"{rule}_fired"] == True]
            if sub.empty:
                continue
            print(f"    {labels[rule]:43} n={len(sub):>4}  avg_net={sub[f'{rule}_net'].mean():>8.4f}  "
                  f"win_rate={((sub[f'{rule}_net']>0).mean()*100):>5.1f}%")

    # How often does B/C agree with A (same bar chosen)?
    print("\n--- Agreement with current rule (A) ---")
    both_fired = results_df[(results_df["A_first_cont_fired"]) & (results_df["B_health_score_fired"])]
    if not both_fired.empty:
        same_bar = (both_fired["A_first_cont_bars_into_window"] == both_fired["B_health_score_bars_into_window"]).mean() * 100
        print(f"B picks the same bar as A in {same_bar:.1f}% of cases where both fire.")
    both_fired_c = results_df[(results_df["A_first_cont_fired"]) & (results_df["C_strict_all4_fired"])]
    if not both_fired_c.empty:
        same_bar_c = (both_fired_c["A_first_cont_bars_into_window"] == both_fired_c["C_strict_all4_bars_into_window"]).mean() * 100
        print(f"C picks the same bar as A in {same_bar_c.round(1)}% of cases where both fire.")


# ==========================================================
# PER-SYMBOL-PER-SIDE SUMMARY TABLE
# ==========================================================
def _print_symbol_side_table(results_df, score_threshold):
    """
    Surfaces the exact split that caught the ICX-longs problem:
    is a low avg_net a TIMING issue (B/C beat A, D's ceiling is high)
    or a SIGNAL issue (even D's oracle can't find net-positive edge)?
    """
    if "symbol" not in results_df.columns:
        return

    print("\n--- Per-symbol / per-side diagnosis (timing problem vs signal problem) ---")
    print(f"{'symbol':12} {'side':6} {'n':>4} {'A_net':>9} {'B_net':>9} {'D_net(ceiling)':>15} {'verdict':>28}")
    print("-" * 90)

    for (symbol, side), grp in results_df.groupby(["symbol", "side"]):
        n = len(grp)
        a_sub = grp[grp["A_first_cont_fired"] == True]
        b_sub = grp[grp["B_health_score_fired"] == True]
        d_sub = grp[grp["D_oracle_fired"] == True]

        a_net = a_sub["A_first_cont_net"].mean() if not a_sub.empty else np.nan
        b_net = b_sub["B_health_score_net"].mean() if not b_sub.empty else np.nan
        d_net = d_sub["D_oracle_net"].mean() if not d_sub.empty else np.nan

        # Verdict heuristic:
        #  - if D's ceiling itself is <= 0, no entry-timing rule can fix this —
        #    it's the underlying VALID_BREAK signal that has no edge here.
        #  - if D's ceiling is clearly positive but A is flat/negative,
        #    that's a real timing opportunity — B/C should help.
        #  - otherwise inconclusive / mixed.
        if pd.isna(d_net):
            verdict = "no oracle data"
        elif d_net <= 0:
            verdict = "SIGNAL problem (no edge)"
        elif not pd.isna(a_net) and a_net <= 0 and d_net > 0:
            verdict = "TIMING problem (B/C should help)"
        elif not pd.isna(b_net) and not pd.isna(a_net) and b_net > a_net * 1.2:
            verdict = "TIMING: B improves on A"
        else:
            verdict = "mixed / inconclusive"

        def _fmt(x):
            return f"{x:>9.4f}" if not pd.isna(x) else f"{'--':>9}"

        print(f"{str(symbol):12} {side:6} {n:>4} {_fmt(a_net)} {_fmt(b_net)} {_fmt(d_net):>15} {verdict:>28}")

    print("\nVerdict legend:")
    print("  SIGNAL problem (no edge)        — even the oracle can't find net-positive trades here.")
    print("                                     No entry-timing rule fixes this; VALID_BREAK_LONG/SHORT")
    print("                                     itself needs work for this symbol/side, or skip it.")
    print("  TIMING problem (B/C should help) — current rule (A) is flat/negative but real edge exists")
    print("                                     (D's ceiling is positive). This is what B is meant to fix.")
    print("  TIMING: B improves on A          — B is already capturing more of the available edge than A.")
    print("  mixed / inconclusive             — no rule has cleanly separated itself; treat with caution.")


# ==========================================================
# OUTLIER CLIPPING (prevents one extreme symbol from
# distorting the pooled aggregate — e.g. YFIUSDT's -150/+170
# avg_net swamping every other symbol's 0.001-30 range)
# ==========================================================
def _clip_outlier_nets(results_df, clip_mult):
    """
    Returns a COPY of results_df with each rule's *_net column clipped
    to +/- (clip_mult * median |net| across all fired events of that
    rule, pooled across every symbol). Does not mutate the input.
    """
    df = results_df.copy()
    rules = ["A_first_cont", "B_health_score", "C_strict_all4", "D_oracle"]

    for rule in rules:
        net_col = f"{rule}_net"
        if net_col not in df.columns:
            continue
        fired_vals = df.loc[df[f"{rule}_fired"] == True, net_col].dropna()
        if fired_vals.empty:
            continue
        median_abs = fired_vals.abs().median()
        if median_abs <= 0 or pd.isna(median_abs):
            continue
        bound = clip_mult * median_abs
        df[net_col] = df[net_col].clip(lower=-bound, upper=bound)

    return df


# ==========================================================
# MULTI-SYMBOL RUNNER
# ==========================================================
def run_multi_symbol_analysis(symbol_dfs, forward_bars=12, score_threshold=3, sides=("long", "short"),
                               outlier_clip_mult=None):
    """
    Runs the same analysis across many symbols and aggregates results,
    so a single-symbol fluke (good or bad) doesn't get mistaken for a
    real, generalizable pattern.

    symbol_dfs: dict of {symbol_name: df}, where each df is the 1H
                dataframe AFTER generate_signal() has run for that symbol.

                Build this however fits your existing pipeline, e.g.:

                    symbol_dfs = {}
                    for sym in SYMBOLS:
                        lltf = load_or_fetch(sym, LLTF_INTERVAL, LLTF_LIMIT, now_utc)
                        ltf  = load_or_fetch(sym, LTF_INTERVAL, LTF_LIMIT, now_utc)
                        htf  = load_or_fetch(sym, HTF_INTERVAL, HTF_LIMIT, now_utc)
                        ltf  = ltf[ltf.index < current_1h_boundary].copy()
                        ltf  = generate_signal(ltf.copy(), htf, symbol=sym)
                        symbol_dfs[sym] = ltf

                    from breakout_health_analysis import run_multi_symbol_analysis
                    combined = run_multi_symbol_analysis(symbol_dfs)

    outlier_clip_mult: if set (e.g. 20), any event's *_net value with
                |net| > outlier_clip_mult * median(|net| across ALL fired
                events of that rule, across all symbols) gets clipped to
                that bound before computing averages. This prevents one
                extreme symbol/event (e.g. a flash-spike or thin-liquidity
                gap on a low-cap token) from swamping the pooled avg_net —
                which is exactly what happened with YFIUSDT's -150/+170
                values dwarfing every other symbol's results (all in the
                0.001-30 range). Clipping changes only the SUMMARY
                statistics; the returned per-event DataFrame retains the
                raw (unclipped) values so you can inspect outliers directly.
                None (default) = no clipping, original behaviour.

    Returns the combined per-event results DataFrame across all symbols
    (each row tagged with its symbol, raw/unclipped values), after
    printing an aggregate summary and the per-symbol-per-side diagnosis
    table (computed on clipped values if outlier_clip_mult is set).
    """
    all_dfs = []

    for symbol, df in symbol_dfs.items():
        try:
            res = run_breakout_health_analysis(
                df, forward_bars=forward_bars, score_threshold=score_threshold,
                sides=sides, symbol=symbol, verbose=False,
            )
        except KeyError as e:
            print(f"[BREAKOUT HEALTH] {symbol} — skipped: {e}")
            continue

        if res is not None and not res.empty:
            all_dfs.append(res)
        else:
            print(f"[BREAKOUT HEALTH] {symbol} — no events, skipped from aggregate")

    if not all_dfs:
        print("\n[BREAKOUT HEALTH] No usable results across any symbol.")
        return pd.DataFrame()

    combined = pd.concat(all_dfs, ignore_index=True)

    summary_df = combined
    if outlier_clip_mult is not None:
        summary_df = _clip_outlier_nets(combined, outlier_clip_mult)
        n_clipped = sum(
            (combined[f"{rule}_net"] != summary_df[f"{rule}_net"]).sum()
            for rule in ["A_first_cont", "B_health_score", "C_strict_all4", "D_oracle"]
        )
        print(f"\n[OUTLIER CLIP] {n_clipped} event-rule values clipped "
              f"(threshold = {outlier_clip_mult}x median |net|). "
              f"Returned DataFrame still has raw values — clipping only affects the summary below.")

    print(f"\n{'='*70}")
    print(f"AGGREGATE ACROSS {len(all_dfs)} SYMBOLS ({len(combined)} total events)")
    print(f"{'='*70}")
    _print_summary(summary_df, forward_bars, score_threshold)
    _print_symbol_side_table(summary_df, score_threshold)

    # Count how many symbol/side combos land in each verdict bucket —
    # gives a single number for "does this generalize."
    print("\n--- Verdict tally across all symbol/side combos ---")
    tally = {"SIGNAL problem (no edge)": 0, "TIMING problem (B/C should help)": 0,
             "TIMING: B improves on A": 0, "mixed / inconclusive": 0, "no oracle data": 0}
    for (symbol, side), grp in summary_df.groupby(["symbol", "side"]):
        d_sub = grp[grp["D_oracle_fired"] == True]
        a_sub = grp[grp["A_first_cont_fired"] == True]
        b_sub = grp[grp["B_health_score_fired"] == True]
        d_net = d_sub["D_oracle_net"].mean() if not d_sub.empty else np.nan
        a_net = a_sub["A_first_cont_net"].mean() if not a_sub.empty else np.nan
        b_net = b_sub["B_health_score_net"].mean() if not b_sub.empty else np.nan

        if pd.isna(d_net):
            tally["no oracle data"] += 1
        elif d_net <= 0:
            tally["SIGNAL problem (no edge)"] += 1
        elif not pd.isna(a_net) and a_net <= 0 and d_net > 0:
            tally["TIMING problem (B/C should help)"] += 1
        elif not pd.isna(b_net) and not pd.isna(a_net) and b_net > a_net * 1.2:
            tally["TIMING: B improves on A"] += 1
        else:
            tally["mixed / inconclusive"] += 1

    total_combos = sum(tally.values())
    for k, v in tally.items():
        pct = (v / total_combos * 100) if total_combos else 0
        print(f"  {k:35} {v:>3} / {total_combos}  ({pct:.0f}%)")

    return combined


# ==========================================================
# THRESHOLD / COMPONENT SENSITIVITY SWEEP
# ==========================================================
def run_sensitivity_sweep(df, forward_bars=12, symbol="?", thresholds=(1, 2, 3, 4)):
    """
    Two things this answers that the main comparison doesn't:

      1. Is score_threshold=3 actually the best cutoff, or would 2 or 4
         do better? Sweeps every threshold and reports avg_net + win_rate
         + fired% for each.

      2. Is the win coming from all 4 components equally, or is one
         component doing all the work (and the others just diluting the
         score)? Runs each of the 4 components ALONE as a single-condition
         rule ("first bar where this ONE component improves") and compares.

    Prints two tables. Returns nothing — this is pure diagnostic output,
    not meant to feed back into run_breakout_health_analysis programmatically.
    """
    required_cols = [
        "VALID_BREAK_LONG", "VALID_BREAK_SHORT", "FLOW_STRENGTH", "VER",
        "DISPLACEMENT_SCORE", "RESISTANCE", "SUPPORT", "close", "high", "low",
    ]
    missing = [c for c in required_cols if c not in df.columns]
    if missing:
        raise KeyError(f"df is missing required columns: {missing}. "
                        f"Pass the df AFTER generate_signal() has run.")

    sides = ("long", "short")

    # ---- Part 1: threshold sweep ----
    print(f"\n=== THRESHOLD SENSITIVITY SWEEP — {symbol} ===")
    print(f"{'threshold':>10} {'fired%':>8} {'avg_mfe':>9} {'avg_mae':>9} {'avg_net':>9} {'win_rate':>9}")
    print("-" * 60)

    # Pre-score all windows once (threshold doesn't change the scoring,
    # only which bar gets selected) — avoids rescoring per threshold.
    all_events = {side: _find_breakout_events(df, side) for side in sides}
    all_scored = {
        side: [(ev, _score_window(df, ev, side)) for ev in all_events[side]]
        for side in sides
    }

    for thresh in thresholds:
        rows = []
        for side in sides:
            for ev, scored_rows in all_scored[side]:
                if not scored_rows:
                    continue
                chosen_row = None
                for r in scored_rows:
                    if r["score"] >= thresh:
                        chosen_row = r
                        break
                if chosen_row is None:
                    rows.append({"fired": False})
                    continue
                fm = _forward_mfe_mae(df, chosen_row["idx"], side, forward_bars)
                if fm is None:
                    rows.append({"fired": False})
                    continue
                rows.append({
                    "fired": True,
                    "mfe": fm["mfe"], "mae": fm["mae"], "net": fm["mfe"] - fm["mae"],
                })

        thresh_df = pd.DataFrame(rows)
        n_total = len(thresh_df)
        fired_pct = thresh_df["fired"].mean() * 100 if n_total else 0.0
        sub = thresh_df[thresh_df["fired"] == True] if n_total else thresh_df
        if sub.empty:
            print(f"{thresh:>10} {fired_pct:>7.1f}% {'--':>9} {'--':>9} {'--':>9} {'--':>9}")
            continue
        avg_mfe = sub["mfe"].mean()
        avg_mae = sub["mae"].mean()
        avg_net = sub["net"].mean()
        win_rate = (sub["net"] > 0).mean() * 100
        print(f"{thresh:>10} {fired_pct:>7.1f}% {avg_mfe:>9.4f} {avg_mae:>9.4f} {avg_net:>9.4f} {win_rate:>8.1f}%")

    print("\nIf avg_net keeps climbing as threshold rises, a stricter cutoff (or even")
    print("requiring all 4, i.e. rule C) is leaving edge on the table by firing too early.")
    print("If avg_net peaks at 2 or 3 and falls at 4, that's the real optimum — going")
    print("stricter just shrinks your sample without buying you anything.")

    # ---- Part 2: single-component ablation ----
    print(f"\n=== SINGLE-COMPONENT ABLATION — {symbol} ===")
    print("Each rule below fires on the first bar where ONLY that one component is true")
    print("(ignoring the other 3) — tells you which component is actually carrying signal.")
    print(f"{'component':20} {'fired%':>8} {'avg_mfe':>9} {'avg_mae':>9} {'avg_net':>9} {'win_rate':>9}")
    print("-" * 70)

    components = ["flow_improving", "ver_improving", "disp_ok", "accepting"]
    for comp in components:
        rows = []
        for side in sides:
            for ev, scored_rows in all_scored[side]:
                if not scored_rows:
                    continue
                chosen_row = None
                for r in scored_rows:
                    if r[comp]:
                        chosen_row = r
                        break
                if chosen_row is None:
                    rows.append({"fired": False})
                    continue
                fm = _forward_mfe_mae(df, chosen_row["idx"], side, forward_bars)
                if fm is None:
                    rows.append({"fired": False})
                    continue
                rows.append({
                    "fired": True,
                    "mfe": fm["mfe"], "mae": fm["mae"], "net": fm["mfe"] - fm["mae"],
                })

        comp_df = pd.DataFrame(rows)
        n_total = len(comp_df)
        fired_pct = comp_df["fired"].mean() * 100 if n_total else 0.0
        sub = comp_df[comp_df["fired"] == True] if n_total else comp_df
        if sub.empty:
            print(f"{comp:20} {fired_pct:>7.1f}% {'--':>9} {'--':>9} {'--':>9} {'--':>9}")
            continue
        avg_mfe = sub["mfe"].mean()
        avg_mae = sub["mae"].mean()
        avg_net = sub["net"].mean()
        win_rate = (sub["net"] > 0).mean() * 100
        print(f"{comp:20} {fired_pct:>7.1f}% {avg_mfe:>9.4f} {avg_mae:>9.4f} {avg_net:>9.4f} {win_rate:>8.1f}%")

    print("\nIf one component's avg_net/win_rate is far ahead of the other three, the")
    print("composite score is being diluted by the weaker components — consider")
    print("weighting that component higher, or dropping the weak ones from the score.")


# ==========================================================
# MULTI-SYMBOL THRESHOLD SWEEP (pooled, split by side)
# ==========================================================
def run_multi_symbol_sensitivity_sweep(symbol_dfs, forward_bars=12, thresholds=(1, 2, 3, 4),
                                        outlier_clip_mult=None):
    """
    Same idea as run_sensitivity_sweep, but pooled across every symbol in
    symbol_dfs AND split by side. Answers: does the threshold curve look
    the same pooled as it did on FIL alone, and do longs/shorts need
    different thresholds?
    """
    required_cols = [
        "VALID_BREAK_LONG", "VALID_BREAK_SHORT", "FLOW_STRENGTH", "VER",
        "DISPLACEMENT_SCORE", "RESISTANCE", "SUPPORT", "close", "high", "low",
    ]

    sides = ("long", "short")
    all_rows = []

    for symbol, df in symbol_dfs.items():
        missing = [c for c in required_cols if c not in df.columns]
        if missing:
            print(f"[SWEEP] {symbol} — skipped, missing columns: {missing}")
            continue

        all_events = {side: _find_breakout_events(df, side) for side in sides}
        all_scored = {
            side: [(ev, _score_window(df, ev, side)) for ev in all_events[side]]
            for side in sides
        }

        for side in sides:
            for ev, scored_rows in all_scored[side]:
                if not scored_rows:
                    continue
                for thresh in thresholds:
                    chosen_row = None
                    for r in scored_rows:
                        if r["score"] >= thresh:
                            chosen_row = r
                            break
                    if chosen_row is None:
                        all_rows.append({"symbol": symbol, "side": side, "threshold": thresh, "fired": False})
                        continue
                    fm = _forward_mfe_mae(df, chosen_row["idx"], side, forward_bars)
                    if fm is None:
                        all_rows.append({"symbol": symbol, "side": side, "threshold": thresh, "fired": False})
                        continue
                    all_rows.append({
                        "symbol": symbol, "side": side, "threshold": thresh, "fired": True,
                        "mfe": fm["mfe"], "mae": fm["mae"], "net": fm["mfe"] - fm["mae"],
                    })

    if not all_rows:
        print("\n[SWEEP] No usable results across any symbol.")
        return

    sweep_df = pd.DataFrame(all_rows)

    if outlier_clip_mult is not None:
        for thresh in thresholds:
            mask = (sweep_df["threshold"] == thresh) & (sweep_df["fired"] == True)
            fired_vals = sweep_df.loc[mask, "net"].dropna()
            if fired_vals.empty:
                continue
            median_abs = fired_vals.abs().median()
            if median_abs <= 0 or pd.isna(median_abs):
                continue
            bound = outlier_clip_mult * median_abs
            sweep_df.loc[mask, "net"] = sweep_df.loc[mask, "net"].clip(lower=-bound, upper=bound)
        print(f"\n[OUTLIER CLIP] applied at {outlier_clip_mult}x median |net|, per-threshold.")

    def _print_threshold_table(label, sub_df):
        print(f"\n=== THRESHOLD SWEEP — {label} (pooled across {sub_df['symbol'].nunique()} symbols) ===")
        print(f"{'threshold':>10} {'fired%':>8} {'avg_mfe':>9} {'avg_mae':>9} {'avg_net':>9} {'win_rate':>9}")
        print("-" * 60)
        for thresh in thresholds:
            t_df = sub_df[sub_df["threshold"] == thresh]
            n_total = len(t_df)
            fired_pct = t_df["fired"].mean() * 100 if n_total else 0.0
            fired_sub = t_df[t_df["fired"] == True]
            if fired_sub.empty:
                print(f"{thresh:>10} {fired_pct:>7.1f}% {'--':>9} {'--':>9} {'--':>9} {'--':>9}")
                continue
            avg_mfe = fired_sub["mfe"].mean()
            avg_mae = fired_sub["mae"].mean()
            avg_net = fired_sub["net"].mean()
            win_rate = (fired_sub["net"] > 0).mean() * 100
            print(f"{thresh:>10} {fired_pct:>7.1f}% {avg_mfe:>9.4f} {avg_mae:>9.4f} {avg_net:>9.4f} {win_rate:>8.1f}%")

    _print_threshold_table("ALL (long + short combined)", sweep_df)
    _print_threshold_table("LONG only", sweep_df[sweep_df["side"] == "long"])
    _print_threshold_table("SHORT only", sweep_df[sweep_df["side"] == "short"])

    print("\nCompare the LONG and SHORT tables: if their avg_net curves peak at different")
    print("thresholds, a single global score_threshold is leaving edge on the table for")
    print("one side or the other — consider separate ENTRY_LONG / ENTRY_SHORT thresholds.")


if __name__ == "__main__":
    print("Import this module and call run_breakout_health_analysis(ltf_df_bt) "
          "after your normal backtest script has built ltf_df_bt via generate_signal().\n"
          "For multi-symbol validation, build a {symbol: df} dict and call "
          "run_multi_symbol_analysis(symbol_dfs, outlier_clip_mult=20).\n"
          "For threshold/component tuning on one symbol, call run_sensitivity_sweep(ltf_df_bt).\n"
          "For threshold tuning pooled across symbols and split by side, call "
          "run_multi_symbol_sensitivity_sweep(symbol_dfs, outlier_clip_mult=20).")