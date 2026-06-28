"""
exit_audit.py — per-bar exit filter diagnostic

Shows exactly how far each active exit filter is from firing on a given bar.
Used in both lifecycle.py (live, every bar) and the backtest (last 12 bars).

Usage:
    from strategy.exit_audit import format_exit_audit

    # inside _check_intrabar (backtest) or update() (live):
    print(format_exit_audit(symbol, side, bars, mfe_r, pnl_r, mae_r,
                            bar_open, bar_high, bar_low, bar_close,
                            stop_loss, initial_stop, R, atr,
                            window_5m))
"""

import pandas as pd
import numpy as np


# ── helpers ──────────────────────────────────────────────────────────────────

def _body(o, c):
    return abs(c - o)

def _two_bar_move(window):
    if len(window) >= 2:
        return abs(float(window.iloc[-1]["close"]) - float(window.iloc[-2]["close"]))
    return 0.0

def _oie_atr(window):
    """Mirrors the ATR resolution logic in opposite_impulse_exit exactly."""
    if "ATR_5M" in window.columns:
        atr = window["ATR_5M"].iloc[-3:].mean()
        if not (pd.isna(atr) or atr <= 0):
            return float(atr)
        atr = window["ATR_5M"].iloc[0]
        if not (pd.isna(atr) or atr <= 0):
            return float(atr)

    if "ATR" in window.columns:
        atr_1h = window["ATR"].iloc[-3:].mean()
        if not (pd.isna(atr_1h) or atr_1h <= 0):
            return float(atr_1h) * 0.20
        atr_1h = window["ATR"].iloc[0]
        if not (pd.isna(atr_1h) or atr_1h <= 0):
            return float(atr_1h) * 0.20

    return None

def _vol_blocked(window):
    if "volume" not in window.columns or len(window) < 10:
        return False
    avg_vol = window["volume"].iloc[-10:].mean()
    last_vol = float(window.iloc[-1]["volume"])
    if pd.isna(avg_vol) or avg_vol <= 0:
        return False
    return last_vol < avg_vol * 0.8

def _location_blocked(pnl_r):
    """OIE location guard — anchored to initial_stop via pnl_r."""
    close_to_initial_stop_r = pnl_r + 1.0
    return close_to_initial_stop_r > 1.75 and pnl_r > 0.0


# ── main formatter ────────────────────────────────────────────────────────────

def format_exit_audit(
    symbol,
    side,           # 1 = long, -1 = short
    bars,           # bars_in_trade
    mfe_r,
    pnl_r,
    mae_r,          # abs(_price_mae) / R
    bar_open,
    bar_high,
    bar_low,
    bar_close,
    stop_loss,      # live stop (may be trailed)
    initial_stop,
    R,
    atr_5m,         # 5m ATR (may be None)
    window_5m,      # pd.DataFrame of 5m bars since entry
    ts=None,
):
    if R <= 0:
        return f"[EXIT AUDIT] {symbol} — R=0, skipping"

    side_str = "L" if side == 1 else "S"
    lines = [
        f"[EXIT AUDIT] {symbol} bar={bars} {side_str} | "
        f"mfe_r={mfe_r:.3f} pnl_r={pnl_r:.3f} mae_r={mae_r:.3f}"
    ]

    # ── 1. DOMINANCE EXIT ────────────────────────────────────────────────────
    # fires: bars==3 AND mfe_r==0 AND mae_r>0.50
    if bars == 3:
        if mfe_r == 0.0:
            margin = mae_r - 0.50
            status = "FIRE" if margin > 0 else "SKIP"
            lines.append(
                f"  dominance_exit  : {status:4s}  bars=3 ✓ | mfe_r=0 ✓ | "
                f"mae_r={mae_r:.3f} {'>' if margin>0 else '<'} 0.50 "
                f"({'fires' if margin>0 else f'need +{-margin:.3f} more'})"
            )
        else:
            lines.append(
                f"  dominance_exit  : SKIP  bars=3 ✓ | mfe_r={mfe_r:.3f} ≠ 0 "
                f"(need mfe_r=0)"
            )
    else:
        lines.append(
            f"  dominance_exit  : SKIP  bars={bars} ≠ 3"
        )

    # ── 2. TRAP REJECTION ────────────────────────────────────────────────────
    # fires: bars<=2 AND mae_r>1.2 AND trap_ratio>4.0
    if bars <= 2:
        if mae_r > 1.2:
            trap_ratio = mae_r / max(mfe_r, 0.05)
            margin = trap_ratio - 4.0
            status = "FIRE" if margin > 0 else "SKIP"
            lines.append(
                f"  trap_rejection  : {status:4s}  bars≤2 ✓ | mae_r={mae_r:.3f}>1.2 ✓ | "
                f"ratio={trap_ratio:.2f} {'>' if margin>0 else '<'} 4.0 "
                f"({'fires' if margin>0 else f'need +{-margin:.2f}'})"
            )
        else:
            lines.append(
                f"  trap_rejection  : SKIP  bars≤2 ✓ | mae_r={mae_r:.3f} < 1.2 "
                f"(need +{1.2-mae_r:.3f}R more adverse)"
            )
    else:
        lines.append(
            f"  trap_rejection  : SKIP  bars={bars} > 2"
        )

    # ── 3. THESIS INVALIDATION ───────────────────────────────────────────────
    # fires: 0.15<=mfe_r<0.5 AND pnl_r<-0.10 AND prox<0.25 AND dd>0.35
    if 0.15 <= mfe_r < 0.5:
        if side == 1:
            prox = (bar_high - initial_stop) / R
        else:
            prox = (initial_stop - bar_low) / R
        drawdown = mfe_r - pnl_r

        conds = []
        fires = True

        pnl_ok = pnl_r < -0.10
        conds.append(f"pnl_r={pnl_r:.3f}{'<-0.10 ✓' if pnl_ok else f' need <-0.10 (gap={pnl_r+0.10:.3f})'}")
        if not pnl_ok:
            fires = False

        prox_ok = prox < 0.25
        conds.append(f"prox={prox:.3f}{'<0.25 ✓' if prox_ok else f' need <0.25 (gap={prox-0.25:.3f})'}")
        if not prox_ok:
            fires = False

        dd_ok = drawdown > 0.35
        conds.append(f"dd={drawdown:.3f}{'>0.35 ✓' if dd_ok else f' need >0.35 (gap={0.35-drawdown:.3f})'}")
        if not dd_ok:
            fires = False

        status = "FIRE" if fires else "SKIP"
        lines.append(
            f"  thesis_invalid  : {status:4s}  mfe_r in [0.15,0.5) ✓ | "
            + " | ".join(conds)
        )
    else:
        if mfe_r < 0.15:
            lines.append(
                f"  thesis_invalid  : SKIP  mfe_r={mfe_r:.3f} < 0.15 "
                f"(need +{0.15-mfe_r:.3f})"
            )
        else:
            lines.append(
                f"  thesis_invalid  : SKIP  mfe_r={mfe_r:.3f} >= 0.5 "
                f"(trail owns this trade)"
            )

    # ── 4. STOP PROXIMITY EXIT ───────────────────────────────────────────────
    # fires: mfe_r<0.5 AND pnl_r<-0.35 AND prox<0.20
    if mfe_r < 0.5:
        if side == 1:
            prox = (bar_close - initial_stop) / R
        else:
            prox = (initial_stop - bar_close) / R

        conds = []
        fires = True

        pnl_ok = pnl_r < -0.35
        conds.append(f"pnl_r={pnl_r:.3f}{'<-0.35 ✓' if pnl_ok else f' need <-0.35 (gap={pnl_r+0.35:.3f})'}")
        if not pnl_ok:
            fires = False

        prox_ok = prox < 0.20
        conds.append(f"prox={prox:.3f}{'<0.20 ✓' if prox_ok else f' need <0.20 (gap={prox-0.20:.3f})'}")
        if not prox_ok:
            fires = False

        status = "FIRE" if fires else "SKIP"
        lines.append(
            f"  stop_proximity  : {status:4s}  mfe_r<0.5 ✓ | " + " | ".join(conds)
        )
    else:
        lines.append(
            f"  stop_proximity  : SKIP  mfe_r={mfe_r:.3f} >= 0.5 "
            f"(trail owns this trade)"
        )

    # ── 5. OIE ───────────────────────────────────────────────────────────────
    # fires: big_candle AND wrong_dir AND NOT location_blocked AND NOT vol_blocked
    if len(window_5m) < 3:
        lines.append(
            f"  oie             : SKIP  window too short ({len(window_5m)} bars, need 3)"
        )
    else:
        last = window_5m.iloc[-1]
        o_last = float(last["open"])
        c_last = float(last["close"])

        oie_atr = _oie_atr(window_5m)
        if oie_atr is None or oie_atr <= 0:
            lines.append(
                f"  oie             : SKIP  no valid ATR in window"
            )
        else:
            body = _body(o_last, c_last)
            tbm = _two_bar_move(window_5m)
            threshold = oie_atr * 1.2
            two_bar_threshold = oie_atr * 1.5
            body_ok = body > threshold
            two_bar_ok = tbm > two_bar_threshold and body > oie_atr * 0.6
            big_candle = body_ok or two_bar_ok

            wrong_dir = (c_last < o_last) if side == 1 else (c_last > o_last)
            loc_blocked = _location_blocked(pnl_r)
            vol_blk = _vol_blocked(window_5m)
            fires = big_candle and wrong_dir and not loc_blocked and not vol_blk

            # build per-condition breakdown
            if body_ok:
                body_note = f"body={body:.5f}>{threshold:.5f} ✓"
            else:
                body_note = f"body={body:.5f}<{threshold:.5f} (need +{threshold-body:.5f})"

            dir_note = f"wrong_dir={'✓' if wrong_dir else '✗ (candle goes right way)'}"
            loc_note = f"loc_block={'✓ blocked' if loc_blocked else '✗'}"
            vol_note = f"vol_block={'✓ blocked' if vol_blk else '✗'}"

            status = "FIRE" if fires else "SKIP"
            lines.append(
                f"  oie             : {status:4s}  {body_note} | {dir_note} | "
                f"{loc_note} | {vol_note}"
            )

    # ── 6. HARD STOP ─────────────────────────────────────────────────────────
    if side == 1:
        stop_dist = bar_low - stop_loss
        stop_hit = bar_low <= stop_loss
    else:
        stop_dist = stop_loss - bar_high
        stop_hit = bar_high >= stop_loss

    status = "FIRE" if stop_hit else "SKIP"
    price_str = f"low={bar_low:.5f}" if side == 1 else f"high={bar_high:.5f}"
    if stop_hit:
        lines.append(
            f"  hard_stop       : {status:4s}  {price_str} reached stop={stop_loss:.5f}"
        )
    else:
        lines.append(
            f"  hard_stop       : {status:4s}  {price_str} vs stop={stop_loss:.5f} "
            f"(margin={abs(stop_dist):.5f} = {abs(stop_dist)/R:.3f}R)"
        )

    return "\n".join(lines)