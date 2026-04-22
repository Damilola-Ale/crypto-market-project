# execution/hourly_runner.py
import os
import json
from datetime import datetime, timezone
from utils.log import debug, info, trade, error
from utils.logger import log

from data_pipeline.updater import update_symbol
from indicators.indicators import generate_signal
from strategy.lifecycle import PositionManager
from execution.notifier import TelegramNotifier
import pandas as pd

SYMBOLS = [
    "ETHUSDT", "FILUSDT", "TRXUSDT", "VETUSDT", "UNIUSDT", "DOGEUSDT", "ETCUSDT",
    "AAVEUSDT", "BCHUSDT", "BANDUSDT", "TIAUSDT", "XLMUSDT", "SUIUSDT", "BTCUSDT",
    "ZENUSDT", "AVAXUSDT", "AXSUSDT", "ORDIUSDT", "LDOUSDT", "LINKUSDT"
]

SIGNAL_STORE       = "data/signals.json"
HOUR_MEMORY_FILE = "data/last_hour_seen.json"

# Interval constants
LLTF_INTERVAL = "5m"
LTF_INTERVAL  = "1h"
HTF_INTERVAL  = "4h"

def _last_5m_file(symbol: str, live: bool) -> str:
    prefix = "live" if live else "replay"
    return f"data/cursors/{prefix}_{symbol}.json"

def run_hourly():
    print("\n==============================")
    print("CRYPTO MARKET PROJECT EXECUTION")
    print("==============================\n")

    os.makedirs("data", exist_ok=True)

    notifier = TelegramNotifier()

    if os.path.exists("data/replay_lock.json"):
        notifier.send_text("🔒 *LIVE SKIPPED*\nReplay lock active — skipping live execution")
        return

    with open("data/last_run.json", "w") as f:
        json.dump(
            {
                "ran_at": datetime.now(timezone.utc).isoformat(),
                "symbols": SYMBOLS
            },
            f,
            indent=2
        )

    symbol_summaries = []
    for symbol in SYMBOLS:
        result = run_hourly_for_symbol(symbol)
        if isinstance(result, tuple):
            summary, _ = result
        else:
            summary = result
        symbol_summaries.append((symbol, summary))

    now = datetime.now(timezone.utc)
    candle_time = now.replace(minute=0, second=0, microsecond=0)
    ran_at = candle_time.strftime("%H:%M UTC")
    active_lines = []
    for symbol, summary in symbol_summaries:
        if isinstance(summary, list):
            opens  = sum(1 for r in summary if r.get("state") == "OPEN")
            closes = sum(1 for r in summary if r.get("state") == "CLOSED")
            parts = []
            if opens:
                parts.append(f"{opens} opened")
            if closes:
                parts.append(f"{closes} closed")
            if parts:
                active_lines.append(f"`{symbol}` — " + ", ".join(parts))

    actual_ran_at = datetime.now(timezone.utc).strftime("%H:%M:%S UTC")
    msg = f"🕐 *LIVE RUN* `{ran_at}` | triggered `{actual_ran_at}`"
    if active_lines:
        msg += "\n" + "\n".join(active_lines)

    notifier.send_text(msg)

    print("\n=== EXECUTION COMPLETE ===\n")

# ==========================================================
# SINGLE SYMBOL ENGINE (UNIFIED LIVE + REPLAY)
# ==========================================================
def run_hourly_for_symbol(symbol: str, forced_time=None, replay=False, notify_override=None, verbose=True, replay_cursor=None):
    is_live = not replay and forced_time is None
    notify = notify_override if notify_override is not None else is_live
    pm = PositionManager(persist=True, notify=notify)
    notifier = TelegramNotifier()

    # =========================
    # 5M STREAM MEMORY (CRITICAL FIX)
    # =========================
    os.makedirs("data/cursors", exist_ok=True)
    last_5m_file = _last_5m_file(symbol, is_live)
    try:
        if os.path.exists(last_5m_file):
            with open(last_5m_file, "r") as f:
                last_seen_raw = json.load(f)
                last_5m_seen = {symbol: last_seen_raw} if isinstance(last_seen_raw, str) else last_seen_raw
        else:
            last_5m_seen = {}
    except Exception as state_err:
        notifier.send_text(
            f"💥 *STATE LOAD FAILED*\n"
            f"`{symbol}`\n"
            f"file=`{last_5m_file}`\n"
            f"error=`{str(state_err)[:200]}`"
        )
        last_5m_seen = {}

    try:
        # -------------------
        # FAST GATE — skip entire symbol if no new 5m bar
        # -------------------
        if is_live:
            raw = last_5m_seen.get(symbol) or (last_5m_seen if isinstance(last_5m_seen, str) else None)
            last_seen_ts = pd.Timestamp(raw) if raw else None

            if last_seen_ts is not None:
                now_check = datetime.now(timezone.utc)
                minutes_floored = (now_check.minute // 5) * 5
                current_5m_boundary = now_check.replace(minute=minutes_floored, second=0, microsecond=0)
                if last_seen_ts >= current_5m_boundary:
                    print(f"[FAST GATE] {symbol} — cursor {last_seen_ts} >= boundary {current_5m_boundary}, skipping")
                    return None

        # -------------------
        # FETCH DATA
        # -------------------
        try:
            if forced_time is None and not replay:
                df, htf_df, lltf_df = update_symbol(symbol)
                df, htf_df, lltf_df = df.iloc[:-1], htf_df.iloc[:-1], lltf_df.iloc[:-1]
            else:
                df, htf_df, lltf_df = update_symbol(symbol)

                if forced_time:
                    df      = df[df.index < forced_time].copy()
                    htf_df  = htf_df[htf_df.index < forced_time].copy()
                    lltf_df = lltf_df[lltf_df.index < forced_time].copy()
                else:
                    df, htf_df, lltf_df = df.iloc[:-1], htf_df.iloc[:-1], lltf_df.iloc[:-1]

        except Exception as fetch_err:
            notifier.send_text(
                f"💥 *UPDATE_SYMBOL FAILED*\n"
                f"`{symbol}` forced_time=`{forced_time}`\n"
                f"Error: `{str(fetch_err)[:300]}`"
            )
            return None

        # -------------------
        # GENERATE & MAP SIGNALS (The Unified Way)
        # -------------------
        df = generate_signal(df.copy(), htf_df.copy())

        lltf_df = lltf_df[lltf_df.index >= df.index[0]].copy()
        lltf_df = map_ltf_to_htf(lltf_df, df)

        lltf_df["final_signal"] = df["final_signal"].reindex(
            lltf_df.index,
            method="ffill"
        )

        if 'final_signal' not in df.columns or len(df) < 2:
            return

        # Precompute rolling ATR on 5m dataframe once (backtest parity)
        tr_5m = pd.concat([
            lltf_df['high'] - lltf_df['low'],
            (lltf_df['high'] - lltf_df['close'].shift()).abs(),
            (lltf_df['low']  - lltf_df['close'].shift()).abs()
        ], axis=1).max(axis=1)
        lltf_df['ATR'] = tr_5m.rolling(14).mean()

        lltf_frozen = lltf_df.copy()
        lltf_frozen = lltf_frozen.dropna(subset=['ltf_index'])
        lltf_frozen['ltf_index'] = lltf_frozen['ltf_index'].astype(int)
        print("FINAL SIGNAL NON-NULL:", lltf_df["final_signal"].notna().sum())

        # ==========================================================
        # NEW 1H CANDLE DETECTION (LIVE ONLY)
        # ==========================================================
        if os.path.exists(HOUR_MEMORY_FILE):
            with open(HOUR_MEMORY_FILE, "r") as f:
                last_hour_seen = json.load(f)
        else:
            last_hour_seen = {}

        latest_hour_ts = df.index[-1].isoformat()
        previous_hour  = last_hour_seen.get(symbol)

        new_hour = latest_hour_ts != previous_hour

        # =========================
        # TRUE STREAMING ENGINE FIX (IMMEDIATE DISPATCH)
        # =========================

        latest_ts = lltf_frozen.index[-1]
        if replay_cursor is not None:
            last_seen = replay_cursor
        else:
            raw = last_5m_seen.get(symbol) or (last_5m_seen if isinstance(last_5m_seen, str) else None)
            last_seen = pd.Timestamp(raw) if raw else None

        if is_live and last_seen == latest_ts:
            return None

        # First live run: seed cursor and exit — never replay full history into live orders
        if last_seen is None and not replay and not forced_time:
            with open(last_5m_file + ".tmp", "w") as f:
                json.dump(latest_ts.isoformat(), f)
            os.replace(last_5m_file + ".tmp", last_5m_file)
            print(f"[FIRST RUN] {symbol} cursor seeded at {latest_ts}, skipping history")

            # Derive open trade state directly from signal data — no disk state needed
            last_signal = int(df["final_signal"].iloc[-1])
            if last_signal != 0:
                dir_text = "LONG" if last_signal == 1 else "SHORT"
                notifier.send_text(
                    f"⚠️ *SYSTEM RESTARTED*\n"
                    f"Symbol: `{symbol}`\n"
                    f"Active signal detected: `{dir_text}`\n"
                    f"Signal bar: `{df.index[-1]}`\n"
                    f"⚠️ A trade may still be running — verify on exchange before acting on next signal"
                )
            else:
                notifier.send_text(
                    f"🔄 *SYSTEM RESTARTED*\n"
                    f"Symbol: `{symbol}`\n"
                    f"No active signal — system is flat"
                )

            return None

        new_bars = (
            lltf_frozen if last_seen is None
            else lltf_frozen[lltf_frozen.index > last_seen]
        )

        if new_bars.empty:
            return None

        bar_results = []

        for _, row_5m in new_bars.iterrows():

            if pd.isna(row_5m["final_signal"]):
                bar_signal = 0
            else:
                bar_signal = int(row_5m["final_signal"])

            ltf_row = df.iloc[int(row_5m["ltf_index"])]

            if not pd.isna(row_5m.get("final_signal", float("nan"))) and row_5m["final_signal"] != 0:
                notifier.send_text(
                    f"🚨 *SIGNAL REACHED LIFECYCLE*\n"
                    f"{symbol}\n"
                    f"ts: `{_}`\n"
                    f"signal: `{row_5m['final_signal']}`"
                )

            result = pm.update(
                df=df,
                symbol=symbol,
                lltf_df=lltf_frozen,
                external_signal=bar_signal,
                external_row=ltf_row,
                current_5m_row=row_5m
            )
            if isinstance(result, dict) and result.get("state") in ("OPEN", "CLOSED"):
                bar_results.append(result)

        # REPLAY: force-close any position still open at end of data
        if replay and symbol in pm.positions:
            pos = pm.positions[symbol]
            last_row = new_bars.iloc[-1]
            closed = pm._close(
                symbol,
                float(last_row["close"]),
                last_row.name,
                "replay_end"
            )
            print(
                f"[REPLAY END CLOSE] {symbol} "
                f"entry={closed['entry_price']} "
                f"bars={closed['bars_in_trade']} "
                f"pnl_r={closed['exit']['pnl_r']:.2f}"
            )

        # update cursor AFTER processing
        if not replay and replay_cursor is None:
            with open(last_5m_file + ".tmp", "w") as f:
                json.dump(new_bars.index[-1].isoformat(), f)
            os.replace(last_5m_file + ".tmp", last_5m_file)

        # ==========================================================
        # SAVE LAST PROCESSED HOUR
        # ==========================================================
        if not replay and not forced_time and new_hour:
            last_hour_seen[symbol] = latest_hour_ts
            with open(HOUR_MEMORY_FILE + ".tmp", "w") as f:
                json.dump(last_hour_seen, f, indent=2)
            os.replace(HOUR_MEMORY_FILE + ".tmp", HOUR_MEMORY_FILE)
        
        pm.flush()

        new_cursor = new_bars.index[-1] if not new_bars.empty else replay_cursor
        return (bar_results if bar_results else None), new_cursor

    except Exception as e:
        import traceback
        notifier.send_text(
            f"💥 *RUNNER EXCEPTION*\n"
            f"`{symbol}` forced_time=`{forced_time}`\n"
            f"error=`{str(e)[:300]}`"
        )
        error(f"[ERROR] {symbol} → {e}")
        traceback.print_exc()
        return None

def map_ltf_to_htf(lltf_df: pd.DataFrame, htf_df: pd.DataFrame):

    htf_times = htf_df.index

    ltf_index = []

    for ts in lltf_df.index:

        # find correct 1H candle start
        idx = htf_times.searchsorted(ts, side="right") - 1

        if idx < 0:
            idx = 0

        ltf_index.append(idx)

    lltf_df = lltf_df.copy()
    lltf_df["ltf_index"] = ltf_index

    return lltf_df