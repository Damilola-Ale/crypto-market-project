# execution/replay_engine.py

import os
import json
import pandas as pd
from execution.hourly_runner import run_hourly_for_symbol, SYMBOLS
from execution.notifier import TelegramNotifier

def _get_state_files():
    files = [
        "data/last_hour_seen.json",
        "data/positions/open_positions.json",
        "data/positions/bar_history.json",
        "data/positions/executed_signals.json",
        "data/positions/reentry_lock.json",
        "data/positions/last_entry_ts.json",
    ]
    # add all per-symbol cursor files
    if os.path.exists("data/cursors"):
        for f in os.listdir("data/cursors"):
            files.append(os.path.join("data/cursors", f))
    return files

REPLAY_CURSOR_FILE = "data/replay_last_5m_seen.json"

def reset_replay_state(symbols=None):
    files = [
        "data/last_hour_seen.json",
        "data/positions/open_positions.json",
        "data/positions/bar_history.json",
        "data/positions/executed_signals.json",
        "data/positions/reentry_lock.json",
        "data/positions/last_entry_ts.json",
    ]
    for f in files:
        if os.path.exists(f):
            os.remove(f)

    # Only wipe cursor files for targeted symbols
        # Wipe cursor files for targeted symbols
    if os.path.exists("data/cursors"):
        for fname in os.listdir("data/cursors"):
            if symbols and not any(sym in fname for sym in symbols):
                continue
            full_path = os.path.join("data/cursors", fname)
            if os.path.exists(full_path):
                os.remove(full_path)

    # Bust parquet cache for targeted symbols so update_symbol fetches fresh
    if symbols and os.path.exists("data/cache"):
        for fname in os.listdir("data/cache"):
            if any(sym in fname for sym in symbols):
                full_path = os.path.join("data/cache", fname)
                if os.path.exists(full_path):
                    os.remove(full_path)

def fast_replay_symbol(symbol: str, from_ts=None, to_ts=None, notify_trades=True):
    notifier = TelegramNotifier()

    from data_pipeline.updater import update_symbol
    df_1h, _, _ = update_symbol(symbol)
    
    # Build 5m timestamp list instead of 1H
    df_5m = pd.read_parquet(f"data/cache/{symbol}_5m.parquet")
    df_5m.index = pd.to_datetime(df_5m.index, utc=True)
    five_min_timestamps = df_5m.index.tolist()

    # apply to_ts filter
    if to_ts:
        five_min_timestamps = [t for t in five_min_timestamps if t <= pd.Timestamp(to_ts, tz="UTC")]

    # warmup: find index where from_ts starts
    warmup_done_idx = 0
    if from_ts:
        from_ts_parsed = pd.Timestamp(from_ts, tz="UTC")
        warmup_done_idx = next(
            (i for i, t in enumerate(five_min_timestamps) if t >= from_ts_parsed),
            len(five_min_timestamps)
        )

    total = len(five_min_timestamps)
    trade_opens = 0
    trade_closes = 0

    notifier.send_text(
        f"🔁 *REPLAY STARTED*\n"
        f"Symbol: `{symbol}`\n"
        f"Total 5m bars: `{total}` (incl. warmup)\n"
        f"Warmup until: `{five_min_timestamps[warmup_done_idx]}`\n"
        f"Signal window from: `{from_ts or 'start'}`\n"
        f"To: `{to_ts or 'end'}`"
    )

    replay_cursor = None

    for i, bar_ts in enumerate(five_min_timestamps):
        # forced_time is the NEXT 5m bar — "what was known just before this bar closed"
        forced_time = five_min_timestamps[i + 1] if i + 1 < len(five_min_timestamps) else None
        if forced_time is None:
            break

        is_progress_bar = (i == 0) or ((i + 1) % 240 == 0)  # every 240 5m bars = 20 hours

        if is_progress_bar:
            notifier.send_text(f"🔄 *LOOP BAR {i+1}/{total}* ts=`{bar_ts}`")

        try:
            outcome = run_hourly_for_symbol(
                symbol,
                forced_time=forced_time,
                replay=True,
                notify_override=notify_trades,
                verbose=is_progress_bar,
                replay_cursor=replay_cursor
            )
            if isinstance(outcome, tuple):
                results, replay_cursor = outcome
            else:
                results = None
            if isinstance(results, list):
                for r in results:
                    if isinstance(r, dict):
                        if r.get("state") == "OPEN":
                            trade_opens += 1
                        elif r.get("state") == "CLOSED":
                            trade_closes += 1
        except Exception as e:
            import traceback
            notifier.send_text(
                f"💥 *REPLAY ITERATION CRASHED*\n"
                f"`{symbol}` bar `{i+1}/{total}`\n"
                f"ts=`{bar_ts}`\n"
                f"Error: `{str(e)[:300]}`"
            )
            traceback.print_exc()
            continue

        if i >= warmup_done_idx and (i - warmup_done_idx + 1) % 240 == 0:
            notifier.send_text(
                f"⏳ *REPLAY PROGRESS*\n"
                f"`{symbol}` — bar {i + 1}/{total}\n"
                f"5m ts: `{bar_ts}`"
            )

    notifier.send_text(
        f"✅ *REPLAY COMPLETE*\n"
        f"Symbol: `{symbol}`\n"
        f"5m bars processed: `{total}`\n"
        f"Trades opened: `{trade_opens}`\n"
        f"Trades closed: `{trade_closes}`"
    )

def fast_replay_all(from_ts=None, to_ts=None, notify_trades=True, symbols=None):
    reset_replay_state(symbols=symbols)
    target_symbols = symbols if symbols else SYMBOLS

    with open("data/replay_lock.json", "w") as f:
        json.dump({"locked": True, "started": pd.Timestamp.now(tz="UTC").isoformat()}, f)
    try:
        for symbol in target_symbols:
            fast_replay_symbol(symbol, from_ts=from_ts, to_ts=to_ts, notify_trades=notify_trades)
    finally:
        if os.path.exists("data/replay_lock.json"):
            os.remove("data/replay_lock.json")

if __name__ == "__main__":
    fast_replay_all()