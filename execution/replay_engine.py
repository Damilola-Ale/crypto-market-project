# execution/replay_engine.py

import os
import json
import pandas as pd
from execution.hourly_runner import run_hourly_for_symbol, SYMBOLS
from execution.notifier import TelegramNotifier

STATE_FILES = [
    "data/last_5m_seen.json",
    "data/last_hour_seen.json",
    "data/replay_last_5m_seen.json",
    "data/positions/open_positions.json",
    "data/positions/bar_history.json",
    "data/positions/executed_signals.json",
    "data/positions/reentry_lock.json",
    "data/positions/last_entry_ts.json",
]

def reset_replay_state():
    for f in STATE_FILES:
        if os.path.exists(f):
            os.remove(f)

def fast_replay_symbol(symbol: str, from_ts=None, to_ts=None, notify_trades=True):
    notifier = TelegramNotifier()

    df_1h = pd.read_parquet(f"data/cache/{symbol}_1h.parquet")
    hourly_timestamps = df_1h.index.tolist()

    # apply to_ts filter if given
    if to_ts:
        hourly_timestamps = [t for t in hourly_timestamps if t <= pd.Timestamp(to_ts, tz="UTC")]

    # from_ts controls where trade entries are expected, but we always
    # start processing from bar 0 so indicators have 800+ bars of warmup.
    # We just skip progress pings and trade notifications before from_ts.
    warmup_done_idx = 0
    if from_ts:
        from_ts_parsed = pd.Timestamp(from_ts, tz="UTC")
        warmup_done_idx = next(
            (i for i, t in enumerate(hourly_timestamps) if t >= from_ts_parsed),
            len(hourly_timestamps)
        )

    total = len(hourly_timestamps)

    notifier.send_text(
        f"🔁 *REPLAY STARTED*\n"
        f"Symbol: `{symbol}`\n"
        f"Total bars: `{total}` (incl. warmup)\n"
        f"Warmup until: `{hourly_timestamps[warmup_done_idx]}`\n"
        f"Signal window from: `{from_ts or 'start'}`\n"
        f"To: `{to_ts or 'end'}`"
    )

    for i, hour_ts in enumerate(hourly_timestamps):
        forced_time = hourly_timestamps[i + 1] if i + 1 < len(hourly_timestamps) else None
        if forced_time is None:
            break

        notifier.send_text(f"🔄 *LOOP BAR {i+1}/{total}* forced=`{forced_time}`")

        try:
            run_hourly_for_symbol(symbol, forced_time=forced_time, notify_override=notify_trades)
        except Exception as e:
            import traceback
            notifier.send_text(
                f"💥 *REPLAY ITERATION CRASHED*\n"
                f"`{symbol}` bar `{i+1}/{total}`\n"
                f"forced_time=`{forced_time}`\n"
                f"Error: `{str(e)[:300]}`"
            )
            traceback.print_exc()
            continue

        # progress ping every 20 bars after warmup
        if i >= warmup_done_idx and (i - warmup_done_idx + 1) % 20 == 0:
            notifier.send_text(
                f"⏳ *REPLAY PROGRESS*\n"
                f"`{symbol}` — bar {i + 1}/{total}\n"
                f"Hour: `{hour_ts}`"
            )

    notifier.send_text(
        f"✅ *REPLAY COMPLETE*\n"
        f"Symbol: `{symbol}`\n"
        f"Bars processed: `{total}`"
    )

def fast_replay_all(from_ts=None, to_ts=None, notify_trades=True):
    reset_replay_state()
    for symbol in SYMBOLS:
        fast_replay_symbol(symbol, from_ts=from_ts, to_ts=to_ts, notify_trades=notify_trades)

if __name__ == "__main__":
    fast_replay_all()