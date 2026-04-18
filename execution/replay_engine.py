# execution/replay_engine.py

import os
import json
import pandas as pd
from execution.hourly_runner import run_hourly_for_symbol, SYMBOLS
from execution.notifier import TelegramNotifier

STATE_FILES = [
    "data/last_5m_seen.json",
    "data/last_hour_seen.json",
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

    if from_ts:
        hourly_timestamps = [t for t in hourly_timestamps if t >= pd.Timestamp(from_ts, tz="UTC")]
    if to_ts:
        hourly_timestamps = [t for t in hourly_timestamps if t <= pd.Timestamp(to_ts, tz="UTC")]

    total = len(hourly_timestamps)

    notifier.send_text(
        f"🔁 *REPLAY STARTED*\n"
        f"Symbol: `{symbol}`\n"
        f"Bars: `{total}`\n"
        f"From: `{hourly_timestamps[0]}`\n"
        f"To: `{hourly_timestamps[-1]}`"
    )

    for i, hour_ts in enumerate(hourly_timestamps):
        forced_time = hourly_timestamps[i + 1] if i + 1 < len(hourly_timestamps) else None
        if forced_time is None:
            break

        run_hourly_for_symbol(symbol, forced_time=forced_time, notify_override=notify_trades)

        # progress ping every 20 bars
        if (i + 1) % 20 == 0:
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