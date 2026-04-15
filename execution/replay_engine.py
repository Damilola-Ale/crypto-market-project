# execution/replay_engine.py

from execution.hourly_runner import run_hourly_for_symbol, SYMBOLS
import config.runtime
config.runtime.STATE_MODE = "MEMORY"

def fast_replay_all():
    for symbol in SYMBOLS:
        run_hourly_for_symbol(symbol, replay=True)

if __name__ == "__main__":
    fast_replay_all()