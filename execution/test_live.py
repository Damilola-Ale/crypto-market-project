# execution/test_live.py
# Runs the FULL live path on Render — real data fetch, real cursor,
# real disk state — but dry_run=True so no real trade notifications fire.
# Change Render start command to: python -m execution.test_live
# Change back to your normal start command when done.

import os
from datetime import datetime, timezone
from execution.hourly_runner import run_hourly_for_symbol
from execution.notifier import TelegramNotifier

TEST_SYMBOLS = ["LDOUSDT"]  # add more if needed

notifier = TelegramNotifier()

if __name__ == "__main__":
    notifier.send_debug("TEST-LIVE-START", (
        f"Full live path — dry\\_run mode\n"
        f"Symbols: `{TEST_SYMBOLS}`\n"
        f"Time: `{datetime.now(timezone.utc).isoformat()}`"
    ))

    for symbol in TEST_SYMBOLS:
        run_hourly_for_symbol(symbol, dry_run=True)

    notifier.send_debug("TEST-LIVE-DONE", "Complete")