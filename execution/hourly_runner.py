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
    # "ETHUSDT", "FILUSDT", "TRXUSDT", "VETUSDT", "UNIUSDT", "DOGEUSDT", "EOSUSDT", "ETCUSDT",
    # "AAVEUSDT", "BCHUSDT", "HNTUSDT", "BANDUSDT", "TIAUSDT", "XLMUSDT", "SUIUSDT", "BTCUSDT",
    # "ZENUSDT", "AVAXUSDT", "MKRUSDT", "AXSUSDT", "ORDIUSDT", "LDOUSDT", "LINKUSDT"
    "LDOUSDT"
]

SIGNAL_STORE       = "data/signals.json"
HOUR_MEMORY_FILE = "data/last_hour_seen.json"

# Interval constants
LLTF_INTERVAL = "5m"
LTF_INTERVAL  = "1h"
HTF_INTERVAL  = "4h"

LAST_5M_FILE = "data/last_5m_seen.json"

def run_hourly():
    print("\n==============================")
    print("CRYPTO MARKET PROJECT EXECUTION")
    print("==============================\n")

    os.makedirs("data", exist_ok=True)

    with open("data/last_run.json", "w") as f:
        json.dump(
            {
                "ran_at": datetime.now(timezone.utc).isoformat(),
                "symbols": SYMBOLS
            },
            f,
            indent=2
        )

    for symbol in SYMBOLS:
        run_hourly_for_symbol(symbol)

    print("\n=== EXECUTION COMPLETE ===\n")


# ==========================================================
# SINGLE SYMBOL ENGINE (UNIFIED LIVE + REPLAY)
# ==========================================================
def run_hourly_for_symbol(symbol: str, forced_time=None, replay=False, notify_override=None):
    is_live = not replay and forced_time is None
    notify = notify_override if notify_override is not None else is_live
    pm = PositionManager(persist=is_live, notify=notify)
    notifier = TelegramNotifier()

    # =========================
    # 5M STREAM MEMORY (CRITICAL FIX)
    # =========================
    if os.path.exists(LAST_5M_FILE):
        with open(LAST_5M_FILE, "r") as f:
            last_5m_seen = json.load(f)
    else:
        last_5m_seen = {}

    try:
        # -------------------
        # FETCH DATA
        # -------------------
        if forced_time is None and not replay:
            df, htf_df, lltf_df = update_symbol(symbol)
            df, htf_df, lltf_df = df.iloc[:-1], htf_df.iloc[:-1], lltf_df.iloc[:-1]

        else:
            df = pd.read_parquet(f"data/cache/{symbol}_1h.parquet")
            htf_df = pd.read_parquet(f"data/cache/{symbol}_4h.parquet")
            lltf_df = pd.read_parquet(f"data/cache/{symbol}_5m.parquet")

            if forced_time:
                df     = df[df.index < forced_time].copy()
                htf_df = htf_df[htf_df.index < forced_time].copy()
                lltf_df = lltf_df[lltf_df.index < forced_time].copy()

        # -------------------
        # GENERATE & MAP SIGNALS (The Unified Way)
        # -------------------
        df = generate_signal(df.copy(), htf_df.copy())

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
        last_seen = pd.Timestamp(last_5m_seen.get(symbol)) if last_5m_seen.get(symbol) else None

        if last_seen == latest_ts:
            return None

        # First live run: seed cursor and exit — never replay full history into live orders
        if last_seen is None and not replay and not forced_time:
            last_5m_seen[symbol] = latest_ts.isoformat()
            with open(LAST_5M_FILE + ".tmp", "w") as f:
                json.dump(last_5m_seen, f, indent=2)
            os.replace(LAST_5M_FILE + ".tmp", LAST_5M_FILE)
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

        for _, row_5m in new_bars.iterrows():

            if pd.isna(row_5m["final_signal"]):
                bar_signal = 0
            else:
                bar_signal = int(row_5m["final_signal"])

            ltf_row = df.iloc[int(row_5m["ltf_index"])]

            result = pm.update(
                df=df,
                symbol=symbol,
                lltf_df=lltf_frozen,
                external_signal=bar_signal,
                external_row=ltf_row,
                current_5m_row=row_5m
            )

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

        # update cursor AFTER processing (live only)
        if not replay and not forced_time:
            last_5m_seen[symbol] = new_bars.index[-1]
            with open(LAST_5M_FILE + ".tmp", "w") as f:
                json.dump(last_5m_seen, f, indent=2)
            os.replace(LAST_5M_FILE + ".tmp", LAST_5M_FILE)

        # ==========================================================
        # SAVE LAST PROCESSED HOUR
        # ==========================================================
        if not replay and not forced_time and new_hour:
            last_hour_seen[symbol] = latest_hour_ts
            with open(HOUR_MEMORY_FILE + ".tmp", "w") as f:
                json.dump(last_hour_seen, f, indent=2)
            os.replace(HOUR_MEMORY_FILE + ".tmp", HOUR_MEMORY_FILE)
        
        pm.flush()

        return None

    except Exception as e:
        import traceback
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