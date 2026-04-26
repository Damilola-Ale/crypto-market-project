import pandas as pd
import numpy as np
from datetime import datetime, timezone

# -------------------------------------------------
# Mock Data Fetcher
# -------------------------------------------------
def mock_update_symbol(symbol: str) -> pd.DataFrame:
    now = datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0)
    periods = 50
    index = pd.date_range(end=now, periods=periods, freq="1h", tz="UTC")

    open_ = np.random.rand(periods) * 100 + 100
    high = open_ + np.random.rand(periods) * 5
    low = open_ - np.random.rand(periods) * 5
    close = low + np.random.rand(periods) * (high - low)
    volume = np.random.rand(periods) * 50 + 10

    df = pd.DataFrame(
        {
            "open": open_,
            "high": high,
            "low": low,
            "close": close,
            "volume": volume,
        },
        index=index,
    )

    # -------------------------------------------------
    # Minimal columns required for generate_signal
    # -------------------------------------------------
    df["ATR_Expansion"] = 1.2
    df["DCW"] = 0.02
    df["DCW_Slope"] = 0.002
    df["EMA_Ribbon_Score"] = 1
    df["EMA_Expansion"] = True
    df["HTF_Polarized"] = True
    df["EARLY_ENTRY_LONG"] = True
    df["EARLY_ENTRY_SHORT"] = False
    df["ATR_Squeeze_OK"] = True
    df["commitment_ok"] = True
    df["Regime_norm"] = 1.0
    df["DC_Pos"] = 0.4
    df["DIR_VOL_REL_norm"] = 0.7
    df["DIR_VOL_EFF_norm"] = 0.6
    df["VOL_Eff_norm"] = 0.6
    df["HTF_Score"] = 1

    return df


# -------------------------------------------------
# Mock Telegram Notifier
# -------------------------------------------------
class MockTelegramNotifier:
    def send_signal(
        self,
        symbol,
        direction,
        timestamp,
        price=None,
        stop_loss=None,
        trade_quality=None,
    ):
        direction_str = "LONG" if direction == 1 else "SHORT"
        print(
            f"[MOCK NOTIFIER] {symbol} {direction_str} @ {timestamp} | "
            f"Price: {price}, Stop: {stop_loss}, Quality: {trade_quality}"
        )


# -------------------------------------------------
# Test Runner
# -------------------------------------------------
def test_execution_engine():
    import execution_engine
    from indicators.indicators import generate_signal
    from execution.signal_store import SignalStore

    # Patch dependencies
    execution_engine.update_symbol = mock_update_symbol
    execution_engine.TelegramNotifier = lambda: MockTelegramNotifier()
    execution_engine.SignalStore = SignalStore

    # Patch generate_signal to FORCE a signal on last candle
    original_generate_signal = generate_signal

    def patched_generate_signal(df, *args, **kwargs):
        df = original_generate_signal(df, *args, **kwargs)
        df.loc[df.index[-1], "final_signal"] = 1
        df.loc[df.index[-1], "confidence"] = 85.0
        df.loc[df.index[-1], "trade_quality"] = 0.75
        return df

    execution_engine.generate_signal = patched_generate_signal

    print("=== START MOCK EXECUTION ENGINE TEST ===")
    execution_engine.run()
    print("=== END MOCK EXECUTION ENGINE TEST ===")


# -------------------------------------------------
# Entrypoint
# -------------------------------------------------
if __name__ == "__main__":
    test_execution_engine()
