import pandas as pd
import numpy as np

# ==========================================================
# Public entrypoint
# ==========================================================
def validate_ohlcv(
    df: pd.DataFrame,
    symbol: str,
    freq: str = "1h",
    max_return_sigma: float = 8.0,
    max_volume_spike: float = 10.0,
    allow_zero_volume: bool = True,
):
    """
    Validate OHLCV dataset for structure, index, price, volume, and returns.
    Raises RuntimeError on critical issues. Warnings are silenced.
    """
    _check_structure(df, symbol)
    _check_index(df, symbol, freq)
    _check_price_sanity(df, symbol)
    _check_volume(df, symbol, max_volume_spike, allow_zero_volume)
    _check_returns(df, symbol, max_return_sigma)


# ==========================================================
# Validators
# ==========================================================
def _check_structure(df: pd.DataFrame, symbol: str):
    required_cols = ["open", "high", "low", "close", "volume"]

    if not isinstance(df, pd.DataFrame):
        raise RuntimeError(f"[{symbol}] Data is not a DataFrame")

    missing = [col for col in required_cols if col not in df.columns]
    if missing:
        raise RuntimeError(f"[{symbol}] Missing columns: {missing}")

    if df.empty:
        raise RuntimeError(f"[{symbol}] DataFrame is empty")

    nan_counts = df[required_cols].isna().sum()
    if nan_counts.sum() > 0:
        raise RuntimeError(f"[{symbol}] NaNs detected:\n{nan_counts}")

    for col in required_cols:
        if not np.issubdtype(df[col].dtype, np.number):
            raise RuntimeError(f"[{symbol}] Column '{col}' is not numeric")


def _check_index(df: pd.DataFrame, symbol: str, freq: str):
    if not isinstance(df.index, pd.DatetimeIndex):
        raise RuntimeError(f"[{symbol}] Index is not a DatetimeIndex")

    if df.index.tz is None:
        raise RuntimeError(f"[{symbol}] Index is not timezone-aware (UTC required)")

    if not df.index.is_monotonic_increasing:
        raise RuntimeError(f"[{symbol}] Index is not sorted")

    if df.index.duplicated().any():
        raise RuntimeError(f"[{symbol}] Duplicate timestamps detected")

    # Exact spacing check
    expected = pd.date_range(
        start=df.index[0],
        periods=len(df),
        freq=freq,
        tz="UTC",
    )
    if not df.index.equals(expected):
        diff = df.index.symmetric_difference(expected)
        raise RuntimeError(
            f"[{symbol}] Time gap / misalignment detected. "
            f"Sample timestamps: {diff[:5].tolist()}"
        )


def _check_price_sanity(df: pd.DataFrame, symbol: str):
    if (df[["open", "high", "low", "close"]] <= 0).any().any():
        raise RuntimeError(f"[{symbol}] Non-positive price detected")

    invalid_high = df["high"] < df[["open", "close"]].max(axis=1)
    invalid_low = df["low"] > df[["open", "close"]].min(axis=1)

    if invalid_high.any():
        ts = df.index[invalid_high][0]
        raise RuntimeError(f"[{symbol}] high < open/close at {ts}")

    if invalid_low.any():
        ts = df.index[invalid_low][0]
        raise RuntimeError(f"[{symbol}] low > open/close at {ts}")

    if (df["high"] < df["low"]).any():
        ts = df.index[df["high"] < df["low"]][0]
        raise RuntimeError(f"[{symbol}] high < low at {ts}")


def _check_volume(df: pd.DataFrame, symbol: str, max_volume_spike: float, allow_zero_volume: bool):
    if (df["volume"] < 0).any():
        raise RuntimeError(f"[{symbol}] Negative volume detected")

    if not allow_zero_volume and (df["volume"] == 0).any():
        ts = df.index[df["volume"] == 0][0]
        raise RuntimeError(f"[{symbol}] Zero volume at {ts}")

    # Volume spikes are now ignored silently


def _check_returns(df: pd.DataFrame, symbol: str, max_return_sigma: float):
    close = df["close"]
    returns = np.log(close / close.shift(1)).dropna()

    if returns.empty:
        return

    mu = returns.mean()
    sigma = returns.std()

    if sigma == 0 or np.isnan(sigma):
        return

    # Extreme returns are now ignored silently
