import pandas as pd


def build_htf(df_ltf: pd.DataFrame, htf: str = "4h") -> pd.DataFrame:
    """
    Convert LTF candles into HTF candles.
    Guarantees perfect alignment.
    """

    print("[HTF] Building higher timeframe:", htf)

    agg = {
        "open": "first",
        "high": "max",
        "low": "min",
        "close": "last",
        "volume": "sum"
    }

    df_htf = df_ltf.resample(htf).agg(agg)

    df_htf = df_htf.dropna()

    print(
        "[HTF] result:",
        len(df_htf),
        "candles",
        df_htf.index[0],
        "→",
        df_htf.index[-1]
    )

    return df_htf