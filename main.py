import pandas as pd
import requests
import matplotlib.pyplot as plt
import time

from indicators.indicators import generate_signal
from backtest import SignalBacktester
from trade_diagnostics import diagnose_trades
from diagnostics import plot_asymmetry


# ==========================================================
# BINANCE DATA FETCHER
# ==========================================================

BINANCE_URL = "https://api.binance.com/api/v3/klines"

def fetch_binance(symbol, interval, limit):

    all_data = []
    end_time = None

    while len(all_data) < limit:

        params = {
            "symbol": symbol,
            "interval": interval,
            "limit": min(1000, limit - len(all_data))
        }

        if end_time:
            params["endTime"] = end_time

        response = requests.get(BINANCE_URL, params=params)
        data = response.json()

        if not data:
            break

        all_data = data + all_data

        first_open_time = data[0][0]
        end_time = first_open_time - 1
        time.sleep(0.25)

        if len(data) < 1000:
            break

    df = pd.DataFrame(all_data, columns=[
        "open_time",
        "open",
        "high",
        "low",
        "close",
        "volume",
        "close_time",
        "quote_asset_volume",
        "num_trades",
        "taker_buy_base",
        "taker_buy_quote",
        "ignore"
    ])

    df = df[["open_time", "open", "high", "low", "close", "volume", "taker_buy_base"]]

    df["timestamp"] = pd.to_datetime(df["open_time"], unit="ms")
    df = df.drop(columns=["open_time"])
    df = df.set_index("timestamp")
    df = df.astype(float)

    return df


# ==========================================================
# CONFIG LINK, AAVE, BTC, TRX, FXS, FIL, RUNE, ARB, OP, UNI, XTZ, ADA, SUI, CRV, MKR, XLM, BIO, JTO | DOGE, NEAR, SNX, LDO, MATIC, YFI
# ==========================================================

SYMBOL = "ARBUSDT"

LTF_INTERVAL = "1h"
HTF_INTERVAL = "4h"

# LTF_LIMIT = 43800   # ~30 days of 1h candles
# HTF_LIMIT = 10950   # ~120 days of 4h candles

# LTF_LIMIT = 35040   # ~30 days of 1h candles
# HTF_LIMIT = 8760   # ~120 days of 4h candles

LTF_LIMIT = 26280   # ~30 days of 1h candles
HTF_LIMIT = 6570   # ~120 days of 4h candles

# LTF_LIMIT = 17520   # ~30 days of 1h candles
# HTF_LIMIT = 4380   # ~120 days of 4h candles

# LTF_LIMIT = 8760   # ~30 days of 1h candles
# HTF_LIMIT = 2190   # ~120 days of 4h candles

# LTF_LIMIT = 4380   # ~30 days of 1h candles
# HTF_LIMIT = 1095   # ~120 days of 4h candles


# ==========================================================
# FETCH DATA
# ==========================================================

print("Downloading LTF data from Binance...")
df = fetch_binance(SYMBOL, LTF_INTERVAL, LTF_LIMIT)

print("Downloading HTF data from Binance...")
htf_df = fetch_binance(SYMBOL, HTF_INTERVAL, HTF_LIMIT)


# ==========================================================
# SIGNAL GENERATION
# ==========================================================

df = generate_signal(df, htf_df)

print(
    f"Long signals: {(df['final_signal'] == 1).sum()}, "
    f"Short signals: {(df['final_signal'] == -1).sum()}"
)


# ==========================================================
# BACKTEST
# ==========================================================

backtester = SignalBacktester(df)

backtest_output = backtester.run()

trade_log = backtest_output["trades"]
equity_curve = backtest_output["equity_curve"]
results = backtest_output["summary"]

print(results)

print("=== TRADE LOG ===")
print(trade_log.head(10))

print("\nColumns:", trade_log.columns)
print("\nNumber of trades:", len(trade_log))
print("LTF candles:", len(df))
print("HTF candles:", len(htf_df))


# ==========================================================
# DIAGNOSTICS
# ==========================================================

diagnostics_df = diagnose_trades(trade_log)


# ==========================================================
# VISUALIZATION
# ==========================================================

# plot_asymmetry(df)

# plt.figure(figsize=(14, 6))
# plt.plot(df['close'], label='Close', linewidth=1)

# plt.scatter(
#     df.index[df['final_signal'] == 1],
#     df['close'][df['final_signal'] == 1],
#     marker='^',
#     label='Long',
#     zorder=3
# )

# plt.scatter(
#     df.index[df['final_signal'] == -1],
#     df['close'][df['final_signal'] == -1],
#     marker='v',
#     label='Short',
#     zorder=3
# )

# plt.title(f"{SYMBOL} – Strategy Signals")
# plt.legend()
# plt.grid(alpha=0.3)
# plt.tight_layout()
# plt.show()