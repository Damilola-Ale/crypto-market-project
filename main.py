# import pandas as pd
# import yfinance as yf
# import matplotlib.pyplot as plt
# from indicators.indicators import SMA, EMA, ATR, RSI, generate_signal, atr_expansion, donchian_channel_width, ema_ribbon, roc_momentum, early_entry
# from backtest import Backtester 
# from trade_diagnostics import diagnose_trades

# # ------------------------------
# # Fetch Data
# # ------------------------------
# coins = [
#     "BTC-USD",   # Bitcoin*
#     "ETH-USD",   # Ethereum
#     "XRP-USD",   # Ripple
#     "ADA-USD",   # Cardano
#     "BNB-USD",   # Binance Coin*
#     "LINK-USD",  # Chainlink
#     "SOL-USD",   # Solana*
#     "AVAX-USD",  # Avalanche
#     "ETC-USD",   # Ethereum Classic
#     "VET-USD"    # VeChain*
# ]
# df = yf.download("LINK-USD", interval="1h", period="21d")
# df = df.reset_index(drop=True)
# df = df[['Open', 'High', 'Low', 'Close', 'Volume']]
# df.columns = ['open', 'high', 'low', 'close', 'volume']

# # ------------------------------
# # Compute Indicators
# # ------------------------------
# df['SMA'] = SMA(df, period=20)
# df['EMA'] = EMA(df, period=20)
# df['RSI'] = RSI(df, period=14)
# df['ATR'] = ATR(df, period=14)

# df = early_entry(df)
# df = ema_ribbon(df)
# df = roc_momentum(df)
# df = donchian_channel_width(df)
# df = atr_expansion(df)

# # ------------------------------
# # Generate Signals with Confidence
# # ------------------------------
# df = generate_signal(df)

# # ------------------------------
# # Diagnostics
# # ------------------------------
# diagnose_trades(df)

# # ------------------------------
# # Count Signals
# # ------------------------------
# num_long = len(df[df['final_signal'] == 1])
# num_short = len(df[df['final_signal'] == -1])
# print(f"Long signals: {num_long}, short signals: {num_short}")

# # ------------------------------
# # Backtest
# # ------------------------------
# backtester = Backtester(df)
# results = backtester.run()
# print(results)
# # ------------------------------
# # Visualisation
# # ------------------------------
# plt.plot(df['close'], label='Close')
# plt.plot(df['SMA'], label='SMA')
# plt.plot(df['EMA'], label='EMA')

# plt.scatter(df.index[df['final_signal'] == 1], df['close'][df['final_signal'] == 1],
#             marker='^', color='green', label='Long')
# plt.scatter(df.index[df['final_signal'] == -1], df['close'][df['final_signal'] == -1],
#             marker='v', color='red', label='Short')

# plt.legend()
# plt.show()
# main.py
from data_pipeline.updater import update_symbol
from _execution_engine import run

if __name__ == "__main__":
    run()