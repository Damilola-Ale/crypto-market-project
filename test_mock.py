import pandas as pd
# live = pd.read_parquet("data/cache/ICXUSDT_1h.parquet")
bt   = pd.read_parquet("data/backtest_cache/ICXUSDT_1h.parquet")
# live.index = pd.to_datetime(live.index, utc=True)
bt.index   = pd.to_datetime(bt.index, utc=True)
cols = ['open','high','low','close','volume']
# print(live.loc["2026-06-12 18:00":"2026-06-13 02:00", cols])
print(bt.loc["2026-06-12 18:00":"2026-06-13 02:00", cols])