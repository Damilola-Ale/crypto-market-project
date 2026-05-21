import pandas as pd

htf = pd.read_parquet("data/backtest_cache/AXSUSDT_4h.parquet")
htf.index = pd.to_datetime(htf.index, utc=True)
print(htf.tail(5)[['open', 'high', 'low', 'close', 'volume']])