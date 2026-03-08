import pandas as pd
import yfinance as yf
import matplotlib.pyplot as plt

from indicators.indicators import generate_signal
from backtest import SignalBacktester
from trade_diagnostics import diagnose_trades
from diagnostics import plot_asymmetry

# ==========================================================
# HELPERS
# ==========================================================
def normalize_yf_columns(df: pd.DataFrame) -> pd.DataFrame:
    if isinstance(df.columns, pd.MultiIndex):
        df.columns = [c[0].lower() for c in df.columns]
    else:
        df.columns = [c.lower() for c in df.columns]
    return df
# BTC (30, 63%), ADA (27, 59%), DOGE (5, 60%), BCH (37, 54%), LINK (32, 59%) XMR, LDO - 52%, ORDI - 49%
# ==========================================================ETC - 42%, 68.85 - 49%, 106.83
# CONFIG BTC - 53%, LINK - 56%, AVAX - 50%, SOL - 52%, PAXG - 50%, ZEN - 51%, ENS - 53%, AXS - 57%, BAND -58%
# ==========================================================ETC - 41%, 53.48 - 47%, 68.43
SYMBOL = "BNB-USD"
LTF_INTERVAL = "1h"
HTF_INTERVAL = "4h"
# LTF_PERIOD = "365d"
# HTF_PERIOD = "365d"
LTF_PERIOD = "730d"
HTF_PERIOD = "730d"

# ==========================================================
# FETCH DATA
# ==========================================================
print("Downloading LTF data...")
df = yf.download(SYMBOL, interval=LTF_INTERVAL, period=LTF_PERIOD)
df = normalize_yf_columns(df)
df = df[['open', 'high', 'low', 'close', 'volume']]
df.index = pd.to_datetime(df.index)
df['timestamp'] = df.index

print("Downloading HTF data...")
htf_df = yf.download(SYMBOL, interval=HTF_INTERVAL, period=HTF_PERIOD)
htf_df = normalize_yf_columns(htf_df)
htf_df = htf_df[['open', 'high', 'low', 'close', 'volume']]
htf_df.index = pd.to_datetime(htf_df.index)
htf_df['timestamp'] = htf_df.index

# ==========================================================
# SIGNAL GENERATION (NO MONEY HERE)
# ==========================================================
df = generate_signal(df, htf_df)

print(
    f"Long signals: {(df['final_signal'] == 1).sum()}, "
    f"Short signals: {(df['final_signal'] == -1).sum()}"
)

# ==========================================================
# BACKTEST (THE ONLY PLACE MONEY EXISTS)
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

# ==========================================================
# DIAGNOSTICS (POST-MORTEM ONLY)
# ==========================================================
diagnostics_df = diagnose_trades(trade_log)

# print("\n=== SIGNAL vs EXECUTION DIAGNOSTICS ===")
# print(
#     diagnostics_df
#         .groupby(["direction", "signal_quality"])
#         .mean(numeric_only=True)
# )

# ==========================================================
# VISUALIZATION / RESEARCH
# ==========================================================
plot_asymmetry(df)

plt.figure(figsize=(14, 6))
plt.plot(df['close'], label='Close', linewidth=1)

plt.scatter(
    df.index[df['final_signal'] == 1],
    df['close'][df['final_signal'] == 1],
    marker='^',
    label='Long',
    zorder=3
)

plt.scatter(
    df.index[df['final_signal'] == -1],
    df['close'][df['final_signal'] == -1],
    marker='v',
    label='Short',
    zorder=3
)

plt.title(f"{SYMBOL} – Strategy Signals")
plt.legend()
plt.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# main.py
# from data_pipeline.updater import update_symbol
# from _execution_engine import run

# if __name__ == "__main__":
#     run()

# import pandas as pd
# import numpy as np

# # ==========================================================
# # Core Indicators
# # ==========================================================
# def SMA(df, period=20):
#     val = df['close'].rolling(period, min_periods=1).mean()
#     return val.bfill()

# def EMA(df, period=20):
#     val = df['close'].ewm(span=period, adjust=False).mean()
#     return val.bfill()

# def ATR(df, period=14):
#     high_low = df['high'] - df['low']
#     high_close = (df['high'] - df['close'].shift()).abs()
#     low_close = (df['low'] - df['close'].shift()).abs()
#     tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
#     atr = tr.rolling(period, min_periods=1).mean()
#     return atr.bfill()

# # ==========================================================
# # Multi-Timeframe Trend
# # ==========================================================

# # ==========================================================
# # Volatility & Structure
# # ==========================================================
# def atr_expansion(df, period=14):
#     atr = ATR(df, period)
#     atr_ma = atr.rolling(period, min_periods=1).mean()
#     df['ATR_Expansion'] = (atr / (atr_ma + 1e-8)).fillna(1)
#     return df

# def donchian_channel_width(df, period=20):
#     dc_high = df['high'].rolling(period, min_periods=1).max()
#     dc_low = df['low'].rolling(period, min_periods=1).min()
#     df['DC_High'] = dc_high.bfill()
#     df['DC_Low'] = dc_low.bfill()
#     df['DCW'] = ((dc_high - dc_low) / (df['close'] + 1e-8)).fillna(0)
#     df['DCW_Slope'] = df['DCW'].diff().fillna(0)
#     df['DC_Pos'] = ((df['close'] - dc_low) / (dc_high - dc_low + 1e-8)).fillna(0.5)
#     return df

# # ==========================================================
# # EMA Ribbon + Trend Ignition
# # ==========================================================
# def ema_ribbon(df, periods=(8,13,21)):
#     for p in periods:
#         df[f'EMA_{p}'] = EMA(df, p)
#     slopes = [df[f'EMA_{p}'].diff().fillna(0) for p in periods]
#     score = pd.Series(0, index=df.index)
#     for s in slopes:
#         score += s.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
#     df['EMA_Ribbon_Score'] = score.fillna(0)
#     ema_fast = df[f'EMA_{periods[0]}']
#     ema_slow = df[f'EMA_{periods[-1]}']
#     df['EMA_Spread'] = (ema_fast - ema_slow).abs().fillna(0)
#     df['EMA_Expansion'] = (df['EMA_Spread'].diff().fillna(0) > 0)
#     return df

# # ==========================================================
# # Momentum
# # ==========================================================
# def roc_momentum(df, period=3):
#     df['ROC'] = df['close'].pct_change(periods=period).fillna(0)
#     atr_val = ATR(df, 14)
#     df['ROC_norm'] = (df['ROC'] / ((atr_val / df['close']) + 1e-8)).fillna(0)
#     return df

# # ==========================================================
# # Directional Volatility
# # ==========================================================

# # ==========================================================
# # Early Entry Logic
# # ==========================================================
# def early_entry(df, range_period=20, atr_sensitivity=1.0):
#     ema_ribbon(df)
#     roc_momentum(df)

#     dc_high = df['high'].rolling(range_period, min_periods=1).max()
#     dc_low = df['low'].rolling(range_period, min_periods=1).min()
#     dc_range = dc_high - dc_low
#     atr_val = ATR(df, period=14)

#     sideways_filter = (dc_range < atr_val * atr_sensitivity)

#     df['EARLY_ENTRY_LONG'] = ((df['EMA_Ribbon_Score'] > 0) &
#                               (df['EMA_Ribbon_Score'].shift().fillna(0) <= 0) &
#                               (df['ROC_norm'] > 0) &
#                               (~sideways_filter)).fillna(False)

#     df['EARLY_ENTRY_SHORT'] = ((df['EMA_Ribbon_Score'] < 0) &
#                                (df['EMA_Ribbon_Score'].shift().fillna(0) >= 0) &
#                                (df['ROC_norm'] < 0) &
#                                (~sideways_filter)).fillna(False)

#     return df

# # ==========================================================
# # ATR Squeeze Filter
# # ==========================================================

# # ==========================================================
# # Participation / Volume Efficiency
# # ==========================================================
# def dynamic_volume_threshold(df, lookback=20, mult=1.0):
#     atr_val = ATR(df, period=lookback)
#     vol_ma = df['volume'].rolling(lookback, min_periods=1).mean().fillna(1)
#     threshold = vol_ma * (1 + mult * (atr_val / atr_val.rolling(lookback, min_periods=1).mean() - 1))
#     threshold = threshold.clip(lower=0)
#     return threshold

# # ==========================================================
# # Post-entry Commitment
# # ==========================================================
# def volume_intent(df, lookback=10, smooth=3):
#     up_vol = df['volume'].where(df['close'] >= df['open'], 0.0)
#     down_vol = df['volume'].where(df['close'] < df['open'], 0.0)

#     vol_up = up_vol.rolling(lookback, min_periods=1).sum()
#     vol_down = down_vol.rolling(lookback, min_periods=1).sum()

#     intent_raw = (vol_up - vol_down) / (vol_up + vol_down + 1e-9)
#     intent_smooth = intent_raw.rolling(smooth, min_periods=1).mean()
#     intent_strength = intent_smooth.abs().clip(0, 1)

#     align = pd.Series(1.0, index=df.index)
#     align[df['signal'] == 1] = ((intent_smooth + 1) / 2).clip(0, 1)
#     align[df['signal'] == -1] = ((-intent_smooth + 1) / 2).clip(0, 1)
#     align[df['signal'] == 0] = 1.0

#     df['vol_intent_raw'] = intent_smooth.fillna(0)
#     df['vol_intent_strength'] = intent_strength.fillna(0)
#     df['vol_intent_align'] = align.fillna(1.0)

# def mark_entry_reference(df):
#     if 'signal' not in df.columns:
#         df['signal'] = 0
#     entries = df['signal'] != 0
#     if 'entry_price' not in df.columns:
#         df['entry_price'] = np.nan
#     if 'entry_index' not in df.columns:
#         df['entry_index'] = np.nan
#     df.loc[entries, 'entry_price'] = df.loc[entries, 'close']
#     df.loc[entries, 'entry_index'] = np.arange(len(df))[entries]
#     return df

# def evaluate_commitment(df, lookahead=3):
#     df['commitment_ok'] = False
#     if lookahead <= 0:
#         return df
#     for i in range(len(df) - lookahead):
#         sig = df['signal'].iloc[i]
#         if sig == 0:
#             continue
#         high_ref = df['high'].iloc[i]
#         low_ref = df['low'].iloc[i]
#         future = df['close'].iloc[i+1:i+1+lookahead]
#         if sig == 1 and (future > high_ref).any():
#             df.at[df.index[i], 'commitment_ok'] = True
#         elif sig == -1 and (future < low_ref).any():
#             df.at[df.index[i], 'commitment_ok'] = True
#     return df

# # ==========================================================
# # Market Regime with Volatility / Shock Filter
# # ==========================================================
# def market_regime(df, max_atr_spike=3.0):
#     df['ATR_Regime'] = df['ATR_Expansion'].rolling(5, min_periods=1).mean().fillna(1)
#     reg_max = df['ATR_Regime'].rolling(50, min_periods=1).max().fillna(1)
#     df['Regime_norm'] = (df['ATR_Regime'] / reg_max).clip(0, 1)
#     df['ATR_Shock'] = df['ATR_Expansion'] > (df['ATR_Expansion'].rolling(20, min_periods=1).mean() * max_atr_spike)
#     return df

# # ==========================================================
# # Liquidity Filter
# # ==========================================================
# def liquidity_filter(df, min_volume=1000):
#     df['Liquidity_OK'] = df['volume'] >= min_volume
#     return df

# def adjust_stop(df, base_stop, tighten, lookback=5):
#     adj_stop = base_stop * tighten
#     for i in range(lookback, len(df)):
#         if df['signal'].iloc[i] == 1:
#             recent_low = df['low'].iloc[i-lookback:i].min()
#             current_stop = df['close'].iloc[i] - adj_stop.iloc[i]
#             if recent_low < current_stop:
#                 adj_stop.iloc[i] *= 1 + (current_stop - recent_low) / df['ATR_Expansion'].iloc[i]
#         elif df['signal'].iloc[i] == -1:
#             recent_high = df['high'].iloc[i-lookback:i].max()
#             current_stop = df['close'].iloc[i] + adj_stop.iloc[i]
#             if recent_high > current_stop:
#                 adj_stop.iloc[i] *= 1 + (recent_high - current_stop) / df['ATR_Expansion'].iloc[i]
#     return adj_stop

# def rolling_trade_quality_threshold(df, window=50, quantile=0.1):
#     return df['trade_quality'].rolling(window, min_periods=1).quantile(quantile)

# def ADX(df, period=14):
#     high = df['high']
#     low = df['low']
#     close = df['close']
#     up_move = high.diff()
#     down_move = low.diff() * -1
#     plus_dm = np.where((up_move > down_move) & (up_move > 0), up_move, 0.0)
#     minus_dm = np.where((down_move > up_move) & (down_move > 0), down_move, 0.0)
#     tr1 = high - low
#     tr2 = (high - close.shift()).abs()
#     tr3 = (low - close.shift()).abs()
#     tr = np.maximum.reduce([tr1, tr2, tr3])
#     atr = pd.Series(tr).ewm(alpha=1/period, adjust=False).mean()
#     plus_di = 100 * pd.Series(plus_dm).ewm(alpha=1/period, adjust=False).mean() / atr
#     minus_di = 100 * pd.Series(minus_dm).ewm(alpha=1/period, adjust=False).mean() / atr
#     dx = (abs(plus_di - minus_di) / (plus_di + minus_di)) * 100
#     adx = dx.ewm(alpha=1/period, adjust=False).mean()
#     return adx.fillna(0)

# def htf_volatility_state(df, htf_period=48, smooth=20):
#     htf_atr = ATR(df, period=htf_period)
#     htf_atr_ma = htf_atr.rolling(smooth, min_periods=1).mean()
#     df['HTF_ATR_EXP'] = (htf_atr / (htf_atr_ma + 1e-8)).fillna(1.0)
#     df['HTF_EXPANDING'] = df['HTF_ATR_EXP'] >= 1.0
#     return df

# # ======================================================
# # VOLUME MOMENTUM
# # ======================================================
# def volume_momentum(df, lookback=5, norm_window=100):
#     vol_diff = df['volume'].diff().fillna(0)
#     vol_mom = vol_diff.rolling(lookback, min_periods=1).mean()
#     rolling_min = vol_mom.rolling(norm_window, min_periods=10).min()
#     rolling_max = vol_mom.rolling(norm_window, min_periods=10).max()
#     df['VOL_MOM'] = ((vol_mom - rolling_min) /
#                      (rolling_max - rolling_min + 1e-9)).fillna(0)
#     return df

# def directional_volume(df):
#     body = df['close'] - df['open']
#     range_ = (df['high'] - df['low']).replace(0, np.nan)
#     body_strength = (body.abs() / range_).clip(0, 1)
#     df['bull_vol'] = np.where(body > 0, df['volume'] * body_strength, 0.0)
#     df['bear_vol'] = np.where(body < 0, df['volume'] * body_strength, 0.0)
#     return df

# def volume_candle_strength(df, lookback=5):
#     bull = df['bull_vol'].rolling(lookback, min_periods=1).sum()
#     bear = df['bear_vol'].rolling(lookback, min_periods=1).sum()
#     total = (bull + bear).replace(0, 1)
#     df['DVS'] = (bull - bear) / total
#     return df

# # ======================================================
# # GENERATE SIGNAL (Cluster #1 redundancy fix only)
# # ======================================================
# def generate_signal(df,
#                     atr_exp_start=1.02,
#                     vol_spike_mult=1.5,
#                     atr_stop_mult=1.5,
#                     adx_period=14,
#                     adx_min=18,
#                     commitment_lookahead=0,
#                     min_volume=1000,
#                     htf_period=48):

#     if df.empty:
#         return df

#     # Core setup
#     ema_ribbon(df)
#     atr_expansion(df)
#     donchian_channel_width(df)
#     early_entry(df)
#     market_regime(df)
#     liquidity_filter(df, min_volume=min_volume)
#     htf_volatility_state(df, htf_period=htf_period)

#     # Volume
#     volume_momentum(df)
#     directional_volume(df)
#     volume_candle_strength(df)

#     # Trend strength
#     df['ADX'] = ADX(df, period=adx_period)
#     atr_vals = ATR(df)
#     vol_ma = df['volume'].rolling(20, min_periods=1).mean()

#     # Hard pause
#     entry_paused = (~df['Liquidity_OK']) | (df['ATR_Shock'])

#     # Absolute floors
#     atr_ok = df['ATR_Expansion'] >= atr_exp_start
#     vol_ok = df['volume'] >= vol_ma * vol_spike_mult
#     dcw_ok = df['DCW'] >= 0.004

#     # Compression → release
#     compression_lb = 30
#     compression_q = 0.2
#     dcw_floor = df['DCW'].rolling(200, min_periods=50).quantile(compression_q)
#     was_compressed = df['DCW'].rolling(compression_lb).min() <= dcw_floor
#     dcw_release = df['DCW'] > df['DCW'].rolling(compression_lb).mean() * 1.3
#     compression_ok = was_compressed & dcw_release

#     # Soft confirmations
#     adx_ok = (df['ADX'] >= adx_min) | (df['ADX'] > df['ADX'].shift(1))
#     vol_mom_ok = df['VOL_MOM'] > 0.5
#     dvs_long_ok = df['DVS'] > 0.15
#     dvs_short_ok = df['DVS'] < -0.15
#     htf_ok = df['HTF_EXPANDING']

#     # ===============================
#     # Cluster #1 Redundancy fix:
#     # unify EMA_Ribbon + EMA_Expansion + ROC_norm + ADX trend checks
#     # ===============================
#     trend_ok_long = (df['EMA_Ribbon_Score'] > 0) & (df['ROC_norm'] > 0) & adx_ok
#     trend_ok_short = (df['EMA_Ribbon_Score'] < 0) & (df['ROC_norm'] < 0) & adx_ok

#     # Final entry logic
#     long_condition = (
#         df['EARLY_ENTRY_LONG'] &
#         trend_ok_long &
#         compression_ok &
#         vol_mom_ok &
#         dvs_long_ok &
#         htf_ok &
#         atr_ok & vol_ok & dcw_ok &
#         (~entry_paused)
#     )

#     short_condition = (
#         df['EARLY_ENTRY_SHORT'] &
#         trend_ok_short &
#         compression_ok &
#         vol_mom_ok &
#         dvs_short_ok &
#         htf_ok &
#         atr_ok & vol_ok & dcw_ok &
#         (~entry_paused)
#     )

#     df['signal'] = 0
#     df.loc[long_condition, 'signal'] = 1
#     df.loc[short_condition, 'signal'] = -1

#     # Post-entry analysis
#     mark_entry_reference(df)
#     evaluate_commitment(df, commitment_lookahead)
#     volume_intent(df)

#     # Stop-loss
#     base_stop = atr_stop_mult * atr_vals * df['ATR_Expansion'].clip(lower=1)
#     tighten = np.ones(len(df))
#     tighten *= np.where(df['HTF_EXPANDING'], 1.0, 0.85)
#     tighten *= 1 / (1 + 0.15 * df['EMA_Expansion'].astype(int))
#     tighten *= np.where(df['vol_intent_align'] < 0.5, 0.85, 1.0)
#     adj_stop = adjust_stop(df, base_stop, tighten, lookback=5)
#     df['stop_loss'] = np.nan
#     df.loc[df['signal'] == 1, 'stop_loss'] = df['close'] - adj_stop
#     df.loc[df['signal'] == -1, 'stop_loss'] = df['close'] + adj_stop

#     df['final_signal'] = df['signal']
#     return df
