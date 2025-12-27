import pandas as pd
import numpy as np

# ==========================================================
# Core Indicators
# ==========================================================
def SMA(df, period=20):
    val = df['close'].rolling(period, min_periods=1).mean()
    return val.bfill()

def EMA(df, period=20):
    val = df['close'].ewm(span=period, adjust=False).mean()
    return val.bfill()

def RSI(df, period=14):
    delta = df['close'].diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    avg_gain = gain.rolling(period, min_periods=1).mean()
    avg_loss = loss.rolling(period, min_periods=1).mean()
    rs = avg_gain / (avg_loss + 1e-8)
    rsi = 100 - (100 / (1 + rs))
    return rsi.fillna(50)  # neutral value

def ATR(df, period=14):
    high_low = df['high'] - df['low']
    high_close = (df['high'] - df['close'].shift()).abs()
    low_close = (df['low'] - df['close'].shift()).abs()
    tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
    atr = tr.rolling(period, min_periods=1).mean()
    return atr.bfill()

# ==========================================================
# Multi-Timeframe Trend
# ==========================================================
def multi_tf_trend(df, periods={'4H':12,'Daily':48,'Weekly':336}, polarity_threshold=0.33):
    scores = pd.Series(0.0, index=df.index)
    for p in periods.values():
        htf_close = df['close'].rolling(p, min_periods=1).mean()
        htf_sma = htf_close.rolling(p, min_periods=1).mean()
        trend = pd.Series(0, index=df.index)
        trend[htf_close > htf_sma] = 1
        trend[htf_close < htf_sma] = -1
        scores += trend
    scores /= len(periods)
    df['HTF_Score'] = scores.fillna(0)
    df['HTF_Polarized'] = (scores.abs() >= polarity_threshold).fillna(False)
    return df

# ==========================================================
# Volatility & Structure
# ==========================================================
def atr_expansion(df, period=14):
    atr = ATR(df, period)
    atr_ma = atr.rolling(period, min_periods=1).mean()
    df['ATR_Expansion'] = (atr / (atr_ma + 1e-8)).fillna(1)
    return df

def donchian_channel_width(df, period=20):
    dc_high = df['high'].rolling(period, min_periods=1).max()
    dc_low = df['low'].rolling(period, min_periods=1).min()
    df['DC_High'] = dc_high.bfill()
    df['DC_Low'] = dc_low.bfill()
    df['DCW'] = ((dc_high - dc_low) / (df['close'] + 1e-8)).fillna(0)
    df['DCW_Slope'] = df['DCW'].diff().fillna(0)
    df['DC_Pos'] = ((df['close'] - dc_low) / (dc_high - dc_low + 1e-8)).fillna(0.5)
    return df

# ==========================================================
# EMA Ribbon + Trend Ignition
# ==========================================================
def ema_ribbon(df, periods=(8,13,21)):
    for p in periods:
        df[f'EMA_{p}'] = EMA(df, p)
    slopes = [df[f'EMA_{p}'].diff().fillna(0) for p in periods]
    score = pd.Series(0, index=df.index)
    for s in slopes:
        score += s.apply(lambda x: 1 if x > 0 else (-1 if x < 0 else 0))
    df['EMA_Ribbon_Score'] = score.fillna(0)
    ema_fast = df[f'EMA_{periods[0]}']
    ema_slow = df[f'EMA_{periods[-1]}']
    df['EMA_Spread'] = (ema_fast - ema_slow).abs().fillna(0)
    df['EMA_Expansion'] = (df['EMA_Spread'].diff().fillna(0) > 0)
    return df

# ==========================================================
# Momentum
# ==========================================================
def roc_momentum(df, period=3):
    df['ROC'] = df['close'].pct_change(periods=period).fillna(0)
    atr_val = ATR(df, 14)
    df['ROC_norm'] = (df['ROC'] / ((atr_val / df['close']) + 1e-8)).fillna(0)
    return df

# ==========================================================
# Directional Volatility
# ==========================================================
def directional_volatility(df, lookback=50):
    df['DIR_VOL'] = ((df['close'] - df['open']).abs() / (df['high'] - df['low'] + 1e-8)).fillna(0)
    dir_vol_baseline = df['DIR_VOL'].rolling(lookback, min_periods=1).median().fillna(1)
    df['DIR_VOL_REL'] = df['DIR_VOL'] / dir_vol_baseline
    rel_max = df['DIR_VOL_REL'].rolling(lookback, min_periods=1).max().fillna(1)
    df['DIR_VOL_REL_norm'] = (df['DIR_VOL_REL'] / rel_max).clip(0,1)
    return df

# ==========================================================
# Early Entry Logic
# ==========================================================
def early_entry(df):
    ema_ribbon(df)
    roc_momentum(df)
    df['EARLY_ENTRY_LONG'] = ((df['EMA_Ribbon_Score'] > 0) &
                              (df['EMA_Ribbon_Score'].shift().fillna(0) <= 0) &
                              (df['ROC_norm'] > 0)).fillna(False)
    df['EARLY_ENTRY_SHORT'] = ((df['EMA_Ribbon_Score'] < 0) &
                               (df['EMA_Ribbon_Score'].shift().fillna(0) >= 0) &
                               (df['ROC_norm'] < 0)).fillna(False)
    return df

# ==========================================================
# ATR Squeeze Filter
# ==========================================================
def atr_squeeze_filter(df, period=20, low_percentile=20):
    atr_vals = ATR(df, period)
    thresh = np.percentile(atr_vals.dropna(), low_percentile)
    df['ATR_Squeeze_OK'] = (atr_vals >= thresh).fillna(False)
    return df

# ==========================================================
# Participation / Volume Efficiency
# ==========================================================
def participation_metrics(df, lookback=3, vol_mult=1.0):
    df['Confirmed_Long'] = False
    df['Confirmed_Short'] = False
    vol_ma = df['volume'].rolling(lookback, min_periods=1).mean().fillna(1)

    for i in range(lookback, len(df)):
        if all(df['close'].iloc[i-j] > df['close'].iloc[i-j-1] for j in range(lookback)) and df['volume'].iloc[i] >= vol_ma.iloc[i]*vol_mult:
            df.loc[df.index[i], 'Confirmed_Long'] = True
        if all(df['close'].iloc[i-j] < df['close'].iloc[i-j-1] for j in range(lookback)) and df['volume'].iloc[i] >= vol_ma.iloc[i]*vol_mult:
            df.loc[df.index[i], 'Confirmed_Short'] = True

    df['VOL_Efficiency'] = ((df['close'] - df['close'].shift()).abs() / (df['volume'] + 1e-8)).fillna(0)
    eff_max = df['VOL_Efficiency'].rolling(50, min_periods=1).max().fillna(1)
    df['VOL_Eff_norm'] = (df['VOL_Efficiency'] / eff_max).clip(0.1,1)

    df['DIR_VOL_EFF'] = ((df['close'] - df['close'].shift()) / (df['volume'] + 1e-8)).fillna(0)
    dir_eff_max = df['DIR_VOL_EFF'].abs().rolling(50, min_periods=1).max().fillna(1)
    df['DIR_VOL_EFF_norm'] = (df['DIR_VOL_EFF'] / dir_eff_max).clip(-1,1)
    return df

# ==========================================================
# Market Regime Awareness
# ==========================================================
def market_regime(df):
    df['ATR_Regime'] = df['ATR_Expansion'].rolling(5, min_periods=1).mean().fillna(1)
    reg_max = df['ATR_Regime'].rolling(50, min_periods=1).max().fillna(1)
    df['Regime_norm'] = (df['ATR_Regime']/reg_max).clip(0,1)
    return df

# ==========================================================
# Post-entry Commitment
# ==========================================================
def post_entry_commitment(df, lookahead=3):
    df['commitment_ok'] = False
    if 'signal' not in df.columns:
        df['signal'] = 0
    for i in range(len(df)):
        sig = df['signal'].iloc[i]
        if sig == 0 or i+lookahead >= len(df):
            continue
        high_ref = df['high'].iloc[i]
        low_ref = df['low'].iloc[i]
        future_closes = df['close'].iloc[i+1:i+1+lookahead]
        if sig==1 and (future_closes>high_ref).any():
            df.loc[df.index[i], 'commitment_ok'] = True
        elif sig==-1 and (future_closes<low_ref).any():
            df.loc[df.index[i], 'commitment_ok'] = True
    return df

# ==========================================================
# Structure Filter
# ==========================================================
def structure_filter(df, dcw_min=0.003, atr_min=1.0, regime_min=0.3):
    df['STRUCTURE_OK'] = ((df['DCW']>dcw_min) &
                          (df['DCW_Slope']>0) &
                          (df['ATR_Expansion']>atr_min) &
                          (df['Regime_norm']>regime_min)).fillna(False)
    return df

def volume_intent(df, lookback=10, smooth=3):
    """
    Computes directional volume intent and alignment.
    Produces:
    - vol_intent_raw      ∈ [-1, 1]
    - vol_intent_strength ∈ [0, 1]
    - vol_intent_align    ∈ [0, 1]   (1 = aligned with signal)
    """

    # Up / down volume
    up_vol = df['volume'].where(df['close'] >= df['open'], 0.0)
    down_vol = df['volume'].where(df['close'] < df['open'], 0.0)

    vol_up = up_vol.rolling(lookback, min_periods=1).sum()
    vol_down = down_vol.rolling(lookback, min_periods=1).sum()

    # Raw intent
    intent_raw = (vol_up - vol_down) / (vol_up + vol_down + 1e-9)

    # Persistence (smoothed)
    intent_smooth = intent_raw.rolling(smooth, min_periods=1).mean()

    # Strength (ignore weak noise)
    intent_strength = intent_smooth.abs().clip(0, 1)

    # Alignment with signal
    align = pd.Series(1.0, index=df.index)
    align[(df['signal'] == 1) & (intent_smooth < 0)] = 0.85
    align[(df['signal'] == -1) & (intent_smooth > 0)] = 0.85

    df['vol_intent_raw'] = intent_smooth.fillna(0)
    df['vol_intent_strength'] = intent_strength.fillna(0)
    df['vol_intent_align'] = align.fillna(1.0)

# ==========================================================
# Generate Signal (unchanged, now safe)
# ==========================================================
def generate_signal(df, conf_threshold=0,
                    atr_exp_start=1.02, vol_spike_mult=1.5,
                    dcw_slope_mult=0.1, atr_stop_mult=1.5,
                    mc_lookback=3, mc_vol_mult=1.0,
                    commitment_lookahead=3):

    if df.empty:
        return df

    # ======================================================
    # CORE INDICATORS
    # ======================================================
    early_entry(df)
    atr_expansion(df)
    donchian_channel_width(df)
    multi_tf_trend(df)
    atr_squeeze_filter(df)
    directional_volatility(df)
    participation_metrics(df, mc_lookback, mc_vol_mult)
    market_regime(df)

    # ======================================================
    # STRUCTURE FILTER (gate, not dominance)
    # ======================================================
    if 'STRUCTURE_OK' not in df.columns:
        df['STRUCTURE_OK'] = True
    structure_filter(df)

    atr_vals = ATR(df).fillna(0)
    vol_ma = df['volume'].rolling(20, min_periods=1).mean()

    # ======================================================
    # ABSOLUTE STRUCTURE FLOORS
    # ======================================================
    dcw_min = 0.003
    atr_ok = df['ATR_Expansion'].fillna(0) >= atr_exp_start
    vol_ok = df['volume'] >= vol_ma * vol_spike_mult

    dcw_thresh = dcw_slope_mult * df['DCW'].rolling(5, min_periods=1).mean()
    dcw_breakout = df['DCW_Slope'].abs() > dcw_thresh

    # ======================================================
    # EARLY ENTRY MATURITY (permissive)
    # ======================================================
    str_dcw = (df['DCW'] / df['DCW'].rolling(20, min_periods=1).max()).clip(0, 1)
    str_atr = (df['ATR_Expansion'] / atr_exp_start).clip(0, 1)
    str_vol = (df['volume'] / (vol_ma * vol_spike_mult)).clip(0, 1)

    early_maturity = (
        0.4 * str_dcw +
        0.3 * str_atr +
        0.3 * str_vol
    ).fillna(0.6).clip(0.4, 1.0)

    # ======================================================
    # RAW ENTRY LOGIC (UNCHANGED)
    # ======================================================
    long_condition = (
        df['EARLY_ENTRY_LONG'] &
        (df['DCW'] > dcw_min) &
        (df['EMA_Ribbon_Score'] > 0) &
        dcw_breakout & atr_ok & vol_ok &
        df['ATR_Squeeze_OK'] &
        df['STRUCTURE_OK']
    )

    short_condition = (
        df['EARLY_ENTRY_SHORT'] &
        (df['DCW'] > dcw_min) &
        (df['EMA_Ribbon_Score'] < 0) &
        dcw_breakout & atr_ok & vol_ok &
        df['ATR_Squeeze_OK'] &
        df['STRUCTURE_OK']
    )

    df['signal'] = 0
    df.loc[long_condition, 'signal'] = 1
    df.loc[short_condition, 'signal'] = -1

    # ======================================================
    # POST-ENTRY COMMITMENT
    # ======================================================
    post_entry_commitment(df, commitment_lookahead)
    df.loc[~df['commitment_ok'], 'signal'] = 0

    # ======================================================
    # VOLUME INTENT (confidence + risk only)
    # ======================================================
    volume_intent(df)

    volume_scalar = (
        1.0
        - 0.15 * df['vol_intent_strength']
        + 0.15 * df['vol_intent_strength'] * df['vol_intent_align']
    ).fillna(1.0).clip(0.85, 1.15)

    # ======================================================
    # DIRECTIONAL STACKING (single pass, capped)
    # ======================================================
    ema_norm = (
        df['EMA_Ribbon_Score'].abs() /
        df['EMA_Ribbon_Score'].abs().rolling(100, min_periods=1).max()
    ).fillna(0).clip(0, 1)

    direction_strength = (
        ema_norm * df['EMA_Expansion'].astype(int) +
        df['DIR_VOL_REL_norm'].fillna(0).clip(0, 1) +
        df['DIR_VOL_EFF_norm'].abs().fillna(0).clip(0, 1) +
        df['HTF_Polarized'].astype(float).clip(0, 1)
    ).clip(0, 2.5) / 2.5

    # ======================================================
    # DC POSITION CONFIDENCE
    # ======================================================
    conf_dc = pd.Series(1.0, index=df.index)
    conf_dc[df['signal'] == 1] = (1 - df['DC_Pos']).clip(0, 1)
    conf_dc[df['signal'] == -1] = df['DC_Pos'].clip(0, 1)

    # ======================================================
    # RELATIVE NORMALIZATION (once, controlled)
    # ======================================================
    relative_boost = (
        0.5 * df['DIR_VOL_REL_norm'].fillna(0) +
        0.5 * df['VOL_Eff_norm'].fillna(0)
    ).clip(0.7, 1.2)

    maturity_scalar = early_maturity * relative_boost * volume_scalar

    # ======================================================
    # TRADE QUALITY
    # ======================================================
    df['trade_quality'] = (
        0.45 * direction_strength +
        0.35 * early_maturity +
        0.20 * conf_dc
    ) * df['Regime_norm'].fillna(0) * maturity_scalar

    quality_thresh = df['trade_quality'].quantile(0.1)
    df.loc[df['trade_quality'] < quality_thresh, 'signal'] = 0

    # ======================================================
    # FINAL CONFIDENCE
    # ======================================================
    df['confidence'] = (
        0.5 * direction_strength +
        0.35 * early_maturity +
        0.15 * conf_dc
    ) * df['Regime_norm'].fillna(0) * maturity_scalar * 100

    # ======================================================
    # STOPS (volume intent tightens only)
    # ======================================================
    df['stop_loss'] = np.nan
    base_stop = atr_stop_mult * atr_vals * df['ATR_Expansion'].fillna(1)

    tighten = np.ones(len(df))
    tighten[df['DIR_VOL_REL_norm'] >= 0.6] *= 0.85
    tighten[df['DIR_VOL_REL_norm'] >= 0.8] *= 0.7

    edge = 0.15
    tighten[(df['DC_Pos'] <= edge) | (df['DC_Pos'] >= 1 - edge)] *= 0.85

    tighten *= 1 / (1 + 0.2 * df['HTF_Score'].abs().fillna(0))
    tighten *= 1 / (1 + 0.15 * df['EMA_Expansion'].astype(int))
    tighten *= np.where(df['commitment_ok'], 1.0, 0.8)

    # Volume intent opposition → tighter stops only
    tighten *= np.where(df['vol_intent_align'] < 1.0, 0.9, 1.0)

    adj_stop = base_stop * tighten
    df.loc[df['signal'] == 1, 'stop_loss'] = df['close'] - adj_stop
    df.loc[df['signal'] == -1, 'stop_loss'] = df['close'] + adj_stop

    # ======================================================
    # FINAL SIGNAL
    # ======================================================
    df['final_signal'] = df['signal']
    if conf_threshold > 0:
        df.loc[df['confidence'] < conf_threshold, 'final_signal'] = 0

    return df
