import pandas as pd
import numpy as np

# ==========================================================
# CORE UTILITIES
# ==========================================================
def EMA(series, period):
    return series.ewm(span=period, adjust=False).mean()

def ATR(df, period=14):
    tr = np.maximum.reduce([
        df['high'] - df['low'],
        (df['high'] - df['close'].shift()).abs(),
        (df['low'] - df['close'].shift()).abs()
    ])
    return pd.Series(tr, index=df.index).rolling(period).mean()

def parkinson_vol(df, period=14):
    hl_ratio = np.log(df['high'] / df['low'])
    return (hl_ratio.pow(2).rolling(period).mean() / (4 * np.log(2))).pow(0.5)

def RSI(series, period=14):
    delta = series.diff()
    gain = delta.clip(lower=0)
    loss = -delta.clip(upper=0)
    rs = gain.rolling(period).mean() / loss.rolling(period).mean()
    return 100 - (100 / (1 + rs))

# ==========================================================
# TREND CONTEXT
# ==========================================================
def trend_bias(df, window=50):

    df['TREND_SLOPE'] = rolling_slope(df['close'], window)
    df['TREND_R2'] = rolling_r2(df['close'], window)

    # Combine direction + velocity + reliability
    df['TREND_QUALITY'] = df['TREND_SLOPE'] * df['TREND_R2']

    # Normalize to stable range (important)
    scale = df['close'].rolling(window).std()
    df['TREND_QUALITY'] = df['TREND_QUALITY'] / (scale + 1e-9)

    return df

# ==========================================================
# WICK ANALYSIS
# ==========================================================
def wick_rejection(df):
    body = (df['close'] - df['open']).abs()
    upper = df['high'] - df[['close', 'open']].max(axis=1)
    lower = df[['close', 'open']].min(axis=1) - df['low']

    df['UPPER_WICK_RATIO'] = upper / (body + 1e-9)
    df['LOWER_WICK_RATIO'] = lower / (body + 1e-9)

    return df

# ==========================================================
# VOLUME CONFIRMATION
# ==========================================================
def volume_confirmation(df, lookback=20):
    df['VOL_MA'] = df['volume'].rolling(lookback).mean()
    df['VOL_RATIO'] = df['volume'] / (df['VOL_MA'] + 1e-9)
    return df

# ==========================================================
# SUPPORT / RESISTANCE
# ==========================================================
def support_resistance(df, lookback=20):
    df['RESISTANCE'] = df['high'].rolling(lookback).max()
    df['SUPPORT'] = df['low'].rolling(lookback).min()
    return df

# ==========================================================
# BREAKOUT LOGIC
# ==========================================================
def breakout_logic(df, atr_k=0.5):
    """
    Volatility-adjusted breakout.
    Break must clear structure by ATR fraction.
    """

    # Ensure ATR exists
    if 'ATR' not in df.columns:
        df['ATR'] = ATR(df)

    resistance = df['RESISTANCE'].shift(1)
    support = df['SUPPORT'].shift(1)

    df['BREAK_RESISTANCE'] = df['close'] > (resistance + atr_k * df['ATR'])
    df['BREAK_SUPPORT'] = df['close'] < (support - atr_k * df['ATR'])

    return df

def volatility_state(df, lookback=200):

    pctl = df['ATR'].rolling(lookback).rank(pct=True)

    df['VOL_STATE'] = np.select(
        [pctl < 0.45, pctl > 0.55],  # relaxed thresholds
        [-1, 1],
        default=0
    )

    return df

def structure_state(df, lookback=50):

    width = df['high'].rolling(lookback).max() - df['low'].rolling(lookback).min()
    norm = width / (df['ATR'] * np.sqrt(lookback) + 1e-9)

    df['STRUCT_STATE'] = np.select(
        [norm < 1.0, norm > 1.5],  # relaxed compression / expansion
        [-1, 1],
        default=0
    )

    return df

def pressure_state(df):

    close_loc = (df['close'] - df['low']) / (df['high'] - df['low'] + 1e-9)
    df['PRESSURE'] = close_loc - 0.5

    return df

# ==========================================================
# INSTITUTIONAL PARTICIPATION (Magnitude-Aware VWM)
# ==========================================================
# ==========================================================
# INSTITUTIONAL PARTICIPATION (SIGNED DOLLAR FLOW MODEL)
# ==========================================================
def participation_state(df, lookback=20, threshold=0.5):

    # ------------------------------------------------------
    # 1️⃣ Signed institutional flow
    # ------------------------------------------------------
    df['FLOW'] = df['volume'] * (df['close'] - df['open'])

    # ------------------------------------------------------
    # 2️⃣ Normalize by rolling volatility (z-score style)
    # ------------------------------------------------------
    flow_std = df['FLOW'].rolling(50).std()

    df['FLOW_Z'] = df['FLOW'] / (flow_std + 1e-9)

    # ------------------------------------------------------
    # 3️⃣ Capital accumulation (rolling flow)
    # ------------------------------------------------------
    df['FLOW_ROLL'] = df['FLOW_Z'].rolling(lookback).mean()

    # ------------------------------------------------------
    # 4️⃣ Institutional accumulation detector
    # ------------------------------------------------------

    # Cumulative capital inflow
    df['ACCUMULATION'] = df['FLOW_Z'].rolling(10).sum()

    # Price drift over same window
    price_drift = df['close'].pct_change(10)

    # Normalize drift by volatility so regime independent
    vol = df['close'].pct_change().rolling(50).std()

    df['PRICE_DRIFT_NORM'] = price_drift / (vol + 1e-9)

    # Stealth accumulation condition
    df['STEALTH_ACCUM'] = (
        (df['ACCUMULATION'] > 1.5) &      # sustained positive flow
        (df['PRICE_DRIFT_NORM'].abs() < 0.5)  # price not moving much
    )

    # Distribution version
    df['STEALTH_DISTRIB'] = (
        (df['ACCUMULATION'] < -1.5) &
        (df['PRICE_DRIFT_NORM'].abs() < 0.5)
    )

    # ------------------------------------------------------
    # 4️⃣ Final flow strength metric
    # ------------------------------------------------------
    df['FLOW_STRENGTH'] = df['FLOW_ROLL']

    # Boost if stealth accumulation detected
    df.loc[df['STEALTH_ACCUM'], 'FLOW_STRENGTH'] += 0.5
    df.loc[df['STEALTH_DISTRIB'], 'FLOW_STRENGTH'] -= 0.5

    # ------------------------------------------------------
    # 5️⃣ Participation classification
    # ------------------------------------------------------
    df['PARTICIPATION'] = np.select(
        [
            df['FLOW_STRENGTH'] > threshold,
            df['FLOW_STRENGTH'] < -threshold
        ],
        [1, -1],
        default=0
    )

    return df

def classify_phase(df):

    df['PHASE'] = 0

    pre_breakout = (
        (df['VOL_STATE'] == -1) &
        (df['STRUCT_STATE'] == -1) &
        (
            (df['PARTICIPATION'] == 1) |
            (df['STEALTH_ACCUM'])
        )
    )

    trend = (
        (df['VOL_STATE'] == 1) &
        (df['STRUCT_STATE'] == 1) &
        (df['PARTICIPATION'] == 1)
    )

    exhaustion = (
        (df['VOL_STATE'] == 1) &
        (df['PARTICIPATION'] == -1)
    )

    df.loc[pre_breakout, 'PHASE'] = 1
    df.loc[trend, 'PHASE'] = 2
    df.loc[exhaustion, 'PHASE'] = 3

    return df

def divergence_state(df, lookback=14):

    # --- returns
    df['PRICE_RET'] = df['close'].pct_change()
    df['PRESSURE_RET'] = df['PRESSURE'].diff()

    # --- rolling correlation
    df['DIV_CORR'] = df['PRICE_RET'].rolling(lookback).corr(df['PRESSURE_RET'])

    # --- divergence strength (magnitude-aware)
    df['DIV_STRENGTH'] = -df['DIV_CORR']

    # --- directional classification
    df['DIV_BULL'] = df['DIV_CORR'] < -0.2
    df['DIV_BEAR'] = df['DIV_CORR'] < -0.2

    # --- discrete signal (kept compatible with your system)
    df['DIVERGENCE'] = 0
    df.loc[df['DIV_CORR'] < -0.2, 'DIVERGENCE'] = np.sign(df['PRESSURE_RET'])

    return df

def vol_compression_slope(df, lookback=50, rv_period=20, alpha=0.2):
    # Compute realized volatility
    df['REALIZED_VOL'] = ewma_realized_vol(df, period=rv_period, alpha=alpha)

    # Compute slope of realized volatility
    df['RV_SLOPE'] = df['REALIZED_VOL'].diff(1)

    # Rolling mean slope -> compression signal
    df['VOL_COMPRESS'] = df['RV_SLOPE'].rolling(lookback).mean() < 0

    return df

def transition_detector(df):
    df['TRANSITION_LONG'] = (
        (df['VOL_COMPRESS']) &
        (df['DIVERGENCE'] >= 0)
    )
    df['TRANSITION_SHORT'] = (
        (df['VOL_COMPRESS']) &
        (df['DIVERGENCE'] <= 0)
    )
    
    df['TRANSITION_SIGNAL'] = 0
    df.loc[df['TRANSITION_LONG'], 'TRANSITION_SIGNAL'] = 1
    df.loc[df['TRANSITION_SHORT'], 'TRANSITION_SIGNAL'] = -1

    return df

# ==========================================================
# CANDLESTICK PATTERNS
# ==========================================================
def candle_body(df):
    df['body'] = df['close'] - df['open']
    df['body_dir'] = np.where(df['body'] > 0, 1, np.where(df['body'] < 0, -1, 0))
    df['body_size'] = df['body'].abs()
    return df

def composite_pressure(df):

    # Ensure VOL_RATIO exists
    if 'VOL_RATIO' not in df.columns:
        df = volume_confirmation(df)

    # Normalize VOL_RATIO to roughly -1..1 around 1
    vol_norm = df['VOL_RATIO'] - 1.0

    # Composite: pressure * normalized volume
    df['COMPOSITE_PRESSURE'] = df['PRESSURE'] * vol_norm

    return df

def validated_breakouts(df, body_ratio=0.6, atr_mult=1.2):
    body = (df['close'] - df['open']).abs()
    range_ = df['high'] - df['low']

    # --- Displacement requirement (impulsive candle)
    DISPLACEMENT_K = 0.8
    df['DISPLACEMENT'] = body > (df['ATR'] * DISPLACEMENT_K)

    # --- Strong body relative to candle range
    df['STRONG_BODY'] = (body / (range_ + 1e-9)) > body_ratio

    # --- ATR expansion confirms real move
    df['ATR_EXPAND'] = df['ATR'] > df['ATR'].rolling(20).mean() * atr_mult

    # --- Reject fake wick breakouts
    df['NO_UPPER_WICK_FAKE'] = df['UPPER_WICK_RATIO'] < df['DYNAMIC_WICK_LIMIT']
    df['NO_LOWER_WICK_FAKE'] = df['LOWER_WICK_RATIO'] < df['DYNAMIC_WICK_LIMIT']

    # --- Final validated breakouts
    # Compression must occur within recent window
    COMPRESSION_LOOKBACK = 10
    prior_compression = df['VOL_COMPRESS'].rolling(COMPRESSION_LOOKBACK).max().shift(1)

    df['VALID_BREAK_LONG'] = (
        df['BREAK_RESISTANCE'] &
        prior_compression &
        df['STRONG_BODY'] &
        df['DISPLACEMENT'] &
        df['NO_UPPER_WICK_FAKE'] &
        (df['COMPOSITE_PRESSURE'] > 0)
    )

    df['VALID_BREAK_SHORT'] = (
        df['BREAK_SUPPORT'] &
        prior_compression &
        df['STRONG_BODY'] &
        df['DISPLACEMENT'] &
        df['NO_LOWER_WICK_FAKE'] &
        (df['COMPOSITE_PRESSURE'] < 0)
    )

    return df

# ==========================================================
# MICRO CONSOLIDATION DETECTOR (INSIDE TRENDS)
# ==========================================================
def micro_consolidation(df, lookback=12, tightness=0.6):

    # local range
    local_high = df['high'].rolling(lookback).max()
    local_low  = df['low'].rolling(lookback).min()
    width = local_high - local_low

    # normalize by ATR so it's regime-independent
    norm_width = width / (df['ATR'] + 1e-9)

    # tight box condition
    df['MICRO_BOX'] = norm_width < tightness

    # breakout levels (shifted so breakout is real)
    df['MICRO_HIGH'] = local_high.shift(1)
    df['MICRO_LOW']  = local_low.shift(1)

    # breakout detection
    df['MICRO_BREAK_LONG'] = df['close'] > df['MICRO_HIGH']
    df['MICRO_BREAK_SHORT'] = df['close'] < df['MICRO_LOW']

    # strength score (normalized)
    expansion_strength = (width / width.rolling(lookback).mean()).clip(0,2)

    df['MICRO_BREAK_SCORE'] = np.select(
        [df['MICRO_BREAK_LONG'], df['MICRO_BREAK_SHORT']],
        [expansion_strength, -expansion_strength],
        default=0
    )

    return df

def supertrend(df, period=10, multiplier=3, eps=1e-6):
    atr = ATR(df, period).round(6)

    hl2 = ((df['high'] + df['low']) / 2).round(6)

    upper_band = (hl2 + multiplier * atr).round(6)
    lower_band = (hl2 - multiplier * atr).round(6)

    final_upper = upper_band.copy()
    final_lower = lower_band.copy()

    trend = pd.Series(1, index=df.index)

    close = df['close'].round(6)

    for i in range(1, len(df)):

        # stable band logic
        if close.iat[i-1] <= final_upper.iat[i-1] + eps:
            final_upper.iat[i] = min(upper_band.iat[i], final_upper.iat[i-1])
        else:
            final_upper.iat[i] = upper_band.iat[i]

        if close.iat[i-1] >= final_lower.iat[i-1] - eps:
            final_lower.iat[i] = max(lower_band.iat[i], final_lower.iat[i-1])
        else:
            final_lower.iat[i] = lower_band.iat[i]

        # stable trend flip detection
        if close.iat[i] > final_upper.iat[i-1] + eps:
            trend.iat[i] = 1
        elif close.iat[i] < final_lower.iat[i-1] - eps:
            trend.iat[i] = -1
        else:
            trend.iat[i] = trend.iat[i-1]

    df['SUPERTREND'] = trend.astype(int)
    return df

def supertrend_htf(df, htf_df, period=10, multiplier=3):
    """
    Computes SuperTrend on HTF and aligns it to LTF df.
    Returns a series of 1 (bull) / -1 (bear)
    """
    htf_df = htf_df.copy()
    htf_df = supertrend(htf_df, period=period, multiplier=multiplier)
    
    # Align to LTF
    return htf_df['SUPERTREND'].reindex(df.index, method='ffill').fillna(0)

# ==========================================================
# RSI RISK FILTER (NON-GATING)
# ==========================================================
def rsi_risk_filter(df, period=14, overbought=70, oversold=30):
    rsi = RSI(df['close'], period)

    long_ok = rsi < overbought
    short_ok = rsi > oversold

    return long_ok.fillna(True), short_ok.fillna(True)

# ==========================================================
# ANCHORED VWAP RISK FILTER (NON-GATING)
# ==========================================================
def anchored_vwap_risk(df, anchor_period=50):

    typical = (df['high'] + df['low'] + df['close']) / 3
    vol_price = typical * df['volume']

    rolling_vol_price = vol_price.rolling(anchor_period).sum()
    rolling_vol = df['volume'].rolling(anchor_period).sum()

    avwap = rolling_vol_price / (rolling_vol + 1e-9)

    long_ok = df['close'] >= avwap
    short_ok = df['close'] <= avwap

    return long_ok.fillna(True), short_ok.fillna(True)

def momentum_continuity(df, window=20, min_move=0.001):

    ret = df['close'].pct_change()

    # ignore tiny moves (noise)
    ret = np.where(np.abs(ret) < min_move, 0, ret)

    sign_ret = np.sign(ret)

    persistence = (
        (sign_ret * pd.Series(sign_ret).shift(1)) > 0
    ).astype(int)

    df['MOMENTUM_CONTINUITY'] = (
        pd.Series(persistence, index=df.index)
        .rolling(window)
        .mean()
    )

    return df

# ==========================================================
# DYNAMIC STATE ENGINE (INSTITUTIONAL GRADE)
# ==========================================================
def dynamic_state_engine(df, window=10):

    # -----------------------------------
    # BASE STATE (composite environment)
    # -----------------------------------
    df['STATE_SCORE'] = (
        0.25 * df['VOL_STATE'] +
        0.25 * df['STRUCT_STATE'] +
        0.25 * df['PARTICIPATION'] +
        0.25 * np.sign(df['COMPOSITE_PRESSURE'])
    )

    # normalize to -1 → 1
    df['STATE_SCORE'] = df['STATE_SCORE'].clip(-1,1)

    # -----------------------------------
    # VELOCITY → first derivative
    # -----------------------------------
    df['STATE_VELOCITY'] = df['STATE_SCORE'].diff()

    # -----------------------------------
    # ACCELERATION → second derivative
    # -----------------------------------
    df['STATE_ACCEL'] = df['STATE_VELOCITY'].diff()

    # -----------------------------------
    # INFLECTION POINTS
    # sign flip of velocity
    # -----------------------------------
    df['STATE_INFLECT'] = (
        np.sign(df['STATE_VELOCITY']) !=
        np.sign(df['STATE_VELOCITY'].shift(1))
    )

    df['PRESSURE_VOL'] = pressure_volatility(df, period=20, alpha=0.2)
    df['PRESSURE_VOL_NORM'] = df['PRESSURE_VOL'] / (df['PRESSURE_VOL'].rolling(200).max() + 1e-9)

    # -----------------------------------
    # STABILITY
    # low variance = stable regime
    # -----------------------------------
    df['STATE_STABILITY'] = (
        1 /
        (df['STATE_SCORE'].rolling(window).std() + 1e-9)
    )

    # normalize stability
    df['STATE_STABILITY'] = (
        df['STATE_STABILITY'] /
        df['STATE_STABILITY'].rolling(window).max()
    ).clip(0,1)

    df['STATE_STABILITY'] *= (1 - 0.5 * df['PRESSURE_VOL_NORM'])  # volatile regimes are less stable

    # -----------------------------------
    # STABILITY DECAY
    # detects regime breakdown
    # -----------------------------------
    df['STABILITY_DECAY'] = df['STATE_STABILITY'].diff()

    # -----------------------------------
    # TRANSITION INTENSITY
    # combines velocity + accel + decay
    # -----------------------------------
    df['TRANSITION_FORCE'] = (
        df['STATE_VELOCITY'].abs() +
        df['STATE_ACCEL'].abs() +
        df['STABILITY_DECAY'].abs()
    )

    # Damp TRANSITION_FORCE based on volatility instability
    df['TRANSITION_FORCE'] *= (1 - 0.5 * df['PRESSURE_VOL_NORM'])  # max 50% damp

    # -----------------------------------
    # VOLATILITY SHOCK REGIME INSTABILITY
    # -----------------------------------

    if 'VOL_SHOCK' in df.columns:
        df['TRANSITION_FORCE'] += 0.5 * df['VOL_SHOCK_INTENSITY']

    return df

# ==========================================================
# TREND QUALITY UTILITIES (Slope + R²)
# ==========================================================

def rolling_slope(series, window=50):
    """
    Linear regression slope over rolling window.
    Measures directional velocity.
    """
    x = np.arange(window)

    def slope_func(y):
        if np.any(np.isnan(y)):
            return np.nan
        return np.polyfit(x, y, 1)[0]

    return series.rolling(window).apply(slope_func, raw=False)


def rolling_r2(series, window=50):
    """
    Rolling R² (trend reliability).
    Measures how clean the trend is.
    """
    x = np.arange(window)

    def r2_func(y):
        if np.any(np.isnan(y)):
            return np.nan
        coeffs = np.polyfit(x, y, 1)
        p = np.poly1d(coeffs)
        y_hat = p(x)
        ss_res = ((y - y_hat) ** 2).sum()
        ss_tot = ((y - y.mean()) ** 2).sum()
        return 1 - ss_res / (ss_tot + 1e-9)

    return series.rolling(window).apply(r2_func, raw=False)

def efficiency_ratio(series, window=50):

    direction = (series - series.shift(window)).abs()
    volatility = series.diff().abs().rolling(window).sum()

    er = direction / (volatility + 1e-9)

    return er.clip(0,1)

# ==========================================================
# VOLATILITY UTILITIES
# ==========================================================
def ewma_realized_vol(df, period=20, alpha=0.2):
    log_ret = np.log(df['close']).diff()
    rv = log_ret.pow(2).ewm(alpha=alpha, adjust=False).mean().pow(0.5)
    return rv

def pressure_volatility(df, period=20, alpha=0.2):
    """
    EWMA volatility of COMPOSITE_PRESSURE
    Captures magnitude jitter and institutional activity instability
    """
    if 'COMPOSITE_PRESSURE' not in df.columns:
        df = composite_pressure(df)  # generate if missing
    pv = df['COMPOSITE_PRESSURE'].ewm(alpha=alpha, adjust=False).std()
    return pv

def parkinson_vol(df, period=14):
    hl_ratio = np.log(df['high'] / df['low'])
    return (hl_ratio.pow(2).rolling(period).mean() / (4 * np.log(2))).pow(0.5)

def htf_structural_stack(df, htf_df,
                         vol_lookback=200,
                         part_lookback=50,
                         regime_window=10,
                         er_window=20):

    htf = htf_df.copy()

    # ======================================================
    # 1️⃣ DIRECTION (Hard Anchor)
    # ======================================================

    htf = supertrend(htf, period=10, multiplier=3)
    htf['HTF_DIRECTION'] = htf['SUPERTREND']  # 1 / -1

    # ======================================================
    # 2️⃣ VOLATILITY STATE (Continuous)
    # ======================================================

    htf['HTF_ATR'] = ATR(htf, 14)
    htf['HTF_VOL_PCTL'] = (
        htf['HTF_ATR']
        .rolling(vol_lookback)
        .rank(pct=True)
    )

    # Normalize to 0–1 range around expansion bias
    htf['VOL_SCORE'] = (htf['HTF_VOL_PCTL'] - 0.5).clip(0, 1)

    # ======================================================
    # 3️⃣ PARTICIPATION (Continuous)
    # ======================================================

    htf['HTF_VOL_MA'] = htf['volume'].rolling(part_lookback).mean()
    htf['HTF_VOL_RATIO'] = htf['volume'] / (htf['HTF_VOL_MA'] + 1e-9)

    htf['PART_SCORE'] = ((htf['HTF_VOL_RATIO'] - 1) / 1).clip(0, 1)

    # ======================================================
    # 4️⃣ REGIME PERSISTENCE (Continuous)
    # ======================================================

    direction_series = htf['HTF_DIRECTION']
    htf['HTF_REGIME_PERSIST'] = (
        direction_series
        .rolling(regime_window)
        .apply(lambda x: abs(x.mean()), raw=False)
    )

    htf['REGIME_SCORE'] = htf['HTF_REGIME_PERSIST'].clip(0, 1)

    # ======================================================
    # 5️⃣ STRUCTURE QUALITY (Continuous)
    # ======================================================

    htf['HTF_ER'] = efficiency_ratio(htf['close'], er_window)
    htf['STRUCTURE_SCORE'] = htf['HTF_ER'].clip(0, 1)

    # ======================================================
    # 6️⃣ COMPOSITE QUALITY SCORE
    # ======================================================

    htf['HTF_QUALITY'] = (
        0.30 * htf['VOL_SCORE'] +
        0.25 * htf['PART_SCORE'] +
        0.25 * htf['REGIME_SCORE'] +
        0.20 * htf['STRUCTURE_SCORE']
    )

    # ======================================================
    # ALIGN TO LTF
    # ======================================================

    aligned = htf[[
        'HTF_DIRECTION',
        'HTF_QUALITY'
    ]].reindex(df.index, method='ffill')

    return aligned.fillna(0)

# ==========================================================
# INSTITUTIONAL EXIT MODEL
# ==========================================================
def institutional_exit_model(df):
    # ------------------------------------------------------
    # CAPITAL WITHDRAWAL (Liquidity leaving trend)
    # ------------------------------------------------------

    # participation momentum
    df['FLOW_MOMENTUM'] = (
        df['FLOW_STRENGTH']
        .diff()
        .ewm(span=3)
        .mean()
    )

    # smoothed participation trend
    df['FLOW_TREND'] = (
        df['FLOW_STRENGTH']
        .rolling(10)
        .mean()
    )

    # momentum decay
    flow_momentum_decay = (
        df['FLOW_MOMENTUM']
        .rolling(5)
        .mean() < 0
    )

    # participation weakening
    flow_trend_decay = (
        df['FLOW_TREND'].diff() < 0
    )

    # ensure a trend is still active
    trend_active = df['TREND_QUALITY'].abs() > 0.1

    # final liquidity withdrawal signal
    df['FLOW_DECAY'] = (
        flow_momentum_decay &
        flow_trend_decay &
        trend_active
    )

    # ------------------------------------------------------
    # 2️⃣ EDGE DECAY (your asymmetry engine weakening)
    # ------------------------------------------------------

    df['ASYM_DECAY'] = (
        df['ASYM_SCORE']
        .diff()
        .rolling(3)
        .mean() < 0
    )

    # ------------------------------------------------------
    # 3️⃣ REGIME WEAKENING (state engine deterioration)
    # ------------------------------------------------------

    df['STATE_WEAKEN'] = (
        (df['STATE_STABILITY'] < 0.45) |
        (df['TRANSITION_FORCE'] > 0.7)
    )

    # ------------------------------------------------------
    # 4️⃣ TREND ENERGY DECAY
    # ------------------------------------------------------

    df['TREND_ENERGY'] = (
        df['TREND_QUALITY'].abs() *
        df['MOMENTUM_CONTINUITY']
    )

    df['TREND_ENERGY_DECAY'] = (
        df['TREND_ENERGY']
        .diff()
        .rolling(5)
        .mean() < 0
    )

    # ------------------------------------------------------
    # 5️⃣ STRUCTURAL EFFICIENCY DECAY
    # ------------------------------------------------------

    df['STRUCT_EFF'] = efficiency_ratio(df['close'], 20)

    df['STRUCT_DECAY'] = (
        df['STRUCT_EFF']
        .diff()
        .rolling(5)
        .mean() < 0
    )

    # ------------------------------------------------------
    # 6️⃣ EXIT PRESSURE SCORE
    # ------------------------------------------------------

    df['NO_EXPANSION'] = (df['ATR_PERCENTILE'] < 0.4)

    df['REGIME_BREAK'] = (
        (df['STATE_STABILITY'] < 0.35) &
        (df['TRANSITION_FORCE'] > 0.8)
    )

    df['EXIT_PRESSURE'] = (
        # 0.25 * df['TREND_ENERGY_DECAY'].astype(int) +
        # 0.20 * df['FLOW_DECAY'].astype(int) +
        # 0.20 * df['STRUCT_DECAY'].astype(int) +
        # 0.15 * df['MICRO_BREAK_SCORE'].abs().gt(1.2).astype(int) +
        # 0.10 * df['NO_EXPANSION'].astype(int) +
        1.0 * df['REGIME_BREAK'].astype(int)
    )

    df['EXIT_PRESSURE'] = df['EXIT_PRESSURE'].ewm(span=3).mean()

    return df

# ==========================================================
# VOLATILITY SHOCK DETECTOR
# ==========================================================
def volatility_shock(df, lookback=20, shock_mult=1.8):

    # baseline volatility
    atr_mean = df['ATR'].rolling(lookback).mean()

    # shock ratio
    shock_ratio = df['ATR'] / (atr_mean + 1e-9)

    df['VOL_SHOCK'] = (shock_ratio > shock_mult).astype(int)

    # intensity (continuous)
    df['VOL_SHOCK_INTENSITY'] = (shock_ratio - 1).clip(0, 3)

    return df

# ==========================================================
# INTEGRATE INTO SIGNAL GENERATION
# ==========================================================
def generate_signal(df, htf_df, atr_mult=1.5):
    if df.empty:
        return df

    # =========================
    # Core processing
    # =========================
    df = trend_bias(df)
    df = wick_rejection(df)
    df = volume_confirmation(df)
    df = support_resistance(df)
    df = breakout_logic(df)

    df['ATR'] = parkinson_vol(df, period=14)

    df = volatility_shock(df)

    # --- ATR percentile for adaptive thresholds
    df['ATR_PERCENTILE'] = (
        df['ATR']
        .rolling(200)
        .rank(pct=True)
    )
    df['DYNAMIC_WICK_LIMIT'] = 1.2 + (df['ATR_PERCENTILE'] * 1.0)

    # =========================
    # STATE ENGINE
    # =========================
    df = volatility_state(df)
    df = structure_state(df)
    df = pressure_state(df)
    df = participation_state(df)
    df = classify_phase(df)
    df = composite_pressure(df)  # 🔹 generate COMPOSITE_PRESSURE metric
    df = vol_compression_slope(df, lookback=50, rv_period=20)
    df = validated_breakouts(df)
    # --- Dynamic state analytics
    df = dynamic_state_engine(df)

    # =========================
    # NEW HTF STRUCTURAL STACK
    # =========================

    htf_stack = htf_structural_stack(df, htf_df)

    df = pd.concat([df, htf_stack], axis=1)

    HTF_QUALITY_TH = 0.45  # tune 0.40–0.60

    HTF_LONG_OK = (
        (df['HTF_DIRECTION'] == 1) &
        (df['HTF_QUALITY'] > HTF_QUALITY_TH)
    )

    HTF_SHORT_OK = (
        (df['HTF_DIRECTION'] == -1) &
        (df['HTF_QUALITY'] > HTF_QUALITY_TH)
    )

    # =========================
    # PREDICTIVE MODULES
    # =========================
    df = divergence_state(df)
    df = transition_detector(df)
    df = micro_consolidation(df)
    df = momentum_continuity(df)

    # ==========================================================
    # 🧠 PREDICTIVE-WEIGHTED ASYMMETRY ENGINE
    # ==========================================================

    # --- Directional pressure sign
    df['DIR'] = np.sign(df['COMPOSITE_PRESSURE']).fillna(0)

    # ======================================================
    # 1️⃣ DIRECTIONAL LEADING SCORES
    # ======================================================

    phase_long = ((df['PHASE'] == 1) | (df['PHASE'] == 2)).astype(int)
    phase_short = (df['PHASE'] == 3).astype(int)

    div_long = (df['DIVERGENCE'] > 0).astype(int)
    div_short = (df['DIVERGENCE'] < 0).astype(int)

    lead_long = (
        df['VOL_COMPRESS'].astype(int) +
        phase_long +
        div_long +
        df['STEALTH_ACCUM'].astype(int)
    )

    lead_short = (
        df['VOL_COMPRESS'].astype(int) +
        phase_short +
        div_short +
        df['STEALTH_DISTRIB'].astype(int)
    )

    lead_total = lead_long + lead_short + 1e-9

    df['LEAD_BIAS'] = (lead_long - lead_short) / lead_total
    lead_norm = df['LEAD_BIAS']

    # ======================================================
    # 2️⃣ CONFIRMATION SCORE (reactive signals)
    # ======================================================

    confirm_score = (
        (df['VALID_BREAK_LONG'] | df['VALID_BREAK_SHORT']).astype(int) +
        df['STRONG_BODY'].astype(int) +
        df['ATR_EXPAND'].astype(int) +
        (df['COMPOSITE_PRESSURE'] > 0).astype(int)
    )

    mom = df['MOMENTUM_CONTINUITY']

    df['MOMENTUM_SCORE'] = (
        (mom - 0.5) / 0.2
    ).clip(-1, 1)

    confirm_norm = (confirm_score / 4) + (0.20 * df['MOMENTUM_SCORE'])

    # ===============================
    # RSI + VWAP RISK ADJUSTMENTS
    # ===============================
    rsi_long_ok, rsi_short_ok = rsi_risk_filter(df)
    vwap_long_ok, vwap_short_ok = anchored_vwap_risk(df)

    risk_penalty = (
        (~rsi_long_ok).astype(int) * 0.2 +
        (~rsi_short_ok).astype(int) * 0.2 +
        (~vwap_long_ok).astype(int) * 0.25 +
        (~vwap_short_ok).astype(int) * 0.25
    )

    fail_score = (
        (df['DIVERGENCE'] != 0).astype(int) +
        (df['UPPER_WICK_RATIO'] > df['DYNAMIC_WICK_LIMIT']).astype(int) +
        (df['LOWER_WICK_RATIO'] > df['DYNAMIC_WICK_LIMIT']).astype(int) +
        (df['PARTICIPATION'] == -1).astype(int)
    )

    fail_norm = (fail_score / 4) + risk_penalty

    # ======================================================
    # 4️⃣ STRUCTURAL CONTEXT WEIGHT
    # ======================================================

    trend_weight = df['TREND_QUALITY'].abs().clip(0, 1)

    # ======================================================
    # 5️⃣ FINAL ASYMMETRY CALCULATION
    # ======================================================

    micro_weight = 1 + df['MICRO_BREAK_SCORE'].clip(-0.6, 0.6)
    state_weight = (
        df['STATE_STABILITY'] *
        (1 - df['TRANSITION_FORCE'].clip(0,1)) *
        (1 + df['STATE_ACCEL'].clip(-0.5,0.5))
    )
    lead_dynamic_weight = 0.3 + 0.7 * df['TRANSITION_FORCE'].clip(0,1)

    df['ASYM_RAW'] = state_weight * micro_weight * trend_weight * (
        (lead_dynamic_weight * lead_norm) +
        (0.7 * confirm_norm) -
        fail_norm
    )

    # -----------------------------------------
    # Directional Asymmetry Injection
    # -----------------------------------------
    df['DIR'] = np.sign(
        df['COMPOSITE_PRESSURE'].ewm(span=3).mean()
    ).fillna(0)

    df['ASYM_RAW'] *= df['DIR']

    # ======================================================
    # 6️⃣ SMOOTHING (REGIME STABILITY)
    # ======================================================

    df['ASYM_SCORE'] = df['ASYM_RAW'].ewm(span=3, adjust=False).mean() # tune here <-

    # =========================
    # INSTITUTIONAL EXIT MODEL
    # =========================

    df = institutional_exit_model(df)

    LONG_CONDITION = (df['VALID_BREAK_LONG'])
    SHORT_CONDITION = (df['VALID_BREAK_SHORT'])

    df['STATE_DIRECTION'] = np.sign(df['STATE_SCORE'])

    LONG_CONDITION &= df['STATE_DIRECTION'] >= 0
    SHORT_CONDITION &= df['STATE_DIRECTION'] <= 0

    state_momentum_decay = (
        df['STATE_VELOCITY']
        .rolling(3)
        .mean()
    )

    LONG_CONDITION &= ~(state_momentum_decay < -0.05)
    SHORT_CONDITION &= ~(state_momentum_decay > 0.05)

    LONG_CONDITION &= HTF_LONG_OK
    SHORT_CONDITION &= HTF_SHORT_OK

    df['signal'] = 0
    df.loc[LONG_CONDITION, 'signal'] = 1
    df.loc[SHORT_CONDITION, 'signal'] = -1

    # ==========================================================
    # INSTITUTIONAL EXIT CONDITIONS
    # ==========================================================

    EXIT_THRESHOLD = 0.50

    EXIT_LONG = df['EXIT_PRESSURE'] > EXIT_THRESHOLD
    EXIT_SHORT = df['EXIT_PRESSURE'] > EXIT_THRESHOLD

    df['final_signal'] = df['signal']

    df.loc[(df['final_signal'] == 1) & EXIT_LONG, 'final_signal'] = 0
    df.loc[(df['final_signal'] == -1) & EXIT_SHORT, 'final_signal'] = 0

    # =========================
    # DIAGNOSTICS
    # =========================
    print("\n=== STATE DIAGNOSTICS ===")
    print("Phase counts:\n", df['PHASE'].value_counts())
    print("Breakouts: Long =", df['BREAK_RESISTANCE'].sum(), "Short =", df['BREAK_SUPPORT'].sum())
    print("Transition signals:", df['TRANSITION_SIGNAL'].value_counts())

    return df