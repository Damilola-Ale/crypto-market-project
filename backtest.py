import pandas as pd
import numpy as np

class SignalBacktester:
    def __init__(
        self,
        df,
        htf_df=None,
        lltf_df=None,
        initial_balance=1000,
        fixed_risk_per_trade=10.0,
        fee=0.0005,
        atr_period=14,
        atr_mult=1.5,
        be_trigger_r=1.2,
        trailing=False,
        leverage=1,
    ):
        self.df = df.copy()
        self.htf_df = htf_df.copy() if htf_df is not None else None

        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.fixed_risk = fixed_risk_per_trade
        self.fee = fee

        self.position = 0
        self.entry_price = None
        self.units = 0
        self.stop_loss = None
        self.trailing_stop = None

        self.be_activated = False
        self.be_trigger_r = be_trigger_r

        self.trades = []

        self.atr_period = atr_period
        self.atr_mult = atr_mult
        self.trailing = trailing

        self.leverage = max(1, leverage)
        self.max_bars_in_trade = 6          # ~6 hours max edge lifespan
        self.expansion_lookback = 3         # detect shrinking expansion
        self.trap_wick_ratio = 0.6          # wick dominance threshold
        self.trap_close_ratio = 0.3         # weak close threshold

        # ==========================================
        # 1H SIGNAL LOCK (prevents revenge entries)
        # ==========================================
        self.current_ltf_index = None      # tracks active 1H candle
        self.trade_taken_this_ltf = False  # did we already trade this 1H idea?

        # -------------------------------------------------
        # Align datasets to common start date
        # -------------------------------------------------
        if lltf_df is not None:
            self.lltf_df = lltf_df.copy()

            # Find common start timestamp across TFs
            start_time = max(self.df.index[0], self.lltf_df.index[0])

            # Trim BOTH datasets so they start together
            self.df = self.df[self.df.index >= start_time]
            self.lltf_df = self.lltf_df[self.lltf_df.index >= start_time]

            # Reset indices after trimming
            self.df = self.df.copy()
            self.lltf_df = self.lltf_df.copy()

            # -------------------------------------------------
            # Map every 5m candle to its parent 1h candle index
            # -------------------------------------------------
            self.lltf_df['final_signal'] = np.nan
            self.lltf_df['ltf_index'] = np.nan

            ltf_times = self.df.index

            for i in range(len(ltf_times)):
                start = ltf_times[i]
                end = ltf_times[i+1] if i+1 < len(ltf_times) else self.lltf_df.index[-1] + pd.Timedelta(seconds=1)

                mask = (self.lltf_df.index >= start) & (self.lltf_df.index < end)

                # ltf_index and signal both come from THIS 1H bar (index i).
                # This matches live: map_ltf_to_htf maps the 13:00 5m bar to
                # the 13:00 1H bar, and the signal on df at 13:00 is used.
                # The 13:00 5m bar is the first bar AFTER the 12:00 1H bar closes —
                # no lookahead, because generate_signal only runs on closed bars.
                # We zero bars strictly inside (13:05–13:55) — those are mid-bar.
                # Only the boundary bar (13:00) is a valid entry.
                self.lltf_df.loc[mask, 'ltf_index'] = i
                # Boundary bar (start) gets the signal; interior bars are zeroed
                boundary_mask = self.lltf_df.index == start
                interior_mask = mask & ~boundary_mask
                self.lltf_df.loc[boundary_mask & mask, 'final_signal'] = self.df['final_signal'].iloc[i]
                self.lltf_df.loc[interior_mask, 'final_signal'] = 0

            # Drop any candles that STILL didn't get mapped (safety)
            self.lltf_df = self.lltf_df.dropna(subset=['ltf_index'])

            # Now conversion is safe
            self.lltf_df['ltf_index'] = self.lltf_df['ltf_index'].astype(int)

        self._prepare_indicators()

    # ------------------------
    # Indicators
    # ------------------------
    def _prepare_indicators(self):
        df = self.df
        tr = pd.concat([
            df['high'] - df['low'],
            (df['high'] - df['close'].shift()).abs(),
            (df['low'] - df['close'].shift()).abs()
        ], axis=1).max(axis=1)
        df['ATR'] = tr.rolling(self.atr_period).mean()
        self.df = df

        # compute 5m ATR on lltf_df for opposite impulse exit
        if hasattr(self, 'lltf_df') and self.lltf_df is not None:
            lltf_tr = pd.concat([
                self.lltf_df['high'] - self.lltf_df['low'],
                (self.lltf_df['high'] - self.lltf_df['close'].shift()).abs(),
                (self.lltf_df['low'] - self.lltf_df['close'].shift()).abs()
            ], axis=1).max(axis=1)
            self.lltf_df['ATR_5M'] = lltf_tr.ewm(span=self.atr_period, adjust=False).mean()

    # ==========================================================
    # TRADE LIFECYCLE SETTINGS
    # ==========================================================

    INCUBATION_BARS = 6        # 30 minutes (6×5m)
    VALIDATION_BARS = 18       # 90 minutes total
    PRESSURE_BARS = 6          # stop proximity exit

    NO_FOLLOW_MFE = 0.3        # 0.3R required after validation
    STOP_PROXIMITY = 0.2       # within 0.2R of stop = danger

    ATR_INIT_MULT = 1.5
    ATR_AFTER_HALF_R = 1.0

    def get_5m_window(self, entry_time, current_time):
        df = self.lltf_df if hasattr(self, 'lltf_df') else self.df
        # Exclude the entry bar itself — OIE should only evaluate bars
        # after entry, matching live where bar_history starts appending
        # from the first bar AFTER the entry candle.
        window = df.loc[entry_time:current_time]
        if len(window) > 1:
            return window.iloc[1:]  # drop entry bar
        return window
    
    def opposite_impulse_exit(self, window, side, trade=None):
        if len(window) < 3:
            return False

        last = window.iloc[-1]

        # ══════════════════════════════════════════
        # 1. ATR — use 5m ATR for candle body comparison
        # ══════════════════════════════════════════
        if "ATR_5M" in window.columns:
            atr = window["ATR_5M"].iloc[-3:].mean()
            if pd.isna(atr) or atr <= 0:
                atr = window["ATR_5M"].iloc[0]
        else:
            atr = None

        if atr is None or pd.isna(atr) or atr <= 0:
            atr_1h = window["ATR"].iloc[-3:].mean() if "ATR" in window.columns else (window['high'] - window['low']).mean()
            if pd.isna(atr_1h) or atr_1h <= 0:
                atr_1h = window["ATR"].iloc[0] if "ATR" in window.columns else float("nan")
            if pd.isna(atr_1h) or atr_1h <= 0:
                return False
            atr = atr_1h * 0.20

        if pd.isna(atr) or atr <= 0:
            return False

        # ══════════════════════════════════════════
        # 2. BODY SIZE
        # ══════════════════════════════════════════
        body = abs(last.close - last.open)
        # Also check the last 2 bars together — gradual reversals
        # don't always produce one big candle, but two medium ones
        # in the wrong direction are equally meaningful.
        two_bar_move = abs(window.iloc[-1].close - window.iloc[-2].close) if len(window) >= 2 else 0
        big_candle = (body > atr * 1.2) or (two_bar_move > atr * 1.5 and body > atr * 0.6)

        # ══════════════════════════════════════════
        # 3. DIRECTION CHECK
        # ══════════════════════════════════════════
        if side == 1:
            wrong_direction = last.close < last.open
        else:
            wrong_direction = last.close > last.open

        # ══════════════════════════════════════════
        # 4. CLOSE LOCATION
        # ══════════════════════════════════════════
        location_blocked = False
        if trade is not None:
            entry = trade["entry_price"]
            stop  = trade["stop_loss"]
            initial_stop = trade.get("initial_stop", stop)
            # use initial_stop as R anchor — matches lifecycle.py exactly
            R = abs(entry - initial_stop)
            if R == 0:
                R = abs(entry - stop)

            if R > 0:
                    mfe_r = trade.get("mfe_r", 0.0)
                    pnl_r_now = trade.get("pnl_r", 0.0)

                    # close_to_initial_stop_r: distance from the ORIGINAL
                    # stop, not the live (trailing) stop_loss. Previously
                    # this guard read trade["stop_loss"] directly, which
                    # moves once update_dynamic_stop's trail activates
                    # (mfe_r >= 0.5). As the trail tightens, that live
                    # stop creeps toward price, shrinking close_to_stop_r
                    # even though the trade's risk position relative to
                    # its ORIGINAL stop hasn't changed at all — this let
                    # OIE fire on healthy, well-protected winners purely
                    # because the trail had moved, not because anything
                    # about the trade changed. Anchoring to initial_stop
                    # decouples OIE's guard from any future trailing-stop
                    # tuning, so trail experiments can no longer silently
                    # weaken this guard the way the 0.15-0.5R tier did.
                    #
                    # Since R = abs(entry - initial_stop), distance from
                    # initial_stop in R-units is always pnl_r_now + 1.0
                    # (true for both long and short, since pnl_r_now is
                    # already side-corrected) — no separate close/stop
                    # arithmetic needed.
                    close_to_initial_stop_r = pnl_r_now + 1.0

                    # Block OIE when the trade is comfortably above the
                    # ORIGINAL stop AND has positive pnl. Same 0.75R
                    # logic as before, just measured against a fixed
                    # reference point instead of a moving one.
                    location_blocked = (close_to_initial_stop_r > 1.75 and pnl_r_now > 0.0)

        # ══════════════════════════════════════════
        # 5. VOLUME CONFIRMATION
        # ══════════════════════════════════════════
        vol_blocked = False
        if "volume" in window.columns:
            avg_vol = window["volume"].iloc[-10:].mean()
            last_vol = last.volume
            if len(window) >= 10 and not pd.isna(avg_vol) and avg_vol > 0:
                if last_vol < avg_vol * 0.8:
                    vol_blocked = True

        return big_candle and wrong_direction and not location_blocked and not vol_blocked
        
    def stop_pressure_exit(self, window, stop_price, side):
        if len(window) < self.PRESSURE_BARS:
            return False

        recent = window.iloc[-self.PRESSURE_BARS:]
        if side == 1:
            dist = (recent.close - stop_price)
        else:
            dist = (stop_price - recent.close)

        return (dist <= self.R * self.STOP_PROXIMITY).all()
    
    def no_follow_through_exit(self, mfe_r, bars_in_trade):
        if bars_in_trade < self.VALIDATION_BARS:
            return False
        return mfe_r < self.NO_FOLLOW_MFE
    
    def stall_exit(self, trade, bars_in_trade):
        entry        = trade.get("entry_price", 0)
        initial_stop = trade.get("initial_stop", trade.get("stop_loss", entry))
        R = abs(entry - initial_stop)
        if R <= 0:
            return False

        mfe_r = trade.get("mfe_r", 0.0)
        mae_r = abs(trade.get("_price_mae", 0.0)) / R

        # If the trailing stop has activated (mfe_r >= 0.3), that mechanism
        # owns this trade. Stall exit only applies to trades that never moved.
        if mfe_r >= 0.30:
            return False

        mae_dominates = mae_r > mfe_r * 2.0

        # Bar 3 (15 min): price went wrong way twice as fast as right way
        if bars_in_trade == 3 and mfe_r < 0.10 and mae_dominates:
            return True

        # Bar 6 (30 min): same condition, slightly wider MFE tolerance
        if bars_in_trade == 6 and mfe_r < 0.15 and mae_dominates:
            return True

        # Bar 12 (60 min): an hour with no trail activation and MAE building
        if bars_in_trade == 12 and mfe_r < 0.20 and mae_r > 0.25:
            return True

        # Bar 18 (90 min): final check — only fires if trail never activated
        if bars_in_trade == self.VALIDATION_BARS and mfe_r < 0.25 and mae_r > 0.20:
            return True

        return False
    
    # Calibrated for front-loaded edge profile (peak at bar 3 / 15 min).
    # Trail activates at 0.3R — early enough to catch the peak,
    # late enough to survive normal entry noise on 5m bars.
    # Breakeven lock only at 0.5R+ to avoid instant stop-to-entry.
    # NOTE: lowering this activation threshold to 0.2R was tried and
    # reverted — it clipped a much larger population of normal winners
    # that dip through 0.2-0.5R before continuing, costing far more
    # than the bleed-back leak it fixed. See stall_exit in
    # _check_intrabar for the narrower fix targeting that leak directly.
    ATR_AFTER_ENTRY   = 1.5   # standard trail before 0.5R
    ATR_AFTER_HALF_R  = 0.55   # tighter once 0.5R secured
    ATR_AFTER_ONE_R   = 0.4   # tight once 1R secured

    # Mirrors PositionManager.BINANCE_CALLBACK_FLOOR_PCT in lifecycle.py.
    # Backtest must respect the same exchange-side minimum stop distance
    # live does — otherwise backtest is testing a trail tightness that's
    # not actually achievable on Binance, and the two environments will
    # keep diverging on exactly which bar a trade exits.
    BINANCE_CALLBACK_FLOOR_PCT = 0.0016

    def update_dynamic_stop(self, trade, current_price, atr):
        mfe_r = trade.get('mfe_r', 0.0)

        # Activate at 0.3R — gives one to two 5m bars of breathing room
        # before locking in, while still catching the bar-3 peak.
        if mfe_r < 0.5:
            return

        bars = trade.get('bars_in_trade', 0)
        last_trail_bar = trade.get('last_trail_bar', 0)

        # Once past 0.5R, update every bar to lock in gains quickly.
        # Before 0.5R, update every 2 bars to avoid instant breakeven stops.
        cadence = 1 if mfe_r >= 0.5 else 2
        if bars - last_trail_bar < cadence:
            return
        trade['last_trail_bar'] = bars

        entry = trade['entry_price']
        current_stop = trade['stop_loss']
        side = trade['side']

        if mfe_r >= 1.0:
            atr_mult = self.ATR_AFTER_ONE_R
        elif mfe_r >= 0.5:
            atr_mult = self.ATR_AFTER_HALF_R
        else:
            atr_mult = self.ATR_AFTER_ENTRY

        # Same floor logic as live's PositionManager._update_dynamic_stop —
        # ATR multipliers unchanged, but the trail distance can never be
        # tighter than what Binance's amend_stop would allow in practice.
        floor_distance = current_price * self.BINANCE_CALLBACK_FLOOR_PCT
        atr_distance    = atr * atr_mult
        trail_distance  = max(atr_distance, floor_distance)

        if side == 1:
            trail_candidate = current_price - trail_distance
            # Only lock to breakeven once 0.5R is secured, not before
            if mfe_r >= 0.5:
                trail_candidate = max(trail_candidate, entry)
            new_stop = max(current_stop, trail_candidate)
        else:
            trail_candidate = current_price + trail_distance
            if mfe_r >= 0.5:
                trail_candidate = min(trail_candidate, entry)
            new_stop = min(current_stop, trail_candidate)

        trade['stop_loss'] = new_stop
        self.stop_loss = new_stop

    # ------------------------
    # Position sizing
    # ------------------------
    def _calc_units(self, entry, stop):
        risk_per_unit = abs(entry - stop)
        if risk_per_unit == 0:
            return 0
        return self.fixed_risk / risk_per_unit

    # ------------------------
    # Entry
    # ------------------------
    def _enter(self, side, price, idx):
        if hasattr(self, 'lltf_df'):
            ltf_idx = self.lltf_df['ltf_index'].iloc[idx]
            # ATR for stop placement comes from the bar that generated the signal
            # (the previous closed 1H bar) — matches live where ATR is forward-filled
            # from the last closed 1H bar onto the entry 5m bar.
            atr_idx = max(0, ltf_idx - 1)
        else:
            ltf_idx = idx
            atr_idx = idx
        atr = self.df['ATR'].iloc[atr_idx]
        if np.isnan(atr):
            return

        if side == 1:
            stop = price - self.atr_mult * atr
        else:
            stop = price + self.atr_mult * atr

        units = self._calc_units(price, stop)
        if units <= 0:
            return

        self.position = side
        self.entry_price = price
        self.stop_loss = stop
        self.trailing_stop = stop
        self.units = units
        self.be_activated = False

        self.current_trade = {
            "side": side,
            "entry_idx": idx,
            "entry_time": self.lltf_df.index[idx] if hasattr(self, 'lltf_df') else self.df.index[idx],
            "entry_price": price,
            "units": units,
            "stop_loss": stop,
            "initial_stop": stop,
            "ATR": atr,
            "MAE": 0.0,
            "MFE": 0.0,
            "bars_in_trade": 1,   # entry bar counts as bar 1, matching live
            "last_trail_bar": 0,
            "mfe_peak_bar": 1,    # bar where _price_mfe last made a new high
            "mfe_r": 0.0,
            "pnl_r": 0.0,
        }

        # Fee applies to notional, not margin — so leverage increases fee cost
        self.balance -= abs(units * price) * self.fee
        
        # Liquidation price tracking
        margin_per_unit = price / self.leverage
        if side == 1:
            self.liquidation_price = price - margin_per_unit * 0.9
        else:
            self.liquidation_price = price + margin_per_unit * 0.9

    # ------------------------
    # Exit
    # ------------------------
    def _exit(self, price, idx, reason):
        raw_pnl = (
            (price - self.entry_price) * self.units
            if self.position == 1 else
            (self.entry_price - price) * self.units
        )

        # Stop loss and liquidation are capped at -1R by definition
        # All other exits (winners, early exits) are amplified by leverage
        if reason in ("stop_loss", "break_even", "liquidated"):
            pnl = raw_pnl
        else:
            pnl = raw_pnl * self.leverage

        if reason == "stop_loss" and self.be_activated:
            reason = "break_even"

        self.balance += pnl
        self.balance -= abs(self.units * price) * self.fee * self.leverage
        self.liquidation_price = None

        entry_i = self.current_trade["entry_idx"]
        bars_held = idx - entry_i

        exit_time = self.lltf_df.index[idx] if hasattr(self, 'lltf_df') else self.df.index[idx]
        entry_time = self.current_trade["entry_time"]

        hours_held = (exit_time - entry_time).total_seconds() / 3600

        entry  = self.current_trade["entry_price"]
        stop   = self.current_trade.get("initial_stop", self.current_trade["stop_loss"])
        R_exit = abs(entry - stop)
        pnl_r_final = (
            ((price - entry) / R_exit) if self.position == 1
            else ((entry - price) / R_exit)
        ) if R_exit > 0 else 0.0

        self.current_trade.update({
            "exit_price": price,
            "exit_idx": idx,
            "exit_time": exit_time,
            "bars_held": bars_held,
            "hours_held": hours_held,
            "pnl": pnl,
            "pnl_r": pnl_r_final,
            "exit_reason": reason
        })
        self.trades.append(self.current_trade)

        self.position = 0
        self.entry_price = None
        self.units = 0
        self.be_activated = False
        self.trade_taken_this_ltf = True

    # ------------------------
    # Excursion tracking
    # ------------------------
    def _update_excursions(self, high, low):
        if self.position == 0:
            return

        if self.position == 1:
            self.current_trade["MAE"] = min(self.current_trade["MAE"], (low  - self.entry_price) * self.units)
            self.current_trade["MFE"] = max(self.current_trade["MFE"], (high - self.entry_price) * self.units)
        else:
            self.current_trade["MAE"] = min(self.current_trade["MAE"], (self.entry_price - high) * self.units)
            self.current_trade["MFE"] = max(self.current_trade["MFE"], (self.entry_price - low)  * self.units)
    
    def _exec_df(self):
        return self.lltf_df if hasattr(self, 'lltf_df') else self.df
        
    # ------------------------
    # Candle anatomy
    # ------------------------
    def _upper_wick(self, i):
        df = self._exec_df()
        row = df.iloc[i]
        return row['high'] - max(row['open'], row['close'])

    def _lower_wick(self, i):
        df = self._exec_df()
        row = df.iloc[i]
        return min(row['open'], row['close']) - row['low']

    def _body_size(self, i):
        df = self._exec_df()
        row = df.iloc[i]
        return abs(row['close'] - row['open'])
    
    # ------------------------
    # Expansion Failure Exit
    # ------------------------
    def _momentum_decay_exit(self, i):
        if self.position == 0:
            return False

        ltf_idx = self.lltf_df['ltf_index'].iloc[i] if hasattr(self, 'lltf_df') else i
        row = self.df.iloc[ltf_idx]

        # trend energy collapsing
        continuation = row['CONTINUATION_STRENGTH']
        velocity     = row['CONTINUATION_VELOCITY']
        stability    = row['STATE_STABILITY']

        if np.isnan(continuation):
            return False

        # core idea:
        # expansion strength is fading + regime stability dropping
        energy_decay = (
            (continuation < 0) or
            (velocity < -0.15)
        )

        regime_breakdown = stability < 0.35

        return energy_decay and regime_breakdown
        
    # ------------------------
    # Trap / Absorption Exit
    # ------------------------
    def _liquidity_reversal_exit(self, i):
        if self.position == 0:
            return False

        ltf_idx = self.lltf_df['ltf_index'].iloc[i] if hasattr(self, 'lltf_df') else i
        row = self.df.iloc[ltf_idx]

        flow = row['FLOW_STRENGTH']
        pressure = row['COMPOSITE_PRESSURE']

        if self.position == 1:
            return (flow < -0.5) and (pressure < 0)

        else:
            return (flow > 0.5) and (pressure > 0)
        
    # ------------------------
    # Time Decay Exit
    # ------------------------
    def _structural_exhaustion_exit(self, i):
        if self.position == 0:
            return False

        ltf_idx = self.lltf_df['ltf_index'].iloc[i] if hasattr(self, 'lltf_df') else i
        row = self.df.iloc[ltf_idx]

        trend_quality = row['TREND_QUALITY']
        transition    = row['TRANSITION_FORCE']

        if np.isnan(trend_quality):
            return False

        # strong trend suddenly enters transition regime
        return (trend_quality > 0.8) and (transition > 1.5)
    
    def _is_new_ltf_candle(self, i):
        if not hasattr(self, 'lltf_df'):
            return True
        if i == 0:
            return False
        return self.lltf_df['ltf_index'].iloc[i] != self.lltf_df['ltf_index'].iloc[i-1]

    # ------------------------
    # Intrabar management
    # ------------------------
    def _check_intrabar(self, high, low, idx):
        if self.position == 0:
            return

        trade = self.current_trade
        side  = trade['side']

        exec_df      = self.lltf_df if hasattr(self, 'lltf_df') else self.df
        current_time = exec_df.index[idx]

        # Best price this bar (matches lifecycle.py convention)
        current_price = high if side == 1 else low

        # ── MFE / MAE tracking ──
        R = abs(trade["entry_price"] - trade.get("initial_stop", trade["stop_loss"]))
        if R == 0:
            R = abs(trade["entry_price"] - trade["stop_loss"])
        self.R = R

        if side == 1:
            price_mfe = high - trade["entry_price"]
            price_mae = low  - trade["entry_price"]
        else:
            price_mfe = trade["entry_price"] - low
            price_mae = trade["entry_price"] - high

        _prior_price_mfe = trade.get("_price_mfe", 0.0)
        _new_price_mfe   = max(_prior_price_mfe, price_mfe)
        if _new_price_mfe > _prior_price_mfe:
            trade["mfe_peak_bar"] = trade.get("bars_in_trade", 0)
        trade["_price_mfe"] = _new_price_mfe
        trade["_price_mae"] = min(trade.get("_price_mae", 0.0), price_mae)

        # dollar excursions for diagnostics (used by _update_excursions output)
        if side == 1:
            trade["MAE"] = min(trade.get("MAE", 0.0), (low  - trade["entry_price"]) * self.units)
            trade["MFE"] = max(trade.get("MFE", 0.0), (high - trade["entry_price"]) * self.units)
        else:
            trade["MAE"] = min(trade.get("MAE", 0.0), (trade["entry_price"] - high) * self.units)
            trade["MFE"] = max(trade.get("MFE", 0.0), (trade["entry_price"] - low)  * self.units)

        mfe_r = trade["_price_mfe"] / R if R > 0 else 0.0
        pnl_r = (
            (current_price - trade["entry_price"]) / R if side == 1
            else (trade["entry_price"] - current_price) / R
        ) if R > 0 else 0.0

        trade["pnl_r"] = pnl_r
        trade["mfe_r"] = mfe_r

        # ── 5m window for exit checks ──────────────────────────
        window_5m = self.get_5m_window(trade["entry_time"], current_time)

        # ── EXIT AUDIT (last 12 bars = 1h, printed after run) ──
        _mae_r_for_audit = abs(trade.get("_price_mae", 0.0)) / R if R > 0 else 0.0
        _exec_row = exec_df.iloc[idx]
        _atr_5m_val = exec_df["ATR_5M"].iloc[idx] if "ATR_5M" in exec_df.columns else None
        if not pd.isna(_atr_5m_val) and _atr_5m_val > 0:
            pass
        else:
            _atr_5m_val = None
        if not hasattr(self, "_audit_log"):
            self._audit_log = []
        self._audit_log.append({
            "ts": current_time,
            "side": side,
            "bars": trade.get("bars_in_trade", 0),
            "mfe_r": mfe_r,
            "pnl_r": pnl_r,
            "mae_r": _mae_r_for_audit,
            "bar_open": float(_exec_row["open"]),
            "bar_high": float(_exec_row["high"]),
            "bar_low":  float(_exec_row["low"]),
            "bar_close": float(_exec_row["close"]),
            "stop_loss": trade["stop_loss"],
            "initial_stop": trade.get("initial_stop", trade["stop_loss"]),
            "R": R,
            "atr_5m": _atr_5m_val,
            "window_5m": window_5m.copy(),
        })

        # ── DYNAMIC TRAILING STOP (mirrors lifecycle.py) ───────
        if "ATR_5M" in exec_df.columns:
            atr = exec_df["ATR_5M"].iloc[idx]
        else:
            atr = exec_df["ATR"].iloc[idx] * 0.20
        if pd.isna(atr) or atr <= 0:
            atr = R * 0.20  # last resort fallback

        self.update_dynamic_stop(trade, current_price, atr)

        # ── DISASTER STOP DIAGNOSTIC ────────────────────────────────────
        # Tracks two distinct populations that a -0.6R floor would catch:
        #
        #   Scenario A (pure loser): mfe_r never reached 0.15R before
        #   pnl_r crossed -0.6R. Trade went wrong immediately and drifted
        #   toward the stop without ever showing real edge.
        #
        #   Scenario B (violent rejection): mfe_r DID reach 0.15–0.5R
        #   before pnl_r crossed -0.6R. Trade proved some edge then got
        #   violently reversed before the trail activated. The more
        #   dangerous case — felt like a winner until it wasn't.
        #
        # For each trade, records the FIRST bar where pnl_r crosses -0.6R
        # (so we get one row per trade, not one row per bar). Then tracks
        # whether pnl_r ever recovered above -0.6R after that crossing —
        # if it did, a hard exit at -0.6R would have been a false positive.
        # If it didn't, the exit would have saved the difference between
        # -0.6R and the actual final pnl_r.
        if not hasattr(self, "_disaster_log"):
            self._disaster_log = {}   # keyed by entry_time str, one entry per trade

        _entry_key = str(trade.get("entry_time", idx))
        DISASTER_THRESHOLD = -0.6

        if pnl_r <= DISASTER_THRESHOLD and mfe_r < 0.5:
            if _entry_key not in self._disaster_log:
                # First bar this trade crossed the threshold — record it
                _scenario = (
                    "B_violent_rejection" if mfe_r >= 0.15
                    else "A_pure_loser"
                )
                self._disaster_log[_entry_key] = {
                    "entry_time":     _entry_key,
                    "side":           side,
                    "scenario":       _scenario,
                    "mfe_r_at_cross": mfe_r,
                    "pnl_r_at_cross": pnl_r,
                    "bars_to_cross":  trade.get("bars_in_trade", 0),
                    "recovered":      False,   # updated below if price comes back
                    "final_pnl_r":    pnl_r,  # will be overwritten on exit
                    "exit_reason":    None,    # will be overwritten on exit
                }
            else:
                # Already crossed — check if price recovered above threshold
                if pnl_r > DISASTER_THRESHOLD:
                    self._disaster_log[_entry_key]["recovered"] = True

        # Keep final_pnl_r and exit_reason current on every bar after crossing
        # so when the trade exits we have accurate final values without needing
        # a separate hook into _exit().
        if _entry_key in self._disaster_log:
            self._disaster_log[_entry_key]["final_pnl_r"] = pnl_r
            # exit_reason will remain None until we read it from trades_df
            # in the run() reporting block — no access to it here yet.

        # ── DISASTER STOP — hard floor at -0.6R when trail inactive ────
        # Two populations this catches, both requiring mfe_r < 0.5 (trail
        # not yet activated — once trail is live it owns the trade):
        #
        #   Scenario A (pure loser): mfe_r stayed below 0.15R the whole
        #   time. Trade went wrong immediately, grinding toward -1R with
        #   no evidence of edge. 100% true-positive rate across ICX/FIL/ZEC.
        #
        #   Scenario B (violent rejection): mfe_r reached 0.15–0.5R then
        #   price reversed hard to -0.6R within 12 bars. The 12-bar guard
        #   is critical — the one false positive in ZEC (2024-10-21) took
        #   41 bars to reach -0.6R, meaning it was a slow grind, not a
        #   violent rejection. All genuine violent rejections in the sample
        #   (FIL: bars 14/14/16/26) crossed within that window.
        #
        # Diagnostic log is still running in parallel — exits labeled
        # "disaster_stop" will appear in the scenario analysis so you can
        # verify the rule fires only on true positives going forward.
        #
        # This check runs BEFORE all other soft exits so the price saved
        # (difference between -0.6R and the actual hard stop at -1R) is
        # never eroded by waiting for OIE/thesis_invalid to fire first.
        if mfe_r < 0.5 and pnl_r <= -0.6:
            _bars_in = trade.get("bars_in_trade", 0)
            _scenario_a = mfe_r < 0.15
            _bars_since_peak = _bars_in - trade.get("mfe_peak_bar", _bars_in)
            _scenario_b = mfe_r >= 0.15 and _bars_since_peak <= 6

            if _scenario_a or _scenario_b:
                entry_time_str = trade.get("entry_time", "?")
                _which = "A_pure_loser" if _scenario_a else "B_violent_rejection"
                # print(f"[DISASTER_STOP] {entry_time_str} | side={side} | "
                #       f"scenario={_which} | "
                #       f"mfe_r={mfe_r:.3f} pnl_r={pnl_r:.3f} bars={_bars_in} | "
                #       f"exit_price={current_price:.5f} R={R:.5f}")
                self._exit(current_price, idx, "disaster_stop")
                return

        # ── STALL EXIT — catches slow bleed trades OIE never sees ──
        # bars_in_trade = trade.get("bars_in_trade", 0)
        # if self.stall_exit(trade, bars_in_trade):
        #     self._exit(current_price, idx, "stall_exit")
        #     return

        # ── DOMINANCE EXIT ──────────────────────────────────────────────
        # Fires when adverse movement is clearly dominating favorable
        # movement — not just when MFE is exactly zero.
        # Bar 3 (15 min): MAE > 0.30R and ratio > 3.0
        #   → adverse is 3x the favorable move, trade hasn't proved itself
        # Bar 6 (30 min): MAE > 0.40R and ratio > 2.5
        #   → two full 1H candles held, still no evidence of edge
        _bars = trade.get("bars_in_trade", 0)
        _mae_r = abs(trade.get("_price_mae", 0.0)) / R if R > 0 else 0.0
        _pressure_ratio = _mae_r / max(mfe_r, 0.01)

        if _bars == 3 and mfe_r == 0.0 and _mae_r > 0.50:
            entry_time_str = trade.get("entry_time", "?")
            # print(f"[DOMINANCE_EXIT BAR3] {entry_time_str} | side={trade['side']} | "
            #       f"mfe_r={mfe_r:.3f} mae_r={_mae_r:.3f} ratio={_pressure_ratio:.2f} | "
            #       f"exit_price={current_price:.5f} | "
            #       f"stop_was={trade['stop_loss']:.5f} R={R:.5f}")
            self._exit(current_price, idx, "dominance_exit")
            return

        # ── TRAP REJECTION EXIT ──────────────────────────────────────
        # Catches bars where the trade had some MFE (so dominance_exit
        # doesn't fire) but the adverse move is so extreme relative to
        # the favorable move that the candle is effectively a trap.
        # Only fires in bars 1–2 to avoid interfering with normal
        # multi-bar trades that dip before continuing.
        #
        # Conditions:
        #   _mae_r > 1.2R  — adverse move is already larger than the
        #                     original stop, meaning the bar blew through
        #                     the stop on a wick (gap/spike scenario)
        #   ratio > 4.0    — adverse is at least 4× the favorable move
        #                     (protects volatile winners like MFE=1.5R /
        #                      MAE=1.6R which have ratio ≈ 1.1 and survive)
        #
        # The ratio guard is the key discriminator:
        #   MFE=0.23R MAE=2.80R → ratio=12.2 → FIRES   (pure rejection)
        #   MFE=0.60R MAE=1.48R → ratio=2.47 → SURVIVES (had real move)
        #   MFE=1.50R MAE=1.60R → ratio=1.07 → SURVIVES (volatile winner)
        if _bars <= 2 and _mae_r > 1.2:
            _trap_ratio = _mae_r / max(mfe_r, 0.05)
            if _trap_ratio > 4.0:
                entry_time_str = trade.get("entry_time", "?")
                print(f"[TRAP_REJECTION] {entry_time_str} | side={trade['side']} | "
                      f"bars={_bars} mfe_r={mfe_r:.3f} mae_r={_mae_r:.3f} "
                      f"trap_ratio={_trap_ratio:.2f} | "
                      f"exit_price={current_price:.5f} R={R:.5f}")
                self._exit(current_price, idx, "trap_rejection")
                return

        # ── THESIS INVALIDATION EXIT ──────────────────────────────────
        # Replaces time-based slow_bleed_exit with a price-structure exit.
        #
        # The old approach used bars_since_peak — a proxy for stagnation
        # that fires at different points in a trade's life depending on
        # symbol tempo (ICX reverses in 5 bars, LINK drifts 50-90 bars).
        # That mismatch is why no single BARS threshold improved all symbols.
        #
        # This version asks the question directly:
        #   "Is this trade underwater AND nearly at the stop?"
        # which is symbol-agnostic and denominated in R.
        #
        # Three conditions must ALL be true:
        #   1. mfe_r in 0.15–0.5: trade showed real but limited promise
        #   2. pnl_r < -0.10:     trade is now meaningfully underwater
        #   3. stop_proximity_r < 0.25: less than 0.25R remains before
        #      the original stop — thesis is about to be formally proven
        #      wrong; exit now rather than wait for the stop to confirm it
        #
        # Additionally requires drawdown_from_peak > 0.35 to ensure price
        # has actually reversed from its high, not just entered from a
        # low base. Prevents firing on trades that were always losing.
        if 0.15 <= mfe_r < 0.5:
            if side == 1:
                stop_proximity_r = (current_price - trade.get("initial_stop", trade["stop_loss"])) / R
            else:
                stop_proximity_r = (trade.get("initial_stop", trade["stop_loss"]) - current_price) / R

            drawdown_from_peak = mfe_r - pnl_r

            if pnl_r < -0.10 and stop_proximity_r < 0.25 and drawdown_from_peak > 0.35:
                entry_time_str = trade.get("entry_time", "?")
                # print(f"[THESIS_INVALID_EXIT] {entry_time_str} | side={trade['side']} | "
                #       f"mfe_r={mfe_r:.3f} pnl_r={pnl_r:.3f} | "
                #       f"stop_proximity_r={stop_proximity_r:.3f} drawdown_from_peak={drawdown_from_peak:.3f} | "
                #       f"exit_price={current_price:.5f} R={R:.5f}")
                self._exit(current_price, idx, "thesis_invalidation_exit")
                return

        # ── STOP PROXIMITY EXIT ──────────────────────────────────────
        # Fires when the trade is already substantially underwater AND
        # within a small buffer of the original stop.
        # Rationale: if pnl_r is already -0.35R and the stop is only
        # 0.20R away, the stop will confirm the failure within 1-2 bars
        # anyway — exit now at a marginally better price rather than
        # waiting for the hard stop.
        #
        # Intentionally separate from THESIS_INVALID_EXIT:
        #   - Thesis invalid targets 0.15–0.5R MFE trades specifically
        #   - This targets any trade (including mfe_r=0) that is bleeding
        #     toward the stop slowly without triggering OIE or dominance
        #
        # Guards that prevent firing on healthy trades:
        #   pnl_r < -0.35   — trade must already be meaningfully wrong
        #   proximity < 0.20 — within 0.20R of the original stop
        #   mfe_r < 0.5     — trail hasn't activated; once trail is live
        #                     it owns the trade, this exit steps aside
        #
        # Debug: [STOP_PROXIMITY_EXIT] lines show every fire with full
        # state so you can audit if it's cutting anything it shouldn't.
        if mfe_r < 0.5 and pnl_r < -0.35:
            if side == 1:
                prox_r = (current_price - trade.get("initial_stop", trade["stop_loss"])) / R
            else:
                prox_r = (trade.get("initial_stop", trade["stop_loss"]) - current_price) / R

            if prox_r < 0.20:
                entry_time_str = trade.get("entry_time", "?")
                print(f"[STOP_PROXIMITY_EXIT] {entry_time_str} | side={trade['side']} | "
                      f"mfe_r={mfe_r:.3f} pnl_r={pnl_r:.3f} prox_r={prox_r:.3f} | "
                      f"bars={_bars} exit_price={current_price:.5f} R={R:.5f}")
                self._exit(current_price, idx, "stop_proximity_exit")
                return

        # ── OPPOSITE IMPULSE EXIT (matches lifecycle.py exactly)
        if self.opposite_impulse_exit(window_5m, side, trade=trade):
            if side == 1:
                oie_stop_proximity = (current_price - trade.get("initial_stop", trade["stop_loss"])) / R
            else:
                oie_stop_proximity = (trade.get("initial_stop", trade["stop_loss"]) - current_price) / R
            oie_drawdown_from_peak = mfe_r - pnl_r
            entry_time_str = trade.get("entry_time", "?")
            # print(f"[OIE_FIRE] {entry_time_str} | side={trade['side']} | "
            #       f"mfe_r={mfe_r:.3f} pnl_r={pnl_r:.3f} | "
            #       f"stop_proximity_r={oie_stop_proximity:.3f} drawdown_from_peak={oie_drawdown_from_peak:.3f} | "
            #       f"bars={_bars} exit_price={current_price:.5f}")
            self._exit(current_price, idx, "opposite_impulse")
            return

        # ── LIQUIDATION (leverage only) ─────────────────────────
        if (self.leverage > 1
                and hasattr(self, 'liquidation_price')
                and self.liquidation_price is not None):
            if side == 1 and low <= self.liquidation_price:
                self._exit(self.liquidation_price, idx, "liquidated")
                return
            elif side == -1 and high >= self.liquidation_price:
                self._exit(self.liquidation_price, idx, "liquidated")
                return

        # ── HARD STOP — checked before impulse exit ─────────────
        if side == 1 and low <= trade["stop_loss"]:
            self._exit(trade["stop_loss"], idx, "stop_loss")
            return
        elif side == -1 and high >= trade["stop_loss"]:
            self._exit(trade["stop_loss"], idx, "stop_loss")
            return

        # increment AFTER all exit checks — matches lifecycle.py ordering
        trade["bars_in_trade"] = trade.get("bars_in_trade", 0) + 1

    # ------------------------
    # Run backtest
    # ------------------------

    def _counterfactual_would_stop(self, trade, exec_df):
        exit_idx = int(trade["exit_idx"])
        stop     = trade["initial_stop"]
        side     = trade["side"]
        future   = exec_df.iloc[exit_idx + 1 : exit_idx + 49]
        if future.empty:
            return False
        if side == 1:
            return (future["low"] <= stop).any()
        else:
            return (future["high"] >= stop).any()
        
    
    def run(self):
        df_5m = self.lltf_df if hasattr(self, 'lltf_df') else self.df
        equity = []
        timestamps = []

        for i in range(len(df_5m) - 1):
            ltf_idx = df_5m['ltf_index'].iloc[i] if hasattr(self, 'lltf_df') else i

            if self.current_ltf_index is None:
                self.current_ltf_index = ltf_idx

            if ltf_idx != self.current_ltf_index:
                # New 1H candle = new trading opportunity
                self.current_ltf_index = ltf_idx
                self.trade_taken_this_ltf = self.position != 0  # ← fix

            signal = df_5m['final_signal'].iloc[i]

            o = df_5m['open'].iloc[i]
            h = df_5m['high'].iloc[i]
            l = df_5m['low'].iloc[i]

            # 2. ENTER (only one trade allowed per 1H candle)
            if self.position == 0 and not self.trade_taken_this_ltf:
                if signal == 1:
                    self._enter(1, o, i)
                elif signal == -1:
                    self._enter(-1, o, i)

            # 3. Update excursions
            if self.position != 0:
                self._update_excursions(h, l)

            # 4. Intrabar exits (trap, expansion failure, time decay, stops)
            if self.position != 0 and (i - self.current_trade["entry_idx"]) >= 1:
                self._check_intrabar(h, l, i)

            equity.append(self.balance)
            timestamps.append(df_5m.index[i])

        if self.position != 0:
            self._exit(df_5m['close'].iloc[-1], len(df_5m) - 1, "end_of_data")

        equity_df = pd.DataFrame({
            "timestamp": timestamps,
            "equity":    equity
        }).set_index("timestamp")

        trades_df = pd.DataFrame(self.trades)
        if not trades_df.empty:
            trades_df["direction"] = trades_df["side"].map({1: "LONG", -1: "SHORT"})
            trades_df["pnl_pct"]   = trades_df["pnl"] / self.initial_balance * 100
            trades_df["truncated"] = trades_df["exit_reason"] == "end_of_data"

            # ── DOMINANCE EXIT COUNTERFACTUAL ──────────────────────────
            # For each dominance exit, look at what happened in the next
            # 48 bars. Did price hit the original stop? Or did it recover?
            exec_df = self.lltf_df if hasattr(self, 'lltf_df') else self.df
            
            # ── STALL EXIT COUNTERFACTUAL ──────────────────────────────
            # Same treatment as dominance_exit: for each stall_exit, look
            # forward from the exit bar to see what actually happened —
            # did price go on to hit the original stop (stall exit was
            # right to cut it), or recover well past where it exited
            # (stall exit cut a trade that was about to work)? Also
            # breaks down by bars_since_peak and retained_frac at exit
            # so the threshold can be tuned against real distribution,
            # not a single guessed cutoff.
            stall_exits = trades_df[trades_df["exit_reason"] == "slow_bleed_exit"]
            if not stall_exits.empty:
                print("\n=== SLOW BLEED EXIT COUNTERFACTUAL ===")
                print(f"{'Date':>22} {'side':>5} {'exit_R':>7} {'would_stop':>10} {'max_recov_R':>12} {'verdict':>10}")
                print("-" * 72)
                stall_saved = 0
                stall_survived = []
                for _, t in stall_exits.iterrows():
                    exit_idx   = int(t["exit_idx"])
                    entry      = t["entry_price"]
                    stop       = t["initial_stop"]
                    side       = t["side"]
                    R_size     = abs(entry - stop)
                    if R_size <= 0:
                        continue

                    future = exec_df.iloc[exit_idx + 1 : exit_idx + 49]
                    if future.empty:
                        continue

                    if side == 1:
                        would_stop  = (future["low"] <= stop).any()
                        max_recov_r = (future["high"].max() - entry) / R_size
                    else:
                        would_stop  = (future["high"] >= stop).any()
                        max_recov_r = (entry - future["low"].min()) / R_size

                    max_recov_r = max(max_recov_r, 0.0)
                    verdict = "SAVED" if would_stop else ("WINNER" if max_recov_r > 0.3 else "FLATLINED")
                    if would_stop:
                        stall_saved += 1
                    else:
                        stall_survived.append(max_recov_r)

                    entry_time_str = str(t["entry_time"])[:16]
                    print(f"{entry_time_str:>22} {'L' if side==1 else 'S':>5} "
                          f"{t['pnl_r']:>7.3f} {str(would_stop):>10} "
                          f"{max_recov_r:>12.3f} {verdict:>10}")

                n_eval = stall_saved + len(stall_survived)
                if n_eval:
                    print(f"\nOf {n_eval} stall exits: {stall_saved} would have hit stop anyway, "
                          f"{len(stall_survived)} would have survived "
                          f"(avg max_recov_R of survivors: {sum(stall_survived)/len(stall_survived):.3f} if any)" if stall_survived else
                          f"\nOf {n_eval} stall exits: {stall_saved} would have hit stop anyway, 0 would have survived.")
                    print("If survivors > stop-outs, the threshold is cutting winners too early.")

                # ── CALIBRATION BREAKDOWN: bars_since_peak vs retained_frac ──
                print("\n--- Stall exit fingerprint (for threshold tuning) ---")
                print(f"{'Date':>22} {'mfe_r':>7} {'bars_since_peak':>16} {'retained_frac':>14}")
                print("-" * 65)
                for _, t in stall_exits.iterrows():
                    mfe_r_val = t["mfe_r"]
                    bars_held_val = int(t["bars_held"])
                    retained = t["pnl_r"] / mfe_r_val if mfe_r_val > 0 else 0.0
                    print(f"{str(t['entry_time'])[:16]:>22} {mfe_r_val:>7.3f} "
                          f"{bars_held_val:>16} {retained:>14.3f}")

        # ── FAST STOP DEBUG ─────────────────────────────────────────────
        # Identifies trades that hit full stop in 1-2 bars despite having
        # some initial MFE — the "trap candle" archetype.
        fast_stops = trades_df[
            (trades_df["exit_reason"] == "stop_loss") &
            (trades_df["bars_held"] <= 2) &
            (trades_df["mfe_r"] > 0.0) &
            (trades_df["pnl_r"] <= -0.80)
        ] if not trades_df.empty else pd.DataFrame()

        if not fast_stops.empty:
            print("\n=== FAST STOP ANALYSIS ===")
            print(f"{'Date':>22} {'side':>5} {'mfe_r':>7} {'mae_r':>7} {'bars':>5} {'pnl_r':>7}")
            print("-" * 60)
            for _, t in fast_stops.iterrows():
                R_size = abs(t["entry_price"] - t["initial_stop"])
                mae_r  = abs(t["_price_mae"]) / R_size if R_size > 0 else 0
                print(f"{str(t['entry_time'])[:16]:>22} "
                      f"{'L' if t['side']==1 else 'S':>5} "
                      f"{t['mfe_r']:>7.3f} "
                      f"{mae_r:>7.3f} "
                      f"{int(t['bars_held']):>5} "
                      f"{t['pnl_r']:>7.3f}")
            print(f"\nTotal fast stops: {len(fast_stops)} | "
                  f"Avg mfe_r: {fast_stops['mfe_r'].mean():.3f} | "
                  f"Avg bars: {fast_stops['bars_held'].mean():.1f}")

        # ── MID-DURATION BLEED-BACK DEBUG ────────────────────────────────
        # Identifies trades that proved real edge (0.15R-0.5R MFE) over
        # multiple bars, but the trail never activated (it only engages
        # at mfe_r >= 0.5), so price round-tripped all the way back to
        # the original full stop. This is the gap between "instant trap"
        # (already covered by dominance_exit/fast_stop) and "trail
        # protected" (mfe_r >= 0.5) that the edge decay data hints at.
        # bleed_back = trades_df[
        #     (trades_df["exit_reason"].isin(["stop_loss", "opposite_impulse"])) &
        #     (trades_df["bars_held"] > 5) &
        #     (trades_df["mfe_r"] >= 0.15) &
        #     (trades_df["mfe_r"] < 0.5) &
        #     (trades_df["pnl_r"] < 0)
        # ] if not trades_df.empty else pd.DataFrame() 

        # if not bleed_back.empty:
        #     print("\n=== MID-DURATION BLEED-BACK ANALYSIS ===")
        #     print(f"{'Date':>22} {'side':>5} {'reason':>17} {'mfe_r':>7} {'bars':>5} {'pnl_r':>7} {'give_back_R':>11}")
        #     print("-" * 80)
        #     for _, t in bleed_back.iterrows():
        #         give_back = t["mfe_r"] - t["pnl_r"]
        #         print(f"{str(t['entry_time'])[:16]:>22} "
        #               f"{'L' if t['side']==1 else 'S':>5} "
        #               f"{t['exit_reason']:>17} "
        #               f"{t['mfe_r']:>7.3f} "
        #               f"{int(t['bars_held']):>5} "
        #               f"{t['pnl_r']:>7.3f} "
        #               f"{give_back:>11.3f}")
        #     print(f"\nTotal bleed-back trades: {len(bleed_back)} | "
        #           f"Avg mfe_r at peak: {bleed_back['mfe_r'].mean():.3f} | "
        #           f"Avg final pnl_r: {bleed_back['pnl_r'].mean():.3f} | "
        #           f"Avg R given back: {(bleed_back['mfe_r'] - bleed_back['pnl_r']).mean():.3f} | "
        #           f"Avg bars held: {bleed_back['bars_held'].mean():.1f}")

            # ── TRAIL ACTIVATION DIAGNOSTIC ──────────────────────────
            # mfe_r in the trades_df is a running max captured every bar,
            # so its final value at exit IS the peak mfe_r the trade ever
            # reached. update_dynamic_stop() gates on `if mfe_r < 0.5: return`
            # so any trade whose peak never reached 0.5R had the trail OFF
            # for its entire life — that's a threshold gap, not a signal
            # problem. Trades that DID cross 0.5R but still bled back point
            # the other way: trail was live, signal/trail-tightness is the
            # likely cause instead.
            # TRAIL_ACTIVATION_R = 0.5  # must match update_dynamic_stop's gate
            # print("\n=== TRAIL ACTIVATION DIAGNOSTIC ===")
            # print(f"{'Date':>22} {'side':>5} {'peak_mfe_r':>10} {'pnl_r':>7} {'trail_active':>12} {'verdict':>16}")
            # print("-" * 80)

            # never_activated = 0
            # activated_but_lost = 0
            # for _, t in bleed_back.iterrows():
            #     peak_mfe_r = t["mfe_r"]
            #     trail_active = peak_mfe_r >= TRAIL_ACTIVATION_R
            #     if trail_active:
            #         activated_but_lost += 1
            #         verdict = "TRAIL FAILED"
            #     else:
            #         never_activated += 1
            #         verdict = "NEVER ACTIVATED"
            #     print(f"{str(t['entry_time'])[:16]:>22} "
            #           f"{'L' if t['side']==1 else 'S':>5} "
            #           f"{peak_mfe_r:>10.3f} "
            #           f"{t['pnl_r']:>7.3f} "
            #           f"{str(trail_active):>12} "
            #           f"{verdict:>16}")

            # n = never_activated + activated_but_lost
            # if n:
            #     print(f"\nOf {n} bleed-back trades:")
            #     print(f"  {never_activated} ({never_activated/n*100:.0f}%) NEVER reached "
            #           f"{TRAIL_ACTIVATION_R}R — trail never turned on. TRAILING STOP threshold issue.")
            #     print(f"  {activated_but_lost} ({activated_but_lost/n*100:.0f}%) reached "
            #           f"{TRAIL_ACTIVATION_R}R+ and the trail WAS live but still lost it — "
            #           f"points to SIGNAL QUALITY / trail looseness, not the activation threshold.")
            #     if never_activated > activated_but_lost:
            #         print("  → Majority never activated: try lowering TRAIL_ACTIVATION_R or adding a sub-0.5R tier.")
            #     else:
            #         print("  → Majority activated and still lost: tighten ATR_AFTER_ENTRY, not the activation floor.")

        # ── PEAK-MFE-TO-STOP GAP ANALYSIS ────────────────────────────────
        # For the pure stop_loss subset of bleed-back trades (OIE never
        # fired), measures how many 5m bars elapsed between the trade's
        # peak MFE and the final stop hit. If the gap is tiny (1-2 bars),
        # even an aggressive trail wouldn't have had time to update and
        # lock in the gain before the reversal ran it over — same failure
        # mode as the "fast assault" case, just triggered from profit
        # instead of from entry. If the gap is wider, a tighter trail in
        # the 0.2-0.5R band would plausibly have caught it.
        # pure_stop_bleed = bleed_back[bleed_back["exit_reason"] == "stop_loss"] if not bleed_back.empty else pd.DataFrame()

        # if not pure_stop_bleed.empty:
        #     print("\n=== PEAK-MFE-TO-STOP GAP ANALYSIS ===")
        #     print(f"{'Date':>22} {'side':>5} {'mfe_r':>7} {'peak_bar':>9} {'exit_bar':>9} {'gap_bars':>9} {'gap_min':>8}")
        #     print("-" * 80)
        #     gaps = []
        #     for _, t in pure_stop_bleed.iterrows():
        #         entry_idx = int(t["entry_idx"])
        #         exit_idx  = int(t["exit_idx"])
        #         side      = t["side"]
        #         entry     = t["entry_price"]
        #         R_size    = abs(entry - t["initial_stop"])
        #         if R_size <= 0:
        #             continue
        #         window = exec_df.iloc[entry_idx + 1: exit_idx + 1]
        #         if window.empty:
        #             continue
        #         if side == 1:
        #             running_mfe = (window["high"] - entry) / R_size
        #         else:
        #             running_mfe = (entry - window["low"]) / R_size
        #         peak_pos     = running_mfe.values.argmax()
        #         peak_bar_idx = entry_idx + 1 + peak_pos
        #         gap_bars     = exit_idx - peak_bar_idx
        #         gaps.append(gap_bars)
        #         print(f"{str(t['entry_time'])[:16]:>22} "
        #               f"{'L' if side == 1 else 'S':>5} "
        #               f"{t['mfe_r']:>7.3f} "
        #               f"{peak_bar_idx:>9} "
        #               f"{exit_idx:>9} "
        #               f"{gap_bars:>9} "
        #               f"{gap_bars * 5:>7}m")
        #     if gaps:
        #         avg_gap = sum(gaps) / len(gaps)
        #         fast_reversals = sum(1 for g in gaps if g <= 2)
        #         print(f"\nAvg gap from peak MFE to stop hit: {avg_gap:.1f} bars ({avg_gap * 5:.0f} min)")
        #         print(f"Fast reversals (≤2 bars from peak to stop): {fast_reversals}/{len(gaps)}")

        # ══════════════════════════════════════════════════════════════
        # ADAPTIVE STOP MINING — which of the 3 stop-tightening options
        # actually fits this system's trade distribution:
        #   (A) Time-decaying risk        — losers sit dead a long time
        #   (B) Volatility-aware tighten   — ATR expands before the stop
        #   (C) Path-to-stop mining        — losers reliably pass through
        #       an early adverse threshold well before -1R, AND winners
        #       rarely touch that same threshold (so tightening wouldn't
        #       clip them)
        # Tune the three constants below and re-run to test other values.
        # ══════════════════════════════════════════════════════════════
        if hasattr(self, "_audit_log") and self._audit_log:
            EARLY_THRESHOLD_R = -0.6   # candidate early-tightening trigger
            DECAY_CHECK_BAR   = 12     # ~1 hour in, for time-decay check
            DECAY_MFE_CEILING = 0.15   # "shown nothing" threshold

            _groups = []
            _cur = []
            for _e in self._audit_log:
                if _e["bars"] == 1 and _cur:
                    _groups.append(_cur)
                    _cur = []
                _cur.append(_e)
            if _cur:
                _groups.append(_cur)

            # Positional alignment: groups and non-"end_of_data" trade rows
            # are both built in strict chronological order within the same
            # forward pass, one group per trade that reached _check_intrabar.
            _trades_aligned = (
                trades_df[trades_df["exit_reason"] != "end_of_data"].reset_index(drop=True)
                if not trades_df.empty else pd.DataFrame()
            )

            _path_rows = []
            _false_positive_winners = []
            _atr_expansion_rows = []
            _decay_rows = []

            for _gi, _grp in enumerate(_groups):
                if not _grp or _gi >= len(_trades_aligned):
                    continue

                _exit_reason = _trades_aligned.iloc[_gi]["exit_reason"]
                _is_stop_loss_exit = _exit_reason == "stop_loss"
                _entry_atr = _grp[0].get("atr_5m")
                _entry_time = _trades_aligned.iloc[_gi]["entry_time"]
                _final_pnl_r = _trades_aligned.iloc[_gi]["pnl_r"]

                # consecutive bars below EARLY_THRESHOLD_R, and whether
                # the trade ever recovered above -0.3R after dipping
                _consec_below = 0
                _max_consec_below = 0
                _recovered_after_dip = False
                _dipped = False
                for _e in _grp:
                    if _e["pnl_r"] <= EARLY_THRESHOLD_R:
                        _dipped = True
                        _consec_below += 1
                        _max_consec_below = max(_max_consec_below, _consec_below)
                    else:
                        if _dipped and _e["pnl_r"] > -0.3:
                            _recovered_after_dip = True
                        _consec_below = 0

                if _dipped:
                    # The correct question isn't "did this end in stop_loss"
                    # — it's "did the trade ever recover to better than the
                    # threshold before it finally exited, by ANY mechanism."
                    # A trade caught by thesis_invalidation/stop_proximity/
                    # dominance_exit AFTER dipping below threshold, at a
                    # final pnl_r still worse than the threshold, is a TRUE
                    # confirmation — tightening here would have cut it
                    # earlier at a better price, not clipped a winner.
                    _recovered_past_threshold = _final_pnl_r > EARLY_THRESHOLD_R
                    if _recovered_past_threshold:
                        _false_positive_winners.append({
                            "entry_time": _entry_time,
                            "final_pnl_r": _final_pnl_r,
                            "exit_reason": _exit_reason,
                        })
                    else:
                        _path_rows.append({
                            "entry_time": _entry_time,
                            "max_consec_below": _max_consec_below,
                            "recovered": _recovered_after_dip,
                            "exit_reason": _exit_reason,
                            "final_pnl_r": _final_pnl_r,
                        })

                # ATR expansion: final 3 bars' ATR vs entry-bar ATR
                if _is_stop_loss_exit and _entry_atr and _entry_atr > 0:
                    _late_atrs = [e["atr_5m"] for e in _grp[-3:] if e.get("atr_5m")]
                    if _late_atrs:
                        _late_atr_avg = sum(_late_atrs) / len(_late_atrs)
                        _atr_expansion_rows.append({
                            "entry_time": _entry_time,
                            "expansion_ratio": _late_atr_avg / _entry_atr,
                        })

                # Time decay: was mfe_r still tiny by DECAY_CHECK_BAR for
                # trades that eventually stopped out anyway?
                if _is_stop_loss_exit:
                    _bar_at_check = next((e for e in _grp if e["bars"] >= DECAY_CHECK_BAR), None)
                    if _bar_at_check is not None:
                        _decay_rows.append({
                            "entry_time": _entry_time,
                            "mfe_r_at_check": _bar_at_check["mfe_r"],
                            "showed_nothing": _bar_at_check["mfe_r"] < DECAY_MFE_CEILING,
                        })

            # print("\n=== ADAPTIVE STOP MINING (which option fits your system) ===")

            # # --- (C) Path-to-stop mining ---
            # print(f"\n--- Path-to-stop: trades dipping below {EARLY_THRESHOLD_R}R ---")
            # if _path_rows:
            #     _n_path = len(_path_rows)
            #     _n_no_recover = sum(1 for r in _path_rows if not r["recovered"])
            #     print(f"{_n_path} stop_loss losers dipped below {EARLY_THRESHOLD_R}R at some point.")
            #     print(f"  {_n_no_recover}/{_n_path} ({_n_no_recover/_n_path*100:.0f}%) never recovered "
            #           f"above -0.3R after the dip.")
            #     _avg_consec = sum(r["max_consec_below"] for r in _path_rows) / _n_path
            #     print(f"  Avg consecutive bars spent below {EARLY_THRESHOLD_R}R: "
            #           f"{_avg_consec:.1f} ({_avg_consec*5:.0f} min)")
            # else:
            #     print(f"  No stop_loss losers dipped below {EARLY_THRESHOLD_R}R pre-stop — "
            #           f"threshold may be too aggressive to test, or sample too small.")

            # if _false_positive_winners:
            #     _n_fp = len(_false_positive_winners)
            #     print(f"\n  ⚠️ {_n_fp} trade(s) ALSO dipped below {EARLY_THRESHOLD_R}R but did NOT "
            #           f"end in stop_loss (would've been cut early by tightening here):")
            #     for r in _false_positive_winners:
            #         print(f"    {str(r['entry_time'])[:16]} exit={r['exit_reason']:>20} "
            #               f"final_pnl_r={r['final_pnl_r']:.3f}")
            # else:
            #     print(f"\n  ✅ Zero trades dipped below {EARLY_THRESHOLD_R}R and still survived/won — "
            #           f"tightening here would not have cost any winners in this sample.")

            # if _path_rows and not _false_positive_winners:
            #     print(f"\n  → VERDICT: {EARLY_THRESHOLD_R}R looks like a strong candidate for "
            #           f"adaptive disaster-stop tightening — confirms losers without clipping winners.")
            # elif _path_rows and _false_positive_winners:
            #     _ratio = len(_false_positive_winners) / len(_path_rows)
            #     _verdict_txt = "too risky, raise the threshold or test deeper" if _ratio > 0.15 else "still workable, but watch this"
            #     print(f"\n  → VERDICT: {len(_false_positive_winners)} false positive(s) vs "
            #           f"{len(_path_rows)} true positive(s) ({_ratio*100:.0f}% false-positive rate) "
            #           f"— {_verdict_txt}.")

            # # --- (B) Volatility-aware tightening ---
            # print(f"\n--- Volatility check: ATR in final 3 bars vs entry ATR (stop_loss losers) ---")
            # if _atr_expansion_rows:
            #     _n_atr = len(_atr_expansion_rows)
            #     _avg_expansion = sum(r["expansion_ratio"] for r in _atr_expansion_rows) / _n_atr
            #     _n_expanded = sum(1 for r in _atr_expansion_rows if r["expansion_ratio"] > 1.5)
            #     print(f"  {_n_atr} stop_loss losers measured | avg late/entry ATR ratio = {_avg_expansion:.2f}x")
            #     print(f"  {_n_expanded}/{_n_atr} ({_n_expanded/_n_atr*100:.0f}%) showed ATR expansion "
            #           f">1.5x before the stop hit.")
            #     if _n_expanded / _n_atr > 0.5:
            #         print("  → VERDICT: majority of losers show real volatility expansion — "
            #               "volatility-aware tightening has empirical support.")
            #     else:
            #         print("  → VERDICT: most losers did NOT show ATR expansion before stopping — "
            #               "this option alone likely wouldn't have caught most of these.")
            # else:
            #     print("  No stop_loss trades with usable ATR data in this run.")

            # # --- (A) Time-decaying risk ---
            # print(f"\n--- Time decay check: mfe_r at bar {DECAY_CHECK_BAR} "
            #       f"(~{DECAY_CHECK_BAR*5}min) for stop_loss losers ---")
            # if _decay_rows:
            #     _n_decay = len(_decay_rows)
            #     _n_nothing = sum(1 for r in _decay_rows if r["showed_nothing"])
            #     print(f"  {_n_decay} stop_loss losers lived past bar {DECAY_CHECK_BAR} | "
            #           f"{_n_nothing}/{_n_decay} ({_n_nothing/_n_decay*100:.0f}%) still had "
            #           f"mfe_r < {DECAY_MFE_CEILING} at that point (showed no edge yet).")
            #     if _n_nothing / _n_decay > 0.5:
            #         print("  → VERDICT: majority of slow losers show zero edge by 1hr in — "
            #               "time-decaying risk has empirical support.")
            #     else:
            #         print("  → VERDICT: most slow losers HAD shown some edge by 1hr — "
            #               "time-decay risks cutting trades that still had a case to stay open.")
            # else:
            #     print(f"  No stop_loss trades lasted past bar {DECAY_CHECK_BAR} in this run — "
            #           f"sample too short-lived to evaluate, or trades resolve fast already.")

            # print("\n(Tune EARLY_THRESHOLD_R / DECAY_CHECK_BAR / DECAY_MFE_CEILING at the top "
            #       "of this block and re-run to test other candidate values.)")

        # ══════════════════════════════════════════════════════════════
        # DISASTER STOP SCENARIO ANALYSIS
        # Breaks down trades that crossed -0.6R into two populations:
        #   A — pure losers (never showed edge before crossing)
        #   B — violent rejections (had 0.15–0.5R MFE then collapsed)
        # For each: how many recovered above -0.6R (false positives for
        # a hard exit) vs how many continued to a worse final pnl_r
        # (true positives where the exit would have saved money).
        # ══════════════════════════════════════════════════════════════
        if hasattr(self, "_disaster_log") and self._disaster_log and not trades_df.empty:

            # Patch in final_pnl_r and exit_reason from trades_df
            # using entry_time as the join key — more reliable than
            # bar index since _disaster_log is also keyed by entry_time.
            _trade_lookup = {
                str(row["entry_time"]): row
                for _, row in trades_df.iterrows()
            }
            for _key, _rec in self._disaster_log.items():
                if _key in _trade_lookup:
                    _t = _trade_lookup[_key]
                    _rec["final_pnl_r"] = _t["pnl_r"]
                    _rec["exit_reason"] = _t["exit_reason"]
                    # Correct recovered flag using actual final pnl_r:
                    # if final_pnl_r > threshold the trade genuinely came back
                    _rec["recovered"] = _t["pnl_r"] > -0.6

            _all = list(self._disaster_log.values())
            _A = [r for r in _all if r["scenario"] == "A_pure_loser"]
            _B = [r for r in _all if r["scenario"] == "B_violent_rejection"]

            # print("\n=== DISASTER STOP SCENARIO ANALYSIS (threshold = -0.6R) ===")

            # for _label, _pop in [("A — Pure losers (mfe_r < 0.15 at crossing)", _A),
            #                       ("B — Violent rejections (mfe_r 0.15–0.5R at crossing)", _B)]:
            #     if not _pop:
            #         print(f"\n{_label}\n  No trades in this population.")
            #         continue

            #     _n = len(_pop)
            #     _n_recovered = sum(1 for r in _pop if r["recovered"])
            #     _n_continued = _n - _n_recovered
            #     _avg_mfe_at_cross = sum(r["mfe_r_at_cross"] for r in _pop) / _n
            #     _avg_pnl_at_cross = sum(r["pnl_r_at_cross"] for r in _pop) / _n
            #     _avg_final_pnl    = sum(r["final_pnl_r"] for r in _pop) / _n
            #     _avg_bars         = sum(r["bars_to_cross"] for r in _pop) / _n
            #     _avg_saving = sum(
            #         abs(r["final_pnl_r"]) - 0.6
            #         for r in _pop if not r["recovered"]
            #     ) / max(_n_continued, 1)

            #     print(f"\n{_label}")
            #     print(f"  Total trades crossing -0.6R : {_n}")
            #     print(f"  Recovered above -0.6R (false positive) : {_n_recovered} "
            #           f"({_n_recovered/_n*100:.0f}%)")
            #     print(f"  Continued to worse exit (true positive) : {_n_continued} "
            #           f"({_n_continued/_n*100:.0f}%)")
            #     print(f"  Avg mfe_r when -0.6R crossed  : {_avg_mfe_at_cross:.3f}R")
            #     print(f"  Avg pnl_r when -0.6R crossed  : {_avg_pnl_at_cross:.3f}R")
            #     print(f"  Avg final pnl_r (actual exit) : {_avg_final_pnl:.3f}R")
            #     print(f"  Avg bars to reach -0.6R       : {_avg_bars:.1f} ({_avg_bars*5:.0f} min)")
            #     print(f"  Avg R saved per true positive : {_avg_saving:.3f}R "
            #           f"(diff between -0.6R exit and actual exit)")

            #     print(f"\n  Per-trade detail:")
            #     print(f"  {'entry_time':>16} {'side':>4} {'mfe@cross':>10} "
            #           f"{'pnl@cross':>10} {'final_pnl':>10} "
            #           f"{'bars':>5} {'recovered':>10} {'exit_reason':>22}")
            #     print(f"  {'-'*95}")
            #     for r in sorted(_pop, key=lambda x: x["bars_to_cross"]):
            #         print(f"  {str(r['entry_time'])[:16]:>16} "
            #               f"{'L' if r['side']==1 else 'S':>4} "
            #               f"{r['mfe_r_at_cross']:>10.3f} "
            #               f"{r['pnl_r_at_cross']:>10.3f} "
            #               f"{r['final_pnl_r']:>10.3f} "
            #               f"{r['bars_to_cross']:>5} "
            #               f"{str(r['recovered']):>10} "
            #               f"{str(r.get('exit_reason','?')):>22}")

            # Overall verdict
            _total = len(_all)
            _total_fp = sum(1 for r in _all if r["recovered"])
            _total_tp = _total - _total_fp
            print(f"\n  ── OVERALL ──")
            print(f"  {_total} trades crossed -0.6R with mfe_r < 0.5R")
            print(f"  {_total_tp} ({_total_tp/_total*100:.0f}%) would benefit from a hard -0.6R exit")
            print(f"  {_total_fp} ({_total_fp/_total*100:.0f}%) recovered — these are the cost of the rule")
            if _total_fp / _total < 0.15:
                print("  → VERDICT: false-positive rate under 15% — "
                      "-0.6R disaster stop is empirically justified for BOTH scenarios.")
            elif _total_fp / _total < 0.30:
                print("  → VERDICT: false-positive rate 15–30% — "
                      "workable but check if Scenario B recoveries skew the number.")
            else:
                print("  → VERDICT: false-positive rate above 30% — "
                      "too many recoveries, raise threshold or split the rule by scenario.")

        liquidations = len(trades_df[trades_df["exit_reason"] == "liquidated"]) if not trades_df.empty else 0

        valid_trades = trades_df[~trades_df["truncated"]] if not trades_df.empty else trades_df

        summary = {
            "initial_balance":  self.initial_balance,
            "final_balance":    round(self.balance, 2),
            "net_profit":       round(self.balance - self.initial_balance, 2),
            "return_pct":       round((self.balance / self.initial_balance - 1) * 100, 2),
            "total_trades":     len(valid_trades),
            "truncated_trades": len(trades_df) - len(valid_trades),
            "win_rate":         round((valid_trades["pnl"] > 0).mean() * 100, 2) if not valid_trades.empty else 0.0,
            "avg_win":          valid_trades.loc[valid_trades["pnl"] > 0, "pnl"].mean() if not valid_trades.empty else 0.0,
            "avg_loss":         valid_trades.loc[valid_trades["pnl"] < 0, "pnl"].mean() if not valid_trades.empty else 0.0,
            "leverage":         self.leverage,
            "liquidations":     liquidations,
        }

        # ── PRINT LAST HOUR OF EXIT AUDIT (for live comparison) ──
        if hasattr(self, "_audit_log") and self._audit_log:
            from strategy.exit_audit import format_exit_audit
            # Group audit log by trade (reset when bars_in_trade resets to 1)
            # and take only the last trade's entries, then filter to last hour
            trades_in_log = []
            current_group = []
            for e in self._audit_log:
                if e["bars"] == 1 and current_group:
                    trades_in_log.append(current_group)
                    current_group = []
                current_group.append(e)
            if current_group:
                trades_in_log.append(current_group)

            # Take the last trade's audit entries, then filter to last hour
            last_trade_log = trades_in_log[-1] if trades_in_log else []
            cutoff_ts = last_trade_log[-1]["ts"] - pd.Timedelta(hours=1) if last_trade_log else None
            recent = [e for e in last_trade_log if cutoff_ts is None or e["ts"] >= cutoff_ts]

            print(f"\n=== EXIT FILTER AUDIT (last hour of most recent trade: {len(recent)} bars) ===")
            for entry in recent:
                print(format_exit_audit(
                    symbol="BACKTEST",
                    side=entry["side"],
                    bars=entry["bars"],
                    mfe_r=entry["mfe_r"],
                    pnl_r=entry["pnl_r"],
                    mae_r=entry["mae_r"],
                    bar_open=entry["bar_open"],
                    bar_high=entry["bar_high"],
                    bar_low=entry["bar_low"],
                    bar_close=entry["bar_close"],
                    stop_loss=entry["stop_loss"],
                    initial_stop=entry["initial_stop"],
                    R=entry["R"],
                    atr_5m=entry["atr_5m"],
                    window_5m=entry["window_5m"],
                    ts=entry["ts"],
                ))
            self._audit_log = []

        return {
            "summary":      summary,
            "equity_curve": equity_df,
            "trades":       trades_df
        }