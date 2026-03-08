import pandas as pd
import numpy as np

class SignalBacktester:
    def __init__(
        self,
        df,
        htf_df=None,
        initial_balance=1000,
        fixed_risk_per_trade=10.0,
        fee=0.0005,
        atr_period=14,
        atr_mult=1.5,
        take_profit_mult=3.0,
        be_trigger_r=1.2,
        trailing=False
    ):
        self.df = df.copy()
        self.htf_df = htf_df.copy() if htf_df is not None else None

        # Account
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.fixed_risk = fixed_risk_per_trade
        self.fee = fee

        # Position state
        self.position = 0
        self.entry_price = None
        self.units = 0
        self.stop_loss = None
        self.take_profit = None
        self.trailing_stop = None

        # BE state
        self.be_activated = False
        self.be_trigger_r = be_trigger_r

        # Tracking trades
        self.trades = []

        # ATR and multipliers
        self.atr_period = atr_period
        self.atr_mult = atr_mult
        self.take_profit_mult = take_profit_mult
        self.trailing = trailing

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
        atr = self.df['ATR'].iloc[idx]
        if np.isnan(atr):
            return

        if side == 1:
            stop = price - self.atr_mult * atr
            tp = price + self.take_profit_mult * atr
        else:
            stop = price + self.atr_mult * atr
            tp = price - self.take_profit_mult * atr

        units = self._calc_units(price, stop)
        if units <= 0:
            return

        self.position = side
        self.entry_price = price
        self.stop_loss = stop
        self.take_profit = tp
        self.trailing_stop = stop
        self.units = units
        self.be_activated = False

        self.current_trade = {
            "side": side,
            "entry_idx": idx,
            "entry_price": price,
            "units": units,
            "stop_loss": stop,
            "take_profit": tp,
            "ATR": atr,
            "MAE": 0.0,
            "MFE": 0.0
        }

        # Subtract fees
        self.balance -= abs(units * price) * self.fee

    # ------------------------
    # Exit
    # ------------------------
    def _exit(self, price, idx, reason):
        pnl = (
            (price - self.entry_price) * self.units
            if self.position == 1 else
            (self.entry_price - price) * self.units
        )

        if reason == "stop_loss" and self.be_activated:
            reason = "break_even"

        self.balance += pnl
        self.balance -= abs(self.units * price) * self.fee

        self.current_trade.update({
            "exit_price": price,
            "exit_idx": idx,
            "pnl": pnl,
            "exit_reason": reason
        })
        self.trades.append(self.current_trade)

        self.position = 0
        self.entry_price = None
        self.units = 0
        self.be_activated = False

    # ------------------------
    # Intrabar management
    # ------------------------
    def _check_intrabar(self, high, low, idx):
        if self.position == 0:
            return

        # Track MAE/MFE
        if self.position == 1:
            self.current_trade["MAE"] = min(
                self.current_trade["MAE"],
                (low - self.entry_price) * self.units
            )
            self.current_trade["MFE"] = max(
                self.current_trade["MFE"],
                (high - self.entry_price) * self.units
            )
        else:
            self.current_trade["MAE"] = min(
                self.current_trade["MAE"],
                (self.entry_price - high) * self.units
            )
            self.current_trade["MFE"] = max(
                self.current_trade["MFE"],
                (self.entry_price - low) * self.units
            )

        # Break-even
        if not self.be_activated:
            if self.current_trade["MFE"] >= self.fixed_risk * self.be_trigger_r:
                if self.position == 1:
                    self.stop_loss = max(self.stop_loss, self.entry_price)
                else:
                    self.stop_loss = min(self.stop_loss, self.entry_price)
                self.be_activated = True

        # Trailing stop
        if self.trailing:
            atr = self.df['ATR'].iloc[idx]
            if not np.isnan(atr):
                if self.position == 1:
                    self.trailing_stop = max(self.trailing_stop, high - self.atr_mult * atr)
                    self.stop_loss = max(self.stop_loss, self.trailing_stop)
                else:
                    self.trailing_stop = min(self.trailing_stop, low + self.atr_mult * atr)
                    self.stop_loss = min(self.stop_loss, self.trailing_stop)

        # Exit by stop-loss or take-profit
        if self.position == 1:
            if low <= self.stop_loss:
                self._exit(self.stop_loss, idx, "stop_loss")
            elif high >= self.take_profit:
                self._exit(self.take_profit, idx, "take_profit")
        else:
            if high >= self.stop_loss:
                self._exit(self.stop_loss, idx, "stop_loss")
            elif low <= self.take_profit:
                self._exit(self.take_profit, idx, "take_profit")

    # ------------------------
    # Run backtest
    # ------------------------
    def run(self):
        df = self.df
        equity = []
        timestamps = []

        for i in range(len(df) - 1):
            signal = df['final_signal'].iloc[i]

            o = df['open'].iloc[i + 1]
            h = df['high'].iloc[i + 1]
            l = df['low'].iloc[i + 1]

            # ---- Manage current position intrabar ----
            if self.position != 0:
                self._check_intrabar(h, l, i)

            # ---- Exit if signal flips to 0 or opposite ----
            if self.position != 0:
                if (self.position == 1 and signal <= 0) or (self.position == -1 and signal >= 0):
                    self._exit(o, i + 1, reason="signal_flip")

            # ---- Enter new position ----
            if self.position == 0:
                if signal == 1:
                    self._enter(1, o, i + 1)
                elif signal == -1:
                    self._enter(-1, o, i + 1)

            equity.append(self.balance)
            timestamps.append(df.index[i])

        # Exit any remaining position at end of data
        if self.position != 0:
            self._exit(df['close'].iloc[-1], len(df) - 1, "end_of_data")

        # Prepare output
        equity_df = pd.DataFrame({
            "timestamp": timestamps,
            "equity": equity
        }).set_index("timestamp")

        trades_df = pd.DataFrame(self.trades)
        if not trades_df.empty:
            trades_df["direction"] = trades_df["side"].map({1: "LONG", -1: "SHORT"})
            trades_df["entry_time"] = trades_df["entry_idx"].apply(lambda x: df.index[x] if x < len(df) else None)
            trades_df["exit_time"] = trades_df["exit_idx"].apply(lambda x: df.index[x] if x < len(df) else None)
            trades_df["pnl_pct"] = trades_df["pnl"] / self.initial_balance * 100

        summary = {
            "initial_balance": self.initial_balance,
            "final_balance": round(self.balance, 2),
            "net_profit": round(self.balance - self.initial_balance, 2),
            "return_pct": round((self.balance / self.initial_balance - 1) * 100, 2),
            "total_trades": len(trades_df),
            "win_rate": round((trades_df["pnl"] > 0).mean() * 100, 2) if not trades_df.empty else 0.0,
            "avg_win": trades_df.loc[trades_df["pnl"] > 0, "pnl"].mean() if not trades_df.empty else 0.0,
            "avg_loss": trades_df.loc[trades_df["pnl"] < 0, "pnl"].mean() if not trades_df.empty else 0.0,
        }

        return {
            "summary": summary,
            "equity_curve": equity_df,
            "trades": trades_df
        }