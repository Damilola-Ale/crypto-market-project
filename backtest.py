import pandas as pd
import numpy as np

class Backtester:
    def __init__(self, df, initial_balance=1000, fee=0.0005,
                 atr_period=14, atr_mult=1.5, take_profit_mult=3.0, trailing=False):
        self.df = df.copy()
        self.initial_balance = initial_balance
        self.balance = initial_balance
        self.position = 0          # 0 = flat, 1 = long, -1 = short
        self.entry_price = 0
        self.position_units = 0
        self.fee = fee
        self.equity_curve = []
        self.trades = []

        # ATR/stop settings
        self.atr_period = atr_period
        self.atr_mult = atr_mult
        self.take_profit_mult = take_profit_mult
        self.trailing = trailing
        self.atr_values = self._calculate_atr()

    def _calculate_atr(self):
        high_low = self.df['high'] - self.df['low']
        high_close = (self.df['high'] - self.df['close'].shift()).abs()
        low_close = (self.df['low'] - self.df['close'].shift()).abs()
        tr = pd.concat([high_low, high_close, low_close], axis=1).max(axis=1)
        return tr.rolling(self.atr_period).mean()

    # -------------------- Entry & Exit --------------------
    def enter_long(self, price, idx):
        self.position = 1
        self.entry_price = price
        self.position_units = self.balance / price
        self.balance -= self.balance * self.fee

        atr = self.atr_values.iloc[idx]
        self.stop_loss = price - self.atr_mult * atr
        self.take_profit = price + self.take_profit_mult * atr
        self.trailing_stop = self.stop_loss

        self.current_trade = {
            "entry_idx": idx,
            "signal": 1,
            "entry_price": price,
            "exit_price": None,
            "pnl": None,
            "exit_idx": None,
            "units": self.position_units,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit
        }

    def exit_long(self, price, idx, reason="signal"):
        pnl = (price - self.entry_price) * self.position_units
        self.balance += pnl
        self.balance -= self.position_units * price * self.fee
        self._record_exit(price, idx, pnl, reason)
        self._reset_position()

    def enter_short(self, price, idx):
        self.position = -1
        self.entry_price = price
        self.position_units = self.balance / price
        self.balance -= self.balance * self.fee

        atr = self.atr_values.iloc[idx]
        self.stop_loss = price + self.atr_mult * atr
        self.take_profit = price - self.take_profit_mult * atr
        self.trailing_stop = self.stop_loss

        self.current_trade = {
            "entry_idx": idx,
            "signal": -1,
            "entry_price": price,
            "exit_price": None,
            "pnl": None,
            "exit_idx": None,
            "units": self.position_units,
            "stop_loss": self.stop_loss,
            "take_profit": self.take_profit
        }

    def exit_short(self, price, idx, reason="signal"):
        pnl = (self.entry_price - price) * self.position_units
        self.balance += pnl
        self.balance -= self.position_units * price * self.fee
        self._record_exit(price, idx, pnl, reason)
        self._reset_position()

    def _record_exit(self, price, idx, pnl, reason):
        self.current_trade["exit_price"] = price
        self.current_trade["exit_idx"] = idx
        self.current_trade["pnl"] = pnl
        self.current_trade["exit_reason"] = reason
        self.trades.append(self.current_trade)

    def _reset_position(self):
        self.position = 0
        self.entry_price = 0
        self.position_units = 0

    # -------------------- Run --------------------
    def run(self):
        longs = 0
        shorts = 0

        for i in range(len(self.df) - 1):
            signal = self.df['final_signal'].iloc[i]
            next_open = self.df['open'].iloc[i + 1]

            # ----- ATR / Stop / Take-Profit / Trailing exit -----
            if self.position != 0:
                self._check_stops(next_open, i)

            # ----- Exit on signal change -----
            if self.position == 1 and signal != 1:
                self.exit_long(next_open, i + 1, reason="signal")
            elif self.position == -1 and signal != -1:
                self.exit_short(next_open, i + 1, reason="signal")

            # ----- Enter new positions if flat -----
            if self.position == 0:
                if signal == 1:
                    self.enter_long(next_open, i + 1)
                    longs += 1
                elif signal == -1:
                    self.enter_short(next_open, i + 1)
                    shorts += 1

            # ----- Mark-to-market equity -----
            if self.position == 1:
                unrealized = (next_open - self.entry_price) * self.position_units
                self.equity_curve.append(self.balance + unrealized)
            elif self.position == -1:
                unrealized = (self.entry_price - next_open) * self.position_units
                self.equity_curve.append(self.balance + unrealized)
            else:
                self.equity_curve.append(self.balance)

        # Close any open position at last bar
        last_close = self.df['close'].iloc[-1]
        if self.position == 1:
            self.exit_long(last_close, len(self.df) - 1, reason="end_of_data")
        elif self.position == -1:
            self.exit_short(last_close, len(self.df) - 1, reason="end_of_data")

        return {
            "final_balance": float(self.balance),
            "profit": float(self.balance - self.initial_balance),
            "long_trades": longs,
            "short_trades": shorts
        }

    # -------------------- Stop/Take-Profit Helper --------------------
    def _check_stops(self, price, idx):
        atr = self.atr_values.iloc[idx]
        if self.position == 1:
            if self.trailing:
                self.trailing_stop = max(self.trailing_stop, price - self.atr_mult * atr)
            if price <= self.stop_loss:
                self.exit_long(price, idx + 1, reason="stop_loss")
            elif price >= self.take_profit:
                self.exit_long(price, idx + 1, reason="take_profit")
            elif self.trailing and price <= self.trailing_stop:
                self.exit_long(price, idx + 1, reason="trailing_stop")
        elif self.position == -1:
            if self.trailing:
                self.trailing_stop = min(self.trailing_stop, price + self.atr_mult * atr)
            if price >= self.stop_loss:
                self.exit_short(price, idx + 1, reason="stop_loss")
            elif price <= self.take_profit:
                self.exit_short(price, idx + 1, reason="take_profit")
            elif self.trailing and price >= self.trailing_stop:
                self.exit_short(price, idx + 1, reason="trailing_stop")
