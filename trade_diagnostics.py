import pandas as pd
import numpy as np

def diagnose_trades(df, start_idx=None, end_idx=None):
    """
    Separates SIGNAL QUALITY from EXECUTION QUALITY.
    Signal Quality: direction, structure, opportunity.
    Execution Quality: capture efficiency, loss management.
    """

    df_diag = df.copy()

    if start_idx is not None and end_idx is not None:
        df_diag = df_diag.iloc[start_idx:end_idx + 1]

    trades = []
    position = 0
    entry_price = None
    entry_idx_abs = None

    max_fav = 0.0
    max_adv = 0.0

    for i in range(len(df_diag) - 1):
        signal = df_diag.iloc[i]['final_signal']
        price = df_diag.iloc[i]['close']
        idx_abs = df_diag.index[i]

        # ---------------- ENTRY ----------------
        if position == 0 and signal != 0:
            position = signal
            entry_price = price
            entry_idx_abs = idx_abs
            max_fav = 0.0
            max_adv = 0.0

        # ---------------- TRACK EXCURSIONS ----------------
        if position != 0:
            if position == 1:  # LONG
                max_fav = max(max_fav, price - entry_price)
                max_adv = min(max_adv, price - entry_price)
            else:  # SHORT
                max_fav = max(max_fav, entry_price - price)
                max_adv = min(max_adv, entry_price - price)

        # ---------------- EXIT ----------------
        if position != 0 and signal != position:
            exit_price = price
            exit_idx_abs = idx_abs

            pnl = (exit_price - entry_price) * position
            pnl_pct = pnl / entry_price * 100
            duration = exit_idx_abs - entry_idx_abs

            row = df_diag.loc[entry_idx_abs]

            # -------- SIGNAL QUALITY --------
            signal_correct = max_fav > abs(max_adv)
            opportunity = max_fav
            structure_valid = (
                row.get("HTF_Score", 0) * position > 0 and
                row.get("ATR_Expansion", 0) >= 1.0 and
                abs(row.get("DCW_Slope", 0)) > 0
            )

            signal_quality = (
                "GOOD" if signal_correct and opportunity > 0 else "BAD"
            )

            # -------- EXECUTION QUALITY --------
            capture_efficiency = (
                pnl / max_fav if max_fav > 0 else 0
            )

            execution_quality = (
                "GOOD" if capture_efficiency >= 0.35 else
                "POOR" if capture_efficiency >= 0 else
                "FAILED"
            )

            trades.append({
                # Core
                "direction": "LONG" if position == 1 else "SHORT",
                "entry_idx": entry_idx_abs,
                "exit_idx": exit_idx_abs,
                "entry_price": entry_price,
                "exit_price": exit_price,
                "pnl": pnl,
                "pnl_pct": pnl_pct,
                "duration_bars": duration,

                # Excursions
                "MFE": max_fav,
                "MAE": max_adv,

                # Signal Quality
                "signal_quality": signal_quality,
                "signal_correct": signal_correct,
                "structure_valid": structure_valid,
                "opportunity": opportunity,

                # Execution Quality
                "execution_quality": execution_quality,
                "capture_efficiency": capture_efficiency,

                # Context (frozen at entry)
                "HTF_Score": row.get("HTF_Score", None),
                "ATR_Expansion": row.get("ATR_Expansion", None),
                "DCW_Slope": row.get("DCW_Slope", None),
                "EMA_Ribbon_Score": row.get("EMA_Ribbon_Score", None),
                "Confidence": row.get("confidence", None),
            })

            # Reset
            position = 0
            entry_price = None
            entry_idx_abs = None

    trades_df = pd.DataFrame(trades)

    if trades_df.empty:
        print("\nNo trades found.")
        return

    # ================= REPORT =================
    print("\n=== TRADE DIAGNOSTICS ===")
    for _, t in trades_df.iterrows():
        print(
            f"\n{t['direction']} | Entry {t['entry_idx']} → Exit {t['exit_idx']}"
        )
        print(
            f"PnL: {t['pnl']:.2f} ({t['pnl_pct']:.2f}%) | "
            f"MFE: {t['MFE']:.2f} | MAE: {t['MAE']:.2f}"
        )
        print(
            f"Signal: {t['signal_quality']} | Execution: {t['execution_quality']} | "
            f"Capture: {t['capture_efficiency']:.2f}"
        )
        print(
            f"HTF: {t['HTF_Score']} | ATR: {t['ATR_Expansion']} | "
            f"DCW: {t['DCW_Slope']} | EMA: {t['EMA_Ribbon_Score']} | "
            f"Confidence: {t['Confidence']}"
        )

    # ================= SUMMARY =================
    print("\n=== SUMMARY ===")
    print(f"Total trades: {len(trades_df)}")
    print(f"Win rate: {(trades_df['pnl'] > 0).mean() * 100:.2f}%")
    print(f"Good signals: {(trades_df['signal_quality'] == 'GOOD').mean() * 100:.2f}%")
    print(f"Good execution: {(trades_df['execution_quality'] == 'GOOD').mean() * 100:.2f}%")
    print(f"Avg capture efficiency: {trades_df['capture_efficiency'].mean():.2f}")
    print(f"Total PnL: {trades_df['pnl'].sum():.2f}")

    return trades_df
