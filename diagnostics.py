import matplotlib.pyplot as plt
import pandas as pd

def plot_asymmetry(df):
    """
    Plot ASYM_STATE over price to visually confirm regime existence
    """
    fig, ax = plt.subplots(figsize=(15,5))

    # Plot price
    ax.plot(df.index, df['close'], label='Close', color='black', alpha=0.8)

    # Highlight regimes
    long_mask = df['ASYM_STATE'] == 1
    short_mask = df['ASYM_STATE'] == -1

    ax.scatter(df.index[long_mask], df['close'][long_mask],
               color='green', marker='^', label='LONG regime', alpha=0.6)

    ax.scatter(df.index[short_mask], df['close'][short_mask],
               color='red', marker='v', label='SHORT regime', alpha=0.6)

    ax.set_title('ASYM_STATE Regimes vs Price')
    ax.set_xlabel('Date')
    ax.set_ylabel('Price')
    ax.legend()
    plt.show()
