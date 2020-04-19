

def ma(df):
    # MA5, MA10, MA20
    df['MA5'] = df['close'][::-1].rolling(5).mean()
    df['MA10'] = df['close'][::-1].rolling(10).mean()
    df['MA20'] = df['close'][::-1].rolling(20).mean()

    return df