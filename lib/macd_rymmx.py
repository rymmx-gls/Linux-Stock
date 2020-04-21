from lib.ema import ema
import pandas as pd


def macd(df=pd.DataFrame()):
    """MACD DIF DEA BAR"""

    df['ema12'] = ema(close=df['close'], length=12, offset=None)
    df['ema26'] = ema(close=df['close'], length=26, offset=None)

    # MACD
    df['DIF'] = df['ema12'] - df['ema26']
    df['DEA'] = df['DIF'].rolling(9).mean()
    df['BAR'] = (df['DIF'] - df['DEA']) * 2

    return df


if __name__ == '__main__':
    from settings import engine_ts
    ts_code = "002480.SZ"
    sql = "select trade_date,close from `daily_{}` order by trade_date DESC limit 100;".format(ts_code)
    df = pd.read_sql_query(sql=sql, con=engine_ts, index_col=None, coerce_float=True, params=None, parse_dates=None, chunksize=None)
    df = df[::-1]
    print macd(df)
    import matplotlib.pyplot as plt
    df.plot(x='trade_date', kind='line', figsize=(20, 8), grid=True)
    plt.show()
