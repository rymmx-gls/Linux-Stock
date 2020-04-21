import pandas as pd


def ma(df=pd.DataFrame()):
    """MA5, MA10, MA20"""
    df['MA5'] = df['close'].rolling(5).mean()
    df['MA10'] = df['close'].rolling(10).mean()
    df['MA20'] = df['close'].rolling(20).mean()
    # df['MA30'] = df['close'].rolling(30).mean()
    # df['MA40'] = df['close'].rolling(40).mean()

    return df


if __name__ == '__main__':
    from settings import engine_ts
    ts_code = "002480.SZ"
    sql = "select trade_date,close from `daily_{}` order by trade_date DESC limit 100;".format(ts_code)
    df = pd.read_sql_query(sql=sql, con=engine_ts, index_col=None, coerce_float=True, params=None, parse_dates=None, chunksize=None)
    df = df[::-1]
    print ma(df)
    import matplotlib.pyplot as plt
    df.plot(x='trade_date', kind='line', figsize=(20, 8), grid=True)
    plt.show()
