
def get_daily_df(pd,con,stock='daily_002480.SZ', limit=50):

    sql = "select trade_date,close from `daily_002480.SZ` order by trade_date DESC limit 50;"
    df = pd.read_sql_query(sql=sql, con=con, index_col=None, coerce_float=True, params=None,
                           parse_dates=None, chunksize=None)