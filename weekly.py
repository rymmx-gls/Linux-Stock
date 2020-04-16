# coding:utf8
import time
from settings import pro, engine_ts

fields='ts_code,trade_date,open,high,low,pre_close,close,change,pct_chg,vol,amount'
end_date=time.strftime("%Y%m%d", time.localtime())
start_date = str(int(end_date)-365)
ts_code = '000001.SZ'


df = pro.weekly(ts_code=ts_code, start_date=start_date, end_date=end_date, fields=fields)
df.to_sql('weekly', engine_ts, index=False, if_exists='replace', chunksize=5000)
# print df