# coding:utf8
import time
from settings import pro, engine_ts, session
from models import StockBasicModel


# fields='ts_code,trade_date,turnover_rate,volume_ratio,pe,pb'
end_date=time.strftime("%Y%m%d", time.localtime())
# start_date = str(int(end_date)-365)
start_date = '20100101'
trade_date = ''


flag = False
ts_code_stop = '603897.SH'

objs = session.query(StockBasicModel).all()
for obj in objs:
    ts_code = obj.ts_code
    if ts_code == ts_code_stop:
        flag = True
    else:
        if not flag:
            continue

    if flag:
        print time.strftime("%Y-%m-%d %H:%M:%S", time.localtime())
        print ts_code
        print ''
        try:
            df = pro.daily_basic(ts_code=ts_code, trade_date=trade_date, start_date=start_date, end_date=end_date, fields="")
            df.to_sql(ts_code, engine_ts, index=False, if_exists='replace', chunksize=5000)
            time.sleep(10)
        except Exception as e:
            ts_code_stop = ts_code
            print 'ts_code_stop', ts_code_stop
            raise e




