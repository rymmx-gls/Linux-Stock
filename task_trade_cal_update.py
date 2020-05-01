# coding:utf8
import time
from settings import pro, engine_ts, session

fields='exchange,cal_date,is_open,pretrade_date'
# end_date=time.strftime("%Y%m%d", time.localtime())
start_date = '20200420'
end_date = '20200421'
df = pro.trade_cal(exchange='', start_date=start_date, end_date=end_date, fields=fields, is_open='')
df.to_sql('trade_cal', engine_ts, index=False, if_exists='replace', chunksize=5000)
for i in df.itertuples():
    exchange, cal_date, is_open, pretrade_date = i.exchange, i.cal_date, i.is_open, i.pretrade_date
    sql = """
    INSERT INTO `trade_cal` ( `exchange`, `cal_date`, `is_open`,`pretrade_date`)
    SELECT  '{}','{}','{}','{}'
    WHERE not exists (select `cal_date` from `trade_cal` where `cal_date` = '{}');
    """.format(exchange,cal_date,is_open,pretrade_date,cal_date)
    print sql

    session.execute(sql)



    # print sql