# coding:utf8
"""
股票选择原则:
1. 长期: >=3年[240*3=720]
2. 规律性强: 周期变化明显,易于判断

"""
import time
from settings import pro, engine_ts, session, cursor, conn
from models import StockBasicModel, StockAttrModel

# end_date=time.strftime("%Y%m%d", time.localtime())
end_date = '20200422'
start_date = '20100101'
TRADE_DATE_COUNT_LIMIT = 2400
L = []
objs = session.query(StockBasicModel).all()
for obj in objs:
    ts_code = obj.ts_code
    sql = '''select COUNT(trade_date) from `daily_{}`'''.format(ts_code)
    cursor.execute(sql)
    trade_date_count = cursor.fetchall()[0][0]
    if trade_date_count >= TRADE_DATE_COUNT_LIMIT:
        # print ts_code
        L.append(ts_code)

print len(L)
print ','.join(L)
cursor.close()
conn.close()
