# coding:utf8
import time
from settings import pro, engine_ts, session
from models import StockBasicModel, StockAttrModel

# end_date=time.strftime("%Y%m%d", time.localtime())
end_date = '20200416'
start_date = '20100101'
while True:
    flag = False
    # ts_code_stop = '000001.SZ'
    res = session.query(StockAttrModel).filter(StockAttrModel.item=='daily_ts_code_stop').first()
    ts_code_stop = res.value
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
                df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                df.to_sql('daily_%s' % ts_code, engine_ts, index=False, if_exists='replace', chunksize=5000)
                time.sleep(10)
            except Exception as e:
                # ts_code_stop = ts_code
                flag = False
                res = session.query(StockAttrModel).filter(StockAttrModel.item=='daily_ts_code_stop').first()
                res.value = ts_code
                session.commit()
                print 'daily_ts_code_stop', ts_code
                print "休眠一小时再继续下载..."
                time.sleep(3600)
                break

    if flag:
        break

session.close()

"""
日线行情
接口：daily
数据说明：交易日每天15点～16点之间。本接口是未复权行情，停牌期间不提供数据。
调取说明：基础积分每分钟内最多调取500次，每次5000条数据，相当于23年历史，用户获得超过5000积分正常调取无频次限制。
描述：获取股票行情数据，或通过通用行情接口获取数据，包含了前后复权数据。

输入参数

名称	类型	必选	描述
ts_code	str	N	股票代码（支持多个股票同时提取，逗号分隔）
trade_date	str	N	交易日期（YYYYMMDD）
start_date	str	N	开始日期(YYYYMMDD)
end_date	str	N	结束日期(YYYYMMDD)
注：日期都填YYYYMMDD格式，比如20181010

输出参数

名称	类型	描述
ts_code	str	股票代码
trade_date	str	交易日期
open	float	开盘价
high	float	最高价
low	float	最低价
close	float	收盘价
pre_close	float	昨收价
change	float	涨跌额
pct_chg	float	涨跌幅 （未复权，如果是复权请用 通用行情接口 ）
vol	float	成交量 （手）
amount	float	成交额 （千元）
"""
