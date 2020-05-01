# coding:utf8
import time
from settings import pro, engine_ts, session
from models import StockBasicModel, StockAttrModel

# end_date=time.strftime("%Y%m%d", time.localtime())
end_date = '20200422'
start_date = '20100101'
while True:
    flag = False
    # ts_code_stop = '000001.SZ'
    res = session.query(StockAttrModel).filter(StockAttrModel.item=='monthly_ts_code_stop').first()
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
                df = pro.monthly(ts_code=ts_code, start_date=start_date, end_date=end_date)
                df = df[::-1]
                df.to_sql('monthly_%s' % ts_code, engine_ts, index=False, if_exists='replace', chunksize=5000)
                time.sleep(10)
            except Exception as e:
                # ts_code_stop = ts_code
                flag = False
                res = session.query(StockAttrModel).filter(StockAttrModel.item=='monthly_ts_code_stop').first()
                res.value = ts_code
                session.commit()
                print 'monthly_ts_code_stop', ts_code
                print "休眠一小时再继续下载..."
                time.sleep(3600)
                break

    if flag:
        break

session.close()


"""
月线行情
接口：monthly
描述：获取A股月线数据
限量：单次最大3700，总量不限制
积分：用户需要至少300积分才可以调取，具体请参阅积分获取办法

输入参数

名称	类型	必选	描述
ts_code	str	N	TS代码 （ts_code,trade_date两个参数任选一）
trade_date	str	N	交易日期 （每月最后一个交易日日期，YYYYMMDD格式）
start_date	str	N	开始日期
end_date	str	N	结束日期
输出参数

名称	类型	默认显示	描述
ts_code	str	Y	股票代码
trade_date	str	Y	交易日期
close	float	Y	月收盘价
open	float	Y	月开盘价
high	float	Y	月最高价
low	float	Y	月最低价
pre_close	float	Y	上月收盘价
change	float	Y	月涨跌额
pct_chg	float	Y	月涨跌幅 （未复权，如果是复权请用 通用行情接口 ）
vol	float	Y	月成交量
amount	float	Y	月成交额
"""