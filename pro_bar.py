# coding:utf8
import time

from settings import pro, engine_ts

end_date=time.strftime("%Y%m%d", time.localtime())
start_date = str(int(end_date)-365)
ts_code = '000001.SZ'
df = pro.pro_bar(ts_code=ts_code, start_date=start_date, end_date=end_date)
# df.to_sql('daily', engine_ts, index=False, if_exists='replace', chunksize=5000)
print df


"""

A股复权行情
接口名称 ：pro_bar
接口说明 ：复权行情通过通用行情接口实现，利用Tushare Pro提供的复权因子进行计算，目前暂时只在SDK中提供支持，http方式无法调取。
Python SDK版本要求： >= 1.2.26



复权说明

类型	算法	参数标识
不复权	无	空或None
前复权	当日收盘价 × 当日复权因子 / 最新复权因子	qfq
后复权	当日收盘价 × 当日复权因子	hfq
注：目前支持A股的日线/周线/月线复权，分钟复权稍后支持



接口参数

名称	类型	必选	描述
ts_code	str	Y	证券代码
start_date	str	N	开始日期 (格式：YYYYMMDD)
end_date	str	N	结束日期 (格式：YYYYMMDD)
asset	str	Y	资产类别：E股票 I沪深指数 C数字货币 F期货 FD基金 O期权，默认E
adj	str	N	复权类型(只针对股票)：None未复权 qfq前复权 hfq后复权 , 默认None
freq	str	Y	数据频度 ：1MIN表示1分钟（1/5/15/30/60分钟） D日线 ，默认D
ma	list	N	均线，支持任意周期的均价和均量，输入任意合理int数值


接口用例

日线复权


#取000001的前复权行情
df = ts.pro_bar(ts_code='000001.SZ', adj='qfq', start_date='20180101', end_date='20181011')

#取000001的后复权行情
df = ts.pro_bar(ts_code='000001.SZ', adj='hfq', start_date='20180101', end_date='20181011')

周线复权


#取000001的周线前复权行情
df = ts.pro_bar( ts_code='000001.SZ', freq='W', adj='qfq', start_date='20180101', end_date='20181011')

#取000001的周线后复权行情
df = ts.pro_bar(ts_code='000001.SZ', freq='W', adj='hfq', start_date='20180101', end_date='20181011')

月线复权


#取000001的月线前复权行情
df = ts.pro_bar(ts_code='000001.SZ', freq='M', adj='qfq', start_date='20180101', end_date='20181011')

#取000001的月线后复权行情
df = ts.pro_bar(ts_code='000001.SZ', freq='M', adj='hfq', start_date='20180101', end_date='20181011')
"""

