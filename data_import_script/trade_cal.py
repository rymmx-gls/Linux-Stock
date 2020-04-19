# coding:utf8
import time
from settings import pro, engine_ts

fields='exchange,cal_date,is_open,pretrade_date'
end_date=time.strftime("%Y%m%d", time.localtime())
df = pro.trade_cal(exchange='', start_date='20100101', end_date=end_date, fields=fields, is_open='')
# print df
df.to_sql('trade_cal', engine_ts, index=False, if_exists='replace', chunksize=5000)


"""
交易日历
接口：trade_cal
描述：获取各大交易所交易日历数据,默认提取的是上交所

输入参数

名称	类型	必选	描述
exchange	str	N	交易所 SSE上交所,SZSE深交所,CFFEX 中金所,SHFE 上期所,CZCE 郑商所,DCE 大商所,INE 上能源,IB 银行间,XHKG 港交所
start_date	str	N	开始日期
end_date	str	N	结束日期
is_open	str	N	是否交易 '0'休市 '1'交易
输出参数

名称	类型	默认显示	描述
exchange	str	Y	交易所 SSE上交所 SZSE深交所
cal_date	str	Y	日历日期
is_open	str	Y	是否交易 0休市 1交易
pretrade_date	str	N	上一个交易日

"""