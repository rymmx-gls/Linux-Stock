# coding:utf8
from __future__ import unicode_literals
import tushare as ts

ts.set_token('e62474d46d346c8465b871c87b5bf4e5d18a9289e72659f4829b5678')
pro = ts.pro_api()

# df = pro.trade_cal(exchange='', start_date='20180901',
#                    end_date='20181001',
#                    fields='exchange,cal_date,is_open,pretrade_date',
#                    is_open='0')

# df = pro.query('trade_cal', exchange='', start_date='20180901', end_date='20191001', fields='exchange,cal_date,is_open,pretrade_date', is_open='0')


# df = pro.trade_cal(exchange='', start_date='20180901', end_date='20181001', fields='exchange,cal_date,is_open,pretrade_date', is_open='0')


data = pro.stock_basic(exchange='', list_status='L', fields='ts_code,symbol,name,area,industry,list_date')

print data

u"""
# 接口测试,需要积分
curl -X POST -d '{"api_name": "stock_basic", "token": "e62474d46d346c8465b871c87b5bf4e5d18a9289e72659f4829b5678", "params": {"list_stauts":"L"}, "fields": "ts_code,name,area,industry,list_date"}' http://api.waditu.com
"""