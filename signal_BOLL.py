# coding:utf8
# from lib.stoch import stoch
from lib.bbands import bbands

import pandas as pd
from settings import engine_ts
ts_code = "000001.SZ"
sql = "select trade_date,close,high,low from `daily_{}` order by trade_date DESC limit 100;".format(ts_code)
df = pd.read_sql_query(sql=sql, con=engine_ts, index_col=None, coerce_float=True, params=None, parse_dates=None, chunksize=None)
boll = bbands(close=df.close, length=None, std=None, mamode=None, offset=None)
df = pd.concat([df, boll], axis=1)
print df
import matplotlib.pyplot as plt
df.plot(x='trade_date', kind='line', figsize=(20, 8), grid=True)
plt.show()












# import matplotlib.pyplot as plt
# from matplotlib import rc
# rc('mathtext', default='regular')
# import seaborn as sns
# sns.set_style('white')
# from matplotlib import dates
# import matplotlib as mpl
#
#
# plt.rcParams["figure.figsize"] = (20,10)
#




# fig = plt.figure(figsize=(20,10))
# fig.set_tight_layout(True)
# ax1 = fig.add_subplot(111)
# #fig.bar(dw.index, dw.volume, align='center', width=1.0)
# ax1.plot(dw.index, dw.close, '-', color='g')
#
# ax2 =ax1.twinx()
# ax2.plot(dw.index, dw.upper, '-', color='r')
# ax2.plot(dw.index, dw.lower, '-', color='r')
# ax2.plot(dw.index, dw.middle, '-.', color='b')
#
# ax1.set_ylabel(u"股票价格(绿色)",fontproperties=myfont, fontsize=16)
# ax2.set_ylabel(u"布林带",fontproperties=myfont, fontsize=16)
# ax1.set_title(u"绿色是股票价格，红色（右轴）布林带",fontproperties=myfont, fontsize=16)
# # plt.xticks(bar_data.index.values, bar_data.barNo.values)
# ax1.set_xlabel(u"布林带",fontproperties=myfont,fontsize=16)
# ax1.set_xlim(left=-1,right=len(dw))
# ax1.grid()