# coding=utf-8
import pandas as pd
from settings import pro, engine_ts

df_Z = pro.hs_const(hs_type='SZ')  # 获取深股通成分
df_H = pro.hs_const(hs_type='SH')  # 获取沪股通成分

df = pd.concat([df_Z, df_H], axis=0, ignore_index=True)
df.to_sql('hs_const', engine_ts, index=False, if_exists='replace', chunksize=5000)
print df


"""
沪深股通成份股
接口：hs_const
描述：获取沪股通、深股通成分数据

输入参数

名称	类型	必选	描述
hs_type	str	Y	类型SH沪股通SZ深股通
is_new	str	N	是否最新 1 是 0 否 (默认1)


输出参数

名称	类型	默认显示	描述
ts_code	str	Y	TS代码
hs_type	str	Y	沪深港通类型SH沪SZ深
in_date	str	Y	纳入日期
out_date	str	Y	剔除日期
is_new	str	Y	是否最新 1是 0否

接口使用

pro = ts.pro_api()

#获取沪股通成分
df = pro.hs_const(hs_type='SH') 

#获取深股通成分
df = pro.hs_const(hs_type='SZ')
数据样例

    ts_code     hs_type  in_date out_date is_new
0    603818.SH      SH  20160613     None      1
1    603108.SH      SH  20161212     None      1
2    600507.SH      SH  20141117     None      1
3    601377.SH      SH  20141117     None      1
4    600309.SH      SH  20141117     None      1
5    600298.SH      SH  20141117     None      1
6    600018.SH      SH  20141117     None      1
7    600483.SH      SH  20151214     None      1
8    600068.SH      SH  20141117     None      1
9    600594.SH      SH  20141117     None      1
10   603806.SH      SH  20160613     None      1
11   600867.SH      SH  20141117     None      1
12   601012.SH      SH  20141117     None      1
13   601231.SH      SH  20141117     None      1
14   601888.SH      SH  20151214     None      1
15   601099.SH      SH  20141117     None      1
16   603025.SH      SH  20151214     None      1
"""