# coding:utf8
import time, datetime
from models import StockBasicModel, StockAttrModel
from settings import cursor, conn, pro, session, engine_ts
from lib.Common import build_insert_sql, outinfo, outerror
import pandas as pd


def strftime_to_second(strftime="15:05:00"):
    _ = strftime.split(":")
    return int(_[0]) * 3600 + int(_[1]) * 60 + int(_[2])


def check_daily_trade_date(ts_code, trade_date):
    """检查该日数据是否已经存在"""

    sql = '''select trade_date from `daily_{}` where trade_date={}'''.format(ts_code, trade_date)
    cursor.execute(sql)
    if cursor.fetchall():
        return True
    return False


def start():
    sql = '''select cal_date, pretrade_date from trade_cal where is_open=1 order by cal_date DESC limit 10'''
    df = pd.read_sql_query(sql=sql, con=engine_ts)
    df = df[::-1]
    date_list = df['cal_date'].tolist()
    # print date_list
    res = session.query(StockAttrModel).filter(StockAttrModel.item == 'daily_update_stop_date').first()
    stop_date = res.value
    for date in date_list:
        if date > stop_date:
            # print date
            # end_date=time.strftime("%Y%m%d", time.localtime())
            end_date = date
            # start_date = '20100101'
            start_date = end_date
            while True:
                flag = False
                # ts_code_stop = '000001.SZ'
                res = session.query(StockAttrModel).filter(StockAttrModel.item == 'daily_update_ts_code_stop').first()
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
                        outinfo(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
                        outinfo("%s %s" % (ts_code, end_date))
                        try:
                            df = pro.daily(ts_code=ts_code, start_date=start_date, end_date=end_date)
                            if not df.empty:
                                df_dict = df.to_dict('records')[0]
                                if not check_daily_trade_date(ts_code, df_dict['trade_date']):
                                    tbname = "daily_" + ts_code
                                    cursor.execute(*build_insert_sql(tbname, df_dict))
                                    conn.commit()
                                    outinfo("insert %s success at %s" % (ts_code, end_date))
                                else:
                                    outinfo("%s at %s already exist" % (ts_code, end_date))
                                time.sleep(0.5)
                            else:
                                outerror(u"股票:%s(日期:%s)存在缺失"%(ts_code, end_date))
                        except Exception as e:
                            # ts_code_stop = ts_code
                            flag = False
                            res = session.query(StockAttrModel).filter(
                                StockAttrModel.item == 'daily_update_ts_code_stop').first()
                            res.value = ts_code
                            session.commit()
                            outinfo('daily_update_ts_code_stop: %s at date:%s'%(ts_code, end_date))
                            outinfo(u"休眠一分钟再继续下载...")
                            time.sleep(60)
                            break

                if flag:
                    break

            session.close()
            conn.close()

    res = session.query(StockAttrModel).filter(StockAttrModel.item == 'daily_update_stop_date').first()
    res.value = date_list[-1:]
    session.commit()
    session.close()


if __name__ == '__main__':
    interval = 60 * 60 * 24
    DEFINITE_TIME = "23:50:00"

    while True:
        now_time = datetime.datetime.now().strftime('%H:%M:%S')
        time_interval = strftime_to_second(now_time) - strftime_to_second(DEFINITE_TIME)
        # 允许一分钟的误差
        if 0 <= time_interval:
            if time_interval <= 6000:
                ####--- exec function ---###
                start()
                ####--- exec function ---###
                now_time = datetime.datetime.now().strftime('%H:%M:%S')
                exec_time = strftime_to_second(now_time) - strftime_to_second(DEFINITE_TIME)
                outinfo("exec_time:%s s" % exec_time)
                sleep_time = interval - exec_time
            else:
                sleep_time = interval - time_interval
        else:
            sleep_time = abs(time_interval)
        time.sleep(sleep_time + 1)
