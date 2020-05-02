# coding:utf8
from models import StockAttrModel
from settings import cursor, conn, pro, session, engine_ts
from lib.Common import build_insert_sql, outinfo, init_logger
import pandas as pd


def check_daily_trade_date(ts_code, trade_date):
    """检查该日数据是否已经存在"""

    sql = '''select trade_date from `daily_{}` where trade_date={}'''.format(ts_code, trade_date)
    try:
        cursor.execute(sql)
        if cursor.fetchall():
            return True
        return False
    except:
        outinfo("创建新股票表:daily_{}".format(ts_code))
        sql = '''create table if not exists stock.`daily_{}`
                (
                    ts_code text null,
                    trade_date text null,
                    open double null,
                    high double null,
                    low double null,
                    close double null,
                    pre_close double null,
                    `change` double null,
                    pct_chg double null,
                    vol double null,
                    amount double null
                );'''.format(ts_code)
        cursor.execute(sql)
        return False


def start():
    sql = '''select cal_date, pretrade_date from trade_cal where is_open=1 order by cal_date DESC limit 10'''
    df = pd.read_sql_query(sql=sql, con=engine_ts)
    df = df[::-1]
    date_list = df['cal_date'].tolist()
    res = session.query(StockAttrModel).filter(StockAttrModel.item == 'daily_update_stop_date').first()
    stop_date = res.value
    for date in date_list:
        if date > stop_date:
            trade_date = date
            df = pro.daily(trade_date=trade_date)
            if not df.empty:
                for i in df.to_dict('records'):
                    if not check_daily_trade_date(i['ts_code'], i['trade_date']):
                        tbname = "daily_" + i['ts_code']
                        cursor.execute(*build_insert_sql(tbname, i))
                        # conn.commit()
                        outinfo("insert %s success at %s" % (i['ts_code'], i['trade_date']))
                    else:
                        outinfo("%s at %s already exist" % (i['ts_code'], i['trade_date']))

    res = session.query(StockAttrModel).filter(StockAttrModel.item == 'daily_update_stop_date').first()
    res.value = date_list[-1:]
    session.commit()
    session.close()
    conn.close()


if __name__ == '__main__':
    init_logger("task_daily_update")
    outinfo("start task_daily_update")
    start()
    outinfo("end task_daily_update")