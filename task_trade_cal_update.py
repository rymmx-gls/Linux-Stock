# coding:utf8
import datetime
import time
from lib.Common import outinfo, init_logger
from settings import pro, cursor, conn


def start():
    end_date = time.strftime("%Y%m%d", time.localtime())
    # start_date = end_date
    start_date = '20200521'
    # start_date = (datetime.datetime.now() - datetime.timedelta(days=1)).strftime('%Y%m%d')
    # end_date = '20200421'
    fields = 'exchange,cal_date,is_open,pretrade_date'
    df = pro.trade_cal(start_date=start_date, end_date=end_date, fields=fields)
    # for i in df.to_dict('records'):
    for index, i in df.iterrows():
        sql = """
            INSERT INTO `trade_cal` ( `exchange`, `cal_date`, `is_open`,`pretrade_date`)
            SELECT  '{}','{}','{}','{}'
            WHERE not exists (select `cal_date` from `trade_cal` where `cal_date` = '{}');
            """.format(i['exchange'], i['cal_date'], i['is_open'], i['pretrade_date'], i['cal_date'])
        cursor.execute(sql)
        outinfo("insert %s success" % (i['cal_date']))

    cursor.close()
    conn.close()


if __name__ == '__main__':
    init_logger("task_trade_cal_update")
    outinfo("start task_trade_cal_update.py")
    start()
    outinfo("end task_trade_cal_update.py")
