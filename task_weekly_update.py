# coding:utf8
import datetime
import time

from lib.Common import outinfo, init_logger

init_logger("task_weekly_update")
outinfo("task_weekly_update")
outinfo(time.strftime("%Y-%m-%d %H:%M:%S", time.localtime()))
# time.sleep(10)
