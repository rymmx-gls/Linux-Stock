# coding:utf8


def time_controller(func=object):
    """时间控制器"""
    interval = 60*60*24  # 时间间隔
    DEFINITE_TIME = "08:40:00"  # 固定时间时间再次启动
    time_bias = 600  # 允许的时间误差的误差(s)

    import datetime, time
    from lib.Common import outinfo

    def strftime_to_second(strftime="15:05:00"):
        """ 23:54:30 ---> 86070s """
        _ = strftime.split(":")
        return int(_[0]) * 3600 + int(_[1]) * 60 + int(_[2])

    def second_to_strftime(second=86070):
        """86070s ---> 23:54:30"""
        hours = second / 3600
        minutes = (second % 3600) / 60
        seconds = (second % 3600) % 60
        return "%s:%s:%s" % (hours, minutes, seconds)

    while True:
        now_time = datetime.datetime.now().strftime('%H:%M:%S')
        time_interval = strftime_to_second(now_time) - strftime_to_second(DEFINITE_TIME)
        outinfo("DEFINITE_TIME: %s"%DEFINITE_TIME)
        # 允许一分钟的误差
        if 0 <= time_interval:
            if time_interval <= time_bias:
                outinfo("start %s"%func.__name__)
                ####--- exec function ---###
                func()
                ####--- exec function ---###
                outinfo("end %s" % func.__name__)
                now_time = datetime.datetime.now().strftime('%H:%M:%S')
                exec_time = strftime_to_second(now_time) - strftime_to_second(DEFINITE_TIME)
                sleep_time = interval - exec_time
            else:
                sleep_time = interval - time_interval
        else:
            sleep_time = abs(time_interval)
        outinfo("sleep_time: %s" % second_to_strftime(sleep_time))
        time.sleep(sleep_time + 1)


def email_to_me(text=""):
    """给我发送邮件"""
    from email.header import Header
    from email.mime.text import MIMEText
    from email.utils import parseaddr, formataddr
    import smtplib
    from lib.Common import outinfo

    def _format_addr(s):
        name, addr = parseaddr(s)
        return formataddr((Header(name, 'utf-8').encode(),
                           addr.encode('utf-8') if isinstance(addr, unicode) else addr))

    from_addr = 'rymmx520@163.com'
    password = 'LVYDYBYGKMPEZLJH'
    to_addr = 'rymmx520@163.com'
    smtp_server = 'smtp.163.com'

    msg = MIMEText(text, 'plain', 'utf-8')
    msg['From'] = _format_addr(u'rymmx520 <%s>' % from_addr)
    msg['To'] = _format_addr(u'rymmx520 <%s>' % to_addr)
    msg['Subject'] = Header(u'Linux-Stock', 'utf-8').encode()

    server = smtplib.SMTP(smtp_server, 25)
    # server.set_debuglevel(1)
    outinfo('开始邮件发送:%s' % text)
    server.login(from_addr, password)
    server.sendmail(from_addr, [to_addr], msg.as_string())
    server.quit()
    outinfo('邮件发送成功:%s' % text)


if __name__ == '__main__':
    email_to_me(text="买入: 000001.SZ(平安银行)")

# if __name__ == '__main__':
#     def hello():
#         import time
#         print "hello world"
#         time.sleep(5)
#
#     time_controller(hello)
