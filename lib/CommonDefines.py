# -*- coding: utf-8 -*-
# uncompyle6 version 3.6.3
# Python bytecode 2.7 (62211)
# Decompiled from: Python 2.7.5 (default, Aug  7 2019, 00:51:29) 
# [GCC 4.8.5 20150623 (Red Hat 4.8.5-39)]
# Embedded file name: /usr/local/u-mail/app/src/lib/CommonDefines.py
# Compiled at: 2019-10-09 14:46:56
import os
CUSTOM_CONF_PATH = '/usr/local/u-mail/config/custom.conf'
APP_ROOT = '/usr/local/u-mail/app'
APP_LOG_PATH = os.path.join(APP_ROOT, 'log')
APP_RUN_PATH = os.path.join(APP_ROOT, 'run')
APP_TPL_PATH = os.path.join(APP_ROOT, 'tpl')
APP_CONF_PATH = os.path.join(APP_ROOT, 'conf')
APP_UPDATE_PATH = os.path.join(APP_ROOT, 'update')
APP_TOOL_PATH = os.path.join(APP_ROOT, 'tools')
APP_SCRIPT_PATH = os.path.join(APP_ROOT, 'script')
APP_DATA_ROOT = os.path.join(APP_ROOT, 'data')
APP_TMP_PATH = os.path.join(APP_DATA_ROOT, 'tmp')
APP_NETDISK_PATH = '/usr/local/u-mail/data/www/webmail/netdisk'
APP_SCRIPT_PYTHON = os.path.join(APP_SCRIPT_PATH, 'start_python.sh')
APP_SCRIPT_GO = os.path.join(APP_SCRIPT_PATH, 'start_go.sh')
CONFIG_INIT_SUCCESS = 0
SYSTEM_ACCT_NAME = set([
 'root', 'system'])
LICENCE_EXCLUDE_LIST = [
 'system']
TASK_NAME_DELIVER = 'deliver'
TASK_NAME_SMTP = 'smtp'
TASK_NAME_DELAY = 'delay'
TASK_NAME_REVIEW = 'review'
TASK_NAME_SEQUESTER = 'sequester'
TASK_NAME_NEW_COMING = 'new_mail'
TASK_NAME_SPACE_CHANGE = 'space_change'
QUEUE_TASK_EXECUTE_SUCCESS = 0
REDIS_TRANSFER_MAIL_QUEUE = 'Proxy_Trans_Mail_Queue'
REDIS_TRANSFER_MAIL_DATA = 'Proxy_Trans_Mail_Data'
REDIS_KEY_WEB_Command = 'Proxy_Web_Command'
REDIS_KEY_SERVER_NUM = 'Proxy_Servernum'
PORT_RULEFILTER_LISTEN = 10026
PORT_ACCOUNT_CHECKER = 10035
PORT_PROXY_LISTEN = 10037
PORT_RULEFILTER_SYNC = 10038
PORT_SEARCH_LISTEN = 10039
PORT_SERVICE_LISTEN = 10041
PORT_FAIL2BAN_LISTEN = 10042
MAILLOG_STATUS_SUCCESS = '1'
MAILLOG_STATUS_FAILURE = '-1'