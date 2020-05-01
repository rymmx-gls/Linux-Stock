# -*- coding: utf-8 -*-
# uncompyle6 version 3.6.3
# Python bytecode 2.7 (62211)
# Decompiled from: Python 2.7.5 (default, Aug  7 2019, 00:51:29) 
# [GCC 4.8.5 20150623 (Red Hat 4.8.5-39)]
# Embedded file name: /usr/local/u-mail/app/src/lib/Common.py
# Compiled at: 2019-11-11 14:24:22
import os, re, sys, fcntl, atexit, time, random, logging, psutil, json, hashlib
from logging.handlers import TimedRotatingFileHandler
from traceback import format_exc
from ConfigParser import ConfigParser
import chardet, Logger
from CommonDefines import *
MAILBOX_ROOT = None
CUSTOM_CONF_PATH = '/usr/local/u-mail/config/custom.conf'
SERVICE_CONF_DIR = '/usr/local/u-mail/config'
APP_ROOT = '/root/PycharmProjects/stock'
APP_LOG_PATH = os.path.join(APP_ROOT, 'log')
APP_RUN_PATH = os.path.join(APP_ROOT, 'run')
APP_TPL_PATH = os.path.join(APP_ROOT, 'tpl')
APP_CONF_PATH = os.path.join(APP_ROOT, 'conf')
APP_UPDATE_PATH = os.path.join(APP_ROOT, 'update')
APP_TOOL_PATH = os.path.join(APP_ROOT, 'tools')
APP_DATA_ROOT = os.path.join(APP_ROOT, 'data')
APP_SBIN_PATH = os.path.join(APP_ROOT, 'sbin')
APP_GOBIN_PATH = os.path.join(APP_ROOT, 'gobin')
APP_TMP_PATH = os.path.join(APP_DATA_ROOT, 'tmp')
APP_GMSSL_CACHE_PATH = os.path.join(APP_DATA_ROOT, 'cache_gmssl')
APP_GMSSL_BIN_PATH = os.path.join(APP_ROOT, 'gmssl')
APP_NETDISK_PATH = '/usr/local/u-mail/data/www/webmail/netdisk'
ENCRYPT_KEY_DIR = '/usr/local/u-mail/app/data/data_encrypt'
ENCRYPT_PRIKEY = os.path.join(ENCRYPT_KEY_DIR, 'ecprivkey.pem')
ENCRYPT_PUBKEY = os.path.join(ENCRYPT_KEY_DIR, 'ecpubkey.pem')
UAMIL_CACHE_EMAIL_DIR = '/usr/local/u-mail/data/www/webmail/mailcache'
CONFIG_INIT_SUCCESS = 0
SYSTEM_ACCT_NAME = set([
 'root', 'system', 'security'])
SECURITY_ACCT_LIST = set([
 'security', 'administrator', 'review'])
LICENCE_EXCLUDE_LIST = [
 'system']
REDIS_TRANSFER_MAIL_QUEUE = 'Proxy_Trans_Mail_Queue'
REDIS_TRANSFER_MAIL_DATA = 'Proxy_Trans_Mail_Data'
REDIS_KEY_WEB_Command = 'Proxy_Web_Command'
REDIS_KEY_SERVER_NUM = 'Proxy_Servernum'
PORT_RULEFILTER_LISTEN = 10026
PORT_ACCOUNT_CHECKER = 10035
PORT_RULEFILTER_SYNC = 10038
PORT_SEARCH_LISTEN = 10039
PORT_SERVICE_LISTEN = 10041
PORT_FAIL2BAN_LISTEN = 10042
cfgDefault = None
dbRedis = None
dbMysql = None
SEEDS = [ chr(i) for i in range(48, 58) ]
SEEDS += [ chr(i) for i in range(65, 91) ]
SEEDS += [ chr(i) for i in range(97, 123) ]

def outinfo(msg):
    outlog(msg, logging.INFO)


def outerror(msg):
    outlog(msg, logging.ERROR)


def outdebug(msg):
    outlog(msg, logging.DEBUG)


def outlog(m, t):
    logging.log(t, m)


def loginfo(logid, msg):
    msg = u'[%s] %s' % (get_unicode(logid), get_unicode(msg))
    outinfo(msg)


def logerror(logid, msg):
    msg = u'[%s] %s' % (get_unicode(logid), get_unicode(msg))
    outerror(msg)


def signal_handle(mode):
    globals()['SHUTDOWN'] = True
    outinfo('catch signal: %s' % mode)
    sys.exit(0)


def signal_init():
    import signal, gevent
    gevent.signal(signal.SIGINT, signal_handle, 'sigterm')
    gevent.signal(signal.SIGTERM, signal_handle, 'sigalrm')
    gevent.signal(signal.SIGALRM, signal_handle, 'sigint')


def generate_task_id(main_id=None, sub_id=None):
    if main_id is None:
        main_id = generate_task_main_id()
    if sub_id is None:
        sub_id = generate_task_sub_id()
    task_id = main_id + '-' + sub_id
    return task_id


def generate_task_main_id():
    main_id = str(time.time())[:10] + get_random_string(5)
    return main_id


def generate_task_sub_id():
    return get_random_string(5)


def parse_task_main_id(task_id):
    return task_id.split('-')[0]


def parse_task_sub_id(task_id):
    return task_id.split('-')[1]


def check_debug(index=1):
    if len(sys.argv) > index and sys.argv[index] == 'debug':
        return True
    return False


def init_run_user():
    run_user_name = get_system_user_name()
    if run_user_name not in ('umail', 'root'):
        outerror('Error: please use "umail" or "root" user runing!')
        sys.exit(-1)
    if run_user_name == 'root':
        os.setuid(get_system_user_id('umail'))
    return True


def init_pid_file(filename):

    def _clear_pidfile(_pidfile):
        if not os.path.exists(_pidfile):
            return
        try:
            os.unlink(_pidfile)
        except:
            pass

    pidfile = os.path.join(APP_RUN_PATH, filename)
    try:
        fd = os.open(pidfile, os.O_RDWR | os.O_CREAT | os.O_NONBLOCK | os.O_DSYNC, 420)
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.write(fd, str(os.getpid()))
        atexit.register(_clear_pidfile, pidfile)
    except IOError:
        print >> sys.stderr, 'Error: program is already running!'
        sys.exit(-2)
    except OSError as e:
        print >> sys.stderr, 'Error: %s' % e
        sys.exit(-2)

    return True


def init_logger(name, onscreen=True, debug=False, stdout=True):
    """
    \xe5\x88\x9d\xe5\xa7\x8b\xe5\x8c\x96\xe6\x97\xa5\xe5\xbf\x97\xe8\xae\xb0\xe5\xbd\x95\xe6\xa8\xa1\xe5\x9d\x97
    :param name: \xe5\xba\x94\xe7\x94\xa8\xe7\xa8\x8b\xe5\xba\x8f\xe6\xa0\x87\xe8\xaf\x86\xe7\xac\xa6\xef\xbc\x8c\xe5\xad\x97\xe7\xac\xa6\xe4\xb8\xb2
    :param onscreen: \xe6\x98\xaf\xe5\x90\xa6\xe8\xa6\x81\xe5\xb0\x86\xe6\x97\xa5\xe5\xbf\x97\xe5\x9c\xa8\xe5\xb1\x8f\xe5\xb9\x95\xe4\xb8\x8a\xe6\x98\xbe\xe7\xa4\xba\xef\xbc\x8c\xe5\xb8\x83\xe5\xb0\x94\xe7\xb1\xbb\xe5\x9e\x8b
    :param debug: \xe6\x98\xaf\xe5\x90\xa6\xe4\xb8\xba\xe8\xb0\x83\xe8\xaf\x95\xe6\xa8\xa1\xe5\xbc\x8f\xef\xbc\x8c\xe5\xb8\x83\xe5\xb0\x94\xe7\xb1\xbb\xe5\x9e\x8b
    :param stdout: \xe6\x98\xaf\xe5\x90\xa6\xe9\x87\x8d\xe5\xae\x9a\xe5\x90\x91\xe7\xb3\xbb\xe7\xbb\x9f\xe6\xa0\x87\xe5\x87\x86\xe8\xbe\x93\xe5\x87\xba\xef\xbc\x8c\xe5\xb8\x83\xe5\xb0\x94\xe7\xb1\xbb\xe5\x9e\x8b
    :return: bool
    """
    level = logging.DEBUG if debug else logging.INFO
    if name is None:
        name = 'default'
    if not os.path.exists(APP_LOG_PATH) or not _is_writable(APP_LOG_PATH):
        print >> sys.stderr, 'Error: no have log path write permission: ' + APP_LOG_PATH
        sys.exit(1)
    log_file = os.path.join(APP_LOG_PATH, name.lower() + '.log')
    err_file = os.path.join(APP_LOG_PATH, 'error.log')
    if os.path.exists(log_file) and not _is_writable(log_file) or os.path.exists(err_file) and not _is_writable(err_file):
        print >> sys.stderr, 'Error: no have logfile write permission: ' + log_file
        sys.exit(1)
    file_owner_uid = get_system_user_id('umail')
    Logger.init_logger(name, log_file, err_file, level, (file_owner_uid, None))
    if not onscreen:
        Logger.remove_screen_output()
    if stdout:
        Logger.redirect_stdout()
    return True


def init_cfg_default():
    if cfgDefault is not None:
        return
    else:
        globals()['cfgDefault'] = make_config_object(CUSTOM_CONF_PATH)
        globals()['MAILBOX_ROOT'] = cfgDefault.get('system', 'mailroot')
        globals()['CONFIG_INIT_SUCCESS'] = 1
        return


def init_db_mysql(err_exit=True):
    global dbMysql
    if dbMysql is not None:
        return
    else:
        import Database
        dbMysql = Database.init_db_mysql(err_exit)
        return


def init_db_redis(err_exit=True):
    global dbRedis
    if dbRedis is not None:
        return
    else:
        import Database
        dbRedis = Database.init_db_redis(err_exit)
        return


def get_system_user_name(uid=None):
    from pwd import getpwuid
    if uid is None:
        uid = os.getuid()
    return getpwuid(uid)[0]


def get_system_user_id(uname):
    from pwd import getpwnam
    return getpwnam(uname)[2]


def get_system_group_id(uname):
    from pwd import getpwnam
    return getpwnam(uname)[3]


def get_system_type():
    path = '/etc/redhat-release'
    if os.path.exists(path):
        with open(path, 'rb+') as (fobj):
            code = fobj.read()
            if 'CentOS' in code and ' 7.' in code:
                return 'el7'
            if 'CentOS' in code and ' 6.' in code:
                return 'el6'
    return 'others'


def _is_writable(path):
    return os.access(path, os.W_OK)


def make_config_object(file_path):
    p = ConfigParser()
    p.read(file_path)
    return p


def get_unicode(string, charset=None):
    st = type(string)
    if st.__name__ == 'unicode':
        return string
    if not string:
        return u''
    if not isinstance(string, str):
        string = str(string)
    try:
        if not charset:
            if len(string) <= 3:
                charset = 'utf-8'
            else:
                charset = chardet.detect(string)['encoding']
        if not charset:
            return string.decode('utf-8', 'ignore')
        return string.decode(charset)
    except Exception as err:
        outerror("string: %s decode '%s' error: '%s'" % (str([string]), str([charset]), get_exception_info()))
        return string.decode('utf-8', 'ignore')


def get_string(code):
    if isinstance(code, unicode):
        try:
            return code.encode('utf-8')
        except:
            return code.encode('utf-8', 'ignore')

    if not isinstance(code, str):
        return str(code)
    if '&#x' in code:
        code = code.replace('&#x', '\\u')
    if '&#' in code:
        lst = re.findall('\\&\\#(\\d+)\\;', code)
        for dec in lst:
            key = hex(int(dec)).replace('0x', '\\u')
            code = code.replace(dec, key)

        code = code.replace('&#', '')
        code = code.replace(';', '')
    if '\\u' in code:
        try:
            code = code.replace(';', '')
            code = code.decode('raw_unicode_escape').encode('utf-8')
        except:
            pass

    return code


def get_random_string(str_len=5):
    return ('').join(random.sample(SEEDS, str_len))


def safe_call(fn, *args, **kwargs):
    try:
        return fn(*args, **kwargs)
    except Exception:
        outerror('call "%s" failure\n%s' % (fn.__name__, get_exception_info()))
        return

    return


def safe_function(funcobj, *args, **kw):

    def wrap(*args, **kw):
        rtn = None
        try:
            rtn = funcobj(*args, **kw)
        except Exception as err:
            outerror('call "%s" failure\n%s' % (funcobj.__name__, get_exception_info()))

        return rtn

    return wrap


def timeout_call(funcobj, *args, **kw):

    def wrap(*args, **kw):
        import gevent
        if 'gevent_timeout' in kw:
            timeout = kw.pop('gevent_timeout')
        else:
            timeout = 30
        with gevent.Timeout(timeout):
            rtn = None
            try:
                rtn = funcobj(*args, **kw)
            except Exception as err:
                print err

            return rtn
        return

    return wrap


def safe_class(cls, *args, **kw):

    def wrap(*args, **kw):
        try:
            return cls(*args, **kw)
        except Exception as err:
            outerror('class "%s" failure\n%s' % (cls.__name__, get_exception_info()))

        return

    return wrap


def lock_call(lock, fn, *args, **kwargs):
    with lock:
        return fn(*args, **kwargs)


def get_exception_info():
    lines = []
    for line in format_exc().strip().split('\n'):
        lines.append('  > ' + line)

    err_msg = ('\n').join(lines)
    return get_unicode(err_msg)


def make_dir(path_list):
    if type(path_list) != type([]):
        path_list = [path_list]
    for path in path_list:
        if os.path.exists(path):
            continue
        recursion_make_dir(path)
        try:
            os.chown(path, get_system_user_id('umail'), get_system_group_id('umail'))
        except:
            outerror(get_exception_info())

    return True


def recursion_make_dir(path, permit=493):
    if path[0] != '/':
        return False
    path_list = os.path.realpath(path).split('/')[1:]
    path_full = ''
    for item in path_list:
        path_full += '/' + item
        if os.path.exists(path_full):
            continue
        os.mkdir(path_full)
        os.chmod(path_full, permit)
        try:
            os.chown(path_full, get_system_user_id('umail'), get_system_group_id('umail'))
        except:
            outerror(get_exception_info())

    return True


def get_memory_info():
    memory = psutil.virtual_memory()
    total_nc = round(float(memory.total) / 1024 / 1024, 2)
    used_nc = round(float(memory.used) / 1024 / 1024, 2)
    free_nc = round(float(memory.free) / 1024 / 1024, 2)
    free_av = round(float(memory.available) / 1024 / 1024, 2)
    syl_nc = round(float(memory.used) / float(memory.total) * 100, 2)
    ret_list = [total_nc, used_nc, free_nc, free_av, syl_nc]
    return ret_list


def get_cpu_nfo():
    cpu_count = psutil.cpu_count(logical=False)
    xc_count = psutil.cpu_count()
    cpu_slv = round(psutil.cpu_percent(1), 2)
    list = [cpu_count, xc_count, cpu_slv]
    return list


def build_insert_sql(tbname, data):
    field_list = data.keys()
    placeholder = ['%s'] * len(field_list)
    sql = 'INSERT INTO `%s` (`%s`) VALUES (%s)' % (
     tbname, ('`, `').join(field_list), (', ').join(placeholder))
    return (
     sql, data.values())


def build_update_sql(tbname, data, where='1', param=None):
    if param is None:
        param = []
    field_list = data.keys()
    value_list = []
    for k in field_list:
        value_list.append(data[k])

    sql = 'UPDATE `%s` SET `%s`=%%s WHERE %s' % (
     tbname, ('`=%s, `').join(field_list), where)
    return (
     sql, value_list + param)


def unpack_data(file_object):
    query_list = []
    while True:
        line = file_object.readline()
        line = line.strip()
        if line == '.':
            break
        if not line:
            break
        query_list.append(line)

    json_line = ('').join(query_list)
    json_line = json_line.strip()
    if not json_line:
        return {}
    json_data = json.loads(json_line)
    return json_data


def packet_data(file_object, json_data):
    file_object.write(json_data + '\n')
    file_object.write('.\n')
    file_object.flush()


def is_new_webmail():
    if not cfgDefault.has_option('system', 'old_webmail'):
        return True
    if str(cfgDefault.get('system', 'old_webmail')) == '1':
        return False
    return True


def is_security_mode():
    cfg = make_config_object(CUSTOM_CONF_PATH)
    if not cfg.has_option('system', 'security_mode'):
        return False
    if str(cfg.get('system', 'security_mode')).strip() == '1':
        return True
    return False


if 'g_logger_list' not in globals():
    g_logger_list = {}

def get_logger(logger_name):
    global g_logger_list
    if logger_name not in g_logger_list:
        logFilePath = '%s/%s.log' % (APP_LOG_PATH, logger_name)
        logger = logging.getLogger(logger_name)
        logger.setLevel(logging.INFO)
        if not logger.handlers:
            handler = TimedRotatingFileHandler(logFilePath, when='d', interval=1, backupCount=7)
            logger.addHandler(handler)
        g_logger_list[logger_name] = logger
    else:
        logger = g_logger_list[logger_name]
    return logger


class Base(object):
    __module__ = __name__

    def loginfo(self, msg):
        msg = '%s %s' % (self, get_string(msg))
        outinfo(msg)

    def logerror(self, msg):
        msg = '%s %s' % (self, get_string(msg))
        outerror(msg)

    def is_debug(self):
        return False

    def load_redis_flag(self, db_redis, flag):

        def get_new_value():
            setattr(self, key_expire, int(time.time()))
            value = db_redis.get(flag)
            setattr(self, key, value)
            return value

        if not db_redis:
            return None
        else:
            key = ('debug_flag_{}').format(flag)
            key_expire = ('timeout_flag_{}').format(flag)
            if not hasattr(self, key):
                return get_new_value()
            now = int(time.time())
            last = getattr(self, key_expire, 0)
            if now - last >= 3:
                return get_new_value()
            return getattr(self, key)

    def logdebug(self, msg):
        if self.is_debug():
            msg = '%s[debug] %s' % (self, get_string(msg))
            outinfo(msg)

    def logfile(self, logger_name, msg):
        global g_logger_list
        logger = get_logger(logger_name)
        try:
            logger.info('%s %s %s' % (time.strftime('%Y-%m-%d %H:%M:%S'), self, msg))
        except Exception as err:
            outerror(get_exception_info())
            g_logger_list = {}


def check_proxy_licence():
    return True


def get_md5_value(src):
    myMd5 = hashlib.md5()
    myMd5.update(src)
    myMd5_Digest = myMd5.hexdigest()
    return myMd5_Digest

