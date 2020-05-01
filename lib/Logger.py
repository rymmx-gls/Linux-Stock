# -*- coding: utf-8 -*-
# uncompyle6 version 3.6.3
# Python bytecode 2.7 (62211)
# Decompiled from: Python 2.7.5 (default, Aug  7 2019, 00:51:29) 
# [GCC 4.8.5 20150623 (Red Hat 4.8.5-39)]
# Embedded file name: /usr/local/u-mail/app/src/lib/Logger.py
# Compiled at: 2019-07-10 11:37:10
import os, sys, logging, logging.handlers
program_name = None
date_format = '%Y-%m-%d %H:%M:%S'

def init_logger(name, logfile=None, logerr=None, level=logging.INFO, file_owner=None):
    globals()['program_name'] = name
    if name:
        for hdr in logging.root.handlers:
            hdr.setFormatter(logging.Formatter(_get_log_formatter(), date_format))

    ext_param = get_logger_ext_config()
    if logfile:
        logging.root.addHandler(_create_file_handle(level, logfile, param=ext_param, file_owner=file_owner))
    if logerr:
        logging.root.addHandler(_create_file_handle(logging.ERROR, logerr, param=ext_param, file_owner=file_owner))
    return True


def get_logger_ext_config():
    ext_param = None
    try:
        import Common, ConfigParser
        cfg_obj = ConfigParser.ConfigParser()
        cfg_obj.read('%s/setting.conf' % Common.APP_CONF_PATH)
        ext_param = {}
        if cfg_obj.has_section('log'):
            if cfg_obj.has_option('log', 'backup_count'):
                ext_param['backup_count'] = int(cfg_obj.get('log', 'backup_count', 7))
            if cfg_obj.has_option('log', 'when'):
                ext_param['when'] = cfg_obj.get('log', 'when', 'midnight')
            if cfg_obj.has_option('log', 'interval'):
                ext_param['interval'] = int(cfg_obj.get('log', 'interval', 1))
    except Exception as err:
        print err

    return ext_param


def remove_screen_output():
    for hdr in logging.root.handlers:
        if isinstance(hdr, logging.StreamHandler):
            logging.root.removeHandler(hdr)
            break


def redirect_stdout():
    sys.stdout = StreamToLogger(logging, logging.INFO)
    sys.stderr = StreamToLogger(logging, logging.ERROR)


def outinfo(msg):
    outlog(msg, logging.INFO)


def outerror(msg):
    outlog(msg, logging.ERROR)


def outwarn(msg):
    outlog(msg, logging.WARNING)


def outdebug(msg):
    outlog(msg, logging.DEBUG)


def outlog(msg, level):
    logging.log(level, msg)


def _create_file_handle(level, filename, param=None, file_owner=None):
    if param is None:
        param = {}
    log_param = {'filename': filename, 
       'when': 'when' in param and param['when'] or 'midnight', 
       'interval': 'interval' in param and param['interval'] or 1, 
       'backup_count': 'backup_count' in param and param['backup_count'] or 7, 
       'file_owner': file_owner}
    import Common
    if Common.is_security_mode():
        log_param['backup_count'] = 180
    handler = TimedRotatingFileHandler(**log_param)
    handler.setLevel(level)
    handler.setFormatter(logging.Formatter(_get_log_formatter(), date_format))
    return handler


def _get_log_formatter():
    if program_name is None:
        fmt = '%(asctime)s [%(levelname)s]: %(message)s'
    else:
        fmt = '%(asctime)s ' + program_name + '[%(levelname)s]: %(message)s'
    return fmt


class StreamToLogger(object):
    """
   Fake file-like stream object that redirects writes to a logger instance.
   """
    __module__ = __name__

    def __init__(self, logger, log_level=logging.INFO):
        self.logger = logger
        self.level = log_level
        self.linebuf = ''

    def write(self, buf):
        for line in buf.rstrip().splitlines():
            self.logger.log(self.level, line.rstrip())

    def flush(self):
        pass


class TimedRotatingFileHandler(logging.handlers.TimedRotatingFileHandler):
    __module__ = __name__
    file_uid = None
    file_gid = None

    def init_file_owner(self, uid, gid=None):
        """
        \xe5\x88\x9d\xe5\xa7\x8b\xe5\x8c\x96\xe6\x97\xa5\xe5\xbf\x97\xe6\x96\x87\xe4\xbb\xb6\xe7\x9a\x84\xe5\xb1\x9e\xe4\xb8\xbb\xe4\xbf\xa1\xe6\x81\xaf
        @param uid: \xe6\x96\x87\xe4\xbb\xb6\xe5\xb1\x9e\xe4\xb8\xbb\xe7\x9a\x84 UID
        @param gid: \xe6\x96\x87\xe4\xbb\xb6\xe5\xb1\x9e\xe4\xb8\xbb\xe7\x9a\x84 GID
        """
        self.file_uid = uid
        self.file_gid = uid if gid is None else gid
        return

    def set_file_owner(self):
        """
        \xe8\xae\xbe\xe7\xbd\xae\xe6\x96\x87\xe4\xbb\xb6\xe7\x9a\x84\xe5\xb1\x9e\xe4\xb8\xbb\xe4\xbf\xa1\xe6\x81\xaf
        """
        if os.getuid() != 0:
            return
        else:
            if self.file_uid is None:
                return
            os.chown(self.baseFilename, self.file_uid, self.file_gid)
            os.chmod(self.baseFilename, 438)
            return

    def __init__(self, filename, when='h', interval=1, backup_count=0, encoding=None, delay=False, utc=False, file_owner=None):
        if file_owner is not None:
            self.init_file_owner(file_owner[0], file_owner[1])
        super(TimedRotatingFileHandler, self).__init__(filename, when, interval, backup_count, encoding, delay, utc)
        self.set_file_owner()
        return

    def doRollover(self):
        super(TimedRotatingFileHandler, self).doRollover()
        self.set_file_owner()


logging.basicConfig(format=_get_log_formatter(), datefmt=date_format, level=logging.DEBUG)