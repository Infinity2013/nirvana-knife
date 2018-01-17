#!/usr/bin/env python

import pandas as pd
import re
import datetime
import re
import os
import hashlib
import MySQLdb


class LogSession:
    LOG_LEVEL_ERROR = 0
    LOG_LEVEL_WARNING = 1
    LOG_LEVEL_INFO = 2
    LOG_LEVEL_DEBUG = 3
    log_level = 2

    def __init__(self, _level, _name):
        self.level = _level
        self.name = _name
        self.ts = ""

    def __enter__(self):
        if self.level <= LogSession.log_level:
            self.ts = datetime.datetime.now()
        if LogSession.log_level >= LogSession.LOG_LEVEL_DEBUG:
            print ("session: %s" % self.name)

    def __exit__(self, exc_type, exc_val, exc_tb):
        if self.level <= LogSession.log_level:
            self.ts = datetime.datetime.now() - self.ts
            print ("session: %s, duration: %s" % (self.name, self.ts))


def session(level):
    def session_wrapper(func):
        def wrapper(*args, **kwargs):
            with LogSession(level, func.__name__):
                return func(*args, **kwargs)
        return wrapper
    return session_wrapper


@session(LogSession.LOG_LEVEL_DEBUG)
def parse_version(path):
    version = "invalid image ver"
    g = re.findall(r'\d\.\d\.\d+', path)
    if len(g) == 1:
        version = g[0]
    return version


@session(LogSession.LOG_LEVEL_DEBUG)
def parse_imei(path):
    imei = "invalid imei"
    g = re.findall(r'86\d{13}', path)
    if len(g) == 1:
        imei = g[0]
    return imei


@session(LogSession.LOG_LEVEL_DEBUG)
def parse_monitor(path):
    monitor = "invalid monitor"
    g = re.findall(r'monitor\-\d{8}', path)
    if len(g) == 1:
        monitor = g[0]
    return monitor

@session(LogSession.LOG_LEVEL_DEBUG)
def parse_ts(line, path=None):
    if len(line) > 50:
        print ("%s is corrupted" % line)
        return 0
    sec = None
    g = re.findall('\-\-\-\-\-\-\-\-\-\-\- (\d+\-\d+\-\d+ \d+:\d+:\d+\.\d+) \-\-\-\-\-\-\-\-\-\-', line)
    if len(g) == 1:
        sec, _ = g[0].split(".")

    if sec is None and path is not None:
        # legecy ts stub without year
        g = re.findall('\-\-\-\-\-\-\-\-\-\-\- (\d+\-\d+ \d+:\d+:\d+\.\d+) \-\-\-\-\-\-\-\-\-\-', line)
        if len(g) == 1:
            sec, _ = g[0].split('.')
            g = re.findall(r'monitor\-(\d{4})', path)
            if len(g) == 1:
                sec = g[0] + "-" + sec
            else:
                sec = None

    total_seconds = 0
    if sec is not None:
        diff = datetime.datetime.strptime(sec, "%Y-%m-%d %H:%M:%S") - datetime.datetime(1970, 1, 1)
        total_seconds = diff.total_seconds()
    return total_seconds


class NodeBase:
    def __init__(self, imei=None, version=None, monitor=None, ts=None, path=None):
        if imei is not None and version is not None and monitor is not None:
            self.imei = imei
            self.version = version
            self.monitor = monitor
        elif path is not None:
            self.imei = parse_imei(path)
            self.version = parse_version(path)
            self.monitor = parse_monitor(path)
        else:
            raise ValueError('invalid args')
        self.ts = ts
        self.hash = get_file_hash(path)

    @session(LogSession.LOG_LEVEL_DEBUG)
    def parse_ts(self, line, path=None):
        self.ts = parse_ts(line, path)

    def __setstate__(self, state):
        return False

    def __getstate__(self):
        return False

    def __repr__(self):
        return self.__str__()

    def __getitem__(self, item):
        return self.__getattr__(item)


def check_initialize(func):
    def wrapper(self, *args, **kwargs):
        if not FileHistoryNode.INITIALIZED:
            raise RuntimeError('call FileHistoryNode.initialize() first')
        return func(self, *args, **kwargs)
    return wrapper


class FileHistoryNode(NodeBase):
    """
    //TODO
    """
    INITIALIZED = False
    TABLE_NAME = 'file_history'
    RESERVED_FIELD_SIZE = 10
    FIELD_DICT = {}
    SQL_TABLE = 'create table if not exists ' + TABLE_NAME + ' (hash varchar(255), version varchar(255), ' \
                'imei varchar(255), ' \
                'monitor varchar(255), ' \
                + '%s int, ' * RESERVED_FIELD_SIZE + 'primary key(hash))'

    INSERT_MANY_PATTERN = 'insert into ' \
                          + TABLE_NAME + \
                          ' (hash, version, imei, monitor, %s) values(%s) ' \
                          'on duplicate key update %s'

    @check_initialize
    def __init__(self, path, node):
        super(FileHistoryNode, self).__init__(path=path)
        if node not in FileHistoryNode.FIELD_DICT:
            raise ValueError("%s hasn't been added yet" % node.__name__)
        field = FileHistoryNode.FIELD_DICT[node]
        self[field] = 1

    @check_initialize
    def sql_args(self):
        arg_list = [self.hash, self.version, self.imei, self.monitor]
        for field in sorted(FileHistoryNode.FIELD_DICT.values()):
            arg_list.append(str(self[field]))
        return tuple(arg_list)

    @check_initialize
    def __getattr__(self, item):
        if item in FileHistoryNode.FIELD_DICT.values():
            if item not in self.__dict__:
                self.__dict__[item] = 0
        return self.__dict__[item]

    @check_initialize
    def __getitem__(self, item):
        return self.__getattr__(item)

    @staticmethod
    def add_node(node, field):
        if len(FileHistoryNode.FIELD_DICT.keys()) > FileHistoryNode.RESERVED_FIELD_SIZE:
            raise IndexError('the size of dict exceeded the limits: %d', FileHistoryNode.RESERVED_FIELD_SIZE)
        if node in FileHistoryNode.FIELD_DICT:
            raise ValueError('duplicated node')
        FileHistoryNode[node] = field

    @staticmethod
    def initialize():
        """
        //TODO
        :return:
        """
        if FileHistoryNode.INITIALIZED:
            return
        field_list = FileHistoryNode.FIELD_DICT.values()
        field_list = sorted(field_list)
        for i in xrange(0, FileHistoryNode.RESERVED_FIELD_SIZE - len(field_list)):
            field_list.append('reserved_field_%d' % i)
        FileHistoryNode.SQL_TABLE = FileHistoryNode.SQL_TABLE % tuple(field_list)

        field_list = FileHistoryNode.FIELD_DICT.values()
        length = len(field_list)
        fields_placeholder = ", ".join(field_list)
        values_placeholder = ", ".join(['%s'] * (length + 4)) # hash version imei monitor
        lst = []
        for field in field_list:
            s = '%s=values(%s)' % (field, field)
            lst.append(s)
        duplicate_placeholder = ", ".join(lst)
        FileHistoryNode.INSERT_MANY_PATTERN = \
            FileHistoryNode.INSERT_MANY_PATTERN % tuple([fields_placeholder,
                                                        values_placeholder,
                                                        duplicate_placeholder])
        FileHistoryNode.INITIALIZED = True


class DbUtil:
    sInstance = None

    DATABASE_DEBUG = 'monitor_debug'
    DATABASE_RELEASE = 'monitor_release'

    TABLE_NAME_FILE_HISTORY = 'file_history'
    TABLE_NAME_PROCESS_MEM = 'process_mem'
    TABLE_NAME_DEVICE_STAT = 'device_stat'
    TABLE_NAME_DEVICE_UP_TIME = 'device_up_time'

    TABLE_DICT = {}

    def __init__(self, debug):
        self.conn = MySQLdb.connect(db=
                                    DbUtil.DATABASE_DEBUG if debug else DbUtil.DATABASE_RELEASE
                                    , user='root', passwd='1')
        cursor = self.conn.cursor()
        for key, val in DbUtil.TABLE_DICT.items():
            sql = 'create table if not exists (%s)' % val
            cursor.execute(sql)
        self.conn.commit()

    def __del__(self):
        self.conn.close()

    @staticmethod
    def init(debug):
        if DbUtil.sInstance is None:
            DbUtil.sInstance = DbUtil(debug)

    @staticmethod
    def get():
        if DbUtil.sInstance is None:
            raise ValueError('sIntance is None')
        return DbUtil.sInstance.conn

    @staticmethod
    def add_table(name, create_cmd):
        DbUtil.TABLE_DICT[name] = create_cmd

    @staticmethod
    def get_df(name):
        if name not in DbUtil.TABLE_DICT.keys():
            raise ValueError('table %s is not added' % name)
        sql = 'select * from %s' % name
        return pd.read_sql(sql, DbUtil.get())


@session(LogSession.LOG_LEVEL_INFO)
def scan_dir(path, kw):
    """
    //TODO
    :param kw:
    :return:
    """
    file_set = set()
    for p, d, f in os.walk(path):
        for _file in f:
            if kw in _file:
                file_set.add(os.path.join(p, _file))
    df = DbUtil.get_df(DbUtil.TABLE_NAME_FILE_HISTORY)
    handled_file_set = set()
    for _file in file_set:
        file_hash = get_file_hash(_file)
        series = df[df['hash'] == file_hash]
        if len(series) == 1:
            target_series = series.ilocp[0]
            if target_series.get[kw] == 1:
                handled_file_set.add(_file)
    return list(file_set - handled_file_set)


@session(LogSession.LOG_LEVEL_DEBUG)
def get_file_hash(path):
    """
    use version + imei + monitor-yymmdd to generate md5sum
    :param path: the file path containing version, imei and monitor-yymmdd
    :return: the md5sum
    """
    # /home/wxl/workspace/log/20180109/1.6.119/864089030004848/15148 53002 056/monitor-20171226
    hash_val = "invalid file hash"
    g = re.findall(r'.*(\d{15}\/\d{13}\/monitor\-\d{8}).*', path)
    if len(g) == 1:
        hash_val = hashlib.md5(g[0]).hexdigest()
    return hash_val

s = '/home/wxl/workspace/log/20180109/1.6.119/864089030004848/1514853002056/monitor-20171226'
FileHistoryNode.initialize()
print (FileHistoryNode.SQL_TABLE)
print (FileHistoryNode.INSERT_MANY_PATTERN)



