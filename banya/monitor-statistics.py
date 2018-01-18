#!/usr/bin/env python
from __future__ import print_function
import re
import os
import operator
import MySQLdb
import warnings
import multiprocessing
import sys
import hashlib
import datetime
import pandas as pd
import numpy as np
warnings.filterwarnings('ignore')

DBG = False
EXCUTE_MANY_COUNT_MAX = 1000
MEM_UNIT = 5 * 1024  # 5M
UPTIME_UNIT = 30 * 60  # 0.5h
MONITOR_INTERVAL = 180
UNIT_DICT = {
    "cpu_utilization": 2,
    "ctx_per_sec": 500,
    "intr_per_sec": 500,
    "softirq_per_sec": 100,
}


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


class UptimeNode:
    def __init__(self, imei, ts, unit, version, up=None, idle=None, sleep=None, line=None):
        self.imei = imei
        self.ts = ts
        self.unit = unit
        self.version = version
        self.line = line
        if line is None or (up is not None and idle is not None and sleep is not None):
            self._up = up
            self._idle = idle
            self._sleep = sleep
        else:
            _up, _idle, _sleep = 0, 0, 0
            # up time: 6 days, 01:13:49, idle time: 05:57:54, sleep time: 5 days, 19:59:51
            g = re.match(r'up time: ((?P<up_day>\d+) days, )*(?P<up_hour>\d+):(?P<up_min>\d+):(?P<up_sec>\d+), '
                         r'idle time: ((?P<idle_day>\d+) days, )*(?P<idle_hour>\d+):(?P<idle_min>\d+):(?P<idle_sec>\d+), '
                         r'sleep time: ((?P<sleep_day>\d+) days, )*(?P<sleep_hour>\d+):(?P<sleep_min>\d+):(?P<sleep_sec>\d+)',
                         line)
            if g is not None:
                domain_dict = g.groupdict()
                for key, val in domain_dict.items():
                    domain_dict[key] = int(val) if val is not None else 0
                _up = datetime.timedelta(days=domain_dict['up_day'],
                                         hours=domain_dict['up_hour'],
                                         minutes=domain_dict['up_min'],
                                         seconds=domain_dict['up_sec']).total_seconds()
                _idle = datetime.timedelta(days=domain_dict['idle_day'],
                                         hours=domain_dict['idle_hour'],
                                         minutes=domain_dict['idle_min'],
                                         seconds=domain_dict['idle_sec']).total_seconds()
                _sleep = datetime.timedelta(days=domain_dict['sleep_day'],
                                         hours=domain_dict['sleep_hour'],
                                         minutes=domain_dict['sleep_min'],
                                         seconds=domain_dict['sleep_sec']).total_seconds()

            self._up = _up
            self._idle = _idle
            self._sleep = _sleep

    def __getattr__(self, item):
        if item in ['up', 'idle', 'sleep']:
            if item not in self.__dict__:
                val = self.__dict__["_" + item]
                val = max(val, self.unit)
                val = (self.unit / 3600.0) * int((val / self.unit))
                self.__dict__[item] = val
        return self.__dict__[item]

    def __str__(self):
        s = 'imei: %s, ts: %d, unit: %d, version: %s, up time: %s, idle time: %s, sleep time: %s' % (
            self.imei, self.ts, self.unit, self.version, self._up, self._idle, self._sleep
        )
        return s

    def __getinitargs__(self):
        return self.imei, self.ts, self.unit, self.version, self._up, self._idle, self._sleep, self.line

    def __getstate__(self):
        return False

    def __setstate__(self, stat):
        return False

    def __repr__(self):
        return self.__str__()


def scan_dir(path, kw):
    file_list = []
    for p, d, f in os.walk(path):
        for _file in f:
            if kw in _file:
                file_list.append(os.path.join(p, _file))
    return file_list


def get_image_ver(path):
    image_ver = "invalid image ver"
    g = re.findall(r'\d\.\d\.\d+', path)
    if len(g) == 1:
        image_ver = g[0]
    return image_ver


def get_file_hash(path):
    # /home/wxl/workspace/log/20180109/1.6.119/864089030004848/15148 53002 056/monitor-20171226
    hash_val = "invalid file hash"
    g = re.findall(r'.*(\d{15}\/\d{13}\/monitor\-\d{8}).*', path)
    if len(g) == 1:
        hash_val = hashlib.md5(g[0]).hexdigest()
    return hash_val


def get_imei(path):
    imei = "invalid imei"
    g = re.findall(r'86\d{13}', path)
    if len(g) == 1:
        imei = g[0]
    return imei


def parse_ts_stub(line, path=None):
    line = line.strip(chr(0x00))
    with LogSession(LogSession.LOG_LEVEL_DEBUG, 'parse_ts_stub(%s, %s)' % (line, path)):
        # ----------- 01-01 08:18:35.004 ---------- length = 41
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


def parse_procrank(path):
    with LogSession(LogSession.LOG_LEVEL_DEBUG, 'parse_procrank'):
        proc_dict = {}
        if not os.path.exists(path):
            return proc_dict
        with open(path, 'r') as f:
            lines = f.readlines()
            #  731  1611376K   97440K   53919K   45344K       0K       0K  system_server
            ts = ""
            for i in xrange(len(lines)):
                line = lines[i]
                g = re.findall('\-\-\-\-\-\-\-\-\-\-\- (?P<ts>\d+\-\d+ \d+:\d+:\d+\.\d+) \-\-\-\-\-\-\-\-\-\-', line)
                if len(g) == 1:
                    ts = g[0]
                    continue

                g = re.match(
                    r'\s*(?P<pid>\d+)\s+(?P<vss>\d+)K\s+(?P<rss>\d+)K\s+'
                    r'(?P<pss>\d+)K\s+(?P<uss>\d+)K\s+(?P<swap>\d+)K\s+(?P<pswap>\d+)K\s+(?P<cmdline>.*)$', line)
                if g is not None:
                    pid = g.group('pid')
                    cmdline = g.group('cmdline')
                    mem = sum(map(int, [g.group('rss'), g.group('swap')]))
                    proc_dict[ts + pid] = [cmdline, get_image_ver(path), mem]
        return proc_dict


def parse_ion(path):
    with LogSession(LogSession.LOG_LEVEL_DEBUG, 'parse_ion'):
        ion_dict = {}
        if not os.path.exists(path):
            return ion_dict
        with open(path, 'r') as f:
            lines = f.readlines()
            ts = ""
            for i in xrange(len(lines)):
                line = lines[i]
                g = re.findall('\-\-\-\-\-\-\-\-\-\-\- (?P<ts>\d+\-\d+ \d+:\d+:\d+\.\d+) \-\-\-\-\-\-\-\-\-\-', line)
                if len(g) == 1:
                    ts = g[0]
                    continue
                g = re.match(r'\s*(?P<process>[\w\.]+)\(\s*\w+\)\s+(?P<pid>\d+)\s+(?P<size>\d+)\s+(?P<addr>\w+)', line)
                if g is not None:
                    pid = g.group('pid')
                    size = int(g.group('size')) / 1024
                    if ts + pid not in ion_dict:
                        ion_dict[ts + pid] = 0
                    ion_dict[ts + pid] += size
        return ion_dict


def parse_gpu(path):
    with LogSession(LogSession.LOG_LEVEL_DEBUG, 'parse_gpu'):
        gpu_dict = {}
        if not os.path.exists(path):
            return gpu_dict

        with open(path, 'r') as f:
            lines = f.readlines()
            ts = ""
            for i in xrange(len(lines)):
                line = lines[i]
                g = re.findall('\-\-\-\-\-\-\-\-\-\-\- (?P<ts>\d+\-\d+ \d+:\d+:\d+\.\d+) \-\-\-\-\-\-\-\-\-\-', line)
                if len(g) == 1:
                    ts = g[0]
                    continue
                g = re.match(r'\s*(?P<addr>[\w\-]+)\s+(?P<page>\d+)\s+(?P<pid>\d+).*', line)
                if g is not None:
                    pid = g.group('pid')
                    size = int(g.group('page')) * 4
                    gpu_dict[ts + pid] = size
        return gpu_dict


def parse_mem(path):
    pwd = os.path.dirname(path)
    procrank_path = os.path.join(pwd, 'procrank')
    ion_path = os.path.join(pwd, 'ion_heap')
    gpu_path = os.path.join(pwd, 'gpu_ion')

    proc_dict = parse_procrank(procrank_path)
    ion_dict = parse_ion(ion_path)
    gpu_dict = parse_gpu(gpu_path)

    # step 1 add ion_heap to proc_mem
    for key, val in ion_dict.items():
        if key in proc_dict:
            element = proc_dict[key]
            # [cmdline, version, mem]
            element[2] += val
            proc_dict[key] = element

    # step 2 add gpu_ion to proc_mem
    for key, val in gpu_dict.items():
        if key in proc_dict:
            element = proc_dict[key]
            # [cmdline, version, mem]
            element[2] += val
            proc_dict[key] = element

    # step 3 ronund up mem with MEM_UNIT
    mem_list = proc_dict.values()
    for i in xrange(len(mem_list)):
        element = mem_list[i]
        element[2] /= MEM_UNIT
        element[2] = max(element[2], 1)
        element[2] *= 5
    return mem_list


def multi_wrapper(input_list, output_list, lock, thread_index, thread_count, handler):
    with LogSession(LogSession.LOG_LEVEL_INFO, 'multi_wrapper'):
        length = len(input_list)
        local_list = input_list[thread_index * length / thread_count: (thread_index + 1) * length / thread_count]
        res_list = []
        for _file in local_list:
            res_list.extend(handler(_file))

        lock.acquire()
        output_list.extend(res_list)
        lock.release()


def do_job(input_list, handler):
    with LogSession(LogSession.LOG_LEVEL_INFO, "do_job(%d, %s)" % (len(input_list), handler.__name__)):
        manager = multiprocessing.Manager()
        lock = manager.Lock()
        output_list = manager.list()
        thread_count = 3
        process_list = []
        for i in xrange(thread_count):
            process = multiprocessing.Process(target=multi_wrapper,
                                              args=(input_list, output_list, lock, i, thread_count, handler,))
            process_list.append(process)
            process.start()
        for process in process_list:
            process.join()

        return output_list


def init_db():
    conn = MySQLdb.connect(db='monitor', user='root', passwd='1')
    return conn


def do_mem_job(path):
    with LogSession(LogSession.LOG_LEVEL_INFO, "do_mem_job(%s)" % path):
        conn = init_db()
        file_list = scan_dir(path, 'procrank')
        file_list = check_unhandled_files(file_list, 'mem')
        if len(file_list) == 0:
            return

        output_list = do_job(file_list, parse_mem)
        with LogSession(LogSession.LOG_LEVEL_INFO, 'covert manger.list to list'):
            output_list = list(output_list)

        if len(output_list) == 0:
            return

        df = pd.DataFrame(output_list)
        df.rename_axis(axis=1, mapper={0: 'process', 1: 'version', 2: 'mem'}, inplace=True)
        total_dist = df.groupby(['process', 'version', 'mem']).size().to_dict()
        dist_list = []
        for key, val in total_dist.items():
            # (process, version, mem): count
            process = key[0]
            version = key[1]
            mem = str(key[2])
            count = val
            md5_val = hashlib.md5(process + version + mem).hexdigest()
            dist_list.append([md5_val, process, version, mem, count])

        conn = init_db()
        cursor = conn.cursor()
        length = len(dist_list)
        with LogSession(LogSession.LOG_LEVEL_INFO, 'insert into process_mem'):
            with LogSession(LogSession.LOG_LEVEL_DEBUG, 'create process_mem if not exists'):
                cursor.execute(
                    'create table if not exists process_mem '
                    '(hash varchar(255), process varchar(255), version varchar(255), mem varchar(255), count int, '
                    'primary key(hash))')
            insert_pattern = \
                'insert into process_mem ' \
                '(hash, process, version, mem, count) ' \
                'values(%s, %s, %s, %s, %s) ' \
                'on duplicate key update count=count+values(count)'
            for i in xrange(0, length, EXCUTE_MANY_COUNT_MAX):
                idx = min(length, i + EXCUTE_MANY_COUNT_MAX)
                cursor.executemany(insert_pattern, dist_list[i: idx])

        with LogSession(LogSession.LOG_LEVEL_INFO, 'insert into file_history'):
            insert_pattern = 'insert into file_history (hash, mem) values(%s, 1) on duplicate key update mem=1'
            length = len(file_list)
            for i in xrange(0, length, EXCUTE_MANY_COUNT_MAX):
                idx = min(i+EXCUTE_MANY_COUNT_MAX, length)
                file_hash_list = []
                for _file in file_list[i: idx]:
                    file_hash_list.append([get_file_hash(_file)])
                cursor.executemany(insert_pattern, file_hash_list)
        conn.commit()
        conn.close()


def check_unhandled_files(file_list, file_type):
    with LogSession(LogSession.LOG_LEVEL_INFO, 'check_unhandled_files(%d, %s)' % (len(file_list), file_type)):
        conn = init_db()
        cursor = conn.cursor()
        cursor.execute('create table if not exists file_history '
                       '(hash varchar(255), mem int, uptime int, stat int, launchtime int, primary key(hash))')
        df = pd.read_sql('select * from file_history', conn)
        conn.close()
        with LogSession(LogSession.LOG_LEVEL_DEBUG, 'convert list to set'):
            file_set = set(file_list)
        proceeded_file_set = set()
        for _file in file_list:
            hash_val = get_file_hash(_file)
            target = df[df['hash'] == hash_val]
            if len(target.index) == 1:
                if target.iloc[0][file_type] == 1:
                    proceeded_file_set.add(_file)
        print("%d files is filterd out" % len(proceeded_file_set))
        file_set = file_set - proceeded_file_set

        with LogSession(LogSession.LOG_LEVEL_DEBUG, 'convert set to list'):
            file_list = list(file_set)

        if DBG:
            length = len(file_list)
            file_list = file_list[:min(length / 10, 100)]
        return file_list


def parse_uptime(path):
    """parse a uptime file
    Args:
        path: uptime file path
    Return:
        parsed list
        pattern:
        [
            [imei, ts, version, up, idle, sleep],
            [imei, ts, version, up, idle, sleep],
            [imei, ts, version, up, idle, sleep],
            ...
            [imei, ts, version, up, idle, sleep],
        ]
    """
    with LogSession(LogSession.LOG_LEVEL_DEBUG, 'parse_uptime(%s)' % path):
        # don't handle files whose timestamp is wrong

        output_list = []
        if 'monitor-2016' in path:
            return output_list

        if not os.path.exists(path):
            return output_list

        imei = get_imei(path)
        version = get_image_ver(path)
        with open(path, 'r') as f:
            lines = f.readlines()

            for i in xrange(0, len(lines) - 1, 2):
                ts = parse_ts_stub(lines[i], path)
                if ts == 0:
                    continue
                node = UptimeNode(imei, ts, UPTIME_UNIT, version, line=lines[i + 1])
                output_list.append(node)
            return output_list


def do_uptime_job(path):
    with LogSession(LogSession.LOG_LEVEL_INFO, 'do_uptime_job(%s)' % path):
        file_list = scan_dir(path, 'uptime')
        file_list = check_unhandled_files(file_list, 'uptime')
        if len(file_list) > 0:
            output_list = do_job(file_list, parse_uptime)
            if len(output_list) > 0:
                imei_dict = {}
                # step 1 group output by imei
                for line in output_list:
                    imei = line.imei
                    if imei not in imei_dict:
                        imei_dict[imei] = []
                    imei_dict[imei].append(line)
                # step 2 sort each group by ts
                for key in imei_dict.keys():
                    imei_dict[key] = sorted(imei_dict[key], key=operator.attrgetter('ts'))
                # step 3 parse uptime
                parsed_output_list = []
                for key in imei_dict.keys():
                    elements = imei_dict[key]
                    for i in xrange(len(elements) - 1):
                        if elements[i + 1].up < elements[i].up:
                            element = elements[i]
                            parsed_output_list.append([element.version, 'up', element.up])
                            parsed_output_list.append([element.version, 'idle', element.idle])
                            parsed_output_list.append([element.version, 'sleep', element.sleep])

                df = pd.DataFrame(parsed_output_list)
                df.rename_axis(axis=1, mapper={0: 'version', 1: 'domain', 2: 'range'}, inplace=True)
                total_dist = df.groupby(['domain', 'version', 'range']).size().to_dict()
                final_dist_list = []
                for key, val in total_dist.items():
                    version = key[1]
                    domain = key[0]
                    range = key[2]
                    count = val
                    hash_val = hashlib.md5(domain+version+str(range)).hexdigest()
                    final_dist_list.append([hash_val, domain, version, range, count])

                conn = init_db()
                cursor = conn.cursor()
                cursor.execute('create table if not exists device_uptime '
                               '(hash varchar(255), domain varchar(255), version varchar(255), '
                               'value_range float, count int, primary key(hash))')
                length = len(final_dist_list)
                for i in xrange(0, length, EXCUTE_MANY_COUNT_MAX):
                    idx = min(i+EXCUTE_MANY_COUNT_MAX, length)
                    insert_pattern = "insert into device_uptime (hash, domain, version, value_range, count)" \
                                     "values (%s, %s, %s, %s, %s) on duplicate key update count=count+values(count)"
                    cursor.executemany(insert_pattern, final_dist_list[i: idx])
                if not DBG:
                    pattern = "insert into file_history (hash, uptime) " \
                                     "values (%s, 1) on duplicate key update uptime=1"
                    hash_file_list = map(lambda x: [x], map(get_file_hash, file_list))
                    length = len(hash_file_list)
                    for i in xrange(0, length, EXCUTE_MANY_COUNT_MAX):
                        idx = min(i+EXCUTE_MANY_COUNT_MAX, length)
                        cursor.executemany(pattern , hash_file_list[i: idx])
                conn.commit()
                conn.close()


class StatNode:
    CPU_INDEX_USER = 0
    CPU_INDEX_NICE = 1
    CPU_INDEX_SYSTEM = 2
    CPU_INDEX_IDLE = 3
    CPU_INDEX_IOWAIT = 4
    CPU_INDEX_IRQ = 5
    CPU_INDEX_SOFTIRQ = 6

    def __init__(self, imei, ts, version, cpu=None, intr=None, ctx=None, softirq=None, block=None):
        self.imei = imei
        self.ts = ts
        self.version = version
        self.block = block
        self.prev = None
        if block is None or (cpu is not None and intr is not None and ctx is not None and softirq is not None):
            self.cpu = cpu
            self.intr = intr
            self.ctx = ctx
            self.softirq = softirq
        else:
            '''
            block pattern:
                cpu  3437145 16762 2675978 4148085 407583 1 6210 0 0 0
                cpu0 2121676 10626 1448313 2300261 294224 0 4548 0 0 0
                intr 269131535 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 0 51203076 ... 0 
                ctxt 522865101
                btime 1515516189
                processes 746606
                procs_running 3
                procs_blocked 0
                softirq 40912401 8607 11445159 12899 1878042 8606 8606 3630772 7085284 135 16834291
            '''
            for line in block:
                line = line.strip()
                if 'cpu ' in line:
                    self.cpu = map(int, line.split()[1:])
                elif 'intr' in line:
                    self.intr = map(int, line.split()[1:])
                elif 'ctx' in line:
                    self.ctx = int(line.split()[1])
                elif 'softirq'in line:
                    self.softirq = map(int, line.split()[1:])

            if self.cpu is None or self.intr is None or self.ctx is None or self.softirq is None:
                raise ValueError('invalid block')

    def __getattr__(self, item):
        if item in ['cpu_utilization', 'intr_per_sec', 'ctx_per_sec', 'softirq_per_sec']:
            if item not in self.__dict__:
                if self.prev is None:
                    return 0
                else:
                    duration = self.ts - self.prev.ts
                    val = 0
                    if item == 'cpu_utilization':
                        idle_diff = self.cpu[StatNode.CPU_INDEX_IDLE] - self.prev.cpu[StatNode.CPU_INDEX_IDLE]
                        total_diff = (sum(self.cpu) - sum(self.prev.cpu))
                        val = 100 * float(total_diff - idle_diff) / total_diff
                    elif item == 'ctx_per_sec':
                        ctx_diff = self.ctx - self.prev.ctx
                        val = ctx_diff / duration
                    elif item == 'intr_per_sec':
                        intr_diff = self.intr[0] - self.prev.intr[0]
                        val = intr_diff / duration
                    elif item == 'softirq_per_sec':
                        softirq_diff = self.softirq[0] - self.prev.softirq[0]
                        val = softirq_diff / duration
                    self.__dict__[item] = val
        return self.__dict__[item]

    def __getitem__(self, item):
        return self.__getattr__(item)

    @property
    def prev(self):
        return self.prev

    @prev.setter
    def set_prev(self, node):
        if isinstance(node, StatNode):
            self.prev = node

    def __getinitargs__(self):
        return self.imei, self.ts, self.version, self.cpu, self.intr, self.ctx, self.softirq, self.block

    def __getstate__(self):
        return False

    def __setstate__(self, state):
        return False

    def __str__(self):
        attr_list = []
        attr_list.append("self:")
        attr_list.append("imei: %s, ts: %s, version: %s" % (self.imei, second_to_date(self.ts), self.version))
        attr_list.append("cpu: %s" % " ".join(map(str, self.cpu)))
        attr_list.append("ctx: %s" % str(self.ctx))
        if self.prev is not None:
            attr_list.append("prev:")
            attr_list.append("imei: %s, ts: %s, version: %s" % (self.prev.imei, second_to_date(self.prev.ts), self.prev.version))
            attr_list.append("cpu: %s" % " ".join(map(str, self.prev.cpu)))
            attr_list.append("ctx: %s" % str(self.prev.ctx))
        return "\n".join(attr_list)

    def __repr__(self):
        return self.__str__()


def parse_stat(path):
    with LogSession(LogSession.LOG_LEVEL_DEBUG, 'parse_stat(%s)' % path):
        output_list = []
        if os.path.exists(path) and 'monitor-2016' not in path:
            imei = get_imei(path)
            version = get_image_ver(path)
            with open(path, 'r') as f:
                lines = f.readlines()
                prev = -1
                for i in xrange(1, len(lines)):
                    if "-----------" in lines[i]:
                        if prev == -1:
                            prev = i
                        else:
                            ts = parse_ts_stub(lines[i], path)
                            if ts == 0:
                                print (path, i)
                                prev = -1
                                continue
                            try:
                                node = StatNode(imei, ts, version, block=lines[prev: i])
                                prev = i
                                output_list.append(node)
                            except ValueError:
                                pass
        return output_list


def custom_round(val, base):
    val = max(val, base)
    val = int(val / base)
    val *= base
    return val


def second_to_date(second):
    days, seconds = divmod(second, 3600 * 24)
    date = datetime.datetime(1970, 1, 1) + datetime.timedelta(days=days, seconds=seconds)
    return date


def do_stat_job(path):
    with LogSession(LogSession.LOG_LEVEL_INFO, "do_stat_job(%s)" % path):
        file_list = scan_dir(path, 'procstat')
        file_list = check_unhandled_files(file_list, 'stat')
        if len(file_list) > 0:
            output_list = do_job(file_list, parse_stat)
            if len(output_list) > 0:
                # step 1 group by imei
                imei_dict = {}
                for node in output_list:
                    if node.imei not in imei_dict:
                        imei_dict[node.imei] = []
                    imei_dict[node.imei].append(node)
                # step 2 sort each group by ts
                for key in imei_dict.keys():
                    single_imei_nodes = imei_dict[key]
                    single_imei_nodes = sorted(single_imei_nodes, key=operator.attrgetter('ts'))
                    imei_dict[key] = single_imei_nodes
                # step calculate per second attrs
                final_node_list = []
                for key in imei_dict.keys():
                    single_imei_nodes = imei_dict[key]
                    for i in xrange (1, len(single_imei_nodes)):
                        if single_imei_nodes[i].ts - single_imei_nodes[i-1].ts < MONITOR_INTERVAL and \
                                single_imei_nodes[i].ctx > single_imei_nodes[i-1].ctx:
                            single_imei_nodes[i].prev = single_imei_nodes[i-1]
                            final_node_list.append(single_imei_nodes[i])

                parsed_node_list = []
                for node in final_node_list:
                    for key in ['cpu_utilization', 'ctx_per_sec', 'intr_per_sec', 'softirq_per_sec']:
                        domain = key
                        val = custom_round(node[domain], UNIT_DICT[domain])
                        parsed_node_list.append([domain, node.version, val])

                df = pd.DataFrame(parsed_node_list)
                df.rename_axis(axis=1, mapper={0: "domain", 1: "version", 2: "value_range"}, inplace=True)
                final_dist_dict = df.groupby(['domain', 'version', 'value_range']).size().to_dict()
                final_dist_list = []
                for key, val in final_dist_dict.items():
                    domain = key[0]
                    version = key[1]
                    value_range = str(key[2])
                    count = val
                    hash_val = hashlib.md5(domain + version + value_range).hexdigest()
                    final_dist_list.append([hash_val, domain, version, value_range, str(count)])

                conn = init_db()
                cursor = conn.cursor()
                cursor.execute('create table if not exists device_stat (hash varchar(255), domain varchar(255), '
                               'version varchar(255), value_range int, count int, primary key(hash))')
                length = len(final_node_list)
                for i in xrange(0, length, EXCUTE_MANY_COUNT_MAX):
                    idx = min(i + EXCUTE_MANY_COUNT_MAX, length)
                    sql_pattern = 'insert into device_stat (hash, domain, version, value_range, count) values' \
                                  '(%s, %s, %s, %s, %s) on duplicate key update count=count+values(count)'
                    cursor.executemany(sql_pattern, final_dist_list[i: idx])

                file_hash_list = map(lambda x:[x], map(get_file_hash, file_list))
                length = len(file_hash_list)
                for i in xrange(0, length, EXCUTE_MANY_COUNT_MAX):
                    idx = min(i + EXCUTE_MANY_COUNT_MAX, length)
                    sql_pattern = 'insert into file_history (hash, stat) values (%s, 1) on duplicate key update stat=1'
                    cursor.executemany(sql_pattern, file_hash_list[i: idx])
                conn.commit()
                conn.close()


do_stat_job('1.6.119')
do_stat_job('1.7.125')
do_stat_job('1.8.130')
do_stat_job('1.8.132')







