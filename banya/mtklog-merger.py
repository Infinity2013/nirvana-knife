#!/usr/bin/env python

import os
import re
import datetime
import operator


def scan_dir(path):
    kw_list = [
        'kernel_log',
        'sys_log',
        'main_log',
        'events_log',
    ]
    target_list = []
    for p, d, f, in os.walk(path):
        for _file in f:
            for kw in kw_list:
                if kw in _file:
                    target_list.append(os.path.join(p, _file))
                    break
    return target_list


def utc2msec(utc):
    _date, _msec = utc.split(".")
    seconds = (datetime.datetime.strptime(_date, "%Y-%m-%d %H:%M:%S") - datetime.datetime(1970, 1, 1)).total_seconds()
    _msec = float("0.%s" % _msec)
    print "%d, %f" % (seconds, _msec)
    msec = seconds * 1000 * 1000 + _msec * 1000 * 1000

    return msec

def main():
    file_list = scan_dir(os.getcwd())
    reference = []
    for _file in file_list:
        if 'kernel_log' in _file:
            with open(_file, 'r') as f:
                for line in f.readlines():
                    #  <4>[ 6144.017817]<0>-(0)[0:swapper/0][thread:149][RT:6144007858353] 2017-12-08 08:21:20.3704 UTC; android time 2017-12-08 16:21:20.3704
                    if 'android time' in line:
                        g = re.match(r'<\d+>\[\s*(?P<uptime>[\d\.]+)\].*android time (?P<utc>\d+\-\d+\-\d+ \d+:\d+:\d+\.\d+).*', line)
                        if g is not None:
                            reference = [float(g.group('uptime')), utc2msec(g.group('utc'))]
                            print "ref: " + ", ".join(map(str, reference))
                        else:
                            print "Failed to parse reference"
                            exit(-1)
                        break
            break
    log_list = []
    for _file in file_list:
        if 'kernel_log' in _file:
            with open(_file, 'r') as f:
                for line in f.readlines():
                    g = re.match(r'<\d+>\[\s*(?P<uptime>[\d\.]+)\].*', line)
                    if g is not None:
                        diff = float(g.group('uptime')) - reference[0]
                        normalized_time = reference[1] + diff * 1000 * 1000
                        log_list.append([normalized_time, line])
                    else:
                        print "invalid line in %s: %s" % (_file, line)
        else:
            with open(_file, 'r') as f:
                for line in f.readlines():
                    # 12-08 16:17:06.286   265  4013 D         : [AAA_Scheduler::importWork]+, mNormalizeM=1, mSensorDevId=1
                    g = re.match(r'(?P<utc>\d+\-\d+ \d+:\d+:\d+\.\d+).*', line)
                    if g is not None:
                        _date, _msec = g.group('utc').split(".")
                        _date = "2017-%s" % _date
                        normalized_time = (datetime.datetime.strptime(_date, "%Y-%m-%d %H:%M:%S") \
                                          - datetime.datetime(1970, 1, 1)).total_seconds() * 1000 * 1000 + long(_msec) * 1000
                        log_list.append([normalized_time, line])
                    else:
                        print "invalid line in %s: %s" % (_file, line)
    log_list = sorted(log_list, key=operator.itemgetter(0))
    with open('merged.log', 'w') as f:
        for log in log_list:
            seconds = log[0] / (1000 * 1000)
            msec = log[0] % (1000 * 1000)
            ts = datetime.datetime(1970, 1, 1) + datetime.timedelta(seconds=seconds)
            ts = "%s.%d" % (ts.strftime("%Y-%m-%d %H:%M:%S"), msec)
            f.write("%s, %s" % (ts, log[1]))


if __name__ == '__main__':
    main()




