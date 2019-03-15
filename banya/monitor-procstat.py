#!/usr/bin/env python

import re
import sys
import datetime
from pyecharts import Line


def parse_stat(fname):
    with open(fname, 'r') as f:
        ts, cpu_total, cpu_idle, ctxt, intr = None, None, None, None, None
        info_list = []
        for line in f.readlines():
            g = re.match('.* (?P<ts>\d+\-\d+\-\d+ \d+:\d+:\d+).*', line)
            if g is not None:
                if ts is not None:
                    info_list.append({'ts': ts, 'cpu_total': cpu_total,
                                      'cpu_idle': cpu_idle, 'ctxt': ctxt,
                                      'intr': intr})
                ts = datetime.datetime.strptime(g.group('ts'), '%Y-%m-%d %H:%M:%S')
            else:
                if 'cpu ' in line:
                    cpu_array = list(map(int, line.split()[1:]))
                    cpu_total = sum(cpu_array)
                    cpu_idle = cpu_array[3]
                elif 'intr' in line:
                    intr = int(line.split()[1])
                elif 'ctxt' in line:
                    ctxt = int(line.split()[1])
    res_list = []
    for i in range(1, len(info_list)):
        duration = (info_list[i]['ts'] - info_list[i - 1]['ts']).total_seconds()
        cpu_total = max(1, info_list[i]['cpu_total'] - info_list[i - 1]['cpu_total'])
        cpu_idle = max(0, info_list[i]['cpu_idle'] - info_list[i - 1]['cpu_idle'])
        ctxt = max(0, info_list[i]['ctxt'] - info_list[i - 1]['ctxt'])
        intr = max(0, info_list[i]['intr'] - info_list[i - 1]['intr'])

        cpu_utilization = (cpu_total - cpu_idle) * 100.0 / cpu_total
        ctxt /= duration
        intr /= duration

        res_list.append({'ts': info_list[i]['ts'].strftime('%m%d %H%M'), 'cpu_utilization': cpu_utilization,
                         'ctxt': int(ctxt), 'intr': int(intr)})
    return res_list


def main():
    res_list = parse_stat(sys.argv[1])
    line = Line(width=1600)
    attr = [item['ts'] for item in res_list]
    cpu_utilization = [item['cpu_utilization'] for item in res_list]
    ctxt = [item['ctxt'] for item in res_list]
    intr = [item['intr'] for item in res_list]

    line.add('cpu', attr, cpu_utilization, is_smooth=True, xaxis_rotate=45, xaxis_interval=15, mark_line=['max', 'average'])
    line.add('ctxt', attr, ctxt, is_smooth=True, xaxis_rotate=45, xaxis_interval=15, mark_line=['max', 'average'])
    line.add('intr', attr, intr, is_smooth=True, xaxis_rotate=45, xaxis_interval=15, mark_line=['max', 'average'])
    line.render('procstat.html')


if __name__ == '__main__':
    main()
