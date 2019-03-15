#!/usr/bin/env python

import re
import sys
import datetime
from pyecharts import Line


def parse_val(line):
    return int(re.findall(r'\d+', line)[0])


def parse_meminfo(fname):
    with open(fname, 'r') as f:
        ts, total_free, swap_free = None, 0, 0
        info_list = []
        for line in f.readlines():
            g = re.match(r'.* (?P<ts>\d+\-\d+\-\d+ \d+:\d+:\d+).*', line)
            if g is not None:
                if ts is not None:
                    info_list.append({'ts': ts, 'total_free': round(total_free / 1024.0, 2),
                                      'swap_free': round(swap_free / 1024.0, 2)})
                    total_free = 0
                ts = datetime.datetime.strptime(g.group('ts'), '%Y-%m-%d %H:%M')
            else:
                if 'MemFree' in line:
                    total_free += parse_val(line)
                elif 'Buffers' in line:
                    total_free += parse_val(line)
                elif 'Cached' in line:
                    total_free += parse_val(line)
                elif 'SwapFree' in line:
                    swap_free = parse_val(line)
                elif 'Mapped' in line:
                    total_free -= parse_val(line)
        return info_list


def main():
    res_list = parse_meminfo(sys.argv[1])
    line = Line(width=1600)
    attr = [item['ts'].strftime('%m%d %H%M%S') for item in res_list]
    swap_free = [item['swap_free'] for item in res_list]
    total_free = [item['total_free'] for item in res_list]
    line.add('swap_free', attr, swap_free, is_smooth=True, xaxis_rotate=45, xaxis_interval=5, mark_line=['max', 'average'])
    line.add('total_free', attr, total_free, is_smooth=True, xaxis_rotate=45, xaxis_interval=5, mark_line=['max', 'average'])
    line.render('meminfo.html')


if __name__ == '__main__':
    main()
