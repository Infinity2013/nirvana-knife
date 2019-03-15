#!/usr/bin/env python

import re
import sys
import datetime
from pyecharts import Line
from pyecharts import Page
import pandas as pd
import operator


def parse_procrank(fname):
    with open(fname, 'r') as f:
        ts, memory, comm = None, 0, None
        info_list = []
        local_map = {}
        for line in f.readlines():
            g = re.match(r'.* (?P<ts>\d+\-\d+\-\d+ \d+:\d+:\d+).*', line)
            if g is not None:
                if ts is not None:
                    info_list.append(local_map)
                    local_map = {}
                ts = datetime.datetime.strptime(g.group('ts'), '%Y-%m-%d %H:%M:%S').strftime('%m%d-%H%M')
                local_map['ts'] = ts
            else:
                g = re.match(r'\s*\d+\s+\d+\s+\d+K\s+(?P<rss>\d+)K\s+(?P<swap>\d+)K\s+\d+\s+(?P<comm>.*)', line)
                if g is not None:
                    comm = g.group('comm')
                    memory = int(g.group('rss')) + int(g.group('swap'))
                    if memory >= 10 * 1024:
                        local_map[comm] = round(memory / 1024.0, 2)
        return info_list


def main():
    res_list = parse_procrank(sys.argv[1])
    df = pd.DataFrame(res_list).fillna(0)
    ts = df['ts']
    df.drop(columns='ts', inplace=True)
    local_list = []
    max_list = []
    for column in df.columns:
        local_max = max(df[column])
        max_list.append(local_max)
        local_map = {
            'comm': column,
            'memory': df[column].tolist(),
            'max': local_max
        }
        local_list.append(local_map)
    local_list = sorted(local_list, key=operator.itemgetter('max'))
    page = Page()
    index = 0
    line = None
    for _map in local_list:
        if index % 3 == 0:
            line = Line(width=1600)
            page.add(line)
        line.add(_map['comm'], ts, _map['memory'], is_smooth=True, xaxis_rotate=45, xaxis_interval=5,
                 mark_line=['max', 'average'])
        index += 1
    page.render('procrank.html')


if __name__ == '__main__':
    main()
