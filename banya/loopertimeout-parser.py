#!/usr/bin/env python

import re
import sys
import os
import pandas as pd
import operator
import multiprocessing


def multiWrapper(totalDict, totalCountDict, lock, traceList, i, threads, cmdline):
    length = len(traceList)
    targetList = traceList[i * length / threads: (i + 1) * length / threads]
    localDict, localCountDict = {}, {}
    for trace in targetList:
        btDict, btCountDict = parseMaincause(trace, cmdline)
        localDict = dict(btDict, **localDict)
        for key, value in btCountDict.items():
            if key in localCountDict:
                localCountDict[key] |= btCountDict[key]
            else:
                localCountDict[key] = btCountDict[key]

    lock.acquire()
    for key in localDict:
        if key not in totalDict:
            totalDict[key] = localDict[key]
    for key, value in localCountDict.items():
        if key in totalCountDict:
            totalCountDict[key] |= localCountDict[key]
        else:
            totalCountDict[key] = localCountDict[key]
    lock.release()


def parseMaincause(fname, cmdline):
    print "%d: %s" % (os.getpid(), fname)
    lines = open(fname, 'r').readlines()
    found = False
    btDict = {}
    btCountDict = {}
    id = ""
    try:
        for i in xrange(len(lines)):
            line = lines[i]
            btList = []
            if not found:
                if '-- loopertimeout --' in line:
                    i += 1
                    g = re.match(r'\-\-\-\-\- (?P<id>[\w\s\-\:]+) \-\-\-\-\-', lines[i])
                    if g is not None:
                        id = g.group('id')
                        i += 1
                        if lines[i].strip() == 'Cmd line: %s' % cmdline:
                            found = True
            if found:
                if line.strip() == '"main" prio=5 tid=1 Native':
                    while lines[i].strip() != "":
                        btList.append(lines[i])
                        i += 1
                    found = False
                    key = ""
                    for bt in btList:
                        g = re.match(r'at .*', bt.strip())
                        if g is not None:
                            key = bt.strip()
                            break
                    if key not in btCountDict:
                        btCountDict[key] = set()
                        btDict[key] = "".join(btList)
                    btCountDict[key].add(id)
    except IndexError:
        pass
    return btDict, btCountDict


manager = multiprocessing.Manager()
lock = manager.Lock()
totalDict, totalCountDict = manager.dict(), manager.dict()
traceList = manager.list()

for p, d, f in os.walk(os.getcwd()):
    for fname in f:
        if 'traces.txt' == fname:
            path = os.path.join(p, 'traces.txt')
            traceList.append(path)

print ("%d traces.txt is found" % len(traceList))
processList = []
threads = 4
for i in xrange(threads):
    process = multiprocessing.Process(target=multiWrapper,
                                      args=(totalDict, totalCountDict, lock, traceList, i, threads, sys.argv[1], ))
    process.start()
    processList.append(process)

for process in processList:
    process.join()


summaryList = []
for key in totalDict.keys():
    summaryList.append([key, len(totalCountDict[key]), totalDict[key]])

summaryList = sorted(summaryList, key=operator.itemgetter(1), reverse=True)

with open('%s.loopersumary' % sys.argv[1], 'w') as f:
    for summary in summaryList:
        line = "%s ---- %d\n %s" % (summary[0], summary[1], summary[2])
        print line
        f.write(line)
