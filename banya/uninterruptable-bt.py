#!/usr/bin/env python

import os
import re
import multiprocessing
import operator

def scan_dir(path, kw='traces.txt'):
    file_list = []
    for p, d, f in os.walk(path):
        for file in f:
            if kw in file:
                file_list.append(os.path.join(p, file))

    return file_list


def uninterruptable_bt_main(path):
    bt_dict, btcount_dict= {}, {}
    with open(path, 'r') as f:
        lines = f.readlines()
        length = len(lines)
        for i in xrange(length):
            line = lines[i]
            if 'state=D' in line:
                id = ""
                g = re.findall(r'sysTid=(\d+)', lines[i - 1])
                if len(g) == 0:
                    return
                else:
                    id = g[0]
                bt_list = []
                for j in xrange(i + 3, length):
                    if len(lines[j].strip()) == 0:
                        break
                    bt_list.append(lines[j])
                key = ""
                for bt in bt_list:
                    if 'at ' in bt:
                        key = bt.strip()
                        break;
                if key == "":
                    print 'invalid %s:%d' % (path, i)
                else:
                    if key not in bt_dict:
                        bt_dict[key] = " ".join(bt_list)
                        btcount_dict[key] = set()
                    btcount_dict[key].add(id)
    return bt_dict, btcount_dict


def multiwrapper(totaldict, totalcount_dict, lock, tracelist, threadidx, threadcount):
    localdict, localcount_dict = {}, {}
    length = len(tracelist)
    local_tracelist = tracelist[threadidx * length / threadcount: (threadidx + 1) * length / threadcount]
    for trace in local_tracelist:
        btdict, btcount_dict = uninterruptable_bt_main(trace)
        localdict = dict(btdict, **localdict)
        for key in btcount_dict.keys():
            if key not in localcount_dict:
                localcount_dict[key] = set()
            localcount_dict[key] |= btcount_dict[key]
    lock.acquire()
    for key in localdict.keys():
        if key not in totaldict:
            totaldict[key] = localdict[key]
    for key in localcount_dict.keys():
        if key not in totalcount_dict:
            totalcount_dict[key] = set()
        totalcount_dict[key] |= localcount_dict[key]
    lock.release()


def main():
    file_list = scan_dir(os.getcwd())
    threadcount = 4

    manager = multiprocessing.Manager()
    lock = manager.Lock()
    totaldict = manager.dict()
    totalcount_dict = manager.dict()

    processList = []
    for i in range(threadcount):
        process = multiprocessing.Process(target=multiwrapper,
                                          args=(totaldict, totalcount_dict, lock, file_list, i, threadcount, ))
        processList.append(process)
        process.start()

    for process in processList:
        process.join()

    summary_list = []
    for key in totaldict.keys():
        summary_list.append([key, len(totalcount_dict[key]), totaldict[key]])

    summary_list = sorted(summary_list, key=operator.itemgetter(1), reverse=True)
    with open('uninterruptable-bt.summary', 'w') as f:
        for summary in summary_list:
            f.write("%s ---- %d\n%s" % (summary[0], summary[1], summary[2]))


if __name__ == '__main__':
    main()



