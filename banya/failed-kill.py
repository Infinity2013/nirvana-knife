#!/usr/bin/env python

import re
import os

def scan_dir(path, keyword='logcat'):
    file_list = []
    for p, d, f in os.walk(path):
        for file in f:
            if keyword in file:
                file_list.append(os.path.join(p, file))
    return file_list


def failed_kill(path):
    local_dir = os.path.dirname(path)
    logcat_file = os.path.join(local_dir, 'logcat')
    ps_file = os.path.join(local_dir, 'ps')

    failed_pid_set = set()
    with open(logcat_file, 'r') as f:
        lines = f.readlines()
        for line in lines:
            # failed to kill 1 processes for processgroup 6882
            if 'failed to kill' in line:
                g = re.match(
                    r'.*failed to kill \d processes for processgroup (?P<pid>\d+).*', line
                )
                if g is not None:
                    failed_pid_set.add(g.group('pid'))
    with open(ps_file, 'r') as f:
        thread_dict = {}
        for line in f.readlines():
            temp = line.split()
            pid = temp[1]
            comm = temp[-1]
            thread_dict[pid] = comm
        for pid in failed_pid_set:
            if pid in thread_dict:
                print "%s: %s, %s" % (local_dir, thread_dict[pid], pid)


def main():
    file_list = scan_dir(os.getcwd())
    for file in file_list:
        failed_kill(file)


if __name__ == '__main__':
    main()



