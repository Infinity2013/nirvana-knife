#!/usr/bin/env python

import re
import sys
import os


def scan_dir(path, keyword='traces.txt'):
    file_list = []
    for p, d, f in os.walk(path):
        for local_file in f:
            if keyword in local_file:
                file_list.append(os.path.join(p, local_file))
    return file_list


def check_valid(path, thresold):
    local_dir = os.path.dirname(path)
    binderinfo_path = os.path.join(local_dir, 'binderinfo')
    if not os.path.exists(binderinfo_path):
        return
    with open(binderinfo_path, 'r') as f:
        lines = f.readlines()
        count = 0
        pending_pid_dict = {}
        for line in lines:
            if 'pending async transaction' in line:
                count += 1
                # pending async transaction 20111199: ffffffc014c96680 from 0:0 to 14706:0 code 5 flags 11 pri 10 r0 node 16470040 size 7280:0 data ffffff8005f5df48
                g = re.match(
                    r'\s*pending async transaction \d+: \w+ from \d+:\d+ to (?P<pid>\d+):.*', line)
                if g is not None:
                    pid = g.group('pid')
                    if pid not in pending_pid_dict:
                        pending_pid_dict[pid] = 0
                    pending_pid_dict[pid] += 1

        valid_trace, valid_events = False, False
        for key, val in pending_pid_dict.items():
            if val > thresold:
                traces_path = os.path.join(local_dir, 'traces.txt')
                if not os.path.exists(traces_path):
                    return
                with open(os.path.join(local_dir, 'traces.txt'), 'r') as trace_f:
                    lines = trace_f.readlines()
                    for line in lines:
                        if 'pid %s' % key in line:
                            valid_trace = True
                            break
                events_path = os.path.join(local_dir, 'events')
                if not os.path.exists(events_path):
                    return
                with open(os.path.join(local_dir, 'events'), 'r') as event_f:
                    lines = event_f.readlines()
                    for line in lines:
                        if 'am_anr' in line and key in line:
                            valid_events = True
                            break
                if valid_trace and valid_events:
                    print "perfect match pid: %s, %s" % (key, local_dir)
                elif valid_trace:
                    print "match pid %s, %s" % (key, local_dir)


def main():
    file_list = scan_dir(os.getcwd())
    for file in file_list:
        check_valid(file, 500)


if __name__ == "__main__":
    main()



