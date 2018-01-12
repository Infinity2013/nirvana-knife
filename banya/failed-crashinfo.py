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


def whose_crashinfo(path):
    with open(path, 'r') as f:
        lines = f.readlines()
        for i in xrange(len(lines)):
            line = lines[i]
            if 'get crashInfo fail' in line:
                for j in xrange(i, 0, -1):
                    line = lines[j]
                    if 'ANRManager: ANR in' in line:
                        print "-" * 50
                        print path
                        print line
                        print lines[j + 1]
                        break


def main():
    for file in scan_dir(os.getcwd()):
        whose_crashinfo(file)


if __name__ == '__main__':
    main()

