#!/usr/bin/env python

import os
import sys


def main():
    cur = os.getcwd()
    cur_list = cur.split("/")
    if len(sys.argv) == 1:
        for d in cur_list:
            if d != "":
                print (d)
    else:
        index = cur_list.index(sys.argv[1])
        print ("/".join(cur_list[:index + 1]))

if __name__ == '__main__':
    main()
