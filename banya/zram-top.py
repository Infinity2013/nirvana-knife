#!/usr/bin/env python

import subprocess
import re
import time

READ_PATH = '/sys/block/zram0/num_reads'
WRITE_PATH = '/sys/block/zram0/num_writes'


def parse_zram():
    p = subprocess.Popen('adb shell cat %s' % READ_PATH, shell=True, stdout=subprocess.PIPE)
    read_pages = int(p.stdout.readlines()[0].strip())
    p = subprocess.Popen('adb shell cat %s' % WRITE_PATH, shell=True, stdout=subprocess.PIPE)
    write_pages = int(p.stdout.readlines()[0].strip())
    return read_pages, write_pages


def main():
    old_read, old_write = parse_zram()
    while True:
        time.sleep(1)
        new_read, new_write = parse_zram()
        read_speed = (new_read - old_read) * 4 / 1024.0
        write_speed = (new_write - old_write) * 4 / 1024.0
        print('%.2f MB/s %.2f MB/s' % (read_speed, write_speed))
        old_read = new_read
        old_write = new_write


if __name__ == '__main__':
    main()
