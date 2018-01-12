#!/usr/bin/env python
import os
import time
import sys
from argparse import ArgumentParser

OUT = ""
SCAN_LIST = [
    '/system/framework',
    '/system/bin',
    '/system/lib',
    '/system/lib64',
    '/system/xbin',
    '/system/app',
    '/system/priv-app',
]

adb_exe = "/home/wxl/workspace/android-sdk-linux/platform-tools/adb"
def init():
    global OUT
    OUT = os.environ.get('ANDROID_PRODUCT_OUT')
    if OUT == "":
        raise ValueError("OUT is NULL")
    

def root():
    cmdList = ['root', 'wait-for-device', 'remount']
    for cmd in cmdList:
        os.system("%s %s" % (adb_exe, cmd))

def rebootDevice():
    os.system("%s reboot" % adb_exe)


def find_and_push(path, thresold=20):
    pushDict = {} 
    for p, d, f in os.walk("%s/%s" % (OUT, path)):
        for tmp in f:
            local_tmp = "%s/%s" % (p, tmp)
            if os.path.exists(local_tmp):
                if time.time() - os.stat(local_tmp).st_mtime < thresold * 60:
                    start = local_tmp.index(path)
                    end = local_tmp.index(tmp)
                    targetPath = local_tmp[start:end]
                    print "Find: %s -> %s" % (local_tmp, targetPath)
                    pushDict[local_tmp] = targetPath

    for key, val in pushDict.items():
        cmd = "%s push %s %s" % (adb_exe, key, val)
        os.system(cmd)


def main():
    init()
    root()

    p = ArgumentParser()
    p.add_argument('-r', action='store_true', dest='reboot')
    p.add_argument('-t', default=20, type=int, dest='thresold')
    a = p.parse_known_args(sys.argv)

    thresold = a[0].thresold
    reboot = a[0].reboot

    for path in SCAN_LIST:
        find_and_push(path, thresold)
    if reboot:
        rebootDevice()
    

if __name__ == '__main__':
    main()
