#!/usr/bin/env python

import  os
import sys
import time

SDK_PATH = "/home/wxl/workspace/android-sdk-linux/platform-tools"
ADB_EXEC = "%s/adb" % SDK_PATH
FASTBOOT_EXEC = "%s/fastboot" % SDK_PATH

sectionDict = {
    'logo': 'logo.bin',
    'boot': 'boot.img',
    'recovery': 'recovery.img',
    'lk': 'lk.bin',
}
def main():
    outPath = os.environ.get('ANDROID_PRODUCT_OUT')
    if outPath == '':
        print ("NO ANDROID_PRODUCT_OUT FOUND")
        return

    section = sys.argv[1]
    targetBin = "%s/%s" % (outPath, sectionDict.get(section))
    targetBinStat = os.stat(targetBin)
    if time.time() - targetBinStat.st_mtime < 10 * 60:
        print ("Warning: %s wasn't modified in 10 minutes" % (targetBin))
    os.system('%s reboot bootloader' % ADB_EXEC)
    os.system('%s flash %s %s' % (FASTBOOT_EXEC, section, targetBin))
    os.system('%s reboot' % FASTBOOT_EXEC)


if __name__ == '__main__':
    main()
