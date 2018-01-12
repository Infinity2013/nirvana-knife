#!/usr/bin/env python
import re
import sys
import subprocess


def dump(seDict, errorList):
    with open('se', 'w') as f:
        for sctx, sctxDict in seDict.items():
            f.write("# %s" % sctx)
            f.write('\n')
            for tctx, tctxDict in sctxDict.items():
                for tcls, tclsSet in tctxDict.items():
                    se = "allow %s %s:%s { %s };\n" % (sctx, tctx, tcls, " ".join(tclsSet))
                    f.write(se)
        f.write("# errorList\n")
        for line in errorList:
            f.write(line)

def doSepolicy(inFd):
    seDict = {}
    errorList = []
    try:
        while True:
            line = inFd.readline()
            if len(line) == 0:
                continue
            elif not line:
                print 'EOF'
                break
            g = re.match(r'.*avc:\s+denied\s+\{\s(?P<action>[\w\_\s]+)\s+\}.*comm="(?P<comm>[\.\w\_\-]+)".*scontext=u:r:(?P<scontext>[\w\_\-]+):s0\s+tcontext=u:(object_)*r:(?P<tcontext>[\w\_\-]+):s0\s+tclass=(?P<tclass>[\w\-\_]+).*', line)
            if g is None:
                g = re.match(r'.*avc:\s+denied\s+\{\s(?P<action>[\w\_\s]+)\s+\}\sfor\sproperty=(?P<comm>.*)\sscontext=u:r:(?P<scontext>[\w\_\-]+):s0\s+tcontext=u:(object_)*r:(?P<tcontext>[\w\_\-]+):s0\s+tclass=(?P<tclass>[\w\-\_]+).*', line)

            if g is not None:
                sepolicy = "allow %s %s:%s %s; #%s" % (g.group('scontext'), g.group('tcontext'), g.group('tclass'), g.group('action'), g.group('comm'))
                print sepolicy
                sctx = g.group('scontext').strip()
                tctx = g.group('tcontext').strip()
                tcls = g.group('tclass').strip()
                action = g.group('action').strip()
                if sctx not in seDict.keys():
                    seDict[sctx] = {}
                sctxDict = seDict[sctx]
                if tctx not in sctxDict.keys():
                    sctxDict[tctx] = {}
                tctxDict = sctxDict[tctx]
                if tcls not in tctxDict.keys():
                    tctxDict[tcls] = set()
                tclsSet = tctxDict[tcls]
                tclsSet.add(action) 
            else:
                print line
                errorList.append(line)
    except KeyboardInterrupt:
        dump(seDict, errorList)
    dump(seDict, errorList)

def main():
    if len(sys.argv) == 2:
        with open(sys.argv[1], 'r') as f:
            doSepolicy(f)
    else:
        p = subprocess.Popen('adb shell "cat /proc/kmsg" | grep avc', shell=True, stdout=subprocess.PIPE)
        doSepolicy(p.stdout)

if __name__ == '__main__':
    main()
