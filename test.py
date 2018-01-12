import re
import pickle

s = 'pending async transaction 20111199: ffffffc014c96680 from 0:0 to 14706:0 code 5 flags 11 pri 10 r0 node 16470040 size 7280:0 data ffffff8005f5df48'
g = re.match(r'\s*pending async transaction \d+: \w+ from \d+:\d+ to (?P<pid>\d+):.*', s)
if g is not None:
    print g.group('pid')

pickle.dump(g, 'fdas')

class A(object):
    def __getinitargs__(self):

