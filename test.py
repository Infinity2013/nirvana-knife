#!/usr/bin/env python
from __future__ import print_function
import datetime


class LogSession:
    LOG_LEVEL_ERROR = 0

    def __init__(self, _level, _name, _arg):
        self.level = _level
        self.name = _name
        self.arg = _arg
        self.ts = 0

    def __enter__(self):
        self.ts = datetime.datetime.now()
        print ('session: %s entered' % self.name)

    def __exit__(self, exc_type, exc_val, exc_tb):
        duration = (datetime.datetime.now() - self.ts).total_seconds()
        print ("session: %s(%s) took %d ms" % (self.name, self.arg, duration))


def kw_to_str(kargs):
    str_list = []
    for key, val in kargs.items():
        if isinstance(val, str):
            if len(val) > 20:
                str_list.append('%s: %s...%s' % (key, val[:10], val[-10:]))
            else:
                str_list.append('%s: %s' % (key, val))
        elif hasattr(val, '__len__'):
            str_list.append("%s: %d" % (key, len(val)))
    return ", ".join(str_list)


def session(level):
    def session_wrapper(func):
        def wrapper(*args, **kw):
            with LogSession(level, func.__name__, kw_to_str(locals())):
                return func(*args, **kw)
        return wrapper
    return session_wrapper


@session(LogSession.LOG_LEVEL_ERROR)
def test(a, b, c):
    print ('test')




def second(func):
    print ('second is called func: %s' % func.__name__)

    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

@second
def first(func):
    print ('first is called func: %s' % func.__name__)

    def wrapper(*args, **kwargs):
        return func(*args, **kwargs)
    return wrapper

@first
@second
def func():
    print ("hello world")


func()

