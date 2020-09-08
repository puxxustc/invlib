#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2020 - , puxxustc

import datetime
import multiprocessing
import multiprocessing.dummy
import threading

from wcwidth import wcswidth

import requests


def has_key(data, key):
    value = data
    for segment in key.split('.'):
        if segment not in value:
            return False
        value = value[segment]
    return True


def get_key(data, key):
    value = data
    for segment in key.split('.'):
        if segment not in value:
            return None
        value = value[segment]
    return value


def wpad(s, width):
    return s + ' ' * (width - wcswidth(s))


def wcut(s, width):
    s = wpad(s, width)
    for i in range(width // 2, width):
        _s = s[:i]
        w = wcswidth(_s)
        if w == width:
            return _s
        elif w > width:
            return _s[:-1] + ' '
    return s


def grace_format(fmt, data, key):
    value = get_key(data, key)
    if value is None:
        try:
            s = fmt % 0
            return ' ' * len(s)
        except TypeError:
            pass
        try:
            s = fmt % ''
            return ' ' * len(s)
        except TypeError:
            pass
        return ''
    else:
        return fmt % value


def day2ts(day):
    if isinstance(day, str):
        return datetime.datetime.strptime(day, '%Y-%m-%d').timestamp()
    elif isinstance(day, tuple):
        return datetime.datetime(*day).timestamp()
    else:
        return day.timestamp()


def day2msts(day):
    return day2ts(day) * 1000


# 并发执行
def parallel_call(max_workers, call_data):
    def call_func(_args):
        func, args, kwargs = _args
        return func(*args, **kwargs)
    pool = multiprocessing.dummy.Pool(max_workers)
    result = pool.map(call_func, call_data)
    return result


#
# 带有连接池的 requests 包装
#

class HttpApi(object):
    def __init__(self, **kwargs):
        self._session_lock = threading.Lock()
        self.init_session()

    def init_session(self):
        session = requests.Session()
        adapter = requests.adapters.HTTPAdapter(
            pool_connections=10,
            pool_maxsize=100,
            max_retries=3)
        session.mount('http://', adapter)
        session.mount('https://', adapter)
        self._session = session
        self._session_pid = multiprocessing.current_process().pid
        # print('HttpApi: init session %d %s' % (self._session_pid, self._session))

    @property
    def s(self):
        while True:
            pid = multiprocessing.current_process().pid
            if self._session_pid == pid:
                return self._session
            locked = self._session_lock.acquire(timeout=0.1)
            if locked:
                if self._session_pid == pid:
                    return self._session
                self.init_session()
                self._session_lock.release()

    def __getattr__(self, method):
        def wrapper(*args, **kwargs):
            fun = getattr(self.s, method)
            return fun(*args, **kwargs)
        return wrapper


httpapi = HttpApi()
