#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2020 - , puxxustc


import datetime
import functools

import requests


UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'


CHINABOND_TERM_MAP = {
    '0': '总值',
    '1': '1年以下',
    '2': '1-3年',
    '3': '3-5年',
    '4': '5-7年',
    '5': '7-10年',
    '6': '10年以上',
}


@functools.lru_cache
def get_chinabond_index_list():
    headers = {
        'Referer': 'http://yield.chinabond.com.cn/',
        'User-Agent': UA,
    }

    url = 'http://yield.chinabond.com.cn/cbweb-mn/indices/queryTree'
    params = {
        'locale': 'zh_CN',
    }
    try:
        r = requests.post(url, data=params, headers=headers, timeout=4)
    except requests.exceptions.RequestException:
        r = requests.post(url, data=params, headers=headers, timeout=10)

    data = r.json()
    indexes = [i for i in data if i['isParent'] == 'false']

    return indexes


@functools.lru_cache
def get_chinabond_index_id_name_map():
    indexes = get_chinabond_index_list()
    id_nam_map = {i['id']: i for i in indexes}
    return id_nam_map


def get_chinabond_index(indexid):
    headers = {
        'Referer': 'http://yield.chinabond.com.cn/',
        'User-Agent': UA,
    }

    url = 'http://yield.chinabond.com.cn/cbweb-mn/indices/singleIndexQuery'
    params = {
        'indexid': indexid,
        'zslxt': 'CFZS',
        'qxlxt': '0,1,2,3,4,5,6',
        'lx': '1',
        'locale': 'zh_CN',
    }
    # zslxt  指数类型，可以多个
    #   CFZS    财富指数
    #   JJZS    净价指数
    #   QJZS    全价指数
    ##
    # qxlxt  期限类型
    #     0     总值
    #     1     1年以下
    #     2     1-3年
    #     3     3-5年
    #     4     5-7年
    #     5     7-10年
    #     6     10年以上
    try:
        r = requests.post(url, data=params, headers=headers, timeout=4)
    except requests.exceptions.RequestException:
        r = requests.post(url, data=params, headers=headers, timeout=10)

    data = r.json()

    indexes = []
    index_id_name_map = get_chinabond_index_id_name_map()
    index_name = index_id_name_map[indexid]['name']
    for key in data:
        if not data[key]:
            continue
        if key.startswith('CFZS_'):
            type_ = '财富'
            term = CHINABOND_TERM_MAP[key[5:]]
        else:
            continue
        name = f'{index_name}-{term}-{type_}'
        history = []
        for ts, val in data[key].items():
            ts = datetime.datetime.fromtimestamp(int(ts) / 1000).strftime('%Y-%m-%d')
            history.append([ts, val])
        history.sort(key=lambda x: x[0])

        index = {
            'source': 'chinabond',
            'code': name,
            'indexid': indexid,
            'name': name,
            'history': history,
        }

        indexes.append(index)

    return indexes
