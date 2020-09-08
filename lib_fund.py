#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2020 - , puxxustc

from collections import ChainMap, defaultdict
from copy import deepcopy
from statistics import stdev, mean

import argparse
import cgitb
import datetime
import functools
import itertools
import json
import math
import multiprocessing
import os
import re
import sys
import time
import threading

import bs4
import requests


cgitb.enable(format='text')


DEBUG = os.environ.get('DEBUG', '') in ['1', 'true', 'True']


UA = 'Mozilla/5.0 (Macintosh; Intel Mac OS X 10_13_3) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/64.0.3282.186 Safari/537.36'


def trace_exc(func):
    @functools.wraps(func)
    def wrapper(*args, **kwargs):
        try:
            return func(*args, **kwargs)
        except Exception as e:
            exc_info = sys.exc_info()
            exc_info = (exc_info[0], exc_info[1], exc_info[2].tb_next)
            tb = cgitb.text(exc_info, context=9)
            print(f'🔥 🔥 {tb}', file=sys.stderr)
            raise e
    return wrapper


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
        def wrapper(url, **kwargs):
            # fun = getattr(requests, method)
            fun = getattr(self.s, method)
            tried = 0
            timeout = kwargs.get('timeout', 0)
            while True:
                headers = ChainMap(
                    {
                        'User-Agent': UA,
                        'Referer': 'http://fund.eastmoney.com/',
                        'cache-control': 'no-cache',
                        'pragma': 'no-cache',
                    },
                    kwargs.get('headers', {}),
                )
                _timeout = timeout + 2.0 + 0.5 * tried
                _kwargs = ChainMap(
                    {'headers': headers},
                    {'timeout': _timeout},
                    kwargs,
                )
                try:
                    t = time.time()
                    r = fun(url, **_kwargs)
                    t = time.time() - t
                    if DEBUG:
                        print(f'{t:4.2f}', url, kwargs, file=sys.stderr)
                    if 500 <= r.status_code < 600:
                        raise Exception('HTTP %d' % r.status_code)
                    return r
                except Exception as e:
                    if tried > 1:
                        print(url, kwargs, _timeout, file=sys.stderr)
                    if tried > 20:
                        raise e
                    tried += 1
                    time.sleep(0.2 * tried)
        return wrapper


httpapi = HttpApi()


def parse_args(parser=None):
    if parser is None:
        parser = argparse.ArgumentParser(add_help=False)
    else:
        parser = argparse.ArgumentParser(add_help=False, parents=[parser])
    parser.add_argument('-h', '--help', action='store_true', dest='help')

    parser.add_argument('codes', nargs='*', metavar='code', help='基金代码')

    args = parser.parse_args()
    args = vars(args)

    if args['help'] or not args['codes']:
        parser.print_help()
        return None, None

    codes = args['codes']

    return args, codes


@functools.lru_cache
def list_all_fund():
    url = 'http://fund.eastmoney.com/js/fundcode_search.js'
    r = httpapi.get(url)
    text = r.text
    data = json.loads(text.split('=')[1].rstrip(';'))
    funds = []
    for code, logogram, name, kind, spell in data:
        if code in ['511880', '003816']:
            kind = '货币型'
        if code in ('002953', '002954', ):
            # 基金已终止
            continue
        funds.append({
            'code': code,
            'name': name,
            'kind': kind,
        })
    return funds


def list_short_bond_fund():
    url = 'http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=zq&rs=&gs=0&sc=2nzf&st=desc&sd=2018-10-26&ed=2019-10-26&qdii=042|&tabSubtype=042,,,,,&pi=1&pn=10000&dx=0&v=0.6421205869532727'
    text = httpapi.get(url, timeout=4).text
    text = text[4:].split('datas:')[1].split(',allRecords')[0]
    data = json.loads(text)
    codes = [i.split(',')[0] for i in data]
    return codes


def list_long_bond_fund():
    url = 'http://fund.eastmoney.com/data/rankhandler.aspx?op=ph&dt=kf&ft=zq&rs=&gs=0&sc=2nzf&st=desc&sd=2018-10-26&ed=2019-10-26&qdii=041|&tabSubtype=041,,,,,&pi=1&pn=10000&dx=0&v=0.6421205869532727'
    text = httpapi.get(url, timeout=4).text
    text = text[4:].split('datas:')[1].split(',allRecords')[0]
    data = json.loads(text)
    codes = [i.split(',')[0] for i in data]
    return codes


def list_pure_bond_fund():
    codes = []
    codes.extend(list_short_bond_fund())
    codes.extend(list_long_bond_fund())
    return codes


def list_bond_index_fund():
    funds = list_all_fund()
    kinds = {'债券指数'}
    codes = [i['code'] for i in funds if i['kind'] in kinds]
    return codes


def list_china_bond_fund():
    funds = list_all_fund()
    exclude = {'000395', '000396'}
    codes = [i['code'] for i in funds if '国债' in i['name'] and i['code'] not in exclude]
    return codes


def list_policy_bank_bond_fund():
    funds = list_all_fund()
    kinds = {'债券型', '债券指数'}
    funds = [i for i in funds if i['kind'] in kinds]
    # TODO 农发, 政策性金融债, 政金债
    keywords = [
        '政金债',
        '金融债',
        '国开债',
        '国开行',
        '国开指数',
        '农发债',
        '农发行',
        '进出口行',
    ]
    codes = [i['code'] for i in funds if any(j in i['name'] for j in keywords)]
    return codes


@functools.lru_cache
def get_ref_nav_date():
    return fast_get_nav_date('510050')


def fund_detail(code, verbose=False, get_fee=False, ignore_new_fund=True):
    if verbose:
        print(f'⏳ fund_detail: {code}', file=sys.stderr)
    fund = {'code': code}
    # 有些基金后端份额会自动跳转对应前端份额，忽略这样的基金
    # url = f'http://fund.eastmoney.com/{code}.html'
    # r = httpapi.get(url)
    # r.encoding = 'utf-8'
    # text = r.text
    # if 'location.href' in text:
    #     return None
    # 基金详细信息
    url = f'http://fund.eastmoney.com/pingzhongdata/{code}.js'
    now = datetime.datetime.now().strftime('%Y%m%d%H%M%S')
    url += f'?v={now}&_={now}&__={now}'
    r = httpapi.get(url)
    text = r.text
    if '<html>' in text or '<head>' in text:
        return None
    text = r.text
    text = text \
        .replace('/*', '\n/*') \
        .replace('*/', '*/\n') \
        .replace(';', ';\n')
    data = {}
    ignore_keywords = [
        'Data_fundSharesPositions',     # 股票仓位测算图
        'Data_ACWorthTrend',            # 累计净值走势
        'Data_grandTotal',              # 累计收益率走势
        'Data_rateInSimilarType',       # 同类排名走势
        'Data_rateInSimilarPersent',    # 同类排名百分比
        'swithSameType',                # 同类型基金涨幅榜
    ]
    for line in text.split('\n'):
        if (any(kw in line for kw in ignore_keywords)):
            continue
        items = line.split('=')
        if len(items) != 2:
            continue
        left, right = items
        key = left.rstrip().split(' ')[-1]
        value = json.loads(right.rstrip(';').replace("'", '"'))
        data[key] = value
    data.pop('Data_ACWorthTrend', None)
    data.pop('Data_grandTotal', None)
    data.pop('Data_rateInSimilarType', None)
    data.pop('Data_rateInSimilarPersent', None)
    data.pop('swithSameType', None)
    fund['raw'] = data
    if 'fS_code' not in data:
        print(f'🔥 fund_detail: {code}', data, '"%s"' % text, file=sys.stderr)
    fund['code'] = data['fS_code']
    fund['name'] = data['fS_name']
    # 获取分红、拆分、折算事件
    fund_event(fund)
    # 计算基金净值
    fund_nav(fund)
    # 计算复权净值
    fund_adjnav(fund)
    # 默认忽略新发基金
    if ignore_new_fund:
        if not fund['navs']:
            return None
    # 基金资产规模
    try:
        fund['asset'] = data['Data_fluctuationScale']['series'][-1]['y']
    except IndexError:
        fund['asset'] = 0
    try:
        asset_data = data['Data_assetAllocation']['series']
        total_asset_history = [i for i in asset_data if i['name'] in ['净资产', '总份额']][0]['data']
        total_asset_history = list(reversed(total_asset_history))
        fund['total_asset_history'] = total_asset_history
        fund['total_asset'] = total_asset_history[0]
    except IndexError:
        fund['total_asset_history'] = []
        fund['total_asset'] = 0
    # 资产配置
    fund['asset_allocation_stock'] = 0
    fund['asset_allocation_stock_history'] = []
    allocs = fund['raw']['Data_assetAllocation']['series']
    for item in allocs:
        if item['name'] == '股票占净比':
            if item['data']:
                asset_allocation_stock_history = list(reversed(item['data']))
                fund['asset_allocation_stock_history'] = asset_allocation_stock_history
                fund['asset_allocation_stock'] = asset_allocation_stock_history[0]
    fund_position_bonds(fund)
    fund_asset_allocation_cb_percent(fund)
    # 基金档案
    fund_info(fund)
    # 计算成立日期、净值更新日期
    if fund['navs']:
        fund['inception_date'] = datetime.datetime.fromtimestamp(fund['adjnavs'][0][0] / 1000)
        fund['inception_date_text'] = fund['inception_date'].strftime('%Y-%m-%d')
        fund['nav_date'] = datetime.datetime.fromtimestamp(fund['adjnavs'][-1][0] / 1000)
        fund['nav_date_text'] = fund['nav_date'].strftime('%Y-%m-%d')
        if (datetime.datetime.now() - fund['nav_date']).days < 14:
            fund['days'] = (fund['nav_date'] - fund['inception_date']).days
        else:
            fund['days'] = (get_ref_nav_date() - fund['inception_date']).days
    else:
        fund['inception_date'] = datetime.datetime.max
        fund['inception_date_text'] = fund['inception_date'].strftime('%Y-%m-%d')
        fund['nav_date'] = datetime.datetime.min
        fund['nav_date_text'] = fund['nav_date'].strftime('%Y-%m-%d')
        fund['days'] = 0
    # 获取基金费率
    if get_fee:
        try:
            fund['fees'] = fund_fee(fund['code'])
        except Exception:
            fund['fees'] = {}
        redeem_fee = fund['fees'].get('redeem', [])
        if len(redeem_fee) == 1:
            fund['7d_redeem_fee'] = 0
        elif len(redeem_fee) >= 2:
            fund['7d_redeem_fee'] = redeem_fee[1][1]
    if verbose:
        print(f'done fund_detail: {code}', file=sys.stderr)
    return fund


def fund_event(fund):
    events1 = fund_event1(fund)
    events2 = fund_event2(fund['code'])
    days = set()
    events = []
    for event in events1:
        if event['time'] not in days:
            events.append(event)
            days.add(event['time'])
    for event in events2:
        if event['time'] not in days:
            events.append(event)
            days.add(event['time'])
    events.sort(key=lambda x: x['time'])
    fund['events'] = events


# 有遗漏分红
def fund_event1(fund):
    events = []
    data = fund['raw']
    if 'Data_netWorthTrend' in data:
        # 解析分红、拆分
        for i in data['Data_netWorthTrend']:
            if i['unitMoney']:
                if i['unitMoney'].startswith('分红'):
                    value = float(i['unitMoney'].split('现金')[1].split('元')[0])
                    events.append({
                        'time': i['x'],
                        'kind': 'dividend',
                        'value': value
                    })
                elif i['unitMoney'].startswith('拆分'):
                    if '折算' in i['unitMoney']:
                        value = float(i['unitMoney'].split('折算')[1].split('份')[0])
                    elif '分拆' in i['unitMoney']:
                        value = float(i['unitMoney'].split('分拆')[1].split('份')[0])
                    events.append({
                        'time': i['x'],
                        'kind': 'sharesplit',
                        'value': value,
                    })
                else:
                    raise ValueError('unknown fund event: %s' % i['unitMoney'])
    return events


# 分红信息准确，但是份额拆分/折算不精确
def fund_event2(code):
    events = []
    while True:
        url = f'http://fundf10.eastmoney.com/fhsp_{code}.html'
        r = httpapi.get(url)
        text = r.text
        m = re.search(r'<div[^>]+?detail.+?(<div[^>]+?boxh4.*?分红.*?</div>)', text, flags=re.DOTALL)
        div = bs4.BeautifulSoup(m[1], 'lxml')
        a = div.select_one('a')
        if a.attrs['href'] == f'http://fund.eastmoney.com/{code}.html':
            break
        # else:
        #     print(f'🔥 🔥 fund_event2: {code} {href}', file=sys.stderr)
    m = re.search(r'<div[^>]+?txt_in.+?(<table.*?</table>).+?(<table.*?</table>)', text, flags=re.DOTALL)
    tables = [
        bs4.BeautifulSoup(m[1], 'lxml'),
        bs4.BeautifulSoup(m[2], 'lxml'),
    ]
    for table in tables:
        if '每份分红' in table.text and '暂无分红信息' not in table.text:
            for tr in table.select('tbody tr'):
                tds = tr.select('td')
                day = tds[2].text
                value = float(tds[3].text.split('每份派现金')[1].split('元')[0])
                events.append({
                    'time': int(datetime.datetime.strptime(day, '%Y-%m-%d').timestamp() * 1000),
                    'kind': 'dividend',
                    'value': value
                })
        if '拆分类型' in table.text and '暂无拆分信息' not in table.text:
            for tr in table.select('tbody tr'):
                tds = tr.select('td')
                day = tds[1].text
                value = tds[3].text
                if value == '暂未披露':
                    continue
                value = float(value.split(':')[1])
                events.append({
                    'time': int(datetime.datetime.strptime(day, '%Y-%m-%d').timestamp() * 1000),
                    'kind': 'sharesplit',
                    'value': value
                })
    events.sort(key=lambda x: x['time'])
    return events


# 有遗漏分红
def fund_event3(code):
    events = []
    url = f'http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery183033388605157499307_1582373175498&fundCode={code}&pageIndex=1&pageSize=100000&startDate=&endDate='
    text = httpapi.get(url).text
    data = json.loads(text.split('(')[1][:-1])['Data']['LSJZList']
    for item in data:
        if item['FHSP']:
            day = item['FSRQ']
            day_ts = int(datetime.datetime.strptime(day, '%Y-%m-%d').timestamp() * 1000)
            if '每份派现金' in item['FHSP']:
                value = float(item['FHSP'].split('每份派现金')[1].split('份')[0])
                events.append({
                    'time': day_ts,
                    'kind': 'sharesplit',
                    'value': value,
                })
            elif '每份基金份额折算' in item['FHSP']:
                value = float(item['FHSP'].split('每份基金份额折算')[1].split('份')[0])
                events.append({
                    'time': day_ts,
                    'kind': 'sharesplit',
                    'value': value,
                })
            elif '每份基金份额分拆' in item['FHSP']:
                value = float(item['FHSP'].split('每份基金份额分拆')[1].split('份')[0])
                events.append({
                    'time': day_ts,
                    'kind': 'sharesplit',
                    'value': value,
                })
    events.sort(key=lambda x: x['time'])
    return events


def fund_nav(fund):
    data = fund['raw']
    if 'Data_millionCopiesIncome' in data:
        # 货币基金
        navs = []
        nav = 1.0
        for timestamp, value in data['Data_millionCopiesIncome']:
            nav *= (1 + value / 10000.0)
            navs.append([
                timestamp,
                nav,
                value / 10000.0
            ])
        fund['navs'] = navs
        fund['7d_aror'] = data['Data_sevenDaysYearIncome']
    else:
        fund['navs'] = [
            [
                i['x'],
                i['y'],
                i['equityReturn'] * 0.01,
            ] for i in data['Data_netWorthTrend']
        ]


def fund_adjnav(fund):
    data = fund['raw']
    if 'Data_millionCopiesIncome' in data:
        # 货币基金
        fund['adjnavs'] = deepcopy(fund['navs'])
    else:
        # 净值型
        if not fund['events']:
            fund['adjnavs'] = deepcopy(fund['navs'])
            return
        if not fund['navs']:
            fund['adjnavs'] = []
            return
        events = {e['time']: e for e in fund['events']}
        navs = fund['navs']
        adjnavs = []
        share = 1.0
        for timestamp, value, change in navs:
            if timestamp in events:
                e = events[timestamp]
                if e['kind'] == 'dividend':
                    share *= (1 + e['value'] / value)
                elif e['kind'] == 'sharesplit':
                    share *= e['value']
                else:
                    raise ValueError()
            adjnavs.append([
                timestamp,
                value * share,
            ])
        adjnavs[0].append(0)
        for i in range(1, len(adjnavs)):
            change = adjnavs[i][1] / adjnavs[i - 1][1] - 1
            adjnavs[i].append(change)
        fund['adjnavs'] = adjnavs


@trace_exc
def fast_get_nav_date(code):
    url = f'http://api.fund.eastmoney.com/f10/lsjz?callback=jQuery183033388605157499307_1582373175498&fundCode={code}&pageIndex=1&pageSize=10&startDate=&endDate='
    text = httpapi.get(url).text
    data = json.loads(text.split('(')[1][:-1])['Data']['LSJZList']
    nav_date = datetime.datetime.strptime(data[0]['FSRQ'], '%Y-%m-%d')
    return nav_date


@trace_exc
def fund_info(fund):
    code = fund['code']
    url = f'http://fund.eastmoney.com/{code}.html'
    r = httpapi.get(url)
    r.encoding = 'utf_8_sig'
    text = r.text
    # 是否已终止
    fund['terminated'] = '本基金已终止' in text
    # 是否有净值异常波动
    fund['is_nav_abnormal_change'] = '基金净值和阶段涨幅出现异常波动' in text
    # 基金类型
    m = re.search(r'(<td>基金类型.*?</td>)', text)
    td = bs4.BeautifulSoup(m[1], 'lxml')
    kind = td.select_one('a').text
    if code in ['511880', '003816']:
        kind = '货币型'
    fund['kind'] = kind
    # 跟踪标的
    if m := re.search(r'(跟踪标的：</a>.*? )', text):
        trace_object = m[1].split('>')[1].strip()
        if trace_object == '--':
            trace_object = ''
        fund['trace_object'] = trace_object
    else:
        fund['trace_object'] = ''
    # 持仓信息 (基金持仓，股票持仓)
    position_funds = []
    position_stocks = []
    m = re.search(r'(<li[^>]+?position_shares.*?持仓占比.*?</li>)', text)
    if m:
        li = bs4.BeautifulSoup(m[1], 'lxml')
        for table in li.select('table'):
            if '基金名称' in table.text:
                for tr in table.select('tr'):
                    a = tr.select_one('a')
                    if a:
                        href = a.attrs['href']
                        code = re.search(r'\d{6}', href)[0]
                        name = a.attrs['title']
                        percent = tr.select('td')[1].text.rstrip('%')
                        position_funds.append({
                            'code': code,
                            'name': name,
                            'percent': percent,
                        })
            if '股票名称' in table.text:
                for tr in table.select('tr'):
                    a = tr.select_one('a')
                    if a:
                        href = a.attrs['href']
                        if m := re.match(r'^http://quote.eastmoney.com/([a-zA-Z]{2}\d{6}).html$', href):
                            code = m.group(1)
                        elif m := re.search(r'^http://quote.eastmoney.com/hk/(\d{5}).html$', href):
                            code = m.group(1)
                        name = a.attrs['title']
                        percent = tr.select('td')[1].text.rstrip('%')
                        position_stocks.append({
                            'code': code,
                            'name': name,
                            'percent': percent,
                        })
    fund['position_funds'] = position_funds
    fund['position_stocks'] = position_stocks
    # 基金经理
    manager_history = []
    if m := re.search(r'(<li[^>]+?fundManagerTab.*?任职时间.*?</li>)', text):
        manager_work_days = defaultdict(lambda: 0)
        li = bs4.BeautifulSoup(m[1], 'lxml')
        table = li.select_one('table')
        for tr in table.select('tr')[1:]:
            tds = tr.select('td')
            start_day, end_day = tds[0].text.split('~')
            m = re.search(r'((?P<year>\d+)年又)?(?P<day>\d+)天', tds[2].text)
            if m:
                work_days = int(m['year'] or 0) * 365 + int(m['day'])
            else:
                work_days = 0
            managers = []
            for a in tds[1].select('a'):
                pk = re.search(r'\d+', a.attrs['href'])[0]
                name = a.text
                manager_work_days[pk] += work_days
                managers.append({
                    'pk': pk,
                    'name': name,
                    'work_days': work_days,
                })
            manager_history.append({
                'start_day': start_day,
                'end_day': end_day,
                'managers': managers,
                'work_days': work_days,
            })
    if len(manager_history) >= 5:
        # 基金经理历史可能显示不全，另从单独的页面查询
        url = f'http://fundf10.eastmoney.com/jjjl_{fund["code"]}.html'
        text = httpapi.get(url).text
        m = re.search(r'<table[^>]+?jloff.+?</table>', text, flags=re.DOTALL)
        if m:
            manager_history = []
            manager_work_days = defaultdict(lambda: 0)
            text = m[0]
            html = bs4.BeautifulSoup(text, 'lxml')
            table = html.select_one('table.jloff')
            for item in table.select('tbody tr'):
                tds = item.select('td')
                start_day = tds[0].text
                end_day = tds[1].text
                m = re.search(r'((?P<year>\d+)年又)?(?P<day>\d+)天', tds[3].text)
                if not m:
                    continue
                work_days = int(m['year'] or 0) * 365 + int(m['day'])
                managers = []
                for a in tds[2].select('a'):
                    pk = re.search(r'\d+', a.attrs['href'])[0]
                    name = a.text
                    manager_work_days[pk] += work_days
                    managers.append({
                        'pk': pk,
                        'name': name,
                        'work_days': work_days,
                    })
                manager_history.append({
                    'start_day': start_day,
                    'end_day': end_day,
                    'managers': managers,
                    'work_days': work_days,
                })
    max_manager_work_days = max([manager_work_days[item['pk']] for item in manager_history[0]['managers']])
    fund['max_manager_work_days'] = max_manager_work_days
    fund['manager_history'] = manager_history
    fund['managers'] = ' '.join([i['name'] for i in fund['manager_history'][0]['managers']])


def fund_profile(code):
    profile = {}
    url = f'http://fundf10.eastmoney.com/jbgk_{code}.html'
    text = httpapi.get(url).text
    # html = bs4.BeautifulSoup(text, 'lxml')
    m = re.search(r'(<div[^>]+?r_cont.*</div>)', text, flags=re.DOTALL)
    html = bs4.BeautifulSoup(m[1], 'lxml')
    content = html.select_one('div.r_cont')
    # 交易状态
    profile['trade_status'] = ''
    for label in content.select('div.basic-new label'):
        l_text = label.text
        if '交易状态' in l_text:
            l_text = l_text.strip().replace('\n', ' ').replace('\xa0', '')
            profile['trade_status'] = l_text.split('交易状态：')[1]
    # 基本概况
    data = content.select_one('div.detail div.txt_in')
    table = data.select_one('table')
    tds = table.select('td')
    fullname = tds[0].text
    kind = tds[3].text
    if code in ['511880', '003816']:
        kind = '货币型'
    issue_date_text = re.sub(r'^(\d{4})年(\d{2})月(\d{2})日$', r'\1-\2-\3', tds[4].text)
    # 跟踪标的
    trace_object = tds[19].text
    if trace_object == '该基金无跟踪标的':
        trace_object = ''
    if code in ('512760', '008281', '008282', ):
        trace_object = '中华交易服务半导体芯片行业指数'
    profile['fullname'] = fullname
    profile['kind'] = kind
    profile['issue_date_text'] = issue_date_text
    if trace_object:
        profile['trace_object'] = trace_object
    for div in data.select('div.boxitem'):
        label = div.select_one('label.left')
        if label.text == '投资目标':
            profile['investment_objective'] = div.select_one('p').text.strip()
        elif label.text == '投资理念':
            profile['investment_philosophy'] = div.select_one('p').text.strip()
        elif label.text == '投资策略':
            profile['investment_strategy'] = div.select_one('p').text.strip()
    return profile


def fund_fee(code):
    fees = {
        'management': None,
        'custodian': None,
        'buy': None,
        'sale_service': None,
        'redeem': [],
    }
    url = f'http://fundf10.eastmoney.com/jjfl_{code}.html'
    text = httpapi.get(url).text
    is_moneyfund = '类型：<span>货币型</span>' in text
    m = re.search(r'(<div[^>]+?txt_in.*</table>)', text, flags=re.DOTALL)
    data = bs4.BeautifulSoup(m[1], 'lxml')
    # html = bs4.BeautifulSoup(text, 'lxml')
    # data = html.find_all('div', {'class': 'detail'})[0]
    # data = data.find_all('div', {'class': 'txt_in'})[0]
    # data.select_one('.box.nb').extract()
    for div in data.select('div.box'):
        if '运作费用' in str(div):
            table = div.select_one('table')
            tds = table.select('td')
            items = [i.text for i in tds]
            try:
                fees['management'] = float(items[1].split('%')[0])
            except ValueError:
                fees['management'] = None
            try:
                fees['custodian'] = float(items[3].split('%')[0])
            except ValueError:
                fees['custodian'] = None
            try:
                fees['sale_service'] = float(items[5].split('%')[0])
                if fees['sale_service'] < 0.00001:
                    fees['sale_service'] = None
            except ValueError:
                pass
        if '申购费率' in str(div) and not is_moneyfund:
            table = div.select_one('table')
            td = table.select('td')[2].text.replace('\xa0', ' ')
            fee = td.split(' ')[-1]
            fee = float(fee.split('%')[0])
            fees['buy'] = fee
        if '赎回费率' in str(div) and not is_moneyfund:
            table = div.select_one('table')
            if not table:
                continue
            tds = table.select('td')
            for i in range(0, len(tds), 3):
                fees['redeem'].append([
                    tds[i + 1].text,
                    float(tds[i + 2].text.split('%')[0])
                ])
    return fees


def fund_position_stocks(fund):
    code = fund['code']
    url = f'http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=jjcc&code={code}&topline=10&year=2020&month=6'
    r = httpapi.get(url)
    text = r.text.split('"')[1]
    if not text:
        fund['position_stocks'] = []
        return
    html = bs4.BeautifulSoup(text, 'lxml')
    tbody = html.select_one('tbody')
    if tbody is None:
        print(f'🔥 fund_position_stocks({code})', file=sys.stderr)
    position_stocks = []
    for tr in tbody.select('tr'):
        tds = tr.select('td')
        position_stocks.append({
            'code': tds[1].text,
            'name': tds[2].text,
            'percent': tds[4].text.rstrip('%'),
        })
    fund['position_stocks'] = position_stocks


def fund_position_bonds(fund):
    code = fund['code']
    url = f'http://fundf10.eastmoney.com/FundArchivesDatas.aspx?type=zqcc&code={code}&year='
    r = httpapi.get(url)
    text = r.text.split('"')[1]
    if not text:
        fund['position_bonds'] = []
        return
    html = bs4.BeautifulSoup(text, 'lxml')
    tbody = html.select_one('tbody')
    if tbody is None:
        print(f'🔥 fund_position_bonds({code})', file=sys.stderr)
    position_bonds = []
    for tr in tbody.select('tr'):
        tds = tr.select('td')
        position_bonds.append({
            'code': tds[1].text,
            'name': tds[2].text,
            'percent': tds[3].text.rstrip('%'),
        })
    fund['position_bonds'] = position_bonds


def fund_asset_allocation_cb_percent(fund):
    bonds = fund['position_bonds']
    percent = 0
    for bond in bonds:
        is_cb = False
        if '转债' in bond['name']:
            is_cb = True
        elif 'EB' in bond['name']:
            is_cb = True
        elif re.match(r'转\d+$', bond['name']):
            is_cb = True
        elif bond['code'][:3] in ['110', '113']:
            # 上证可转债
            is_cb = True
        elif bond['code'][:3] in ['132', '137']:
            # 上证可交换债
            is_cb = True
        elif bond['code'][:3] in ['123', '127', '128']:
            # 深证可转债
            is_cb = True
        elif bond['code'][:3] in ['120', '121']:
            # 深证可交换债
            is_cb = True
        if is_cb:
            try:
                percent += float(bond['percent'])
            except ValueError:
                pass
    fund['asset_allocation_cb'] = percent


def calc_ror(adjnavs, days, offset=0):
    end = adjnavs[-1][0]
    end = end - 3600 * 24 * 1000 * offset
    # end = adjnavs[-1][0] - 3600 * 24 * 1000 * offset
    start = end - 3600 * 24 * 1000 * days
    for timestamp, value, change in reversed(adjnavs):
        worth = value
        if timestamp <= end:
            break
    for timestamp, value, change in reversed(adjnavs):
        base = value
        if timestamp <= start:
            break
    return worth / base - 1


def calc_aror(adjnavs, days, offset=0):
    roi = calc_ror(adjnavs, days, offset)
    return math.exp(math.log(roi + 1) / days * 365) - 1


def calc_stdev(adjnavs, days, offset=0):
    end = adjnavs[-1][0] - 3600 * 24 * 1000 * offset
    start = end - 3600 * 24 * 1000 * days
    changes = (i[2] for i in adjnavs if i[0] >= start and i[0] <= end)
    return stdev(changes)


def calc_max_drawdown_by_value(values):
    if not values:
        return 0
    drawdowns = []
    max_so_far = values[0]
    for i in range(len(values)):
        if values[i] > max_so_far:
            drawdown = 0
            drawdowns.append(drawdown)
            max_so_far = values[i]
        else:
            drawdown = 1 - (values[i] / max_so_far)
            drawdowns.append(drawdown)
    return max(drawdowns)


def calc_max_drawdown(adjnavs, days=0, offset=0):
    if days != 0:
        end = adjnavs[-1][0]
        end = end - 3600 * 24 * 1000 * offset
        start = end - 3600 * 24 * 1000 * days
        adjnavs = [i for i in adjnavs if i[0] >= start]
    if not adjnavs:
        return 0
    # _start = datetime.datetime.strptime(start, '%Y-%m-%d').timestamp() * 1000
    # _end = datetime.datetime.strptime(end, '%Y-%m-%d').timestamp() * 1000
    values = [i[1] for i in adjnavs if i[0]]
    drawdowns = []
    max_so_far = values[0]
    for i in range(len(values)):
        if values[i] > max_so_far:
            drawdown = 0
            drawdowns.append(drawdown)
            max_so_far = values[i]
        else:
            drawdown = 1 - (values[i] / max_so_far)
            drawdowns.append(drawdown)
    return max(drawdowns)


def calc_year_ror(fund):
    ts_2013 = datetime.datetime(2013, 1, 1).timestamp() * 1000
    ts_2014 = datetime.datetime(2014, 1, 1).timestamp() * 1000
    ts_2015 = datetime.datetime(2015, 1, 1).timestamp() * 1000
    ts_2016 = datetime.datetime(2016, 1, 1).timestamp() * 1000
    ts_2017 = datetime.datetime(2017, 1, 1).timestamp() * 1000
    ts_2018 = datetime.datetime(2018, 1, 1).timestamp() * 1000
    ts_2019 = datetime.datetime(2019, 1, 1).timestamp() * 1000
    ts_2020 = datetime.datetime(2020, 1, 1).timestamp() * 1000

    prices_2013 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2013 and i[0] < ts_2014]
    prices_2014 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2014 and i[0] < ts_2015]
    prices_2015 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2015 and i[0] < ts_2016]
    prices_2016 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2016 and i[0] < ts_2017]
    prices_2017 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2017 and i[0] < ts_2018]
    prices_2018 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2018 and i[0] < ts_2019]
    prices_2019 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2019 and i[0] < ts_2020]
    prices_2020 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2020]
    if not prices_2020:
        return

    ror_2020 = None
    ror_2019 = None
    ror_2018 = None
    ror_2017 = None
    ror_2016 = None
    ror_2015 = None
    ror_2014 = None
    try:
        ror_2020 = (prices_2020[-1] / prices_2019[-1] - 1) * 100
        ror_2019 = (prices_2019[-1] / prices_2018[-1] - 1) * 100
        ror_2018 = (prices_2018[-1] / prices_2017[-1] - 1) * 100
        ror_2017 = (prices_2017[-1] / prices_2016[-1] - 1) * 100
        ror_2016 = (prices_2016[-1] / prices_2015[-1] - 1) * 100
        ror_2015 = (prices_2015[-1] / prices_2014[-1] - 1) * 100
        ror_2014 = (prices_2014[-1] / prices_2013[-1] - 1) * 100
    except IndexError:
        pass

    ror = {
        '2020': ror_2020,
        '2019': ror_2019,
        '2018': ror_2018,
        '2017': ror_2017,
        '2016': ror_2016,
        '2015': ror_2015,
        '2014': ror_2014,
    }
    ror = {k: v for (k, v) in ror.items() if v is not None}
    fund.setdefault('ror', {}).update(ror)

    mdd = {}
    price = next((i[1] for i in reversed(fund['adjnavs'])))
    mdd['current'] = (1 - price / max(itertools.chain(prices_2020, prices_2019))) * 100
    if prices_2020:
        mdd['2020'] = calc_max_drawdown_by_value(prices_2020) * 100
    if prices_2019:
        mdd['2019'] = calc_max_drawdown_by_value(prices_2019) * 100
    if prices_2018:
        mdd['2018'] = calc_max_drawdown_by_value(prices_2018) * 100
    if prices_2017:
        mdd['2017'] = calc_max_drawdown_by_value(prices_2017) * 100
    if prices_2016:
        mdd['2016'] = calc_max_drawdown_by_value(prices_2016) * 100
    if prices_2015:
        mdd['2015'] = calc_max_drawdown_by_value(prices_2015) * 100
    if prices_2014:
        mdd['2014'] = calc_max_drawdown_by_value(prices_2014) * 100
    if '2018' in mdd:
        mdd['3y_avg'] = mean([mdd['2020'], mdd['2019'], mdd['2018']])
        if '2017' in mdd:
            mdd['4y_avg'] = mean([mdd['2020'], mdd['2019'], mdd['2018'], mdd['2017']])
            if '2016' in mdd:
                mdd['5y_avg'] = mean([mdd['2020'], mdd['2019'], mdd['2018'], mdd['2017'], mdd['2016']])
                if '2015' in mdd:
                    mdd['6y_avg'] = mean([mdd['2020'], mdd['2019'], mdd['2018'], mdd['2017'], mdd['2016'], mdd['2015']])
    fund.setdefault('mdd', {}).update(mdd)


def calc_half_year_ror(fund):
    ts_2014h2 = datetime.datetime(2014, 7, 1).timestamp() * 1000
    ts_2015h1 = datetime.datetime(2015, 1, 1).timestamp() * 1000
    ts_2015h2 = datetime.datetime(2015, 7, 1).timestamp() * 1000
    ts_2016h1 = datetime.datetime(2016, 1, 1).timestamp() * 1000
    ts_2016h2 = datetime.datetime(2016, 7, 1).timestamp() * 1000
    ts_2017h1 = datetime.datetime(2017, 1, 1).timestamp() * 1000
    ts_2017h2 = datetime.datetime(2017, 7, 1).timestamp() * 1000
    ts_2018h1 = datetime.datetime(2018, 1, 1).timestamp() * 1000
    ts_2018h2 = datetime.datetime(2018, 7, 1).timestamp() * 1000
    ts_2019h1 = datetime.datetime(2019, 1, 1).timestamp() * 1000
    ts_2019h2 = datetime.datetime(2019, 7, 1).timestamp() * 1000
    ts_2020h1 = datetime.datetime(2020, 1, 1).timestamp() * 1000
    ts_2020h2 = datetime.datetime(2020, 7, 1).timestamp() * 1000
    ts_2021h1 = datetime.datetime(2021, 1, 1).timestamp() * 1000

    prices_2014h2 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2014h2 and i[0] < ts_2015h1]
    prices_2015h1 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2015h1 and i[0] < ts_2015h2]
    prices_2015h2 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2015h2 and i[0] < ts_2016h1]
    prices_2016h1 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2016h1 and i[0] < ts_2016h2]
    prices_2016h2 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2016h2 and i[0] < ts_2017h1]
    prices_2017h1 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2017h1 and i[0] < ts_2017h2]
    prices_2017h2 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2017h2 and i[0] < ts_2018h1]
    prices_2018h1 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2018h1 and i[0] < ts_2018h2]
    prices_2018h2 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2018h2 and i[0] < ts_2019h1]
    prices_2019h1 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2019h1 and i[0] < ts_2019h2]
    prices_2019h2 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2019h2 and i[0] < ts_2020h1]
    prices_2020h1 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2020h1 and i[0] < ts_2020h2]
    prices_2020h2 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2020h2 and i[0] < ts_2021h1]
    if not prices_2020h1:
        return

    ror_2020h2 = None
    ror_2020h1 = None
    ror_2019h2 = None
    ror_2019h1 = None
    ror_2018h2 = None
    ror_2018h1 = None
    ror_2017h2 = None
    ror_2017h1 = None
    ror_2016h2 = None
    ror_2016h1 = None
    ror_2015h2 = None
    ror_2015h1 = None
    try:
        ror_2020h2 = (prices_2020h2[-1] / prices_2020h1[-1] - 1) * 100
        ror_2020h1 = (prices_2020h1[-1] / prices_2019h2[-1] - 1) * 100
        ror_2019h2 = (prices_2019h2[-1] / prices_2019h1[-1] - 1) * 100
        ror_2019h1 = (prices_2019h1[-1] / prices_2018h2[-1] - 1) * 100
        ror_2018h2 = (prices_2018h2[-1] / prices_2018h1[-1] - 1) * 100
        ror_2018h1 = (prices_2018h1[-1] / prices_2017h2[-1] - 1) * 100
        ror_2017h2 = (prices_2017h2[-1] / prices_2017h1[-1] - 1) * 100
        ror_2017h1 = (prices_2017h1[-1] / prices_2016h2[-1] - 1) * 100
        ror_2016h2 = (prices_2016h2[-1] / prices_2016h1[-1] - 1) * 100
        ror_2016h1 = (prices_2016h1[-1] / prices_2015h2[-1] - 1) * 100
        ror_2015h2 = (prices_2015h2[-1] / prices_2015h1[-1] - 1) * 100
        ror_2015h1 = (prices_2015h1[-1] / prices_2014h2[-1] - 1) * 100
    except IndexError:
        pass

    ror = {
        '2020h2': ror_2020h2,
        '2020h1': ror_2020h1,
        '2019h2': ror_2019h2,
        '2019h1': ror_2019h1,
        '2018h2': ror_2018h2,
        '2018h1': ror_2018h1,
        '2017h2': ror_2017h2,
        '2017h1': ror_2017h1,
        '2016h2': ror_2016h2,
        '2016h1': ror_2016h1,
        '2015h2': ror_2015h2,
        '2015h1': ror_2015h1,
    }
    ror = {k: v for (k, v) in ror.items() if v is not None}
    fund.setdefault('ror', {}).update(ror)

    mdd = {}
    if prices_2020h2:
        mdd['2020h2'] = calc_max_drawdown_by_value(prices_2020h2) * 100
    if prices_2020h1:
        mdd['2020h1'] = calc_max_drawdown_by_value(prices_2020h1) * 100
    if prices_2019h2:
        mdd['2019h2'] = calc_max_drawdown_by_value(prices_2019h2) * 100
    if prices_2019h1:
        mdd['2019h1'] = calc_max_drawdown_by_value(prices_2019h1) * 100
    if prices_2018h2:
        mdd['2018h2'] = calc_max_drawdown_by_value(prices_2018h2) * 100
    if prices_2018h1:
        mdd['2018h1'] = calc_max_drawdown_by_value(prices_2018h1) * 100
    if prices_2017h2:
        mdd['2017h2'] = calc_max_drawdown_by_value(prices_2017h2) * 100
    if prices_2017h1:
        mdd['2017h1'] = calc_max_drawdown_by_value(prices_2017h1) * 100
    if prices_2016h2:
        mdd['2016h2'] = calc_max_drawdown_by_value(prices_2016h2) * 100
    if prices_2016h1:
        mdd['2016h1'] = calc_max_drawdown_by_value(prices_2016h1) * 100
    fund.setdefault('mdd', {}).update(mdd)


def calc_quarter_ror(fund):
    ts_2017q4 = datetime.datetime(2017, 10, 1).timestamp() * 1000
    ts_2018q1 = datetime.datetime(2018, 1, 1).timestamp() * 1000
    ts_2018q2 = datetime.datetime(2018, 4, 1).timestamp() * 1000
    ts_2018q3 = datetime.datetime(2018, 7, 1).timestamp() * 1000
    ts_2018q4 = datetime.datetime(2018, 10, 1).timestamp() * 1000
    ts_2019q1 = datetime.datetime(2019, 1, 1).timestamp() * 1000
    ts_2019q2 = datetime.datetime(2019, 4, 1).timestamp() * 1000
    ts_2019q3 = datetime.datetime(2019, 7, 1).timestamp() * 1000
    ts_2019q4 = datetime.datetime(2019, 10, 1).timestamp() * 1000
    ts_2020q1 = datetime.datetime(2020, 1, 1).timestamp() * 1000
    ts_2020q2 = datetime.datetime(2020, 4, 1).timestamp() * 1000
    ts_2020q3 = datetime.datetime(2020, 7, 1).timestamp() * 1000
    ts_2020q4 = datetime.datetime(2020, 10, 1).timestamp() * 1000

    prices_2017q4 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2017q4 and i[0] < ts_2018q1]
    prices_2018q1 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2018q1 and i[0] < ts_2018q2]
    prices_2018q2 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2018q2 and i[0] < ts_2018q3]
    prices_2018q3 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2018q3 and i[0] < ts_2018q4]
    prices_2018q4 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2018q4 and i[0] < ts_2019q1]
    prices_2019q1 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2019q1 and i[0] < ts_2019q2]
    prices_2019q2 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2019q2 and i[0] < ts_2019q3]
    prices_2019q3 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2019q3 and i[0] < ts_2019q4]
    prices_2019q4 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2019q4 and i[0] < ts_2020q1]
    prices_2020q1 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2020q1 and i[0] < ts_2020q2]
    prices_2020q2 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2020q2 and i[0] < ts_2020q3]
    prices_2020q3 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2020q3 and i[0] < ts_2020q4]
    if not prices_2020q3:
        return

    ror_2020q3 = None
    ror_2020q2 = None
    ror_2020q1 = None
    ror_2019q4 = None
    ror_2019q3 = None
    ror_2019q2 = None
    ror_2019q1 = None
    ror_2018q4 = None
    ror_2018q3 = None
    ror_2018q2 = None
    ror_2018q1 = None
    try:
        ror_2020q3 = (prices_2020q3[-1] / prices_2020q2[-1] - 1) * 100
        ror_2020q2 = (prices_2020q2[-1] / prices_2020q1[-1] - 1) * 100
        ror_2020q1 = (prices_2020q1[-1] / prices_2019q4[-1] - 1) * 100
        ror_2019q4 = (prices_2019q4[-1] / prices_2019q3[-1] - 1) * 100
        ror_2019q3 = (prices_2019q3[-1] / prices_2019q2[-1] - 1) * 100
        ror_2019q2 = (prices_2019q2[-1] / prices_2019q1[-1] - 1) * 100
        ror_2019q1 = (prices_2019q1[-1] / prices_2018q4[-1] - 1) * 100
        ror_2018q4 = (prices_2018q4[-1] / prices_2018q3[-1] - 1) * 100
        ror_2018q3 = (prices_2018q3[-1] / prices_2018q2[-1] - 1) * 100
        ror_2018q2 = (prices_2018q2[-1] / prices_2018q1[-1] - 1) * 100
        ror_2018q1 = (prices_2018q1[-1] / prices_2017q4[-1] - 1) * 100
    except IndexError:
        pass

    ror = {
        '2020q3': ror_2020q3,
        '2020q2': ror_2020q2,
        '2020q1': ror_2020q1,
        '2019q4': ror_2019q4,
        '2019q3': ror_2019q3,
        '2019q2': ror_2019q2,
        '2019q1': ror_2019q1,
        '2018q4': ror_2018q4,
        '2018q3': ror_2018q3,
        '2018q2': ror_2018q2,
        '2018q1': ror_2018q1,
    }
    ror = {k: v for (k, v) in ror.items() if v is not None}
    fund.setdefault('ror', {}).update(ror)

    mdd = {}
    if prices_2020q3:
        mdd['2020q3'] = calc_max_drawdown_by_value(prices_2020q3) * 100
    if prices_2020q2:
        mdd['2020q2'] = calc_max_drawdown_by_value(prices_2020q2) * 100
    if prices_2020q1:
        mdd['2020q1'] = calc_max_drawdown_by_value(prices_2020q1) * 100
    if prices_2019q4:
        mdd['2019q4'] = calc_max_drawdown_by_value(prices_2019q4) * 100
    if prices_2019q3:
        mdd['2019q3'] = calc_max_drawdown_by_value(prices_2019q3) * 100
    if prices_2019q2:
        mdd['2019q2'] = calc_max_drawdown_by_value(prices_2019q2) * 100
    if prices_2019q1:
        mdd['2019q1'] = calc_max_drawdown_by_value(prices_2019q1) * 100
    if prices_2018q4:
        mdd['2018q4'] = calc_max_drawdown_by_value(prices_2018q4) * 100
    if prices_2018q3:
        mdd['2018q3'] = calc_max_drawdown_by_value(prices_2018q3) * 100
    if prices_2018q2:
        mdd['2018q2'] = calc_max_drawdown_by_value(prices_2018q2) * 100
    if prices_2018q1:
        mdd['2018q1'] = calc_max_drawdown_by_value(prices_2018q1) * 100
    fund.setdefault('mdd', {}).update(mdd)


def calc_month_ror(fund):
    ts_2019m12 = datetime.datetime(2019, 12, 1).timestamp() * 1000
    ts_2020m1 = datetime.datetime(2020, 1, 1).timestamp() * 1000
    ts_2020m2 = datetime.datetime(2020, 2, 1).timestamp() * 1000
    ts_2020m3 = datetime.datetime(2020, 3, 1).timestamp() * 1000
    ts_2020m4 = datetime.datetime(2020, 4, 1).timestamp() * 1000
    ts_2020m5 = datetime.datetime(2020, 5, 1).timestamp() * 1000
    ts_2020m6 = datetime.datetime(2020, 6, 1).timestamp() * 1000
    ts_2020m7 = datetime.datetime(2020, 7, 1).timestamp() * 1000
    ts_2020m8 = datetime.datetime(2020, 8, 1).timestamp() * 1000
    ts_2020m9 = datetime.datetime(2020, 9, 1).timestamp() * 1000

    prices_2019m12 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2019m12 and i[0] < ts_2020m1]
    prices_2020m1 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2020m1 and i[0] < ts_2020m2]
    prices_2020m2 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2020m2 and i[0] < ts_2020m3]
    prices_2020m3 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2020m3 and i[0] < ts_2020m4]
    prices_2020m4 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2020m4 and i[0] < ts_2020m5]
    prices_2020m5 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2020m5 and i[0] < ts_2020m6]
    prices_2020m6 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2020m6 and i[0] < ts_2020m7]
    prices_2020m7 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2020m7 and i[0] < ts_2020m8]
    prices_2020m8 = [i[1] for i in fund['adjnavs'] if i[0] >= ts_2020m8 and i[0] < ts_2020m9]
    if not prices_2020m7:
        return

    ror_2020m7 = None
    ror_2020m6 = None
    ror_2020m5 = None
    ror_2020m4 = None
    ror_2020m3 = None
    ror_2020m2 = None
    ror_2020m1 = None
    try:
        ror_2020m7 = (prices_2020m7[-1] / prices_2020m6[-1] - 1) * 100
        ror_2020m6 = (prices_2020m6[-1] / prices_2020m5[-1] - 1) * 100
        ror_2020m5 = (prices_2020m5[-1] / prices_2020m4[-1] - 1) * 100
        ror_2020m4 = (prices_2020m4[-1] / prices_2020m3[-1] - 1) * 100
        ror_2020m3 = (prices_2020m3[-1] / prices_2020m2[-1] - 1) * 100
        ror_2020m2 = (prices_2020m2[-1] / prices_2020m1[-1] - 1) * 100
        ror_2020m1 = (prices_2020m1[-1] / prices_2019m12[-1] - 1) * 100
    except IndexError:
        pass

    ror = {
        '2020m7': ror_2020m7,
        '2020m6': ror_2020m6,
        '2020m5': ror_2020m5,
        '2020m4': ror_2020m4,
        '2020m3': ror_2020m3,
        '2020m2': ror_2020m2,
        '2020m1': ror_2020m1,
    }
    ror = {k: v for (k, v) in ror.items() if v is not None}
    fund.setdefault('ror', {}).update(ror)

    mdd = {}
    if prices_2020m8:
        mdd['2020m8'] = calc_max_drawdown_by_value(prices_2020m8) * 100
    if prices_2020m7:
        mdd['2020m7'] = calc_max_drawdown_by_value(prices_2020m7) * 100
    if prices_2020m6:
        mdd['2020m6'] = calc_max_drawdown_by_value(prices_2020m6) * 100
    if prices_2020m5:
        mdd['2020m5'] = calc_max_drawdown_by_value(prices_2020m5) * 100
    if prices_2020m4:
        mdd['2020m4'] = calc_max_drawdown_by_value(prices_2020m4) * 100
    if prices_2020m3:
        mdd['2020m3'] = calc_max_drawdown_by_value(prices_2020m3) * 100
    if prices_2020m2:
        mdd['2020m2'] = calc_max_drawdown_by_value(prices_2020m2) * 100
    if prices_2020m1:
        mdd['2020m1'] = calc_max_drawdown_by_value(prices_2020m1) * 100
    fund.setdefault('mdd', {}).update(mdd)


def calc_range_ror(fund):
    price = next((i[1] for i in reversed(fund['adjnavs'])))

    # last = datetime.datetime.now()
    last = datetime.datetime.fromtimestamp(fund['adjnavs'][-1][0] / 1000)
    ts_1y = last.replace(year=last.year - 1).timestamp() * 1000
    ts_2y = last.replace(year=last.year - 2).timestamp() * 1000
    ts_3y = last.replace(year=last.year - 3).timestamp() * 1000
    ts_4y = last.replace(year=last.year - 4).timestamp() * 1000
    ts_5y = last.replace(year=last.year - 5).timestamp() * 1000
    ts_6y = last.replace(year=last.year - 6).timestamp() * 1000
    ts_7y = last.replace(year=last.year - 7).timestamp() * 1000
    ts_8y = last.replace(year=last.year - 8).timestamp() * 1000

    ts_20131231 = datetime.datetime(2013, 12, 31).timestamp() * 1000
    ts_20140601 = datetime.datetime(2014, 6, 1).timestamp() * 1000
    ts_20141231 = datetime.datetime(2014, 12, 31).timestamp() * 1000
    ts_20150612 = datetime.datetime(2015, 6, 12).timestamp() * 1000
    ts_20151231 = datetime.datetime(2015, 12, 31).timestamp() * 1000
    ts_20160128 = datetime.datetime(2016, 1, 28).timestamp() * 1000
    ts_20160630 = datetime.datetime(2016, 6, 30).timestamp() * 1000
    ts_20161231 = datetime.datetime(2016, 12, 31).timestamp() * 1000
    ts_20180124 = datetime.datetime(2018, 1, 24).timestamp() * 1000
    ts_20181231 = datetime.datetime(2018, 12, 31).timestamp() * 1000
    ts_20191231 = datetime.datetime(2019, 12, 31).timestamp() * 1000

    ror_1y = None
    ror_2y = None
    ror_3y = None
    ror_4y = None
    ror_5y = None
    ror_6y = None
    ror_7y = None
    ror_8y = None
    try:
        price_1y = [i[1] for i in fund['adjnavs'] if i[0] <= ts_1y][-1]
        ror_1y = (price / price_1y - 1) * 100
        price_2y = [i[1] for i in fund['adjnavs'] if i[0] <= ts_2y][-1]
        ror_2y = (price / price_2y - 1) * 100
        price_3y = [i[1] for i in fund['adjnavs'] if i[0] <= ts_3y][-1]
        ror_3y = (price / price_3y - 1) * 100
        price_4y = [i[1] for i in fund['adjnavs'] if i[0] <= ts_4y][-1]
        ror_4y = (price / price_4y - 1) * 100
        price_5y = [i[1] for i in fund['adjnavs'] if i[0] <= ts_5y][-1]
        ror_5y = (price / price_5y - 1) * 100
        price_6y = [i[1] for i in fund['adjnavs'] if i[0] <= ts_6y][-1]
        ror_6y = (price / price_6y - 1) * 100
        price_7y = [i[1] for i in fund['adjnavs'] if i[0] <= ts_7y][-1]
        ror_7y = (price / price_7y - 1) * 100
        price_8y = [i[1] for i in fund['adjnavs'] if i[0] <= ts_8y][-1]
        ror_8y = (price / price_8y - 1) * 100
    except IndexError:
        pass
    ror_2019_yet = None
    ror_20180124 = None
    ror_2017_yet = None
    ror_2017_2019 = None
    ror_20160701_yet = None
    ror_20160701_2019 = None
    ror_20160701_2018 = None
    ror_20160128 = None
    ror_2016_yet = None
    ror_2016_2019 = None
    ror_2016_2018 = None
    ror_20150612 = None
    ror_2015_2019 = None
    ror_2015_yet = None
    ror_20140601 = None
    ror_2014_yet = None
    try:
        price_20191231 = next(i[1] for i in reversed(fund['adjnavs']) if i[0] <= ts_20191231)
        price_20181231 = next(i[1] for i in reversed(fund['adjnavs']) if i[0] <= ts_20181231)
        ror_2019_yet = (price / price_20181231 - 1) * 100
        price_20180124 = next(i[1] for i in reversed(fund['adjnavs']) if i[0] <= ts_20180124)
        ror_20180124 = (price / price_20180124 - 1) * 100
        price_20161231 = next(i[1] for i in reversed(fund['adjnavs']) if i[0] <= ts_20161231)
        ror_2017_yet = (price / price_20161231 - 1) * 100
        ror_2017_2019 = (price_20191231 / price_20161231 - 1) * 100
        price_20160630 = next(i[1] for i in reversed(fund['adjnavs']) if i[0] <= ts_20160630)
        ror_20160701_yet = (price / price_20160630 - 1) * 100
        ror_20160701_2019 = (price_20191231 / price_20160630 - 1) * 100
        ror_20160701_2018 = (price_20181231 / price_20160630 - 1) * 100
        price_20160128 = next(i[1] for i in reversed(fund['adjnavs']) if i[0] <= ts_20160128)
        ror_20160128 = (price / price_20160128 - 1) * 100
        price_20151231 = next(i[1] for i in reversed(fund['adjnavs']) if i[0] <= ts_20151231)
        ror_2016_yet = (price / price_20151231 - 1) * 100
        ror_2016_2019 = (price_20191231 / price_20151231 - 1) * 100
        ror_2016_2018 = (price_20181231 / price_20151231 - 1) * 100
        price_20150612 = next(i[1] for i in reversed(fund['adjnavs']) if i[0] <= ts_20150612)
        ror_20150612 = (price / price_20150612 - 1) * 100
        price_20141231 = next(i[1] for i in reversed(fund['adjnavs']) if i[0] <= ts_20141231)
        ror_2015_2019 = (price_20191231 / price_20141231 - 1) * 100
        ror_2015_yet = (price / price_20141231 - 1) * 100
        price_20140601 = next(i[1] for i in reversed(fund['adjnavs']) if i[0] <= ts_20140601)
        ror_20140601 = (price / price_20140601 - 1) * 100
        price_20131231 = next(i[1] for i in reversed(fund['adjnavs']) if i[0] <= ts_20131231)
        ror_2014_yet = (price / price_20131231 - 1) * 100
    except (StopIteration, IndexError):
        pass

    ror = {
        '1y': ror_1y,
        '2y': ror_2y,
        '3y': ror_3y,
        '4y': ror_4y,
        '5y': ror_5y,
        '6y': ror_6y,
        '7y': ror_7y,
        '8y': ror_8y,
        '2019_yet': ror_2019_yet,
        '20180124_yet': ror_20180124,
        '20160128_yet': ror_20160128,
        '20160701_yet': ror_20160701_yet,
        '20150612_yet': ror_20150612,
        '20140601_yet': ror_20140601,
        '2017_yet': ror_2017_yet,
        '2016_yet': ror_2016_yet,
        '2015_yet': ror_2015_yet,
        '2014_yet': ror_2014_yet,
        '20160701_2018': ror_20160701_2018,
        '20160701_2019': ror_20160701_2019,
        '2016_2019': ror_2016_2019,
        '2016_2018': ror_2016_2018,
        '2015_2019': ror_2015_2019,
        '2017_2019': ror_2017_2019,
    }
    if fund['raw']['syl_1y']:
        ror['1m'] = float(fund['raw']['syl_1y'])
    if fund['raw']['syl_3y']:
        ror['3m'] = float(fund['raw']['syl_3y'])
    if fund['raw']['syl_6y']:
        ror['6m'] = float(fund['raw']['syl_6y'])
    if fund['days'] >= 30.42 * 9 + 10:
        ror['9m'] = calc_ror(fund['adjnavs'], 30.42 * 9) * 100
    ror = {k: v for (k, v) in ror.items() if v is not None}
    fund.setdefault('ror', {}).update(ror)

    prices_1y = [i[1] for i in fund['adjnavs'] if i[0] > ts_1y]
    prices_2y = [i[1] for i in fund['adjnavs'] if i[0] > ts_2y]
    prices_3y = [i[1] for i in fund['adjnavs'] if i[0] > ts_3y]
    prices_4y = [i[1] for i in fund['adjnavs'] if i[0] > ts_4y]
    prices_5y = [i[1] for i in fund['adjnavs'] if i[0] > ts_5y]
    prices_6y = [i[1] for i in fund['adjnavs'] if i[0] > ts_6y]
    prices_7y = [i[1] for i in fund['adjnavs'] if i[0] > ts_7y]
    prices_20140601 = [i[1] for i in fund['adjnavs'] if i[0] > ts_20140601]
    prices_20141231 = [i[1] for i in fund['adjnavs'] if i[0] > ts_20141231]
    prices_20180124 = [i[1] for i in fund['adjnavs'] if i[0] > ts_20180124]

    mdd = {}
    if fund['days'] >= 30.42 * 3:
        mdd['3m'] = calc_max_drawdown(fund['adjnavs'], 30.42 * 3) * 100
    if fund['days'] >= 30.42 * 6:
        mdd['6m'] = calc_max_drawdown(fund['adjnavs'], 30.42 * 6) * 100
    if fund['days'] >= 30.42 * 9:
        mdd['9m'] = calc_max_drawdown(fund['adjnavs'], 30.42 * 9) * 100
    ts_first = fund['adjnavs'][0][0]
    if ts_first <= ts_1y:
        mdd['1y'] = calc_max_drawdown_by_value(prices_1y) * 100
    if ts_first <= ts_2y:
        mdd['2y'] = calc_max_drawdown_by_value(prices_2y) * 100
    if ts_first <= ts_3y:
        mdd['3y'] = calc_max_drawdown_by_value(prices_3y) * 100
    if ts_first <= ts_4y:
        mdd['4y'] = calc_max_drawdown_by_value(prices_4y) * 100
    if ts_first <= ts_5y:
        mdd['5y'] = calc_max_drawdown_by_value(prices_5y) * 100
    if ts_first <= ts_6y:
        mdd['6y'] = calc_max_drawdown_by_value(prices_6y) * 100
    if ts_first <= ts_7y:
        mdd['7y'] = calc_max_drawdown_by_value(prices_7y) * 100
    if ts_first <= ts_20140601:
        mdd['20140601_yet'] = calc_max_drawdown_by_value(prices_20140601) * 100
    if ts_first <= ts_20141231:
        mdd['2015_yet'] = calc_max_drawdown_by_value(prices_20141231) * 100
    if ts_first <= ts_20180124:
        mdd['20180124_yet'] = calc_max_drawdown_by_value(prices_20180124) * 100
    fund.setdefault('mdd', {}).update(mdd)


# 注意: 需要先调用 calc_range_ror
def calc_range_aror(fund):
    adjnavs = fund['adjnavs']
    ror = fund['ror']
    aror = {}
    if '1m' in ror:
        aror['1m'] = (1 + ror['1m'] / 100) ** 12 * 100 - 100
    if '3m' in ror:
        aror['3m'] = (1 + ror['3m'] / 100) ** 4 * 100 - 100
    if '6m' in ror:
        aror['6m'] = (1 + ror['6m'] / 100) ** 2 * 100 - 100
    if fund['days'] >= 30.42 * 9 + 10:
        aror['9m'] = calc_aror(adjnavs, 30.42 * 9) * 100
    if '1y' in ror:
        aror['1y'] = ror['1y']
    if '2y' in ror:
        aror['2y'] = (1 + ror['2y'] / 100) ** (1 / 2) * 100 - 100
    if '3y' in ror:
        aror['3y'] = (1 + ror['3y'] / 100) ** (1 / 3) * 100 - 100
    if '4y' in ror:
        aror['4y'] = (1 + ror['4y'] / 100) ** (1 / 4) * 100 - 100
    if '5y' in ror:
        aror['5y'] = (1 + ror['5y'] / 100) ** (1 / 5) * 100 - 100
    if '6y' in ror:
        aror['6y'] = (1 + ror['6y'] / 100) ** (1 / 6) * 100 - 100
    if '7y' in ror:
        aror['7y'] = (1 + ror['7y'] / 100) ** (1 / 7) * 100 - 100
    # if '2015_yet' in ror:
    #     days = (fund['nav_date'] - datetime.datetime(2015, 1, 1)).days
    #     aror['2015_yet'] = (math.exp(math.log(ror['2015_yet'] / 100 + 1) / days * 365) - 1) * 100
    # if '2015_2019' in ror:
    #     aror['2015_2019'] = (((ror['2015_2019'] / 100 + 1) ** 0.25) - 1) * 100
    fund.setdefault('aror', {}).update(aror)


def ror2aror(ror, years):
    return math.exp(math.log(1 + ror / 100) / years) * 100 - 100


def add_year_rors(rors):
    val = 1.0
    for ror in rors:
        val *= (1 + ror / 100)
    return (val - 1) * 100


def calc_annual_year_rors(rors):
    ror = add_year_rors(rors)
    return math.exp(math.log(1 + ror / 100) / len(rors)) * 100 - 100
