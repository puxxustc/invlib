#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2020 - , puxxustc

from functools import partial
import cgitb
import io
import multiprocessing.dummy


from wcwidth import wcswidth
import bs4
import ujson as json

from lib_util import wpad
from lib_fund import (
    parse_args, httpapi,
    fund_detail, fund_profile,
    # calc_year_ror, calc_half_year_ror, calc_quarter_ror, calc_range_ror, calc_range_aror,
)


cgitb.enable(format='text')


def parallel_call(call_data):
    def call_func(_args):
        func, args, kwargs = _args
        return func(*args, **kwargs)
    pool = multiprocessing.dummy.Pool(10)
    result = pool.map(call_func, call_data)
    return result


def fund_total_asset_history(code):
    asset_history = []
    url = 'http://fundf10.eastmoney.com/zcpz_%s.html' % code
    text = httpapi.get(url).text
    html = bs4.BeautifulSoup(text, 'lxml')
    table = html.select_one('table.tzxq')
    if not table:
        return []
    for item in table.select('tbody tr'):
        tds = item.select('td')
        day = tds[0].text
        asset = float(tds[-1].text.replace(',', ''))
        asset_history.append({
            'day': day,
            'asset': asset,
        })
    return asset_history


def fund_manager_history(code):
    managers = []
    url = 'http://fundf10.eastmoney.com/jjjl_%s.html' % code
    text = httpapi.get(url).text
    html = bs4.BeautifulSoup(text, 'lxml')
    table = html.select_one('table.jloff')
    for item in table.select('tbody tr'):
        tds = item.select('td')
        start_day = tds[0].text
        end_day = tds[1].text
        manager = tds[2].text
        duration = tds[3].text
        managers.append({
            'start_day': start_day,
            'end_day': end_day,
            'manager': manager,
            'duration': duration,
        })
    return managers


def fund_fee(code):
    f = io.StringIO()
    print('基金费率:', file=f)
    url = 'http://fundf10.eastmoney.com/jjfl_%s.html' % code
    text = httpapi.get(url).text
    html = bs4.BeautifulSoup(text, 'lxml')
    data = html.find_all('div', {'class': 'detail'})[0]
    data = data.find_all('div', {'class': 'txt_in'})[0]
    data.select_one('.box.nb').extract()
    # data.select_one('.sgfltip').extract()
    # data.select_one('.sgfltip').extract()
    # data.select_one('.tfoot2').extract()
    is_moneyfund = '类型：<span>货币型</span>' in text
    for div in data.find_all('div', {'class': 'box'}, recursive=False):
        if '运作费用' in str(div):
            table = div.select_one('table')
            tds = table.select('td')
            fmt = '%s: %-10s  %s: %-10s  %s: %-10s'
            print('    ' + fmt % tuple([i.text for i in tds]), file=f)
        if '申购费率' in str(div) and not is_moneyfund:
            table = div.select_one('table')
            td = table.select('td')[2].text.replace('\xa0', ' ')
            print('    ' + '申购费率: ' + td.split(' ')[-1], file=f)
        if '赎回费率' in str(div) and not is_moneyfund:
            table = div.select_one('table')
            if not table:
                continue
            tds = table.select('td')
            print('    ' + '赎回费率:', file=f)
            width = max([wcswidth(tds[i + 1].text) for i in range(0, len(tds), 3)])
            for i in range(0, len(tds), 3):
                fmt = '    %s%s:  %s'
                print('    ' + fmt % (
                    tds[i + 1].text,
                    ' ' * (width + 4 - wcswidth(tds[i + 1].text)),
                    tds[i + 2].text,
                ), file=f)
    return f.getvalue()


def get_fund_show_data(code):
    # fund = fund_detail(code)
    # if not fund:
    #     return None
    # profile = fund_profile(code)
    # fund.update(profile)
    # fund['total_asset_history'] = fund_total_asset_history(code)
    # fund['manager_history'] = fund_manager_history(code)
    # fund['fee_text'] = fund_fee(code)
    data = parallel_call([
        (partial(fund_detail, ignore_new_fund=False), [code], {}),
        (fund_profile, [code], {}),
        (fund_total_asset_history, [code], {}),
        (fund_manager_history, [code], {}),
        (fund_fee, [code], {}),
    ])
    if not data[0]:
        return None
    fund, profile, total_asset_history, manager_history, fee_text = data
    fund.update(profile)
    fund['total_asset_history'] = total_asset_history
    fund['manager_history'] = manager_history
    fund['fee_text'] = fee_text
    # calc_year_ror(fund)
    # calc_half_year_ror(fund)
    # calc_quarter_ror(fund)
    # calc_range_ror(fund)
    # calc_range_aror(fund)
    # 处理资产配置数据
    _data = fund['raw']['Data_assetAllocation']
    asset_allocation = [{'day': i} for i in _data['categories']]
    for series in _data['series']:
        for i in range(len(series['data'])):
            asset_allocation[i][series['name']] = series['data'][i]
    fund['asset_allocation'] = list(reversed(asset_allocation))
    return fund


def fund_pos(code):
    url = 'http://fund.eastmoney.com/Data/FundCompare_Interface.aspx?t=2&bzdm=%s' % code
    text = httpapi.get(url).text
    gpcc = text.split('gpcc:')[1].split(',zqcc:')[0]
    gpcc = json.loads(gpcc)[0]
    zqcc = text.split('zqcc:')[1].split('};')[0]
    zqcc = json.loads(zqcc)[0]
    print('股票持仓:')
    print('    ' + '  '.join(gpcc[:5]))
    if len(gpcc) > 5:
        print('    ' + '  '.join(gpcc[5:]))
    print('债券持仓:')
    print('    ' + '  '.join(zqcc))


def main():
    options, codes = parse_args()
    if not options:
        return

    # 获取基金数据
    pool = multiprocessing.dummy.Pool(40)
    funds = pool.map(get_fund_show_data, codes)
    funds = [i for i in funds if i]

    # ***** 打印结果 ***** #
    for fund in funds:
        # from IPython import embed; embed()
        print('代码名称: %s %s' % (fund['code'], fund['name']))
        print('基金全称: %s' % fund['fullname'])
        print('基金类型: %s' % fund['kind'])
        # -----
        if fund['days'] > 0:
            s = '成立日期: %s    运作时间: %s' % (
                fund['inception_date'].strftime('%Y-%m-%d'),
                '%.2f年' % (fund['days'] / 365.0),
            )
            print(s)
        else:
            s = '发行日期: %s' % fund['issue_date_text']
            print(s)
        # -----
        print('跟踪标的: %s' % fund['trace_object'])
        # -----
        print('资产规模:')
        s = '  '.join([i['day'] for i in fund['total_asset_history'][:10]])
        print(' ' * 4 + s)
        s = '  '.join(['%-10.2f' % i['asset'] for i in fund['total_asset_history'][:10]])
        print(' ' * 4 + s)
        # -----
        if fund['asset_allocation'] and '股票占净比' in fund['asset_allocation'][0]:
            print('资产配置:')
            s = (' ' * 19).join([i['day'] for i in fund['asset_allocation']])
            print(' ' * 4 + s)
            s = '      '.join([
                '股票:%-6.2f 债券:%-6.2f' % (i['股票占净比'], i['债券占净比'])
                for i in fund['asset_allocation']
            ])
            print(' ' * 4 + s)
            print(' ' * 4 + '可转债:%-6.2f' % fund['asset_allocation_cb'])
        # -----
        if fund['raw']['Data_holderStructure']['categories']:
            print('持有人结构:')
            s = '  '.join(['%s %.1f ' % (i['name'][:2], i['data'][0]) for i in fund['raw']['Data_holderStructure']['series']])
            print(' ' * 4 + s)
        # -----
        print('基金经理:')
        manager_width = max([wcswidth(i['manager']) for i in fund['manager_history']])
        s = '\n'.join([
            '    %s   %s   %s   %s' % (
                i['start_day'],
                wpad(i['end_day'], 10),
                wpad(i['manager'], manager_width),
                i['duration'],
            )
            for i in fund['manager_history']
        ])
        print(s)
        # -----
        print(fund['fee_text'])
        # fund_fee(fund['code'])
        # -----
        if len(funds) > 1:
            print('\n')


if __name__ == '__main__':
    main()
