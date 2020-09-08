#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2020 - , puxxustc

import cgitb


from lib_util import grace_format, wpad


cgitb.enable(format='text')


__all__ = [
    'print_fund_brief',
    'print_fund_ror_short',
    'print_fund_ror_long',
    'print_fund_ror_long2',
    'print_fund_ror_year',
    'print_fund_ror_half_year',
    'print_fund_ror_range',
    'print_fund_ror_range2',
]


def print_fund_brief(funds, file=None):
    data = []
    data.append(''.join([
        ' ' * 90,
        '基金经理',
    ]))
    data.append(''.join([
        '%-10s' % '类型',
        '%-5s' % '代码',
        '%-37s' % '名称',
        '%-8s' % '成立日期',
        '%s   ' % '净值日期',
        '%s ' % '运作时间',
        '%s  ' % '在任时间',
        '%-4s' % '总资产',
        '%-4s' % '仓位',
        '%-5s' % '基金经理',
    ]))
    data.append('-' * 140)
    for fund in funds:
        if 'inception_date_text' in fund:
            fmt = ''.join([
                '%-s',
                '%s ',
                '%-s  ',
                '%s  ',
                '%s  ',
                '%s ',
                '%s ',
                '%s  ',
                '%s ',
                '%6s ',
                '%5s  ',
                '%s',
            ])
            s = fmt % (
                wpad(fund['kind'], 12),
                fund['code'],
                wpad(fund['name'], 37),
                fund['inception_date_text'],
                fund.get('nav_date_text', ''),
                '%5.2f年' % (fund['days'] / 365.0),
                '%5.2f年' % (fund['max_manager_work_days'] / 365.0),
                '%7.1f' % fund['total_asset'],
                '%4.1f' % fund['asset_allocation_stock'],
                grace_format('% .02f', fund, 'sterling_ratio.2017_2019'),
                grace_format('% .02f', fund, 'sterling_ratio.2015_2019'),
                ' '.join([i['name'] for i in fund['manager_history'][0]['managers']]),
            )
        else:
            fmt = ''.join([
                '%-s',
                '%s ',
                '%-s',
            ])
            s = fmt % (
                wpad(fund['kind'], 12),
                fund['code'],
                wpad(fund['name'], 37),
            )
        s = s.rstrip()
        data.append(s)
    data.append('')
    s = '\n'.join(data)
    if file:
        print(s, file=file)
    else:
        print(s)


def print_fund_ror_short(funds):
    print(''.join([
        '%-47s' % '',
        '%-6s' % '年化收益率',
        '%-40s' % '',
        '%-5s' % '区间最大回撤',
    ]))
    print(''.join([
        '%-5s' % '代码',
        '%-26s' % '名称',
        '%-6s' % '成立日期',
        '  ',
        '%-7s' % '1m',
        '%-7s' % '3m',
        '%-7s' % '6m',
        '%-7s' % '8m',
        '%-7s' % '9m',
        '%-7s' % '10m',
        '%-7s' % '12m',
        '  ',
        '%-5s' % '当前',
        '%-7s' % '2020q3',
        '%-7s' % '2020q2',
        '%-7s' % '2020q1',
        '%-7s' % '2019h2',
        '%-7s' % '2019h1',
        '%-s' % '2018',
    ]))
    print('-' * 140)
    for fund in funds:
        fmt = ''.join([
            '%-7s',
            '%-s',
            '%-11s',
            '',
            '%-7s',
            '%-7s',
            '%-7s',
            '%-7s',
            '%-7s',
            '%-7s',
            '%-7s',
            '  ',
            '%-7s',
            '%-7s',
            '%-7s',
            '%-7s',
            '%-7s',
            '%-7s',
            '%-7s',
        ])
        s = fmt % (
            fund['code'],
            wpad(fund['name'], 28),
            fund['inception_date_text'],
            grace_format('% .03f', fund, 'aror.1m'),
            grace_format('% .03f', fund, 'aror.3m'),
            grace_format('% .03f', fund, 'aror.6m'),
            grace_format('% .03f', fund, 'aror.8m'),
            grace_format('% .03f', fund, 'aror.9m'),
            grace_format('% .03f', fund, 'aror.10m'),
            grace_format('% .03f', fund, 'aror.1y'),
            grace_format('% .03f', fund, 'mdd.current'),
            grace_format('% .03f', fund, 'mdd.2020q3'),
            grace_format('% .03f', fund, 'mdd.2020q2'),
            grace_format('% .03f', fund, 'mdd.2020q1'),
            grace_format('% .03f', fund, 'mdd.2019h2'),
            grace_format('% .03f', fund, 'mdd.2019h1'),
            grace_format('% .03f', fund, 'mdd.2018'),
        )
        print(s)


def print_fund_ror_long(funds):
    print(''.join([
        '%-5s' % '代码',
        '%-26s' % '名称',
        '%-6s' % '成立日期',
        '%-2s' % '',
        '%-6s' % '年化收益率',
        '%-46s' % '',
        '%-5s' % '区间最大回撤',
    ]))
    print(''.join([
        '%-47s' % '',
        '%-7s' % '1年',
        '%-7s' % '2年',
        '%-7s' % '3年',
        '%-7s' % '4年',
        '%-7s' % '5年',
        '%-7s' % '6年',
        '%-7s' % '7年',
        ' ',
        '%-6s' % '1年',
        '%-6s' % '2年',
        '%-6s' % '3年',
        '%-6s' % '4年',
        '%-6s' % '5年',
        '%s  ' % '6年',
    ]))
    print('-' * 140)
    for fund in funds:
        fmt = ''.join([
            '%-7s',
            '%-s',
            '%-11s',
            '',
            '%-8s',
            '%-8s',
            '%-8s',
            '%-8s',
            '%-8s',
            '%-8s',
            '%-8s',
            ' ',
            '%-7s',
            '%-7s',
            '%-7s',
            '%-7s',
            '%-7s',
            '%-7s',
        ])
        s = fmt % (
            fund['code'],
            wpad(fund['name'], 28),
            fund['inception_date_text'],
            grace_format('% .03f', fund, 'aror.1y'),
            grace_format('% .03f', fund, 'aror.2y'),
            grace_format('% .03f', fund, 'aror.3y'),
            grace_format('% .03f', fund, 'aror.4y'),
            grace_format('% .03f', fund, 'aror.5y'),
            grace_format('% .03f', fund, 'aror.6y'),
            grace_format('% .03f', fund, 'aror.7y'),
            grace_format('% .03f', fund, 'mdd.1y'),
            grace_format('% .03f', fund, 'mdd.2y'),
            grace_format('% .03f', fund, 'mdd.3y'),
            grace_format('% .03f', fund, 'mdd.4y'),
            grace_format('% .03f', fund, 'mdd.5y'),
            grace_format('% .03f', fund, 'mdd.6y'),
        )
        print(s)


def print_fund_ror_long2(funds):
    print(''.join([
        '%-5s' % '代码',
        '%-26s' % '名称',
        '%-6s' % '成立日期',
        '%-2s' % '',
        '%-6s' % '累计收益率',
        '%-54s' % '',
        '%-5s' % '区间最大回撤',
    ]))
    print(''.join([
        '%-47s' % '',
        '%-7s' % '1年',
        '%-7s' % '2年',
        '%-7s' % '3年',
        '%-7s' % '4年',
        '%-7s' % '5年',
        '%-7s' % '6年',
        '%-7s' % '7年',
        '%-7s' % '8年',
        ' ',
        '%-6s' % '1年',
        '%-6s' % '2年',
        '%-6s' % '3年',
        '%-6s' % '4年',
        '%-6s' % '5年',
        # '%s  ' % '6年',
    ]))
    print('-' * 140)
    for fund in funds:
        fmt = ''.join([
            '%-7s',
            '%-s',
            '%-11s',
            '',
            '%-8s',
            '%-8s',
            '%-8s',
            '%-8s',
            '%-8s',
            '%-8s',
            '%-8s',
            '%-8s',
            ' ',
            '%-7s',
            '%-7s',
            '%-7s',
            '%-7s',
            '%-7s',
            # '%-7s',
        ])
        s = fmt % (
            fund['code'],
            wpad(fund['name'], 28),
            fund['inception_date_text'],
            grace_format('% .03f', fund, 'ror.1y'),
            grace_format('% .03f', fund, 'ror.2y'),
            grace_format('% .03f', fund, 'ror.3y'),
            grace_format('% .03f', fund, 'ror.4y'),
            grace_format('% .03f', fund, 'ror.5y'),
            grace_format('% .03f', fund, 'ror.6y'),
            grace_format('% .03f', fund, 'ror.7y'),
            grace_format('% .03f', fund, 'ror.8y'),
            grace_format('% .03f', fund, 'mdd.1y'),
            grace_format('% .03f', fund, 'mdd.2y'),
            grace_format('% .03f', fund, 'mdd.3y'),
            grace_format('% .03f', fund, 'mdd.4y'),
            grace_format('% .03f', fund, 'mdd.5y'),
        )
        print(s)


def print_fund_ror_year(funds):
    print(''.join([
        '%-5s' % '代码',
        '%-26s' % '名称',
        '%-6s' % '成立日期',
        '%-2s' % '',
        '%-6s' % '年度收益率',
        '%-46s' % '',
        '%-5s' % '年度最大回撤',
    ]))
    print(''.join([
        '%-47s' % '',
        '%-8s' % '2020',
        '%-8s' % '2019',
        '%-8s' % '2018',
        '%-8s' % '2017',
        '%-8s' % '2016',
        '%-8s' % '2015',
        '%-8s' % '2014',
        ' ',
        '%s' % '当前  ',
        '%-6s' % '2020',
        '%-6s' % '2019',
        '%-6s' % '2018',
        '%-6s' % '2017',
        '%-6s' % '2016',
        '%s' % '2015',
    ]))
    print('-' * 140)
    for fund in funds:
        fmt = ''.join([
            '%-7s',
            '%-s',
            '%-11s',
            '',
            '%-8s',     # ror.2020
            '%-8s',     # ror.2019
            '%-8s',     # ror.2018
            '%-8s',     # ror.2017
            '%-8s',     # ror.2016
            '%-8s',     # ror.2015
            '%-8s',     # ror.2014
            ' ',
            '%-6s',
            '%-6s',
            '%-6s',
            '%-6s',
            '%-6s',
            '%-7s',
            '%-7s',
        ])
        s = fmt % (
            fund['code'],
            wpad(fund['name'], 28),
            fund['inception_date_text'],
            grace_format('% .03f', fund, 'ror.2020'),
            grace_format('% .03f', fund, 'ror.2019'),
            grace_format('% .03f', fund, 'ror.2018'),
            grace_format('% .03f', fund, 'ror.2017'),
            grace_format('% .03f', fund, 'ror.2016'),
            grace_format('% .03f', fund, 'ror.2015'),
            grace_format('% .03f', fund, 'ror.2014'),
            grace_format('% .02f', fund, 'mdd.current'),
            grace_format('% .02f', fund, 'mdd.2020'),
            grace_format('% .02f', fund, 'mdd.2019'),
            grace_format('% .02f', fund, 'mdd.2018'),
            grace_format('% .02f', fund, 'mdd.2017'),
            grace_format('% .02f', fund, 'mdd.2016'),
            grace_format('% .02f', fund, 'mdd.2015'),
        )
        print(s)


def print_fund_ror_half_year(funds):
    print(''.join([
        '%-5s' % '代码',
        '%-26s' % '名称',
        '%-6s' % '成立日期',
        '%-2s' % '',
        '%-6s' % '半年度收益率',
        '%-45s' % '',
        '%-5s' % '半年度最大回撤',
    ]))
    print(''.join([
        '%-47s' % '',
        '%-8s' % '2020h2',
        '%-8s' % '2020h1',
        '%-8s' % '2019h2',
        '%-8s' % '2019h1',
        '%-8s' % '2018h2',
        '%-8s' % '2018h1',
        '%-8s' % '2017h2',
        # '%-8s' % '2017h1',
        ' ',
        '%-7s' % '2020h2',
        '%-7s' % '2020h1',
        '%-7s' % '2019h2',
        '%-7s' % '2019h1',
        '%-7s' % '2018h2',
        '%-7s' % '2018h1',
    ]))
    print('-' * 140)
    for fund in funds:
        fmt = ''.join([
            '%-7s',
            '%-s',
            '%-11s',
            '',
            '%-8s',     # ror.2020h2
            '%-8s',     # ror.2020h1
            '%-8s',     # ror.2019h2
            '%-8s',     # ror.2019h1
            '%-8s',     # ror.2018h2
            '%-8s',     # ror.2018h1
            '%-8s',     # ror.2017h2
            # '%-8s',     # ror.2017h1
            ' ',
            '%-7s',     # mdd.2020h2
            '%-7s',     # mdd.2020h1
            '%-7s',     # mdd.2019h2
            '%-7s',     # mdd.2019h1
            '%-7s',     # mdd.2018h2
            '%-7s',     # mdd.2018h1
        ])
        s = fmt % (
            fund['code'],
            wpad(fund['name'], 28),
            fund['inception_date_text'],
            grace_format('% .03f', fund, 'ror.2020h2'),
            grace_format('% .03f', fund, 'ror.2020h1'),
            grace_format('% .03f', fund, 'ror.2019h2'),
            grace_format('% .03f', fund, 'ror.2019h1'),
            grace_format('% .03f', fund, 'ror.2018h2'),
            grace_format('% .03f', fund, 'ror.2018h1'),
            grace_format('% .03f', fund, 'ror.2017h2'),
            # grace_format('% .03f', fund, 'ror.2017h1'),
            grace_format('% .03f', fund, 'mdd.2020h2'),
            grace_format('% .03f', fund, 'mdd.2020h1'),
            grace_format('% .03f', fund, 'mdd.2019h2'),
            grace_format('% .03f', fund, 'mdd.2019h1'),
            grace_format('% .03f', fund, 'mdd.2018h2'),
            grace_format('% .03f', fund, 'mdd.2018h1'),
        )
        print(s)


def print_fund_ror_range(funds):
    print(''.join([
        '%-48s' % '',
        '%-6s' % '区间收益率',
        ' ' * 45,
        '%-5s' % '区间最大回撤',
        ' ' * 18,
        '%s' % 'Sterling'
    ]))
    print(''.join([
        '%-5s' % '代码',
        '%-26s' % '名称',
        '%-9s' % '成立日期',
        '%-8s' % '14➔',
        '%-7s' % '15➔',
        '%-8s' % '150612➔',
        '%-9s' % '160128➔',
        '%-7s' % '17➔19',
        '%-8s' % '180124➔',
        '%-7s' % '19➔',
        '  ',
        '%-5s' % '15➔',
        '%-5s' % '2016',
        '%-8s' % '180124➔',
        '%-6s' % '2019',
        '%-5s' % '2020',
        ' ',
        '%-s ' % '15➔19',
        '%-s' % '17➔19',
    ]))
    print('-' * 140)
    for fund in funds:
        # mdds = []
        # for year in ['2020', '2019', '2018', '2017', '2016', '2015']:
        #     if has_key(fund, 'mdd.%s' % year):
        #         mdds.append(fund['mdd'][year])
        # mdd_mean = mean(mdds)
        fmt = ''.join([
            '%-7s',
            '%-s',
            '%-11s',
            ' ',
            '%-8s',     # ror.2014_yet
            '%-8s',     # ror.2015_yet
            '%-7s',     # ror.20150612_yet
            '%7s  ',    # ror.20160128_yet
            '%-8s',     # ror.2017_2019
            '%-7s',     # ror.20180124_yet
            '%-8s',     # ror.2019_yet
            ' ',
            '%5s ',    # mdd.2015_yet
            '%5s ',    # mdd.2015_yet
            '%5s ',    # mdd.20180124_yet
            '%5s ',    # mdd.2019
            '%5s ',    # mdd.2020
            ' ',
            '%5s ',     # sterling_ratio.2015_2019
            '%-s',      # sterling_ratio.2017_2019
        ])
        s = fmt % (
            fund['code'],
            wpad(fund['name'], 28),
            fund['inception_date_text'],
            grace_format('% .02f', fund, 'ror.2014_yet'),
            grace_format('% .02f', fund, 'ror.2015_yet'),
            grace_format('% .02f', fund, 'ror.20150612_yet'),
            grace_format('% .02f', fund, 'ror.20160128_yet'),
            grace_format('% .02f', fund, 'ror.2017_2019'),
            grace_format('% .02f', fund, 'ror.20180124_yet'),
            grace_format('% .02f', fund, 'ror.2019_yet'),
            grace_format('% .01f', fund, 'mdd.2015_yet'),
            grace_format('% .01f', fund, 'mdd.2016'),
            grace_format('% .01f', fund, 'mdd.20180124_yet'),
            grace_format('% .01f', fund, 'mdd.2019'),
            grace_format('% .01f', fund, 'mdd.2020'),
            grace_format('% .02f', fund, 'sterling_ratio.2015_2019'),
            grace_format('% .02f', fund, 'sterling_ratio.2017_2019'),
        )
        print(s)


def print_fund_ror_range2(funds):
    print(''.join([
        '%-48s' % '',
        '%-6s' % '区间收益率',
        ' ' * 46,
        '%-5s' % '区间最大回撤',
    ]))
    print(''.join([
        '%-5s' % '代码',
        '%-26s' % '名称',
        '%-9s' % '成立日期',
        '%-7s' % '15➔',
        '%-8s' % '1607➔18',
        '%-9s' % '1607➔19',
        '%-8s' % '1607➔',
        '%-7s' % '17➔',
        '%-9s' % '180124➔',
        '%-7s' % '19➔',
        '  ',
        '%s' % '当前  ',
        '%-6s' % '2020',
        '%-6s' % '2019',
        '%-6s' % '2018',
        '%-6s' % '2017',
        '%-6s' % '2016',
        '%s' % '2015',
    ]))
    print('-' * 140)
    for fund in funds:
        fmt = ''.join([
            '%-7s',
            '%-s',
            '%-10s',
            ' ',
            '%8s',      # ror.2015_yet
            '%7s ',     # ror.20160701_2018
            '%7s ',     # ror.20160701_2019
            '%7s ',     # ror.20160701_yet
            '%7s ',     # ror.2017_yet
            '%7s ',     # ror.20180124_yet
            '%7s ',     # ror.2019_yet
            ' ',
            '%6s ',     # mdd.current
            '%6s',      # mdd.2020
            '%6s',      # mdd.2019
            '%6s',      # mdd.2018
            '%6s',      # mdd.2017
            '%6s',
            '%6s',
        ])
        s = fmt % (
            fund['code'],
            wpad(fund['name'], 28),
            fund['inception_date_text'],
            grace_format('% .02f', fund, 'ror.2015_yet'),
            grace_format('% .02f', fund, 'ror.20160701_2018'),
            grace_format('% .02f', fund, 'ror.20160701_2019'),
            grace_format('% .02f', fund, 'ror.20160701_yet'),
            grace_format('% .02f', fund, 'ror.2017_yet'),
            grace_format('% .02f', fund, 'ror.20180124_yet'),
            grace_format('% .02f', fund, 'ror.2019_yet'),
            grace_format('% .02f', fund, 'mdd.current'),
            grace_format('% .02f', fund, 'mdd.2020'),
            grace_format('% .02f', fund, 'mdd.2019'),
            grace_format('% .02f', fund, 'mdd.2018'),
            grace_format('% .02f', fund, 'mdd.2017'),
            grace_format('% .02f', fund, 'mdd.2016'),
            grace_format('% .02f', fund, 'mdd.2015'),
        )
        print(s)
