#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2020 - , puxxustc

from collections import defaultdict
from functools import partial
from statistics import mean, stdev
import argparse
import cgitb
import multiprocessing.dummy


from wcwidth import wcswidth


from lib_util import grace_format, get_key, has_key
from lib_fund import (
    parse_args, fund_detail,
    calc_aror, calc_year_ror, calc_half_year_ror, calc_quarter_ror, calc_month_ror,
    calc_range_ror, calc_range_aror,
    calc_max_drawdown,
)
from lib_fund_print import (
    print_fund_ror_long,
    print_fund_ror_long2,
    print_fund_ror_short,
    print_fund_ror_year,
    print_fund_ror_half_year,
    print_fund_ror_range,
    print_fund_ror_range2,
)


cgitb.enable(format='text')


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--sort', help='按指定的指标排序')
    parser.add_argument('--uniq', action='store_true', help='同一基金的不同份额只保留一个')
    parser.add_argument('--short', action='store_true', help='查看较短时间的收益率')
    parser.add_argument('--long', action='store_true', help='查看较长时间的收益率')
    parser.add_argument('--long2', action='store_true', help='查看较长时间的收益率')
    parser.add_argument('--range', action='store_true', help='查看区间收益率 (2015至今, 20180124至今)')
    parser.add_argument('--range2', action='store_true', help='查看区间收益率 (2015至2019, 20180124至今)')
    parser.add_argument('--year', action='store_true', help='查看年度收益率')
    parser.add_argument('--hy', action='store_true', help='查看半年度收益率')
    parser.add_argument('--quarter', action='store_true', help='查看季度收益率')
    options, codes = parse_args(parser)
    if not options:
        return

    # 获取基金数据
    pool = multiprocessing.dummy.Pool(40)
    funds = pool.map(partial(fund_detail, verbose=False), codes)
    funds = [i for i in funds if i]

    # 去重
    if options['uniq']:
        uniq = defaultdict(list)
        for fund in funds:
            fn = fund['fullname']
            uniq[fn].append(fund)
        for fn in uniq:
            uniq[fn].sort(key=lambda x: (-x['days'], get_key(x, 'fees.sale_service') or 0, x['name']))
        uniq = {i[0]['code'] for i in uniq.values()}
        funds = [i for i in funds if i['code'] in uniq]

    #
    # ***** 计算年化收益率 ***** #
    #
    for fund in funds:
        adjnavs = fund['adjnavs']
        calc_range_ror(fund)
        calc_range_aror(fund)
        # last = datetime.datetime.fromtimestamp(adjnavs[-1][0] / 1000)
        # ytd_days = (last - datetime.datetime(2018, 12, 31)).days
        # if fund['days'] > ytd_days:
        #     fund['aror']['ytd'] = calc_aror(adjnavs, ytd_days) * 100
        if options['short']:
            if fund['days'] >= 30.42 * 2 + 30:
                fund['aror']['2m'] = calc_aror(adjnavs, 30.42 * 2) * 100
            if fund['days'] >= 30.42 * 8 + 30:
                fund['aror']['8m'] = calc_aror(adjnavs, 30.42 * 8) * 100
        if fund['days'] >= 30.42 * 9 + 30:
            fund['aror']['9m'] = calc_aror(adjnavs, 30.42 * 9) * 100
        if fund['days'] >= 30.42 * 10 + 30:
            fund['aror']['10m'] = calc_aror(adjnavs, 30.42 * 10) * 100
        if fund['days'] >= 365 * 1.0 + 30:
            fund['aror']['12m'] = float(fund['raw']['syl_1n'])
        if fund['days'] >= 365 * 2.0 + 30:
            fund['aror']['24m'] = calc_aror(adjnavs, 365 * 2) * 100
        if fund['days'] >= 365 * 3.0 + 30:
            fund['aror']['36m'] = calc_aror(adjnavs, 365 * 3) * 100
        if fund['days'] >= 365 * 4.0 + 30:
            fund['aror']['48m'] = calc_aror(adjnavs, 365 * 4) * 100

    #
    # ***** 计算最大回撤 ***** #
    #
    for fund in funds:
        adjnavs = fund['adjnavs']
        fund['drawdown'] = {}
        fund['drawdown']['current'] = (1 - adjnavs[-1][1] / max([i[1] for i in adjnavs[-60:]])) * 100
        fund['drawdown']['3m'] = calc_max_drawdown(adjnavs, 30.42 * 3) * 100
        if options['short']:
            if fund['days'] >= 30.42 * 8:
                fund['drawdown']['8m'] = calc_max_drawdown(adjnavs, 30.42 * 8) * 100
        # last = datetime.datetime.fromtimestamp(adjnavs[-1][0] / 1000)
        # ytd_days = (last - datetime.datetime(2018, 12, 31)).days
        # if fund['days'] >= ytd_days:
        #     fund['drawdown']['ytd'] = calc_max_drawdown(adjnavs, ytd_days) * 100
        if fund['days'] >= 30.42 * 6:
            fund['drawdown']['6m'] = calc_max_drawdown(adjnavs, 30.42 * 6) * 100
        if fund['days'] >= 30.42 * 9:
            fund['drawdown']['9m'] = calc_max_drawdown(adjnavs, 30.42 * 9) * 100
        if fund['days'] >= 30.42 * 10:
            fund['drawdown']['10m'] = calc_max_drawdown(adjnavs, 30.42 * 10) * 100
        if fund['days'] >= 365 * 1.0:
            fund['drawdown']['12m'] = calc_max_drawdown(adjnavs, 365 * 1.0) * 100
        if fund['days'] >= 365 * 1.5:
            fund['drawdown']['18m'] = calc_max_drawdown(adjnavs, 365 * 1.5) * 100
        if fund['days'] >= 365 * 2.0:
            fund['drawdown']['24m'] = calc_max_drawdown(adjnavs, 365 * 2.0) * 100
        if fund['days'] >= 365 * 3.0:
            fund['drawdown']['36m'] = calc_max_drawdown(adjnavs, 365 * 3.0) * 100
        if fund['days'] >= 365 * 3.5:
            fund['drawdown']['42m'] = calc_max_drawdown(adjnavs, 365 * 3.5) * 100
        if fund['days'] >= 365 * 4.0 + 30:
            fund['drawdown']['48m'] = calc_max_drawdown(adjnavs, 365 * 4.0) * 100

    #
    # ***** 计算短时间收益 ***** #
    #
    if options['short']:
        for fund in funds:
            calc_month_ror(fund)
            calc_quarter_ror(fund)
            calc_half_year_ror(fund)
            calc_range_ror(fund)
            calc_range_aror(fund)
            calc_year_ror(fund)

    #
    # ***** 计算长时间年化收益 ***** #
    #
    if options['long']:
        for fund in funds:
            calc_range_ror(fund)
            calc_range_aror(fund)

    #
    # ***** 计算长时间累计收益 ***** #
    #
    if options['long2']:
        for fund in funds:
            calc_range_ror(fund)

    #
    # ***** 计算区间收益 ***** #
    #
    if options['range'] or options['range2']:
        for fund in funds:
            calc_year_ror(fund)
            calc_range_ror(fund)

    #
    # ***** 计算年度收益 ***** #
    #
    if options['hy']:
        for fund in funds:
            calc_half_year_ror(fund)

    #
    # ***** 计算年度收益 ***** #
    #
    if options['year'] or options['sort'] == 'lv':
        for fund in funds:
            calc_year_ror(fund)

    #
    # ***** 计算季度收益 ***** #
    #
    if options['quarter']:
        for fund in funds:
            calc_quarter_ror(fund)

    # 排序
    if options['sort']:
        if options['sort'] == 'lv':
            # 低波动优先排序
            def rank(fund):
                rors = [fund['ror']['2019'], fund['ror']['2018'], fund['ror']['2017']]
                if has_key(fund, 'ror.2016'):
                    rors.append(fund['ror']['2016'])
                ror_mean = mean(rors)
                ror_stdev = stdev(rors)
                mdds = [fund['mdd']['2020'], fund['mdd']['2019'], fund['mdd']['2018'], fund['mdd']['2017']]
                if has_key(fund, 'mdd.2016'):
                    mdds.append(fund['mdd']['2016'])
                mdd_mean = mean(mdds)
                r = ror_mean - ror_stdev * 0.25 - mdd_mean * 0.25
                return -r
            funds.sort(key=rank)
        else:
            factors = []
            for factor in options['sort'].split(','):
                factor = factor.strip()
                if not factor:
                    continue
                if factor[0] == '-':
                    reverse = False
                    factor = factor[1:]
                else:
                    reverse = True
                samples = (get_key(i, factor) for i in funds)
                samples = [i for i in samples if i]
                if not samples:
                    continue
                sample = samples[0]
                if isinstance(sample, (int, float)):
                    padding = 0
                elif isinstance(sample, str):
                    padding = ''
                else:
                    padding = None
                factors.append({
                    'factor': factor,
                    'reverse': reverse,
                    'padding': padding,
                })
            for factor in reversed(factors):
                funds.sort(key=lambda x: get_key(x, factor['factor']) or factor['padding'], reverse=factor['reverse'])

    #
    # ***** 打印结果 ***** #
    #
    if options['long']:
        print_fund_ror_long(funds)
    elif options['long2']:
        print_fund_ror_long2(funds)
    elif options['short']:
        print_fund_ror_short(funds)
    elif options['range']:
        print_fund_ror_range(funds)
    elif options['range2']:
        print_fund_ror_range2(funds)
    elif options['year']:
        print_fund_ror_year(funds)
    elif options['hy']:
        print_fund_ror_half_year(funds)
    elif options['quarter']:
        print(''.join([
            '%-5s' % '代码',
            '%-26s' % '名称',
            '%-6s' % '成立日期',
            '%-2s' % '',
            '%-6s' % '季度收益率',
            '%-46s' % '',
            '%-5s' % '季度最大回撤',
        ]))
        print(''.join([
            '%-47s' % '',
            '%-8s' % '2020q2',
            '%-8s' % '2020q1',
            '%-8s' % '2019q4',
            '%-8s' % '2019q3',
            '%-8s' % '2019q2',
            '%-8s' % '2019q1',
            '%-8s' % '2018q4',
            ' ',
            '%-7s' % '2020q2',
            '%-7s' % '2020q1',
            '%-7s' % '2019q4',
            '%-7s' % '2019q3',
            '%-7s' % '2019q2',
        ]))
        print('-' * 140)
        for fund in funds:
            fmt = ''.join([
                '%-7s',
                '%-s' + ' ' * (28 - wcswidth(fund['name'])),
                '%-11s',
                '',
                '%-8s',     # ror.2020q2
                '%-8s',     # ror.2020q1
                '%-8s',     # ror.2019q4
                '%-8s',     # ror.2019q3
                '%-8s',     # ror.2019q2
                '%-8s',     # ror.2019q1
                '%-8s',     # ror.2018q4
                ' ',
                '%-7s',     # mdd.2020q2
                '%-7s',     # mdd.2020q1
                '%-7s',     # mdd.2019q4
                '%-7s',     # mdd.2019q3
                '%-7s',     # mdd.2019q2
            ])
            s = fmt % (
                fund['code'],
                fund['name'],
                fund['inception_date'].strftime('%Y-%m-%d'),
                grace_format('% .03f', fund, 'ror.2020q2'),
                grace_format('% .03f', fund, 'ror.2020q1'),
                grace_format('% .03f', fund, 'ror.2019q4'),
                grace_format('% .03f', fund, 'ror.2019q3'),
                grace_format('% .03f', fund, 'ror.2019q2'),
                grace_format('% .03f', fund, 'ror.2019q1'),
                grace_format('% .03f', fund, 'ror.2018q4'),
                grace_format('% .03f', fund, 'mdd.2020q2'),
                grace_format('% .03f', fund, 'mdd.2020q1'),
                grace_format('% .03f', fund, 'mdd.2019q4'),
                grace_format('% .03f', fund, 'mdd.2019q3'),
                grace_format('% .03f', fund, 'mdd.2019q2'),
            )
            print(s)
    else:
        print(''.join([
            '%-5s' % '代码',
            '%-26s' % '名称',
            '%-6s' % '成立日期',
            '%-2s' % '',
            '%-6s' % '年化收益率',
            '%-51s' % '',
            '%-5s' % '区间最大回撤',
        ]))
        print(''.join([
            '%-47s' % '',
            '%-7s' % '1m',
            '%-7s' % '3m',
            '%-7s' % '6m',
            '%-8s' % '10m',
            '%-7s' % '1年',
            '%-7s' % '2年',
            '%-7s' % '3年',
            '%-7s' % '4年',
            ' ',
            '%-7s' % '6m',
            '%-6s' % '1年',
            '%-6s' % '2年',
            '%-6s' % '3年',
            '%s' % '42m',
        ]))
        print('-' * 140)
        for fund in funds:
            fmt = ''.join([
                '%-7s',
                '%-s' + ' ' * (28 - wcswidth(fund['name'])),
                '%-11s',
                '',
                '%-7s',
                '%-7s',
                '%-7s',
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
            ])
            s = fmt % (
                fund['code'],
                fund['name'],
                fund['inception_date'].strftime('%Y-%m-%d'),
                '% .03f' % fund['aror']['1m'],
                grace_format('% .03f', fund, 'aror.3m'),
                grace_format('% .03f', fund, 'aror.6m'),
                grace_format('% .04f', fund, 'aror.10m'),
                grace_format('% .04f', fund, 'aror.12m'),
                grace_format('% .04f', fund, 'aror.24m'),
                grace_format('% .04f', fund, 'aror.36m'),
                grace_format('% .04f', fund, 'aror.48m'),
                grace_format('% .03f', fund, 'drawdown.6m'),
                grace_format('% .03f', fund, 'drawdown.12m'),
                grace_format('% .03f', fund, 'drawdown.24m'),
                grace_format('% .03f', fund, 'drawdown.36m'),
                grace_format('% .03f', fund, 'drawdown.42m'),
            )
            print(s)


if __name__ == '__main__':
    main()
