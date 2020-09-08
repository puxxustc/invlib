#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2020 - , puxxustc

from functools import partial
import argparse
import cgitb
import datetime
import math
import multiprocessing.dummy


from matplotlib.dates import date2num, num2date, DateFormatter
import matplotlib
import matplotlib.pyplot as plt
import numpy as np


from lib_fund import parse_args, fund_detail
from lib_fund_print import print_fund_brief


cgitb.enable(format='text')


def parse_date(date_text):
    try:
        t = datetime.datetime.strptime(date_text, '%Y-%m-%d')
        return t
    except ValueError:
        pass
    try:
        t = datetime.datetime.strptime(date_text + '-01', '%Y-%m-%d')
        return t
    except ValueError:
        pass
    t = datetime.datetime.strptime(date_text + '-01-01', '%Y-%m-%d')
    return t


def main():
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--year', type=float)
    parser.add_argument('--day', type=float)
    parser.add_argument('--date', type=str)
    parser.add_argument('--date-end', type=str)
    parser.add_argument('--log', action='store_true', help='对数坐标')
    parser.add_argument('--diff', action='store_true', help='增加差值')
    parser.add_argument('--diff-only', action='store_true', help='只显示差值')
    options, codes = parse_args(parser)
    if not options:
        return

    # 处理日期参数
    days = 365
    if options['year']:
        days = options['year'] * 365
    elif options['day']:
        days = options['day']
    elif options['date']:
        ts = parse_date(options['date']).timestamp() * 1000
        days = None
    if days:
        ts = (datetime.datetime.now() - datetime.timedelta(days=days)).timestamp() * 1000
    if options['date_end']:
        ts_end = parse_date(options['date_end']).timestamp() * 1000
    else:
        ts_end = None

    # 获取基金数据
    pool = multiprocessing.dummy.Pool(40)
    funds = pool.map(partial(fund_detail, verbose=False), codes)
    funds = [i for i in funds if i]

    # ***** 打印结果 ***** #
    print_fund_brief(funds)

    # ***** 画图 ***** #

    matplotlib.rcParams['font.sans-serif'] = ['Source Han Sans SC']
    matplotlib.rcParams['font.family'] = 'sans-serif'
    matplotlib.rcParams['axes.unicode_minus'] = False

    # 设置图表标题、日期格式
    title = '收益率对比'
    if options['log']:
        title += ' - 对数坐标'
    fig = plt.figure(title, figsize=[6.4 * 1.5, 4.8 * 1.2])
    ax = plt.gca()
    ax.xaxis.set_major_formatter(DateFormatter('%y-%m-%d'))

    lines = []
    series = []
    for fund in funds:
        # adjnavs = fund['adjnavs'][-500:]
        adjnavs = [i for i in fund['adjnavs'] if i[0] >= ts]
        if ts_end:
            adjnavs = [i for i in adjnavs if i[0] <= ts_end]
        if options['log']:
            rors = [math.log2(i[1] / adjnavs[0][1]) for i in adjnavs]
        else:
            rors = [(i[1] / adjnavs[0][1] - 1) * 100 for i in adjnavs]
        x = [i[0] for i in adjnavs]
        series.append([x, rors, fund])
        if not options['diff_only']:
            x = np.array([date2num(datetime.datetime.fromtimestamp(i[0] / 1000)) for i in adjnavs])
            y = np.array(rors)
            label = fund['code'] + '  ' + fund['name']
            line, = plt.plot_date(x, y, fmt=',-', label=label, linewidth=1)
            lines.append(line)

    if options['diff'] or options['diff_only']:
        x0, y0, fund0 = series[0]
        m0 = {x0[i]: y0[i] for i in range(len(x0))}
        for x, y, fund in series[1:]:
            m = {x[i]: y[i] for i in range(len(x))}
            x = [i for i in m0 if i in m]
            y = [m[i] - m0[i] for i in x]
            x = [date2num(datetime.datetime.fromtimestamp(i / 1000)) for i in x]
            label = f'差值: {fund["name"]} - {fund0["name"]}'
            line, = plt.plot_date(x, y, fmt=',-', label=label, linewidth=1.5)
            lines.append(line)
        plt.grid()

    # toottip
    fig = plt.gcf()
    ax = plt.gca()
    annot = ax.annotate("", xy=(0, 0), xytext=(-96, 30), textcoords="offset points",
                        bbox=dict(boxstyle="round", fc="w"),
                        arrowprops=dict(arrowstyle="->"))
    annot.set_visible(False)

    def update_annot(line, annot, ind):
        x, y = line.get_data()
        _x = x[ind["ind"][0]]
        _y = y[ind["ind"][0]]
        annot.xy = (_x, _y)
        label = line.get_label()
        day = datetime.datetime.fromtimestamp(num2date(_x).timestamp()).strftime('%Y-%m-%d')
        text = '%s\n%s  %.2f' % (label, day, _y)
        annot.set_text(text)

    def hover(event):
        """ update and show a tooltip while hovering an object; hide it otherwise """
        if event.inaxes == ax:
            an_artist_is_hovered = False
            for line in lines:
                contains, ind = line.contains(event)
                if contains:
                    an_artist_is_hovered = True
                    update_annot(line, annot, ind)
                    annot.set_visible(True)
                    fig.canvas.draw_idle()
            if not an_artist_is_hovered:
                # one wants to hide the annotation only if no artist in the graph is hovered
                annot.set_visible(False)
                fig.canvas.draw_idle()

    # call 'hover' if there is a mouse motion
    fig.canvas.mpl_connect("motion_notify_event", hover)

    if options['log']:
        legend_title = '收益率 (对数坐标)'
    else:
        legend_title = '收益率 (%)'
    plt.legend(title=legend_title, fontsize='small')
    # plt.legend(mode='expand')
    # plt.legend(bbox_to_anchor=(1.05, 1), loc='upper left', borderaxespad=0.)
    plt.show()


if __name__ == '__main__':
    main()
