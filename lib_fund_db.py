#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2020 - , puxxustc

from lib_dbs import Table


# 索引
INDEXES = [
    'name',
    'fullname',
    'kind',
    'days',
    'nav_date_text',
    'is_nav_abnormal_change',
    'trace_object',
    'max_manager_work_days',
    'managers',
    # 资产配置
    'total_asset_history',
    'asset_allocation_stock',
    'asset_allocation_cb',
    # 收益率指标
    'aror.1y',
    'aror.2y',
    'aror.3y',
    'aror.4y',
    'ror.2015_yet',
    'ror.2015_2019',
    'ror.2017_2019',
    'ror.2019_yet',
    # 回撤指标
    'mdd.2020',
    'mdd.2019',
    'mdd.2018',
]


Fund = Table(
    'data/fund.ldb',
    'Fund',
    'code',
    INDEXES,
    ['raw', 'navs', 'adjnavs', '7d_aror'],
)
