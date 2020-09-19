#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2020 - , puxxustc

from lib_dbs import Table


# 索引字段
INDEXES = [
    'code',
    'name',
    'update_time',
    'ror',
]


Index = Table(
    'data/index.ldb',
    'Index',
    'code',
    INDEXES,
    ['history'],
)
