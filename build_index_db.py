#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2020 - , puxxustc

import cgitb


from lib_chinabond_index import (
    get_chinabond_index_list,
    get_chinabond_index,
)


from lib_index_db import Index


cgitb.enable(format='text')


def update_chinabond_indexes():
    #
    # 中债指数
    #
    Index.ensure_index()
    indexids = [i['id'] for i in get_chinabond_index_list()]
    names = set()
    for indexid in indexids:
        _indexes = get_chinabond_index(indexid)
        for index in _indexes:
            names.add(index['name'])
            print(index['name'])
            Index.save(index)
    # 删除
    for name in set(Index.filter(source='chinabond').list_field('name')) - set(names):
        Index.delete(name)
        print(f'delete {name}')


def main():
    update_chinabond_indexes()


if __name__ == '__main__':
    main()
