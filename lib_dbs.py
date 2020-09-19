#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2020 - , puxxustc

from typing import List, Dict
import datetime
import functools
import itertools

import more_itertools
import msgpack
import lsm


from lib_filter import Q


# patch msgpack.Unpacker
msgpack._Unpacker = msgpack.Unpacker
msgpack.Unpacker = functools.partial(msgpack._Unpacker, max_buffer_size=512 * 1024 * 1204)


class LSM_DB_Wrapper():
    def __init__(self, db_uri: str):
        self.db = lsm.LSM(db_uri)

    def get(self, key: bytes):
        try:
            return self.db.fetch(key)
        except KeyError:
            return None

    def put(self, key: bytes, value: bytes):
        self.db.insert(key, value)

    def delete(self, key: bytes):
        self.db.delete(key)

    def multi_get(self, keys: List[bytes]) -> Dict[bytes, bytes]:
        return self.db.fetch_bulk(keys)

    def multi_put(self, data: Dict[bytes, bytes]):
        self.db.update(data)

    def scan_keys(self, prefix: bytes = None) -> List[bytes]:
        if not prefix:
            return list(self.db.keys())

        with self.db.cursor() as cursor:
            try:
                cursor.seek(prefix, lsm.SEEK_GE)
            except KeyError:
                return []
            keys = []
            while True:
                keys.append(cursor.key())
                try:
                    cursor.next()
                except StopIteration:
                    break
            return keys


'''
数据库格式


索引  i_{pk}

数据  d0_{pk}
数据  d1_{pk}

'''


class Table:
    def __init__(self, db_uri, name, pk, indexes, heavy_keys):
        self.db_uri = db_uri
        self._db = LSM_DB_Wrapper(db_uri)
        self.name = name
        self.pk = pk
        self.indexes = indexes
        self.index_keys = {
            k: b'i_%s' % k.encode('utf-8')
            for k in itertools.chain([pk], indexes)
        }
        self.heavy_keys = heavy_keys

    def __str__(self):
        return '<Table #%s>' % self.name

    def __repr__(self):
        return self.__str__()

    def iter_pk(self):
        vals = self.list_pk()
        for val in vals:
            yield val

    def list_pk(self):
        db = self._db
        key = self.index_keys[self.pk]
        data = msgpack.unpackb(db.get(key))
        return list(data.keys())

    def ensure_index(self):
        db = self._db
        pk = self.pk
        index_keys = self.index_keys
        prefix = b'd0_'
        keys = db.scan_keys(prefix)
        pkvals = {}
        for key in keys:
            pkval = key[len(prefix):].decode('utf8')
            pkvals[pkval] = pkval
        db.put(index_keys[pk], msgpack.packb(pkvals))

        data = {}
        for key in index_keys:
            data.setdefault(key, {})
        for item in self.filter(shallow=True):
            for key in index_keys:
                data.setdefault(key, {})
                val = get_key(item, key)
                if val is not None:
                    data[key][item[pk]] = get_key(item, key)
        batch = {}
        for key in index_keys:
            batch[index_keys[key]] = msgpack.packb(data[key])
        db.multi_put(batch)

    def get_meta_key(self, pkval):
        return b'd0_%s' % pkval.encode('utf8')

    def get_data_key(self, pkval):
        return b'd1_%s' % pkval.encode('utf8')

    def get_by_pk(self, pkval, shallow=False):
        db = self._db
        if shallow:
            key = self.get_meta_key(pkval)
            data = db.get(key)
            if data:
                item = msgpack.unpackb(data)
                _unpack_datetime(item)
                return item
        else:
            keys = [
                self.get_meta_key(pkval),
                self.get_data_key(pkval),
            ]
            data = db.multi_get(keys)
            key = keys[0]
            if key in data and data[key]:
                item = msgpack.unpackb(data[key])
                key = keys[1]
                if key in data and data[key]:
                    item.update(msgpack.unpackb(data[key]))
                _unpack_datetime(item)
                return item

    def iter_bulk_get_by_pk(self, pkvals, shallow=False):
        db = self._db
        if shallow:
            keys = [self.get_meta_key(pkval) for pkval in pkvals]
        else:
            keys = []
            for pkval in pkvals:
                keys.append(self.get_meta_key(pkval))
                keys.append(self.get_data_key(pkval))
        data = db.multi_get(keys)
        for pkval in pkvals:
            key = self.get_meta_key(pkval)
            if key in data and data[key]:
                item = msgpack.unpackb(data[key])
                key = self.get_data_key(pkval)
                if key in data and data[key]:
                    item.update(msgpack.unpackb(data[key]))
                _unpack_datetime(item)
                yield item

    def bulk_get_by_pk(self, pkvals, shallow=False):
        return list(self.iter_bulk_get_by_pk(pkvals, shallow=shallow))

    def _filter(self, q=None, shallow=False):
        db = self._db
        index_keys = self.index_keys
        pkvals = self.list_pk()

        if q:
            db_keys = []
            for key in q.keys():
                if key.split('.')[0] in index_keys:
                    key = key.split('.')[0]
                if key in index_keys:
                    db_key = index_keys[key]
                    db_keys.append(db_key)
            if db_keys:
                db_data = db.multi_get(db_keys)
                data = {k: msgpack.unpackb(v) for k, v in db_data.items()}
                index = {}
                for key, db_key in index_keys.items():
                    if db_key in data:
                        for k, v in data[db_key].items():
                            index.setdefault(k, {})
                            put_key(index[k], key, v)
                pkvals = [i for i in pkvals if q.match(index.get(i, {}), shallow_match=True)]

        if not isinstance(pkvals, list):
            pkvals = list(pkvals)
        for _pkvals in more_itertools.sliced(pkvals, 200):
            for item in self.iter_bulk_get_by_pk(_pkvals, shallow=shallow):
                if q:
                    if q.match(item):
                        yield item
                else:
                    yield item

    def filter(self, q=None, shallow=False, **kwargs):
        if q is None and kwargs:
            q = Q.from_kwargs(kwargs)
        return QuerySet(self, q=q, shallow=shallow)

    def get(self, *args, **kwargs):
        return self.filter(*args, **kwargs).first()

    def list(self, *args, **kwargs):
        return list(self.filter(*args, **kwargs))

    def list_field(self, field):
        return self.filter().list_field(field)

    def list_fields(self, *fields):
        return self.filter().list_fields(*fields)

    def save(self, item, do_not_update_cache=False):
        db = self._db
        if self.pk not in item and not isinstance(item[self.pk], str):
            raise ValueError(self.__str__() + '.save(): primary key not valid')
        index_keys = self.index_keys
        heavy_keys = self.heavy_keys
        _pack_datetime(item)
        batch = {}
        pkval = item[self.pk]
        if not do_not_update_cache:
            # 更新索引
            db_data = db.multi_get(list(index_keys.values()))
            db_data = {k: msgpack.unpackb(v) for k, v in db_data.items()}
            for key, db_key in index_keys.items():
                data = db_data[db_key]
                val = get_key(item, key)
                if data.get(pkval) != val:
                    data[pkval] = val
                    batch[db_key] = msgpack.packb(data)
        # 更新 item 数据
        heavy_keys = self.heavy_keys
        _data = {k: v for k, v in item.items() if k in heavy_keys}
        _meta = {k: v for k, v in item.items() if k not in _data}
        meta_key = self.get_meta_key(pkval)
        data_key = self.get_data_key(pkval)
        batch[meta_key] = msgpack.packb(_meta)
        batch[data_key] = msgpack.packb(_data)
        # 写入数据库
        db.multi_put(batch)

    def bulk_save(self, items):
        for item in items:
            self.save(item, do_not_update_cache=True)
        self.ensure_index()

    def delete(self, pkval):
        db = self._db
        index_keys = self.index_keys
        batch = {}
        # 更新索引
        for key, db_key in index_keys.items():
            data = msgpack.unpackb(db.get(db_key))
            if pkval in data:
                data.pop(pkval, None)
                batch[db_key] = msgpack.packb(data)
        # 写入数据库
        db.multi_put(batch)
        # 删除数据
        meta_key = self.get_meta_key(pkval)
        data_key = self.get_data_key(pkval)
        db.delete(meta_key)
        db.delete(data_key)


class QuerySet:
    def __init__(self, table, q, shallow):
        self.table = table
        self.q = q
        self.shallow = shallow

    def __str__(self):
        return '<QuerySet %s q=%s>' % (self.table.name, self.q or '')

    def __repr__(self):
        return self.__str__()

    def __iter__(self):
        return self.table._filter(q=self.q, shallow=self.shallow)

    def filter(self, q=None, **kwargs):
        if q is None and kwargs:
            q = Q.from_kwargs(kwargs)
        if q is None:
            q = self.q
        else:
            q = self.q & q
        return QuerySet(self.table, q=q, shallow=self.shallow)

    def first(self):
        try:
            return next(self.__iter__())
        except StopIteration:
            return None

    def list(self):
        return list(self.__iter__())

    def list_field(self, field):
        if field in self.table.heavy_keys:
            it = self.table._filter(q=self.q, shallow=False)
            return [i[field] for i in it]
        else:
            it = self.table._filter(q=self.q, shallow=True)
            return [i[field] for i in it]

    def list_fields(self, *fields):
        if any((field in self.table.heavy_keys for field in fields)):
            it = self.table._filter(q=self.q, shallow=False)
            return [[i[field] for field in fields] for i in it]
        else:
            it = self.table._filter(q=self.q, shallow=True)
            return[[i[field] for field in fields] for i in it]

    def count(self):
        pk = self.table.pk
        return len(self.list_field(pk))


def get_key(data, key):
    value = data
    for segment in key.split('.'):
        if segment not in value:
            return None
        value = value[segment]
    return value


def put_key(data, key, value):
    node = data
    keys = key.split('.')
    for segment in keys[:-1]:
        node.setdefault(segment, {})
        node = node[segment]
    segment = keys[-1]
    node[segment] = value


def _pack_datetime(item):
    for key, value in item.items():
        if isinstance(value, datetime.datetime):
            item[key] = {
                '__datetime__': True,
                'timestamp': value.timestamp() * 1000,
            }


def _unpack_datetime(item):
    for key, value in item.items():
        if isinstance(value, dict) and '__datetime__' in value:
            item[key] = datetime.datetime.fromtimestamp(value['timestamp'] / 1000)
