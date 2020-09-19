#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# Copyright (C) 2020 - , puxxustc


import re

from lib_util import get_key


__all__ = [
    'F', 'Q',
]


class Q():
    '''query'''
    def __init__(self, *args):
        if not args:
            self.op = '#EMPTY#'
            self.left = None
            self.right = None
            self.complex = None
        elif len(args) == 1:
            try:
                ops = [
                    '===', '==', '!=', '<=', '<', '>=', '>', '!~', '~', '$in',
                    '$e', '$ne', '$true', '$false',
                ]
                s = args[0].strip()
                op = None
                for i in ops:
                    if i in s:
                        op = i
                        left, right = s.split(op)
                        break
                else:
                    # 不含有运算符，但是只含有左操作数，等同于 $true
                    if re.match(r'^[\w.]+$', s):
                        op = '$true'
                        left = s
                        right = ''
                    else:
                        raise ValueError()
                left = left.strip()
                if any((i in left for i in ' =!<>~$')):
                    raise ValueError()
                right = right.strip()
                if right:
                    right = eval(right, {'__builtins__': None})
                self.op = op
                self.left = left
                self.right = right
                self.complex = None
            except Exception:
                raise ValueError('Q() bad init args: %s' % repr(args))
        elif len(args) == 3:
            op, left, right = args
            self.op = op
            self.left = left
            self.right = right
            self.complex = None
        else:
            raise ValueError('Q() bad init args: %s' % repr(args))

    def __str__(self):
        if self.op == '#EMPTY#':
            return 'Q()'
        if self.complex and self.complex[0] == '~':
            q1 = self.complex[1]
            if q1.complex:
                return '~(%s)' % q1
            else:
                return '~%s' % q1
        elif self.complex and (self.complex[0] in ['&', '|']):
            op = self.complex[0]
            q1 = self.complex[1]
            q2 = self.complex[2]
            if op == '&':
                if q1.complex and not q1._is_all_and():
                    left = '(%s)' % q1
                else:
                    left = '%s' % q1
                if q2.complex and not q2._is_all_and():
                    right = '(%s)' % q2
                else:
                    right = '%s' % q2
                return '%s & %s' % (left, right)
            elif op == '|':
                if q1.complex and not q1._is_all_or():
                    left = '(%s)' % q1
                else:
                    left = '%s' % q1
                if q2.complex and not q2._is_all_or():
                    right = '(%s)' % q2
                else:
                    right = '%s' % q2
                return '%s | %s' % (left, right)
        else:
            if self.op in ['$e', '$ne', '$true', '$false']:
                return 'Q(%s %s)' % (self.left, self.op)
            else:
                return 'Q(%s %s %s)' % (self.left, self.op, repr(self.right))

    def __repr__(self):
        return self.__str__()

    def __and__(self, value):
        if self.op == '#EMPTY#':
            return value
        elif value.op == '#EMPTY#':
            return self
        q = Q(None, None, None)
        q.complex = ('&', self, value)
        return q

    def __or__(self, value):
        if self.op == '#EMPTY#':
            return self
        elif value.op == '#EMPTY#':
            return value
        q = Q(None, None, None)
        q.complex = ('|', self, value)
        return q

    def __invert__(self):
        q = Q(None, None, None)
        q.complex = ('~', self)
        return q

    def _is_all_and(self):
        if self.complex:
            if self.complex[0] == '&':
                q1 = self.complex[1]
                q2 = self.complex[2]
                return q1._is_all_and() and q2._is_all_and()
            else:
                return False
        else:
            return True

    def _is_all_or(self):
        if self.complex:
            if self.complex[0] == '|':
                q1 = self.complex[1]
                q2 = self.complex[2]
                return q1._is_all_or() and q2._is_all_or()
            else:
                return False
        else:
            return True

    def keys(self):
        if self.op == '#EMPTY#':
            return []
        elif self.complex:
            if self.complex[0] == '~':
                q1 = self.complex[1]
                return q1.keys()
            else:
                q1 = self.complex[1]
                q2 = self.complex[2]
                keys = set(q1.keys())
                keys.update(q2.keys())
                return list(keys)
        else:
            return [self.left]

    def match(self, data, none_as_match=True, shallow_match=False):
        if self.op == '#EMPTY#':
            return True
        elif self.complex and self.complex[0] == '~':
            q1 = self.complex[1]
            return not q1.match(data, none_as_match=none_as_match, shallow_match=shallow_match)
        elif self.complex and self.complex[0] in ['&', '|']:
            op = self.complex[0]
            q1 = self.complex[1]
            q2 = self.complex[2]
            left = q1.match(data, none_as_match=none_as_match, shallow_match=shallow_match)
            if op == '&':
                if left:
                    return q2.match(data, none_as_match=none_as_match, shallow_match=shallow_match)
                else:
                    return False
            elif op == '|':
                if left:
                    return True
                else:
                    return q2.match(data, none_as_match=none_as_match, shallow_match=shallow_match)
        else:
            op = self.op
            left = get_key(data, self.left)
            right = self.right
            if left is None:
                if not shallow_match:
                    if self.op in ['===', '$e', '$true', '$false']:
                        return False
                if none_as_match:
                    return True
                else:
                    return False
            match = False
            if op == '===':
                match = left == right
            elif op == '==':
                match = left == right
            elif op == '!=':
                match = left != right
            elif op == '<':
                if isinstance(left, list):
                    match = left and all((i < right for i in left))
                elif isinstance(left, dict):
                    match = left and all((i < right for i in left.values()))
                else:
                    match = left < right
            elif op == '<=':
                if isinstance(left, list):
                    match = left and all((i <= right for i in left))
                elif isinstance(left, dict):
                    match = left and all((i <= right for i in left.values()))
                else:
                    match = left <= right
            elif op == '>':
                if isinstance(left, list):
                    match = left and all((i > right for i in left))
                elif isinstance(left, dict):
                    match = left and all((i > right for i in left.values()))
                else:
                    match = left > right
            elif op == '>=':
                if isinstance(left, list):
                    match = left and all((i >= right for i in left))
                elif isinstance(left, dict):
                    match = left and all((i >= right for i in left.values()))
                else:
                    match = left >= right
            elif op == '!~':
                if isinstance(left, str):
                    match = right.lower() not in left.lower()
                elif isinstance(left, list):
                    match = right not in left
            elif op == '~':
                if isinstance(left, str):
                    match = right.lower() in left.lower()
                elif isinstance(left, list):
                    match = right in left
            elif op == '$in':
                match = left in right
            elif op == '$e':
                match = left is not None
            elif op == '$ne':
                match = left is None
            elif op == '$true':
                match = left is not None and bool(left)
            elif op == '$false':
                match = left is not None and not bool(left)
            return match

    @classmethod
    def from_kwargs(cls, kwargs):
        r = None
        for key, value in kwargs.items():
            q = Q('===', key, value)
            if r is None:
                r = q
            else:
                r = r & q
        return r

    @classmethod
    def raw(cls, **kwargs):
        return cls.from_kwargs(kwargs)


class F():
    def __init__(self, key=None):
        self.key = key

    def __call__(self, key):
        return _F(key)

    def __getattr__(self, attr):
        if not self.key:
            if not attr.startswith('__'):
                return F(attr)
        else:
            raise AttributeError("type object 'F' has no attribute '%s'" % attr)

    def __str__(self):
        if self.key:
            return 'F.%s' % self.key
        else:
            return 'F'

    def __repr__(self):
        return self.__str__()

    def __eq__(self, value):
        op = '=='
        left = self.key
        right = value
        return Q(op, left, right)

    def __ne__(self, value):
        op = '!='
        left = self.key
        right = value
        return Q(op, left, right)

    def __gt__(self, value):
        op = '>'
        left = self.key
        right = value
        return Q(op, left, right)

    def __ge__(self, value):
        op = '>='
        left = self.key
        right = value
        return Q(op, left, right)

    def __lt__(self, value):
        op = '<'
        left = self.key
        right = value
        return Q(op, left, right)

    def __le__(self, value):
        op = '<='
        left = self.key
        right = value
        return Q(op, left, right)


_F = F

F = _F()


def parse_filter(filter):
    filter = filter.strip()
    if not filter:
        return Q()
    try:
        q = Q()
        for item in filter.split(','):
            item = item.strip()
            q = q & Q(item)
        return q
    except ValueError:
        raise ValueError('过滤器错误: 过滤器 "%s" 格式错误' % filter)
