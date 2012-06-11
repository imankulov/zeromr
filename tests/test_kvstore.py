# -*- coding: utf-8 -*-
from nose.tools import eq_
from zeromr.kvstore import RedisStore


def test_kvstore():
    store = RedisStore()
    store.cleanup()
    store.save('foo', 'bar')
    store.save('foo', 'baz')
    key, values = store.get_all().next()
    eq_(key, 'foo')
    eq_(values, ['bar', 'baz'])
    store.cleanup()
    eq_(list(store.get_all()), [])
