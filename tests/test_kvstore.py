# -*- coding: utf-8 -*-
from nose.tools import eq_
from zeromr.kvstore import RedisStore


def test_kvstore():
    store = RedisStore('prefix')
    store.cleanup()
    # store two values with the same key
    store.save(u'foö', u'bar')
    store.save(u'foö', 1)
    # get this key
    key, values = store.get_all().next()
    eq_(key, u'foö')
    eq_(values, [u'bar', 1])
    # get and delete random key
    key, values = store.get_random()
    # nothing left here
    eq_(store.has_keys(), False)
    store.cleanup()
