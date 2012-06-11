# -*- coding: utf-8 -*-
import redis
import hashlib


class RedisStore(object):

    def __init__(self):
        self.r = redis.Redis()
        self.prefix = 'shuffler_prefix'  # TODO: make it random
        self.all_keys = '{0}:keys'.format(self.prefix)

    def save(self, key, value):
        self.r.sadd(self.all_keys, key)
        key_hash = hashlib.md5(key).hexdigest()
        self.r.rpush('{0}:{1}'.format(self.prefix, key_hash), value)

    def cleanup(self):
        keys_to_delete = [self.all_keys]
        for key in self.r.smembers(self.all_keys):
            key_hash = hashlib.md5(key).hexdigest()
            keys_to_delete.append('{0}:{1}'.format(self.prefix, key_hash))
        self.r.delete(*keys_to_delete)

    def get_all(self):
        for key in self.r.smembers(self.all_keys):
            key_hash = hashlib.md5(key).hexdigest()
            value = self.r.lrange('{0}:{1}'.format(self.prefix, key_hash), 0, -1)
            yield key, value
