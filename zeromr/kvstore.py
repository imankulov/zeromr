# -*- coding: utf-8 -*-
import redis
import json
import hashlib


class RedisStore(object):

    def __init__(self, prefix):
        self.r = redis.Redis()
        self.prefix = prefix
        self.all_keys = '{0}:keys'.format(self.prefix)

    def save(self, key, value):
        self.r.sadd(self.all_keys, key)
        key_hash = hashlib.md5(key.encode('utf-8')).hexdigest()
        self.r.rpush('{0}:{1}'.format(self.prefix, key_hash), json.dumps(value))

    def cleanup(self):
        keys_to_delete = [self.all_keys]
        for key in self.r.smembers(self.all_keys):  # list of strings
            key_hash = hashlib.md5(key).hexdigest()
            keys_to_delete.append('{0}:{1}'.format(self.prefix, key_hash))
        self.r.delete(*keys_to_delete)

    def get_all(self):
        for key in self.r.smembers(self.all_keys):  # list of strings
            yield self._get_by_key(key, delete=False)

    def has_keys(self):
        return self.r.scard(self.all_keys) > 0

    def get_random(self):
        """
        Return and remove random element from the store
        """
        key = self.r.spop(self.all_keys)
        return self._get_by_key(key, delete=True)

    def _get_by_key(self, key, delete=False):
        key_hash = hashlib.md5(key).hexdigest()
        values = self.r.lrange('{0}:{1}'.format(self.prefix, key_hash), 0, -1)
        if delete:
            self.r.delete('{0}:{1}'.format(self.prefix, key_hash))
        decoded_values = [json.loads(val) for val in values]
        return key.decode('utf-8'), decoded_values

