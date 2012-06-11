# -*- coding: utf-8 -*-
import zmq
import json
from zeromr.kvstore import RedisStore


class Shuffler(object):

    def __init__(self, my_socket):
        self.context = zmq.Context()
        self.shuffler = self.context.socket(zmq.PULL)
        self.shuffler.bind(my_socket)
        self.kvstore = RedisStore()

    def start(self):
        while True:
            key, value = json.loads(self.shuffler.recv())
            self.kvstore.save(key, value)
            print key, value
