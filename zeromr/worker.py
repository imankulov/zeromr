# -*- coding: utf-8 -*-
import os
import zmq
import json
import importlib


class Worker(object):

    def __init__(self, sender_socket, shuffler_socket, mapper):
        self.context = zmq.Context()
        self.mapper = self.get_mapper(mapper)

        self.receiver = self.context.socket(zmq.PULL)
        self.receiver.connect(sender_socket)

        self.map_sender = self.context.socket(zmq.PUSH)
        self.map_sender.connect(shuffler_socket)


    def start(self):
        while True:
            source = self.receiver.recv()
            print source
            for key, value in self.yield_source_content(source):
                for result in self.mapper(key, value):
                    self.map_sender.send(json.dumps(result))

    def get_mapper(self, mapper):
        if callable(mapper):
            return mapper
        module_name, func_name = mapper.rsplit('.', 1)
        module = importlib.import_module(module_name)
        return getattr(module, func_name)

    def yield_source_content(self, source):
        """
        Can be overriden in subclasses
        """
        if not os.path.isfile(source):
            return
        with open(source) as fd:
            for i, line in enumerate(fd.readlines()):
                yield i, line.strip()
