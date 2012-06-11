# -*- coding: utf-8 -*-
import zmq


class Sender(object):

    def __init__(self, my_socket):
        self.context = zmq.Context()
        self.sender = self.context.socket(zmq.PUSH)
        self.sender.bind(my_socket)

    def start(self, source_list):
        for source_item in source_list:
            self.sender.send(source_item)
