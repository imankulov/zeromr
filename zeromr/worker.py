# -*- coding: utf-8 -*-
"""
Messages which worker can send/receive

* Send message: "{type: init, name: worker-XXXX}"

  Worker notifies that it is ready to accept tasks

* Recv message "{type: map, mapper: module.func, source: source,
                 source_reader: source_reader}"

  :param type: Type equals to "map" means that worker have to execute "map"
               type of the task.
  :param mapper: map function to be executed.
  :param source: source identifier (file/URL/database record).
  :param source_reader: function which actually reads the contents of the source
                        and yields it in the key/value form

* Recv message "{type: shutdown}"

  Stop the worker
"""
import zmq
import importlib

from .base import BaseEndpoint


class Worker(BaseEndpoint):

    endpoint_type = 'worker'

    def __init__(self, sender_socket, mapper):
        super(Worker, self).__init__()
        self.sock = self.context.socket(zmq.REQ)
        self.sock.setsockopt(zmq.IDENTITY, self.name)
        self.sock.connect(sender_socket)
        self.result_queue_length = 4
        self.result_queue = []

    def start(self):
        self.send_json('init')  # i'm ready
        while True:
            message_type, message = self.recv_json()  # what to do now ?
            do_break = self.handle_message(message_type, message)
            if do_break:
                break
        self.sock.close()

    def handle_map_message(self, message, source):
        mapper = get_callable(message['mapper'])
        source_reader = get_callable(message['source_reader'])
        for key, value in source_reader(message['source']):
            for result in mapper(key, value):
                self.push_to_result_queue(result, flush_target='map')
        self.flush_result_queue('map')
        self.send_json('map_end')
        return False

    def handle_reduce_message(self, message, source):
        reducer = get_callable(message['reducer'])
        key = message['key']
        values = message['values']
        for result in reducer(key, values):
            self.push_to_result_queue(result, flush_target='reduce')
        self.flush_result_queue('reduce')
        self.send_json('reduce_end')
        return False

    def handle_shutdown_message(self, message, source):
        return True

    def push_to_result_queue(self, result, flush_target):
        self.result_queue.append(result)
        if len(self.result_queue) >= self.result_queue_length:
            self.flush_result_queue(flush_target=flush_target)

    def flush_result_queue(self, flush_target):
        message_type = '{0}_chunks'.format(flush_target)
        self.send_json(message_type, message=self.result_queue)
        self.result_queue = []
        message_type, message = self.recv_json()
        assert message_type == 'keep_going'


def get_callable(func):
    if callable(func):
        return func
    module_name, func_name = func.rsplit('.', 1)
    module = importlib.import_module(module_name)
    return getattr(module, func_name)
