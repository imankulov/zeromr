# -*- coding: utf-8 -*-
import zmq

from .base import BaseEndpoint
from .kvstore import RedisStore


class Sender(BaseEndpoint):

    endpoint_type = 'sender'

    def __init__(self, my_socket, source_list):
        super(Sender, self).__init__()
        self.source_list = source_list
        self.sock = self.context.socket(zmq.ROUTER)
        self.sock.setsockopt(zmq.IDENTITY, self.name)
        self.sock.bind(my_socket)
        self.map_store = RedisStore(prefix='{0}:map'.format(self.name))
        self.map_store.cleanup()
        self.reduce_store = RedisStore(prefix='{0}:reduce'.format(self.name))
        self.reduce_store.cleanup()
        self.available_workers = set()
        self.busy_workers = set()

    def start(self):
        while True:
            source, message_type, message = self.recv_json_from()
            self.handle_message(message_type, message, source=source)
            worker = self.get_available_worker()
            if worker:
                if self.has_map_tasks():
                    self.send_map_task(worker)
                elif self.has_reduce_tasks():
                    self.send_reduce_task(worker)
                else:
                    self.send_shutdown_task(worker)
            if not self.has_map_tasks() and \
                    not self.has_reduce_tasks() and \
                    not self.available_workers and \
                    not self.busy_workers:
                break
        self.sock.close()
        self.map_store.cleanup()
        for key, values in self.reduce_store.get_all():
            print key, values
        self.reduce_store.cleanup()

    def handle_init_message(self, message, source):
        self.available_workers.add(source)

    def handle_map_chunks_message(self, message, source):
        for key, value in message:
            self.map_store.save(key, value)
        self.send_json('keep_going', target=source)

    def handle_reduce_chunks_message(self, message, source):
        for key, value in message:
            self.reduce_store.save(key, value)
        self.send_json('keep_going', target=source)

    def handle_map_end_message(self, message, source):
        if source in self.busy_workers:
            self.busy_workers.remove(source)
        return self.handle_init_message(message, source)

    def handle_reduce_end_message(self, message, source):
        if source in self.busy_workers:
            self.busy_workers.remove(source)
        return self.handle_init_message(message, source)

    def get_available_worker(self):
        try:
            return self.available_workers.pop()
        except KeyError:
            return None

    def has_map_tasks(self):
        return bool(self.source_list)

    def send_map_task(self, worker):
        source = self.source_list.pop()
        message = {
            'mapper': 'zeromr.samples.word_count_map',
            'source': source,
            'source_reader': 'zeromr.samples.read_file',
        }
        self.busy_workers.add(worker)
        self.send_json('map', message=message, target=worker)

    def has_reduce_tasks(self):
        return self.map_store.has_keys()

    def send_reduce_task(self, worker):
        key, values = self.map_store.get_random()
        message = {
            'reducer': 'zeromr.samples.word_count_reduce',
            'key': key,
            'values': values,
        }
        self.busy_workers.add(worker)
        self.send_json('reduce', message=message, target=worker)

    def send_shutdown_task(self, worker):
        self.send_json('shutdown', target=worker)
        if worker in self.available_workers:
            del self.available_workers[worker]
        if worker in self.busy_workers:
            del self.busy_workers[worker]
