# -*- coding: utf-8 -*-
import os
import json
import socket
import zmq

from zeromr import logger

class BaseEndpoint(object):

    endpoint_type = 'endpoint'

    def __init__(self):
        self.name = '{0}-{1}@{2}'.format(self.endpoint_type, os.getpid(),
                                                             socket.getfqdn())
        self.context = zmq.Context()

    def handle_message(self, message_type, message, source=None):
        handler = 'handle_{0}_message'.format(message_type)
        return getattr(self, handler)(message, source=source)

    def send_json(self, message_type, message=None, target=None):
        logger.debug('Send message of type %s to %s', message_type, target)

        # send target info
        if target:
            self.sock.send(target, zmq.SNDMORE)
            self.sock.send('', zmq.SNDMORE)

        # send message type
        self.sock.send(message_type, zmq.SNDMORE)

        # send message
        wire_message = json.dumps(message)
        logger.debug('Payload: %s', wire_message)
        self.sock.send(wire_message)

    def recv_json(self):
        """
        Receive json

        Return tuple of:
            message_type, message
        """
        message_type = self.sock.recv()
        logger.debug('Recv message of type: %s', message_type)
        wire_message = self.sock.recv()
        message = json.loads(wire_message)
        return message_type, message

    def recv_json_from(self):
        """
        Receive json from specified address
        """
        address = self.sock.recv()
        self.sock.recv()  # empty
        message_type, message = self.recv_json()
        return address, message_type, message
