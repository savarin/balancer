import datetime
import os
import random
import socket
import sys
from datetime import datetime as dt

from helpers import Queue, parse_arguments, bind_socket, dispatch_status, \
    encode_bencode, decode_bencode


NODE_IP = os.getenv('NODE_IP_ADDRESS')
BALANCER_IP = os.getenv('BALANCER_IP_ADDRESS')


class Node(object):
    def __init__(self, sock, source, peers):
        self.sock = sock
        self.source = source
        self.peers = peers
        self.data = {}
        self.queue = Queue(10)
        self.counter = 0
        self.record = 0

    def replay(self, payload, target):
        '''
        Re-sends message in buffer to balancer.
        '''
        dispatch_status(payload[1], 're-response', 'to', target)
        message = self.queue.data[payload[2]]
        self.sock.sendto(message, (BALANCER_IP, target))

    def broadcast(self):
        if not self.data:
            sys.stderr.write(str(dt.now()) + ' WARN database empty\n')
            return None

        sys.stderr.write(str(dt.now()) + ' INFO database contents start\n')
        for key, value in self.data.iteritems():
            sys.stderr.write(key + ': ' + value + '\n')

        sys.stderr.write(str(dt.now()) + ' INFO database contents end\n')

    def reset(self):
        self.data = {}
        sys.stderr.write(str(dt.now()) + ' WARN database reset\n')

    def deliver(self, message, target, drop_probability=0.2, identifier=None):
        '''
        Sends message to balancer.
        '''
        self.counter += 1

        if identifier is not None:
            self.queue.put(identifier, message)

        if random.random() > drop_probability:
            self.sock.sendto(message, (BALANCER_IP, target))

    def transmit(self, message, peer):
        '''
        Sends message to other nodes
        '''
        self.record += 1
        self.sock.sendto(message, (NODE_IP, peer))

    def relay(self, task, key, value=''):
        '''
        Sends message to all nodes to find specific key
        '''
        payload = ['relay', task, str(self.record), 'key', key, 'value', value]
        message = encode_bencode(payload)

        for peer in self.peers:
            dispatch_status(payload[1], payload[0], 'to', peer)
            self.transmit(message, peer)

            try:
                self.sock.settimeout(1)
                response, address = self.sock.recvfrom(1024)
                dispatch_status(payload[1], payload[0], 'from', peer)
                result = decode_bencode(response)
                if result[6]:
                    return result[6]

            except socket.timeout:
                sys.stderr.write(str(dt.now()) + ' WARN timeout\n')

    def get(self, payload, target):
        if payload[0] == 'request':
            value = self.data.get(payload[4], '')

            if not value:
                value = self.relay(payload[1], payload[4]) or ''  # returns empty string if result is None

            result = ['response', 'get', self.counter, 'key', payload[4], 'value', value]
            message = encode_bencode(result)

            dispatch_status('get', 'response', 'to', target)
            self.deliver(message, target, identifier=payload[2])

        elif payload[0] == 'relay':
            value = self.data.get(payload[4], '')

            result = ['relay', 'get', self.counter, 'key', payload[4], 'value', value]
            message = encode_bencode(result)

            dispatch_status('get', 'relay', 'to', target)
            self.transmit(message, target)

    def set(self, payload, target):
        if payload[0] == 'request':
            value = self.relay(payload[1], payload[4], payload[6]) or ''

            if not value:
                self.data[payload[4]] = payload[6]

            result = ['response', 'set', self.record, 'key', payload[4], 'value', payload[6]]
            message = encode_bencode(result)

            dispatch_status('set', 'response', 'to', target)
            self.deliver(message, target, identifier=payload[2])

        elif payload[0] == 'relay':
            value = ''

            if payload[4] in self.data:
                self.data[payload[4]] = payload[6]
                value = payload[6]

            result = ['relay', 'set', self.record, 'key', payload[4], 'value', value]
            message = encode_bencode(result)

            dispatch_status('set', 'relay', 'to', target)
            self.transmit(message, target)

    def execute(self, message, target):
        payload = decode_bencode(message)

        if payload[2] in self.queue.data:  # check if message in buffer
            dispatch_status(payload[1], 're-request', 'from', target)
            self.replay(payload, target)
            return None

        if payload[1] == 'broadcast':
            dispatch_status('broadcast', 'request', 'from', target)
            self.broadcast()

        elif payload[1] == 'reset':
            dispatch_status('reset', 'request', 'from', target)
            self.reset()

        elif payload[1] == 'get':
            dispatch_status('get', payload[0], 'from', target)
            self.get(payload, target)

        elif payload[1] == 'set':
            dispatch_status('set', payload[0], 'from', target)
            self.set(payload, target)

    def listen(self):
        while True:
            try:
                self.sock.settimeout(3)
                request, address = self.sock.recvfrom(1024)
                self.execute(request, address[1])
            except socket.timeout:
                sys.stderr.write('.\n')


if __name__ == '__main__':
    source, peers = parse_arguments()
    sock = bind_socket(NODE_IP, source)

    node = Node(sock, source, peers)
    node.listen()
