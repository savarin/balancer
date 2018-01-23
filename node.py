import datetime
import os
import random
import socket
import sys
from datetime import datetime as dt

from helpers import Queue, parse_arguments, bind_socket, dispatch_status, \
    encode_bencode, decode_bencode


NODE_IP = os.getenv('NODE_IP_ADDRESS')


class Node(object):
    def __init__(self, sock, source, peers, timeout=1):
        self.sock = sock
        self.source = source
        self.peers = peers
        self.timeout = timeout
        self.data = {}
        self.queue = Queue()
        self.record = 0

    def replay(self, payload, address):
        '''
        Re-sends message in buffer to balancer.
        '''
        message = self.queue.get(payload[2])[0]
        self.sock.sendto(message, address)
        dispatch_status(payload[1], 're-response', 'to', address[1])

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
        self.queue = Queue()
        sys.stderr.write(str(dt.now()) + ' WARN database reset\n')

    def transmit(self, message, address):
        '''
        Sends message to other nodes
        '''
        self.record += 1
        self.sock.sendto(message, address)

    def relay(self, task, key, value=''):
        '''
        Sends message to all nodes to find specific key
        '''
        payload = ['relay', task, str(self.record), 'key', key, 'value', value]
        message = encode_bencode(payload)

        for peer in self.peers:
            self.transmit(message, (NODE_IP, peer))
            dispatch_status(payload[1], payload[0], 'to', peer)

            try:
                self.sock.settimeout(self.timeout)
                response, address = self.sock.recvfrom(1024)
                dispatch_status(payload[1], payload[0], 'from', peer)
                result = decode_bencode(response)
                if result[6]:
                    return result[6]

            except socket.timeout:
                sys.stderr.write(str(dt.now()) + ' WARN timeout\n')

    def deliver(self, message, address, identifier=None, drop_probability=0.2):
        '''
        Sends message to balancer.
        '''
        if identifier is not None:
            self.queue.put(identifier, (message, dt.now()))

        if random.random() > drop_probability:
            self.sock.sendto(message, address)

    def get(self, payload, address):
        message = ''

        if payload[0] == 'request':
            value = self.data.get(payload[4], '')

            if not value:
                value = self.relay(payload[1], payload[4]) or ''  # returns empty string if result is None

            result = ['response', 'get', self.queue.status(), 'key', payload[4], 'value', value]
            message = encode_bencode(result)

            self.deliver(message, address, identifier=payload[2])
            dispatch_status('get', 'response', 'to', address[1])

        elif payload[0] == 'relay':
            value = self.data.get(payload[4], '')

            result = ['relay', 'get', self.record, 'key', payload[4], 'value', value]
            message = encode_bencode(result)

            self.transmit(message, address)
            dispatch_status('get', 'relay', 'to', address[1])

        return message

    def set(self, payload, address):
        message = ''

        if payload[0] == 'request':
            value = self.relay(payload[1], payload[4], payload[6]) or ''

            if not value:
                self.data[payload[4]] = payload[6]

            result = ['response', 'set', self.queue.status(), 'key', payload[4], 'value', payload[6]]
            message = encode_bencode(result)

            self.deliver(message, address, identifier=payload[2])
            dispatch_status('set', 'response', 'to', address[1])

        elif payload[0] == 'relay':
            value = ''

            if payload[4] in self.data:
                self.data[payload[4]] = payload[6]
                value = payload[6]

            result = ['relay', 'set', self.record, 'key', payload[4], 'value', value]
            message = encode_bencode(result)

            self.transmit(message, address)
            dispatch_status('set', 'relay', 'to', address[1])

        return message

    def execute(self, request, address):
        payload = decode_bencode(request)

        if self.queue.get(payload[2]):  # check if message in buffer
            dispatch_status(payload[1], 're-request', 'from', address[1])
            self.replay(payload, address)
            return None

        if payload[1] == 'broadcast':
            dispatch_status('broadcast', 'request', 'from', address[1])
            self.broadcast()

        elif payload[1] == 'reset':
            dispatch_status('reset', 'request', 'from', address[1])
            self.reset()

        elif payload[1] == 'get':
            dispatch_status('get', payload[0], 'from', address[1])
            self.get(payload, address)

        elif payload[1] == 'set':
            dispatch_status('set', payload[0], 'from', address[1])
            self.set(payload, address)

    def listen(self):
        while True:
            try:
                self.sock.settimeout(3)
                request, address = self.sock.recvfrom(1024)
                self.execute(request, address)

            except socket.timeout:
                sys.stderr.write('.\n')


if __name__ == '__main__':
    source, peers = parse_arguments()
    sock = bind_socket(NODE_IP, source)

    node = Node(sock, source, peers)
    node.listen()
