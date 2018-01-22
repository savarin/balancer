import datetime
import os
import random
import socket
import sys
from datetime import datetime as dt

from helpers import Queue, parse_arguments, bind_socket, dispatch_status, \
    encode_bencode, decode_bencode


SOURCE_IP = os.getenv('NODE_IP_ADDRESS')


class Node(object):
    def __init__(self, sock, source, peers):
        self.sock = sock
        self.source = source
        self.peers = peers
        self.data = {}
        self.queue = Queue(10)
        self.counter = 0
        self.record = 0

    def send(self, message, target, ip_address, drop_probability=0.2, identifier=None):
        self.counter += 1

        if identifier is not None:
            self.queue.put(int(identifier), message)

        if random.random() > drop_probability:
            self.sock.sendto(message, (ip_address, target))

    def get(self, key):
        return self.data.get(key, '')

    def set(self, key, value):
        self.data[key] = value

    def reset(self):
        self.data = {}

    def relay(self, key):
        payload = ['relay', 'get', str(self.counter), 'key', key, 'value', '']
        message = encode_bencode(payload)

        for peer in self.peers:
            dispatch_status(payload[1], payload[0], 'to', peer)
            self.send(message, peer, SOURCE_IP, drop_probability=0)
            try:
                self.sock.settimeout(1)
                response, address = self.sock.recvfrom(1024)
                dispatch_status(payload[1], payload[0], 'from', peer)
                result = decode_bencode(response)
                if result[6]:
                    return result[6]
            except socket.timeout:
                sys.stderr.write(str(dt.now()) + ' WARN timeout\n')

    def execute(self, message, target, ip_address):
        payload = decode_bencode(message)
        method = payload[0]
        identifier = payload[2]

        if payload[2] in self.queue.data:  # check if message in buffer
            dispatch_status(payload[1], 're-request', 'from', target)
            dispatch_status(payload[1], 're-response', 'to', target)
            message = self.queue.data[payload[2]]
            self.send(message, target, ip_address)
            return None  

        if payload[1] == 'broadcast':
            if not self.data:
                sys.stderr.write(str(dt.now()) + ' WARN database empty\n')
                return None

            sys.stderr.write(str(dt.now()) + ' INFO database content\n')
            for key, value in self.data.iteritems():
                sys.stderr.write(key + ': ' + value + '\n')

        elif payload[1] == 'reset':
            self.reset()
            sys.stderr.write(str(dt.now()) + ' INFO database reset\n')

        elif payload[1] in ['get', 'set']:
            dispatch_status(payload[1], payload[0], 'from', target)
            key = payload[4]
            value = self.get(key) if payload[1] == 'get' else payload[6]

            if payload[0] == 'request' and payload[1] == 'get' and not value:
                value = self.relay(key) or ''  # returns empty string if result is None

            if payload[1] == 'set':
                self.set(key, value)

            payload = ['response', payload[1], self.counter, 'key', key, 'value', value]
            dispatch_status(payload[1], payload[0], 'to', target)
            message = encode_bencode(payload)

            if method == 'relay':
                self.send(message, target, ip_address, drop_probability=0)
                return None
            self.send(message, target, ip_address, identifier=identifier)

    def listen(self):
        while True:
            try:
                self.sock.settimeout(3)
                request, address = self.sock.recvfrom(1024)
                self.execute(request, address[1], address[0])
            except socket.timeout:
                sys.stderr.write('.\n')


if __name__ == '__main__':
    source, peers = parse_arguments()
    sock = bind_socket(SOURCE_IP, source)

    node = Node(sock, source, peers)
    node.listen()
