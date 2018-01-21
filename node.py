import datetime
import os
import socket
import sys

from helpers import parse_arguments, bind_socket, encode_bencode, decode_bencode


SOURCE_IP = os.getenv('NODE_IP_ADDRESS')


class Node(object):
    def __init__(self, sock, source, peers):
        self.sock = sock
        self.source = source
        self.peers = peers
        self.data = {}
        self.counter = 0
        self.record = 0

    def send(self, message, target, ip_address):
        self.sock.sendto(message, (ip_address, target))
        self.counter += 1

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
            sys.stderr.write(str(datetime.datetime.now()) + ' INFO ' + payload[1] + ' ' + payload[0] + ' to ' + str(peer) + '\n')
            self.send(message, peer, SOURCE_IP)
            try:
                self.sock.settimeout(1)
                response, address = self.sock.recvfrom(1024)
                sys.stderr.write(str(datetime.datetime.now()) + ' INFO ' + payload[1] + ' ' + payload[0] + ' from ' + str(peer) + '\n')
                result = decode_bencode(response)
                if result[6]:
                    return result[6]
            except socket.timeout:
                sys.stderr.write(str(datetime.datetime.now()) + ' WARN timeout\n')

    def execute(self, message, target, ip_address):
        payload = decode_bencode(message)

        if payload[1] == 'broadcast':
            if not self.data:
                sys.stderr.write(str(datetime.datetime.now()) + ' WARN database empty\n')
                pass

            sys.stderr.write(str(datetime.datetime.now()) + ' INFO database content\n')
            for key, value in self.data.iteritems():
                sys.stderr.write(key + ': ' + value + '\n')

        elif payload[1] == 'reset':
            self.reset()
            sys.stderr.write(str(datetime.datetime.now()) + ' INFO database reset\n')

        elif payload[1] in ['get', 'set']:
            sys.stderr.write(str(datetime.datetime.now()) + ' INFO ' + payload[1] + ' ' + payload[0] + ' from ' + str(target) + '\n')
            key = payload[4]
            value = self.get(key) if payload[1] == 'get' else payload[6]

            if payload[0] == 'request' and payload[1] == 'get' and not value:
                value = self.relay(key) or ''  # returns empty string if result in None

            if payload[1] == 'set':
                self.set(key, value)

            payload = ['response', payload[1], self.counter, 'key', key, 'value', value]
            message = encode_bencode(payload)
            sys.stderr.write(str(datetime.datetime.now()) + ' INFO ' + payload[1] + ' ' + payload[0] + ' to ' + str(target) + '\n')
            self.send(message, target, ip_address)

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
