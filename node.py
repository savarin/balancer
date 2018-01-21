import datetime
import os
import socket
import sys

from helpers import parse_arguments, bind_socket, encode_bencode, decode_bencode


SOURCE_IP = os.getenv('BALANCER_IP_ADDRESS')
TARGET_IP = os.getenv('NODE_IP_ADDRESS')


class Node(object):
    def __init__(self, sock, source, peers):
        self.sock = sock
        self.source = source
        self.peers = peers
        self.data = {}
        self.counter = 0
        self.record = 0

    def compose(self, task, key, value=None):
        payload = ['res', task, self.counter, 'key', key, 'value', value]
        return encode_bencode(payload)

    def send(self, message, target):
        self.sock.sendto(message, (TARGET_IP, target))
        self.counter += 1

    def get(self, key):
        return self.data.get(key, '')

    def set(self, key, value):
        self.data[key] = value

    def reset(self):
        self.data = {}

    def execute(self, message, target):
        payload = decode_bencode(message)

        if payload[1] == 'broadcast':
            if not self.data:
                sys.stderr.write(str(datetime.datetime.now()) + ' WARN database empty\n')
                pass

            for key, value in self.data.iteritems():
                sys.stderr.write(key + ': ' + value + '\n')

        elif payload[1] == 'reset':
            self.reset()
            sys.stderr.write(str(datetime.datetime.now()) + ' INFO database reset\n')

        elif payload[1] in ['get', 'set']:
            key = payload[4]
            value = self.get(key) if payload[1] == 'get' else payload[6]

            if payload[1] == 'set':
                self.set(key, value)

            if int(payload[2]) >= 0:    # negative counter values ensures no response
                message = self.compose(payload[1], key, value)
                self.send(message, target)

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
    sock = bind_socket(SOURCE_IP, source)

    node = Node(sock, source, peers)
    node.listen()
