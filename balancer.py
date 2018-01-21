import os
import sys

from helpers import parse_arguments, bind_socket, encode_bencode, decode_bencode


SOURCE_IP = os.getenv('BALANCER_IP_ADDRESS')
TARGET_IP = os.getenv('NODE_IP_ADDRESS')


class Balancer(object):
    def __init__(self, sock, source, targets):
        self.sock = sock
        self.source = source
        self.targets = targets
        self.counter = 0

    def compose(self, task, key, value=None):
        payload = ['req', task, self.counter, 'key', key, 'value', value]
        return encode_bencode(payload)

    def send(self, message, target):
        self.sock.sendto(message, (TARGET_IP, target))
        self.counter += 1

    def broadcast(self):
        for target in self.targets:
            payload = ['req', 'broadcast', str(self.counter)]
            message = encode_bencode(payload)
            self.send(message, target)

    def execute(self, command):
        if command[0] == 'broadcast':
            self.broadcast()

    def listen(self):
        while True:
            command = raw_input('> ').split()
            self.execute(command)


if __name__ == '__main__':
    source, targets = parse_arguments(balancer=True)
    sock = bind_socket(SOURCE_IP, source)

    balancer = Balancer(sock, source, targets)
    balancer.listen()
