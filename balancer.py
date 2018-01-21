import os
import sys

from helpers import parse_arguments, bind_socket


SOURCE_IP = os.getenv('BALANCER_IP_ADDRESS')
TARGET_IP = os.getenv('NODE_IP_ADDRESS')


class Balancer(object):
    def __init__(self, sock, source, targets):
        self.sock = sock
        self.source = source
        self.targets = targets
        self.counter = 0

    def listen(self):
        while True:
            command = raw_input('> ')
            sys.stderr.write(command + '\n')


if __name__ == '__main__':
    source, targets = parse_arguments(balancer=True)
    sock = bind_socket(SOURCE_IP, source)

    balancer = Balancer(sock, source, targets)
    balancer.listen()
