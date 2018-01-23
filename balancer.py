import datetime
import os
import random
import socket
import sys
import threading
from datetime import datetime as dt

from helpers import parse_arguments, bind_socket, dispatch_status, \
    encode_bencode, decode_bencode


BALANCER_IP = os.getenv('BALANCER_IP_ADDRESS')
NODE_IP = os.getenv('NODE_IP_ADDRESS')


class Balancer(object):
    def __init__(self, sock, source, targets):
        self.sock = sock
        self.source = source
        self.targets = targets
        self.counter = 0
        self.status = {}

    def send(self, message, target, drop_probability=0.2, increment=True):
        if increment:
            self.counter += 1

        if random.random() > drop_probability:
            self.sock.sendto(message, (NODE_IP, target))

    def broadcast(self, reset=False):
        task = 'reset' if reset else 'broadcast'
        payload = ['request', task, str(self.counter)]
        message = encode_bencode(payload)

        for target in self.targets:
            self.send(message, target, drop_probability=0)

    def choose(self):
        return self.targets[self.counter % len(self.targets)]

    def execute(self, command):
        if command[0] == 'broadcast':
            if os.getenv('BALANCER_DEBUG'):
                sys.stderr.write(str(dt.now()) + ' INFO cluster broadcast\n')
            self.broadcast()

        elif command[0] == 'reset':
            if os.getenv('BALANCER_DEBUG'):
                sys.stderr.write(str(dt.now()) + ' WARN database reset\n')
            self.broadcast(reset=True)

        elif command[0] in ['get', 'set']:
            key = command[1]
            value = '' if command[0] == 'get' else command[2]
            payload = ['request', command[0], self.counter, 'key', key, 'value', value]
            message = encode_bencode(payload)

            target = self.choose()
            if os.getenv('BALANCER_DEBUG'):
                dispatch_status(payload[1], payload[0], 'to', target)

            attempt = 0
            while attempt < 3:
                if os.getenv('BALANCER_DEBUG'):
                    sys.stderr.write(str(dt.now()) + ' INFO attempt ' + str(attempt + 1) + ' of 3\n')

                increment = True if attempt == 0 else False  # only increment if 1st attempt
                self.send(message, target, increment=increment)

                try:
                    self.sock.settimeout(1)
                    response, address = self.sock.recvfrom(1024)
                    if os.getenv('BALANCER_DEBUG'):
                        dispatch_status(command[0], 'response', 'from', target)
                    result = decode_bencode(response)
                    output = result[6] + '\n' if command[0] == 'get' else 'success!\n'
                    sys.stderr.write(output)
                    break

                except socket.timeout:
                    if os.getenv('BALANCER_DEBUG'):
                        sys.stderr.write(str(dt.now()) + ' WARN timeout\n')

                attempt += 1
                if attempt == 3:
                    sys.stderr.write('failed!\n')

    def read(self):
        while True:
            command = raw_input('> ').split()

            if command[0] == 'get' and len(command) != 2:
                sys.stderr.write('get requests requires only a key!\n')
                continue

            if command[0] == 'set' and len(command) != 3:
                sys.stderr.write('set requests requires only a key and a value!\n')
                continue

            if command[0] == 'status':
                sys.stderr.write(str(self.status) + '\n')

            if command[0] == 'exit':
                sys.exit(0)

            self.execute(command)

    def update(self, response, address):
        result = decode_bencode(response)
        self.status[address[1]] = result[4]

    def listen(self):
        thread = threading.Thread(target=self.read)
        thread.start()

        while True:
            try:
                self.sock.settimeout(3)
                response, address = self.sock.recvfrom(1024)
                self.update(response, address)

            except socket.timeout:
                pass


if __name__ == '__main__':
    source, targets = parse_arguments()
    sock = bind_socket(BALANCER_IP, source)

    balancer = Balancer(sock, source, targets)
    balancer.listen()
