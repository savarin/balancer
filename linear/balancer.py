import datetime
import numpy
import os
import random
import socket
import sys
from datetime import datetime as dt

sys.path.insert(0, "..")
from helpers.connect import parse_arguments, bind_socket, dispatch_status
from helpers.bencode import encode_bencode, decode_bencode


BALANCER_IP = os.getenv('BALANCER_IP_ADDRESS')
NODE_IP = os.getenv('NODE_IP_ADDRESS')


class Balancer(object):
    def __init__(self, sock, source, targets):
        self.sock = sock
        self.source = source
        self.targets = targets
        self.counter = 0
        self.status = {target: 0 for target in self.targets}

    def send(self, message, address, drop_probability=0.2, increment=True):
        if increment:
            self.counter += 1

        if random.random() > drop_probability:
            self.sock.sendto(message, address)

    def broadcast(self):
        payload = ['request', 'status', str(self.counter)]
        message = encode_bencode(payload)

        for target in self.targets:
            address = (NODE_IP, target)
            self.send(message, address, drop_probability=0)

            try:
                self.sock.settimeout(1)
                response, address = self.sock.recvfrom(1024)
                result = decode_bencode(response)
                self.status[target] = result[2]

            except socket.timeout:
                pass

        if not self.status:
            return None

        for key, value in self.status.iteritems():
            sys.stderr.write(str(key) + ': ' + str(value) + '\n')

    def reset(self):
        payload = ['request', 'reset', str(self.counter)]
        message = encode_bencode(payload)

        for target in self.targets:
            address = (NODE_IP, target)
            self.send(message, address, drop_probability=0)

        self.counter = 0
        self.status = {target: 0 for target in self.targets}

    def choose(self, simple=False):
        if simple or not self.status:
            return self.targets[self.counter % len(self.targets)]

        size = len(self.status.values())
        total = numpy.sum([numpy.exp(_) for _ in self.status.values()])

        def scale(x, size, total):
            return (total - numpy.exp(x)) / ((size - 1) * total)

        probabilities = {k: scale(v, size, total) for k, v in self.status.iteritems()}

        threshold = random.random()
        cumulative = 0

        for target, probability in probabilities.iteritems():
            cumulative += probability

            if threshold < cumulative:
                return target

    def execute(self, command):
        if command[0] == 'status':
            if os.getenv('BALANCER_DEBUG'):
                sys.stderr.write(str(dt.now()) + ' INFO status broadcast\n')
            self.broadcast()

        elif command[0] == 'reset':
            if os.getenv('BALANCER_DEBUG'):
                sys.stderr.write(str(dt.now()) + ' WARN database reset\n')
            self.reset()

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
                address = (NODE_IP, target)
                self.send(message, address, increment=increment)

                try:
                    self.sock.settimeout(1)
                    response, address = self.sock.recvfrom(1024)
                    if os.getenv('BALANCER_DEBUG'):
                        dispatch_status(command[0], 'response', 'from', target)
                    result = decode_bencode(response)
                    output = result[6] + '\n' if command[0] == 'get' else 'success!\n'
                    self.status[target] = result[2]

                    if os.getenv('EXAMPLE'):
                        activity = [self.status[target] for target in self.targets]
                        sys.stderr.write(str(activity)[1:-1] + '\n')
                        break
                    sys.stderr.write(output)
                    break

                except socket.timeout:
                    if os.getenv('BALANCER_DEBUG'):
                        sys.stderr.write(str(dt.now()) + ' WARN timeout\n')

                attempt += 1
                if attempt == 3 and not os.getenv('EXAMPLE'):
                    sys.stderr.write('failed!\n')

    def listen(self):
        while True:
            command = raw_input('> ').split()

            if command[0] == 'get' and len(command) != 2:
                sys.stderr.write('get requests requires only a key!\n')
                continue

            if command[0] == 'set' and len(command) != 3:
                sys.stderr.write('set requests requires only a key and a value!\n')
                continue

            if command[0] == 'exit':
                sys.exit(0)

            self.execute(command)

    def scan(self, instruction):
        command = instruction.split()
        self.execute(command)


if __name__ == '__main__':
    source, targets = parse_arguments()
    sock = bind_socket(BALANCER_IP, source)

    balancer = Balancer(sock, source, targets)
    balancer.listen()
