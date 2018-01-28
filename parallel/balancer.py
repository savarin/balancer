import os
import random
import socket
import sys
import thread
import time
from datetime import datetime as dt
from Queue import Queue

sys.path.insert(0, "..")
from helpers.bencode import encode_bencode, decode_bencode
from helpers.connect import parse_arguments, bind_socket, dispatch_status
from helpers.hashqueue import HashQueue


BALANCER_IP = os.getenv('BALANCER_IP_ADDRESS')
NODE_IP = os.getenv('NODE_IP_ADDRESS')


class Balancer(object):
    def __init__(self, sock, source, targets):
        self.sock = sock
        self.source = source
        self.targets = targets
        self.activity = {}
        self.ingress = Queue(maxsize=100)
        self.collection = HashQueue(maxsize=100)
        self.output = Queue(maxsize=100)
        self.identifier = 0
        self.end = False

    def deliver(self, message, address, drop_probability=0.0, identifier=None):
        # use is not None so check works for identifier = 0
        if identifier is not None:
            self.identifier += 1

        if random.random() > drop_probability:
            self.sock.sendto(message, address)

    def broadcast(self, message):
        for target in self.targets:
            address = (NODE_IP, target)
            self.deliver(message, address, drop_probability=0)

    def status(self):
        payload = ['request', 'status', str(self.identifier)]
        message = encode_bencode(payload)
        self.broadcast(message)

        time.sleep(0.1)
        if not self.activity:
            return None

        for key, value in self.activity.iteritems():
            sys.stderr.write(str(key) + ': ' + str(value) + '\n')

    def reset(self):
        payload = ['request', 'reset', str(self.identifier)]
        message = encode_bencode(payload)
        self.broadcast(message)

        self.identifier = 0
        self.activity = {target: 0 for target in self.targets}

    def exit(self):
        payload = ['request', 'exit', str(self.identifier)]
        message = encode_bencode(payload)
        self.broadcast(message)

        self.end = True
        sys.exit(0)

    def choose(self):
        return self.targets[self.identifier % len(self.targets)]

    def send(self, message, address, identifier):
        for i in xrange(3):
            if os.getenv('BALANCER_DEBUG'):
                sys.stderr.write(str(dt.now()) + ' INFO attempt ' + str(i + 1) + ' of 3\n')

            self.deliver(message, address, identifier=i if i == 0 else None)
            time.sleep(0.1)

            if identifier in self.collection.data:
                return 'success!'

        return 'failed!'

    def get(self, command, send=True):
        target = self.choose()
        if os.getenv('BALANCER_DEBUG'):
            dispatch_status('get', 'request', 'to', target)

        payload = ['request', 'get', self.identifier, 'key', command[1]]
        message = encode_bencode(payload)
        address = (NODE_IP, target)

        if send:
            result = self.send(message, address, identifier=payload[2])
            sys.stderr.write(result + '\n')

        return message

    def set(self, command, send=True):
        target = self.choose()
        if os.getenv('BALANCER_DEBUG'):
            dispatch_status('set', 'request', 'to', target)

        payload = ['request', 'set', self.identifier, 'key', command[1],
                   'value', command[2]]
        message = encode_bencode(payload)
        address = (NODE_IP, target)

        if send:
            result = self.send(message, address, identifier=payload[2])
            sys.stderr.write(result + '\n')

        return message

    def flush(self):
        while not self.output.empty():
            text = self.output.get()
            sys.stderr.write(text + '\n')

    def execute(self, command):
        if command[0] == 'status':
            if os.getenv('BALANCER_DEBUG'):
                sys.stderr.write(str(dt.now()) + ' INFO system status\n')
            self.status()

        elif command[0] == 'reset':
            if os.getenv('BALANCER_DEBUG'):
                sys.stderr.write(str(dt.now()) + ' WARN system reset\n')
            self.reset()

        elif command[0] == 'exit':
            if os.getenv('BALANCER_DEBUG'):
                sys.stderr.write(str(dt.now()) + ' WARN system exit\n')
            self.exit()

        elif command[0] == 'get':
            self.get(command)

        elif command[0] == 'set':
            self.set(command)

        elif command[0] == 'flush':
            self.flush()

    def process(self):
        while True:
            if not self.ingress.empty():
                response, address = self.ingress.get()
                result = decode_bencode(response)

                if result[1] == 'status':
                    self.activity[address[1]] = result[2]

                elif result[1] in ['get', 'set']:
                    self.collection.put(result[2], (result[4], result[6]))
                    self.activity[address[1]] = result[8]

                    if result[1] == 'get' and result[6]:
                        self.output.put(result[4] + ': ' + result[6])

                    elif result[1] == 'set':
                        pass

            if self.end:
                thread.exit()

    def listen(self):
        while True:
            try:
                sock.settimeout(3)
                response, address = sock.recvfrom(1024)
                self.ingress.put((response, address))

            except socket.timeout:
                pass

            if self.end:
                thread.exit()

    def read(self):
        while True:
            command = raw_input('> ').split()

            if command[0] == 'get' and len(command) != 2:
                sys.stderr.write('get requests requires only a key!\n')

            elif command[0] == 'set' and len(command) != 3:
                sys.stderr.write('get requests requires only a key and a value!\n')

            self.execute(command)


if __name__ == '__main__':
    source, targets = parse_arguments()
    sock = bind_socket(NODE_IP, source)

    balancer = Balancer(sock, source, targets)
    thread.start_new_thread(balancer.listen, ())
    thread.start_new_thread(balancer.process, ())

    balancer.read()
