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


NODE_IP = os.getenv('NODE_IP_ADDRESS')


class Node(object):
    def __init__(self, sock, source, peers):
        self.sock = sock
        self.source = source
        self.peers = peers
        self.target = ()
        self.data = {}
        self.ingress = Queue(maxsize=100)
        self.egress = HashQueue(maxsize=100)
        self.outcome = HashQueue(maxsize=100)
        self.identifier = 0
        self.end = False

    def deliver(self, message, address, drop_probability=0.0, identifier=None):
        # use 'is not None' so check works for identifier = 0
        if identifier is not None:
            self.egress.put(identifier, (message, dt.now()))

        if random.random() > drop_probability:
            self.sock.sendto(message, address)

    def replay(self, payload, address):
        message = self.egress.get(payload[2])[0]
        self.deliver(message, address)
        dispatch_status(payload[1], 're-response', 'to', address[1])

    def heartbeat(self, address):
        result = ['response', 'status', self.egress.status()]
        message = encode_bencode(result)

        self.deliver(message, address, drop_probability=0)

    def status(self, address):
        if not self.data:
            sys.stderr.write(str(dt.now()) + ' WARN database empty\n')
            return None

        sys.stderr.write(str(dt.now()) + ' INFO contents start\n')
        for key, value in self.data.iteritems():
            sys.stderr.write(key + ': ' + value + '\n')
        sys.stderr.write(str(dt.now()) + ' INFO contents end\n')

        self.heartbeat(address)
        dispatch_status('status', 'response', 'to', address[1])

    def reset(self):
        sys.stderr.write(str(dt.now()) + ' WARN system reset\n')
        self.data = {}

    def exit(self):
        sys.stderr.write(str(dt.now()) + ' WARN system reset\n')
        self.end = True
        sys.exit(0)

    def transmit(self, message, address):
        self.identifier += 1
        self.sock.sendto(message, address)

    def relay(self, task, key, value=''):
        payload = ['relay-request', task, str(self.identifier), 'key', key,
                   'value', value]
        message = encode_bencode(payload)

        for peer in self.peers:
            address = (NODE_IP, peer)
            self.transmit(message, address)
            dispatch_status(task, 'relay-request', 'to', peer)

        time.sleep(0.01)

        if key in self.outcome.data:
            return self.outcome.get(key)

        return ''

    def get(self, payload, address):
        message = ''

        if payload[0] == 'request':
            value = self.data.get(payload[4], '')

            if not value:
                value = self.relay(payload[1], payload[4])

            result = ['response', 'get', payload[2], 'key', payload[4],
                      'value', value, 'status', self.egress.status()]
            message = encode_bencode(result)

            self.deliver(message, address, identifier=payload[2])
            dispatch_status('get', 'response', 'to', address[1])

        elif payload[0] == 'relay-request':
            value = self.data.get(payload[4], '')

            result = ['relay-response', 'get', self.identifier, 'key', payload[4],
                      'value', value]
            message = encode_bencode(result)

            self.transmit(message, address)
            dispatch_status('get', 'relay-response', 'to', address[1])

    def set(self, payload, address):
        message = ''

        if payload[0] == 'request':
            value = self.relay(payload[1], payload[4], payload[6])

            if not value:
                self.data[payload[4]] = payload[6]

            result = ['response', 'set', payload[2], 'key', payload[4],
                      'value', payload[6], 'status', self.egress.status()]
            message = encode_bencode(result)

            self.deliver(message, address, identifier=payload[2])
            dispatch_status('set', 'response', 'to', address[1])

        elif payload[0] == 'relay-request':
            value = ''

            if payload[4] in self.data:
                self.data[payload[4]] = payload[6]
                value = payload[6]

            result = ['relay-response', 'set', self.identifier, 'key', payload[4],
                      'value', value]
            message = encode_bencode(result)

            self.transmit(message, address)
            dispatch_status('set', 'relay-response', 'to', address[1])

    def process(self, payload, address):
        if payload[0] == 'request':
            self.target = address

        elif payload[0] == 'relay-response':
            dispatch_status(payload[1], 'relay-response', 'from', address[1])

            if payload[6]:
                self.outcome.put(payload[4], payload[6])

            return None

        self.ingress.put((payload, address))

    def listen(self):
        counter = 0

        while True:
            counter += 1

            try:
                sock.settimeout(1)
                request, address = sock.recvfrom(1024)
                payload = decode_bencode(request)

                self.process(payload, address)

            except socket.timeout:
                if not counter % 3:
                    sys.stderr.write('.\n')

                    if self.target:
                        self.heartbeat(self.target)

            if self.end:
                thread.exit()

    def execute(self):
        while True:
            if not self.ingress.empty():
                payload, address = self.ingress.get()

                if payload[0][:5] != 'relay' and self.egress.get(payload[2]):
                    dispatch_status(payload[1], 're-request', 'from', address[1])
                    self.replay(payload, address)
                    continue

                if payload[1] == 'status':
                    dispatch_status('status', 'request', 'from', address[1])
                    self.status(address)

                elif payload[1] == 'reset':
                    dispatch_status('reset', 'request', 'from', address[1])
                    self.reset()

                elif payload[1] == 'exit':
                    dispatch_status('exit', 'request', 'from', address[1])
                    self.exit()

                elif payload[1] == 'get':
                    dispatch_status('get', payload[0], 'from', address[1])
                    self.get(payload, address)

                elif payload[1] == 'set':
                    dispatch_status('set', payload[0], 'from', address[1])
                    self.set(payload, address)


if __name__ == '__main__':
    source, peers = parse_arguments()
    sock = bind_socket(NODE_IP, source)

    node = Node(sock, source, peers)
    thread.start_new_thread(node.listen, ())
    node.execute()
