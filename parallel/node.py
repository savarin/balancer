import os
import socket
import sys
import thread
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
        self.data = {}
        self.ingress = Queue(maxsize=100)
        self.egress = HashQueue(maxsize=100)
        self.end = False

    def status(self):
        if not self.data:
            sys.stderr.write(str(dt.now()) + ' WARN database empty\n')
            return None

        sys.stderr.write(str(dt.now()) + ' INFO contents start\n')
        for key, value in self.data.iteritems():
            sys.stderr.write(key + ': ' + value + '\n')

        sys.stderr.write(str(dt.now()) + ' INFO contents end\n')

    def reset(self):
        sys.stderr.write(str(dt.now()) + ' WARN system reset\n')
        self.data = {}

    def exit(self):
        sys.stderr.write(str(dt.now()) + ' WARN system reset\n')
        self.end = True
        sys.exit(0)

    def deliver(self, message, address, identifier):
        if identifier is not None:
            self.egress.put(identifier, (message, dt.now()))

        self.sock.sendto(message, address)

    def get(self, payload, address):
        value = self.data.get(payload[4], '')
        result = ['response', 'get', self.egress.status(), 'key', payload[4], 'value', value]

        message = encode_bencode(result)
        self.deliver(message, address, payload[2])

        dispatch_status('get', 'response', 'to', address[1])

    def set(self, payload, address):
        self.data[payload[4]] = payload[6]
        result = ['response', 'set', self.egress.status(), 'key', payload[4], 'value', payload[6]]

        message = encode_bencode(result)
        self.deliver(message, address, payload[2])

        dispatch_status('set', 'response', 'to', address[1])

    def listen(self):
        while True:
            try:
                sock.settimeout(3)
                request, address = sock.recvfrom(1024)

                payload = decode_bencode(request)
                self.ingress.put((payload, address))

            except socket.timeout:
                sys.stderr.write('.\n')

            if self.end:
                thread.exit()

    def execute(self):
        while True:
            if not self.ingress.empty():
                payload, address = self.ingress.get()

                if payload[1] == 'status':
                    dispatch_status('status', 'request', 'from', address[1])
                    self.status()

                elif payload[1] == 'reset':
                    dispatch_status('reset', 'request', 'from', address[1])
                    self.reset()

                elif payload[1] == 'exit':
                    dispatch_status('exit', 'request', 'from', address[1])
                    self.exit()

                elif payload[1] == 'get':
                    dispatch_status('get', 'request', 'from', address[1])
                    self.get(payload, address)

                elif payload[1] == 'set':
                    dispatch_status('set', 'request', 'from', address[1])
                    self.set(payload, address)


if __name__ == '__main__':
    source, peers = parse_arguments()
    sock = bind_socket(NODE_IP, source)

    node = Node(sock, source, peers)
    thread.start_new_thread(node.listen, ())
    node.execute()
