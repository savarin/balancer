import datetime
import os
import socket
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

    def send(self, message, target):
        self.sock.sendto(message, (TARGET_IP, target))
        self.counter += 1

    def broadcast(self, reset=False):
        task = 'reset' if reset else 'broadcast'
        payload = ['req', task, str(self.counter)]
        message = encode_bencode(payload)

        for target in self.targets:
            self.send(message, target)

    def execute(self, command):
        if command[0] == 'broadcast':
            self.broadcast()
            pass

        elif command[0] == 'reset':
            self.broadcast(reset=True)
            pass

        elif command[0] in ['get', 'set']:
            key = command[1]
            value = '' if command[0] == 'get' else command[2]
            payload = ['req', command[0], self.counter, 'key', key, 'value', value]
            message = encode_bencode(payload)

            if os.getenv('BALANCER_DEBUG'):
                sys.stderr.write(str(datetime.datetime.now()) + ' INFO ' + command[0] + '\n')

            destination = self.targets[self.counter % len(self.targets)]
            self.send(message, destination)

            try:
                self.sock.settimeout(1)
                response, address = self.sock.recvfrom(1024)
                result = decode_bencode(response)
                output = result[6] + '\n' if command[0] == 'get' else 'success!\n'
                sys.stderr.write(output)

            except socket.timeout:
                if os.getenv('BALANCER_DEBUG'):
                    sys.stderr.write(str(datetime.datetime.now()) + ' WARN timeout\n')

    def listen(self):
        while True:
            command = raw_input('> ').split()
            self.execute(command)


if __name__ == '__main__':
    source, targets = parse_arguments()
    sock = bind_socket(SOURCE_IP, source)

    balancer = Balancer(sock, source, targets)
    balancer.listen()
