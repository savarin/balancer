import datetime
import os
import socket
import sys

from helpers import parse_arguments, bind_socket, encode_bencode, decode_bencode


SOURCE_IP = os.getenv('BALANCER_IP_ADDRESS')
TARGET_IP = os.getenv('NODE_IP_ADDRESS')


class Node(object):
    def __init__(self, sock, source):
        self.sock = sock
        self.source = source
        self.data = {}
        self.counter = 0

    def compose(self, task, key, value=None):
        payload = ['req', task, self.counter, 'key', key, 'value', value]
        return encode_bencode(payload)

    def send(self, message, target):
        self.sock.sendto(message, (TARGET_IP, target))
        self.counter += 1

    def execute(self, message, target):
        payload = decode_bencode(message)

        if payload[1] == 'broadcast':
            if not self.data:
                sys.stderr.write(str(datetime.datetime.now()) + ' WARN database empty\n')
                pass

            for key, value in self.data.iteritems():
                sys.stderr.write(key + ': ' + value + '\n')

    def listen(self):
        while True:
            try:
                self.sock.settimeout(3)
                request, address = self.sock.recvfrom(1024)
                self.execute(request, address[1])
            except socket.timeout:
                sys.stderr.write('.\n')


def parse_arguments():
    try:
        port = int(sys.argv[1])
    except IndexError:
        exit_with_stderr("please specify a port number!")
    except ValueError:
        exit_with_stderr("please specify an integer!")

    return port


if __name__ == '__main__':
    source = parse_arguments()
    sock = bind_socket(SOURCE_IP, source)

    node = Node(sock, source)
    node.listen()
