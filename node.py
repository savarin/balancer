import os
import sys

from helpers import parse_arguments, bind_socket


SOURCE_IP = os.getenv('BALANCER_IP_ADDRESS')
TARGET_IP = os.getenv('NODE_IP_ADDRESS')


class Node(object):
    def __init__(self, sock, source):
        self.sock = sock
        self.source = source
        self.data = {}
        self.counter = 0

    def listen(self):
        while True:
            try:
                self.sock.settimeout(3)
                message, address = self.sock.recvfrom(1024)
                sys.stderr.write(message + '\n')
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

    node = Node(source, sock)
    node.listen()
