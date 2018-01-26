import os
import socket
import sys
import thread
from datetime import datetime as dt

sys.path.insert(0, "..")
from helpers.bencode import encode_bencode
from helpers.connect import parse_arguments, bind_socket


BALANCER_IP = os.getenv('BALANCER_IP_ADDRESS')
NODE_IP = os.getenv('NODE_IP_ADDRESS')


class Balancer(object):
    def __init__(self, sock, source, targets):
        self.sock = sock
        self.source = source
        self.targets = targets
        self.counter = 0
        self.exit = False

    def execute(self, command):
        sys.stderr.write(str(command) + '\n')

    def listen(self):
        while True:
            try:
                sock.settimeout(3)
                response, address = sock.recvfrom(1024)
                payload = decode_bencode(request)

            except socket.timeout:
                pass

            if self.exit:
                thread.exit()

            self.counter += 1

    def read(self):
        while True:
            command = raw_input('> ').split()

            if command[0] == 'status':
                sys.stderr.write(str(self.counter) + '\n')

            elif command[0] == 'exit':
                if os.getenv('BALANCER_DEBUG'):
                    sys.stderr.write(str(dt.now()) + ' WARN system exit\n')

                self.exit = True
                sys.exit(0)

            self.execute(command)


if __name__ == '__main__':
    source, targets = parse_arguments()
    sock = bind_socket(NODE_IP, source)

    balancer = Balancer(sock, source, targets)
    thread.start_new_thread(balancer.listen, ())

    balancer.read()
