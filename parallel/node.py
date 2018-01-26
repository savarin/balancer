import os
import socket
import sys
import thread

sys.path.insert(0, "..")
from helpers.bencode import decode_bencode
from helpers.connect import parse_arguments, bind_socket


NODE_IP = os.getenv('NODE_IP_ADDRESS')


source, peers = parse_arguments()
sock = bind_socket(NODE_IP, source)

stack = []


def listen():
    counter = 0

    while True:
        try:
            sock.settimeout(3)
            request, address = sock.recvfrom(1024)

            payload = decode_bencode(request)
            stack.append(payload)

            if payload[0] == 'exit':
                sys.exit(0)

        except socket.timeout:
            sys.stderr.write('.\n')


def execute():
    while True:
        if stack:
            payload = stack.pop()

            if payload[0] == 'exit':
                sys.exit(0)

            print str(payload)


thread.start_new_thread(listen, ())
execute()
