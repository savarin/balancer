import os
import socket
import sys

sys.path.insert(0, "..")
from helpers import parse_arguments, bind_socket, encode_bencode


BALANCER_IP = os.getenv('BALANCER_IP_ADDRESS')


source, target = parse_arguments()
sock = bind_socket(BALANCER_IP, source)


while True:
    command = raw_input('> ').split()
    message = encode_bencode(command)

    address = (BALANCER_IP, target[0])
    sock.sendto(message, address)
