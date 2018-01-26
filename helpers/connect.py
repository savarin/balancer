from datetime import datetime as dt
import socket
import sys


def exit_with_stderr(comments):
    sys.stderr.write(comments + '\n')
    sys.exit(1)


def parse_arguments():
    try:
        source = int(sys.argv[1])
        targets = [int(_) for _ in sys.argv[2:]]
    except IndexError:
        exit_with_stderr("please specify a port number!")
    except ValueError:
        exit_with_stderr("please specify integers!")

    return source, targets


def bind_socket(ip_address, port):
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    sock.bind((ip_address, port))
    return sock


def dispatch_status(task, method, direction, location):
    sys.stderr.write('{} INFO {} {} {} {} \n'
        .format(dt.now(), task, method, direction, str(location)))
