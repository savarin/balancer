import itertools
import re
import socket
import sys
from datetime import datetime as dt, timedelta
from Queue import Queue as queue


class Queue(object):
    def __init__(self, maxsize=100):
        self.maxsize = maxsize
        self.queue = queue(maxsize)
        self.data = {}

    def put(self, key, value):
        if self.queue.full():
            item = self.queue.get()
            self.data.pop(item, None)

        self.queue.put(key)
        self.data[key] = value

    def get(self, key):
        return self.data.get(key, '')

    def status(self, interval=30):
        counter = 0

        for message, timestamp in self.data.values():
            if timestamp > dt.now() - timedelta(seconds=interval):
                counter += 1

        return counter


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


def encode_bencode(element):
    result = ''

    if element is None:
        return result

    elif isinstance(element, int):
        return 'i' + str(element) + 'e'

    elif isinstance(element, str) or isinstance(element, unicode):
        return str(len(element)) + ':' + str(element)

    elif isinstance(element, list):
        return 'l' + ''.join([encode_bencode(item) for item in element]) + 'e'

    elif isinstance(element, dict):
        collection = []
        for pairs in sorted(element.iteritems(), key=lambda (x, y): x):
            for item in pairs:
                collection.append(item)
        return 'd' + ''.join([encode_bencode(item) for item in collection]) + 'e'

    else:
        raise ValueError('Neither int, string, list or dictionary.')


def decode_bencode(string):

    def decode(string):
        digits = [str(item) for item in xrange(10)]

        if string == '':
            return None, ''

        elif string.startswith('i'):
            match = re.match('i(-?\d+)e', string)
            return int(match.group(1)), string[match.span()[1]:]

        elif any([string.startswith(item) for item in digits]):
            match = re.match('(\d+):', string)
            start = match.span()[1]
            end = start + int(match.group(1))
            return string[start:end], string[end:]

        elif string.startswith('l') or string.startswith('d'):
            elements = []
            rest = string[1:]
            while not rest.startswith('e'):
                element, rest = decode(rest)
                elements.append(element)
            rest = rest[1:]
            if string.startswith('l'):
                return elements, rest
            else:
                return {k: v for k, v in itertools.izip(elements[::2], elements[1::2])}, rest

        else:
            raise ValueError('Malformed string.')

    return decode(string)[0]
