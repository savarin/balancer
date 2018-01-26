import itertools
import re


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
