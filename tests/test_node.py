import os
import random
import sys
import unittest

from helpers.queue import Queue
from helpers.bencode import encode_bencode, decode_bencode
from node import Node


NODE_IP = os.getenv('NODE_IP_ADDRESS')


class TestCase(unittest.TestCase):

    def setUp(self):
        self.source, self.peers = 7000, [8000, 9000]
        self.sock = bind_socket(NODE_IP, self.source)
        self.node = Node(self.sock, self.source, self.peers, timeout=0.1)

    def test_set(self):
        payload = ['request', 'set', 0, 'key', 'a', 'value', 'alpha']
        result = self.node.set(payload, random.choice(self.peers))
        message = decode_bencode(result)
        self.assertEqual(message, ['response', 'set', 0, 'key', 'a', 'value', 'alpha'])

    def test_get(self):
        payload = ['request', 'set', 0, 'key', 'a', 'value', 'alpha']
        result = self.node.set(payload, random.choice(self.peers))
        payload = ['request', 'get', 1, 'key', 'a', 'value', '']
        result = self.node.get(payload, random.choice(self.peers))
        message = decode_bencode(result)
        self.assertEqual(message, ['response', 'get', 1, 'key', 'a', 'value', 'alpha'])
