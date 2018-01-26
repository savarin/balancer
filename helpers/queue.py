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
