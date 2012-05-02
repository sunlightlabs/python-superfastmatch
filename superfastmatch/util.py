import itertools
import stream
from collections import defaultdict

def parse_doctype_range(rangestr):
    """Return a list of the doctypes in the range specified expanded as a list 
    of integers. This is used to validate arguments. The actual range strings
    are passed onto the superfastmatch server.

    >>> parse_doctype_range('1-2:5:7-9')
    [1, 2, 5, 7, 8, 9]
    >>> parse_doctype_range('')
    >>> parse_doctype_range('1')
    [1]
    >>> parse_doctype_range('7-7')
    [7]
    """
    if not rangestr:
        raise Exception('Invalid doctype range ({0})'.format(rangestr))

    split_on_hyphen = lambda s: s.split('-')

    def expand(rng):
        if len(rng) == 1:
            return int(rng[0])
        elif len(rng) == 2:
            return range(int(rng[0]), int(rng[1]) + 1)
        else:
            raise Exception('Unrecognized range data type')

    return (stream.Stream(rangestr.split(':'))
            >> stream.map(split_on_hyphen)
            >> stream.map(expand)
            >> stream.flatten
            >> list)


def merge_doctype_mappings(mapping):
    """
    >>> sorted(merge_doctype_mappings({1: 'a', 2: 'b', 3: 'a'}))
    [('1:3', 'a'), ('2', 'b')]
    >>> sorted(merge_doctype_mappings({10: 'z', 11: 'y'}))
    [('10', 'z'), ('11', 'y')]
    """
    inverse_mapping = defaultdict(list)
    for (doctype, client) in mapping.iteritems():
        inverse_mapping[client].append(doctype)

    merged_mapping = [(':'.join([str(d) for d in doctypes]), client)
                      for (client, doctypes) in inverse_mapping.iteritems()]

    return merged_mapping


class PushBackIterator(object):
    def __init__(self, subiter):
        self.subiter = iter(subiter)

    def __iter__(self):
        return self

    def next(self):
        return self.subiter.next()

    def pushback(self, obj):
        self.subiter = itertools.chain([obj], self.subiter)


class ChunkedIterator(object):
    """
    >>> it = ChunkedIterator(range(0, 6), chunksize=2, key=lambda obj: 1)
    >>> [list(chunk) for chunk in it]
    [[0, 1], [2, 3], [4, 5]]
    >>> it = ChunkedIterator(range(0, 5), chunksize=3, key=lambda obj: 1)
    >>> [list(chunk) for chunk in it]
    [[0, 1, 2], [3, 4]]
    >>> it = ChunkedIterator(range(0, 7), chunksize=3, key=lambda obj: obj)
    >>> [list(chunk) for chunk in it]
    [[0, 1, 2], [3], [4], [5], [6]]
    >>> it = ChunkedIterator([], chunksize=2, key=lambda obj: 1)
    >>> [list(chunk) for chunk in it]
    []
    """
    def __init__(self, subiter, chunksize, key):
        self.subiter = PushBackIterator(iter(subiter))
        self.chunksize = chunksize
        self.key = key

    def __iter__(self):
        return self

    def next(self):
        chunk = []
        current_size = 0
        try:
            while current_size < self.chunksize:
                obj = self.subiter.next()
                obj_size = self.key(obj)
                new_size = current_size + obj_size
                if new_size > self.chunksize and len(chunk) > 0:
                    self.subiter.pushback(obj)
                    return chunk
                chunk.append(obj)
                current_size = new_size
        except StopIteration:
            if len(chunk) == 0:
                raise

        return chunk


class UnpicklerIterator(object):
    def __init__(self, unpickler):
        self.unpickler = unpickler

    def __iter__(self):
        return self

    def next(self):
        try:
            return self.unpickler.load()
        except EOFError:
            raise StopIteration
