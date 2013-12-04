import itertools
import stream
from copy import deepcopy
from collections import defaultdict

def eliminate_overlap(ranges):
    def merged(a, b):
        (t, u) = a
        (v, w) = b
        return (min(t, v), max(u, w))

    def compare_bounds(a, b):
        (t, u) = a
        (v, w) = b
        if t == v:
            return w - u
        else:
            return t - v

    def subsumes(a, b):
        (t, u) = a
        (v, w) = b
        return t <= v and w <= u

    def overlaps(a, b):
        (t, u) = a
        (v, w) = b
        return t <= v < u

    old_ranges = deepcopy(ranges)
    old_ranges.sort(cmp=compare_bounds)
    new_ranges = []

    while len(old_ranges) > 0:
        a = old_ranges.pop(0)
        if len(old_ranges) == 0:
            new_ranges.append(a)
        else:
            while len(old_ranges) > 0:
                b = old_ranges.pop(0)

                if subsumes(a, b):
                    pass # ignore b
                elif subsumes(b, a):
                    a = deepcopy(b)
                elif overlaps(a, b):
                    a = merged(a, b)
                else:
                    old_ranges = [b] + old_ranges
                    break
            new_ranges.append(a)
    return new_ranges

class SparseRange(object):
    def __init__(self, ranges):
        self.ranges = eliminate_overlap(ranges)
        self.len = sum([b - a + 1 for (a, b) in self.ranges])
        self.min = min((a for (a, b) in self.ranges))
        self.max = max((b for (a, b) in self.ranges))

    def __len__(self):
        return self.len

    def __contains__(self, x):
        for (a, b) in self.ranges:
            if a <= x <= b:
                return True
        return False

    def __iter__(self):
        return ((x for (a, b) in self.ranges for x in xrange(a, b + 1)))

    def __unicode__(self):
        return "SparseRange({0.min}.../...{0.max})".format(self)

def parse_docid_range(rangestr):
    """
    Converts a string of the form n-m,i,j,x-y to a function that determines
    whether it's argument is in the range described.

    The approach and implementation differ from parse_doctype_range due to
    the difference in size of the described ranges. Returning a list of all
    values described for docids would be wasteful of memory.

    Returns a function of type str -> bool
    """

    range_strings = rangestr.split(',')
    if len(range_strings) == 0:
        raise Exception('Empty docid range: {0}'.format(rangestr))

    ranges = []
    for rng_str in range_strings:
        if rng_str.isdigit():
            ranges.append((int(rng_str), int(rng_str)))
        elif '-' in rng_str:
            (a, b) = rng_str.split('-')
            ranges.append((int(a), int(b)))
        else:
            raise Exception('Unrecognized docid range data type: {0}'.format(rng_str))

    return SparseRange(ranges)


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
