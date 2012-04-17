import logging
from .util import merge_doctype_mappings

log = logging.getLogger(__name__)

def maxindexof(l):
    return len(l) - 1

class PeekableIterator(object):
    """
    >>> list(PeekableIterator(iter('python')))
    ['p', 'y', 't', 'h', 'o', 'n']

    >>> p = PeekableIterator('python')
    >>> pyt = [p.peek(), p.peek(), p.next(), p.peek(), p.next(), p.next()]
    >>> pyt.extend(list(p))
    >>> pyt
    ['p', 'p', 'p', 'y', 'y', 't', 'h', 'o', 'n']

    >>> list(PeekableIterator(iter([])))
    []
    """
    def __init__(self, it):
        self.it = iter(it)
        self.buf = []

    def __iter__(self):
        return self

    def peek(self):
        if len(self.buf) == 0:
            self.buf.append(self.it.next())
        return self.buf[0]

    def next(self):
        if len(self.buf) == 0:
            return self.it.next()
        else:
            return self.buf.pop(0)


class DocumentIterator(object):
    """Iterates through the documents on a superfastmatch server. The order is determined 
    by the `order_by` argument. It should be the name of a metadata field, optionally prefixed
    by a hyphen (-) to indicate a reversal of the natural order. The `chunksize` option is
    available for optimization. It determines how many documents are retrieved from the server
    per request. The `doctype` argument can be used to limit the iteration to the a specific
    range of doctypes.
    """

    def __init__(self, client, order_by, doctype=None, chunksize=100, start_at=None):
        assert hasattr(client, 'documents'), 'The first argument to DocumentIterator() must implement the superfastmatch.client.Client methods.'
        self.client = client
        # response: the most recent response from the server
        self.response = None
        # chunk: a list of documents returned from the server
        self.chunk = None
        # index: the index into `chunk` of the previously-returned document
        self.index = None
        # cursor: the 'next' cursor from the most recent response, stored
        # outside of self.response so it can be faked for the 'start_at' parameter
        self.next_cursor = None if start_at is None else str(start_at)

        self.chunksize = chunksize
        self.doctype = doctype
        self.order_by = order_by

    def __iter__(self):
        return self

    def next(self):
        if self.chunk is None or self.index == maxindexof(self.chunk):
            self.fetch_chunk()
        else:
            self.index += 1
        return self.current()

    def current(self):
        if self.chunk is None or self.index is None:
            return None
        return self.chunk[self.index]

    def fetch_chunk(self):
        if self.next_cursor is None:
            log.debug('Fetching first chunk of size {limit} ordered by {order_by}'.format(
                limit=self.chunksize, order_by=self.order_by))
            response = self.client.documents(doctype=self.doctype,
                                             order_by=self.order_by,
                                             limit=self.chunksize)
            self.accept_response(response)

        else:
            if self.next_cursor == u'':
                raise StopIteration()

            log.debug('Fetching chunk of size {limit} at {next_cursor} ordered by {order_by}'.format(
                limit=self.chunksize, next_cursor=self.next_cursor, order_by=self.order_by))
            response = self.client.documents(doctype=self.doctype, 
                                             page=self.next_cursor, 
                                             order_by=self.order_by,
                                             limit=self.chunksize)
            self.accept_response(response)

    def accept_response(self, response):
        try:
            if response['success'] == False or len(response['rows']) == 0:
                raise StopIteration()
        except KeyError:
            import ipdb; ipdb.set_trace()

        self.response = response
        self.chunk = response['rows']
        self.next_cursor = response['cursors']['next']
        self.index = 0


class FederatedDocumentIterator(object):
    def __init__(self, client_mapping, order_by, doctype=None, chunksize=100, start_at=None):
        self.reverse_order = order_by.startswith('-')
        self.order_by = order_by.lstrip('-')

        self.client_mapping = client_mapping
        self.search_mapping = dict(merge_doctype_mappings(client_mapping))

        self.iterators = [PeekableIterator(DocumentIterator(client, order_by, doctype or cldoctype, chunksize, start_at))
                          for (cldoctype, client) in self.search_mapping.iteritems()]
        self.current = None

    def __iter__(self):
        return self

    def _documents(self):
        documents = []
        empty_iterators = []

        for it in self.iterators:
            try:
                documents.append((it, it.peek()))
            except StopIteration:
                empty_iterators.append(it)

        for it in empty_iterators:
            self.iterators.remove(it)

        return documents

    def next(self):
        if len(self.iterators) == 0:
            raise StopIteration

        documents = list(sorted(self._documents(), key=lambda doc: doc[1][self.order_by], reverse=self.reverse_order))
        if len(documents) == 0:
            raise StopIteration

        (it, doc) = documents[0]
        return it.next()


if __name__ == "__main__":
    import doctest
    doctest.testmod()

