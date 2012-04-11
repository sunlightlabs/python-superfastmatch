import logging
from .client import Client

log = logging.getLogger(__name__)

def maxindexof(l):
    return len(l) - 1

class DocumentIterator(object):
    """Iterates through the documents on a superfastmatch server. The order is determined 
    by the `order_by` argument. It should be the name of a metadata field, optionally prefixed
    by a hyphen (-) to indicate a reversal of the natural order. The `chunksize` option is
    available for optimization. It determines how many documents are retrieved from the server
    per request. The `doctype` argument can be used to limit the iteration to the a specific
    range of doctypes.
    """

    def __init__(self, client, order_by, doctype=None, chunksize=100):
        assert isinstance(client, Client), 'The first argument to DocumentIterator() must be an instance of superfastmatch.client.Client.'
        self.client = client
        # response: the most recent response from the server
        self.response = None
        # chunk: a list of documents returned from the server
        self.chunk = None
        # index: the index into `chunk` of the previously-returned document
        self.index = None

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
        if self.response is None:
            log.debug('Fetching first chunk of size {limit} ordered by {order_by}'.format(
                limit=self.chunksize, order_by=self.order_by))
            response = self.client.documents(doctype=self.doctype,
                                             order_by=self.order_by,
                                             limit=self.chunksize)
            self.accept_response(response)

        else:
            next_cursor = self.response['cursors']['next']
            if next_cursor == u'':
                raise StopIteration()

            log.debug('Fetching chunk of size {limit} at {next_cursor} ordered by {order_by}'.format(
                limit=self.chunksize, next_cursor=next_cursor, order_by=self.order_by))
            response = self.client.documents(doctype=self.doctype, 
                                             page=next_cursor, 
                                             order_by=self.order_by,
                                             limit=self.chunksize)
            self.accept_response(response)

    def accept_response(self, response):
        if response['success'] == False or len(response['rows']) == 0:
            raise StopIteration()

        self.response = response
        self.chunk = response['rows']
        self.index = 0



