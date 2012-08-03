import logging
from copy import deepcopy
from .util import merge_doctype_mappings
from .client import SuperFastMatchError


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

    def __init__(self, client, order_by, doctype=None, chunksize=100, start_at=None, fetch_text=False):
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
        self.fetch_text = fetch_text

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
        docmeta = self.chunk[self.index]
        if self.fetch_text == False:
            return docmeta
        
        docresponse = self.client.document(docmeta['doctype'], docmeta['docid'])
        if docresponse['success'] == False:
            raise SuperFastMatchError('Unable to fetch document ({doctype}, {docid}).'.format(**doc))
        
        # This copies the docmeta dict and then inserts the fetched text to avoid 
        # keeping a reference to the text in the chunk buffer.
        newdoc = deepcopy(docmeta)
        newdoc['text'] = docresponse['text']
        return newdoc

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


class FaultTolerantDocumentIterator(DocumentIterator):
    """
    Occasionally a DocumentIterator will fail because superfastmatch returns invalid JSON.
    This class will, if the order_by field is numeric, attempt to continue past the document
    yielding invalid JSON by incrementing the cursor value and retrying the fetch operation.
    This is intended for testing and debugging purposes, to be used with order_by=docid.
    """
    def __init__(self, client, order_by, doctype=None, chunksize=100, start_at=None, fetch_text=False):
        super(FaultTolerantDocumentIterator, self).__init__(client, order_by, doctype=doctype, chunksize=chunksize, start_at=start_at, fetch_text=fetch_text)
        self.in_fault = False
        self.original_chunksize = chunksize
        self.inaccessible_documents = []

    def fetch_chunk(self):
        while self.next_cursor != u'':
            try:
                super(FaultTolerantDocumentIterator, self).fetch_chunk()
                self.in_fault = False
                self.chunksize = self.original_chunksize
                return
            except Exception as e:
                parts = self.next_cursor.split(':')
                if len(parts) != 3:
                    raise Exception("Cannot tolerate {0} fault because the next cursor ({1!r}) is not recognized.".format(type(e), self.next_cursor))
                if not parts[2].isdigit():
                    raise Exception("Cannot tolerate {0} fault because the order_by field ({1}) is not numeric.".format(type(e), self.order_by))

                if self.in_fault:
                    # Since self.in_fault is True only after we set
                    # self.chunksize to 1, we can be sure the document
                    # requested is causing the JSON parsing error.
                    logging.debug("Inaccessible document found ({0}, {1}).".format(int(parts[1]), int(parts[2])))
                    self.inaccessible_documents.append((parts[1], parts[2]))
                    parts[2] = str(int(parts[2]) + 1)
                    self.next_cursor = ':'.join(parts)
                else:
                    # We don't know which of the documents requested caused
                    # the JSON parsing error so we first set the chunksize
                    # to 1 and try again.
                    logging.debug("Document iteration fault when requesting {0} documents beginning at cursor {1}".format(self.chunksize, self.next_cursor))
                    self.chunksize = 1
                    self.in_fault = True


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

