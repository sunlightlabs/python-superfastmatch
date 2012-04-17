# -*- coding: utf-8 -*-

import logging
import gevent
import gevent.pool
from .iterators import FederatedDocumentIterator
from .util import merge_doctype_mappings

class FederatedClient(object):
    """
    Implements a limited client interface to dispatch API calls to autonomous clients.
    Each doctype is mapped to a client object; multiple doctypes can be mapped to the
    same client.

    TODO:
      - Determine whether update_associations() should expose errors when the
        caller tries to associate between servers.
    """

    def __init__(self, client_mapping):
        """
        `client_mapping`: A dict mapping doctype values to Client objects.
        """
        self.client_mapping = client_mapping
        self.search_mapping = dict(merge_doctype_mappings(client_mapping))
        self.pool = gevent.pool.Pool(len(self.search_mapping))

    def client(self, doctype):
        if doctype not in self.client_mapping:
            raise Exception('No server mapped to doctype {doctype}'.format(doctype=doctype))
        return self.client_mapping[doctype]

    def add(self, doctype, docid, text, defer=False, **kwargs):
        return self.client(doctype).add(doctype, docid, text, **kwargs)

    def delete(self, doctype, docid):
        return self.client(doctype).delete(doctype, docid)

    def document(self, doctype, docid):
        return self.client(doctype).document(doctype, docid)

    def documents(self, doctype=None, page=None, order_by=None, limit=None):
        """
        Mimics the `GET /document/` document listing enough to implement enumeration. It does not
        provide first, last, or previous cursors.
        """

        if doctype:
            return self.client(doctype).documents(doctype, page, order_by, limit)
        else:
            dociter = FederatedDocumentIterator(
                client_mapping=self.search_mapping,
                doctype=doctype,
                order_by=order_by,
                start_at=page)
            
            results = {
                'success': True,
                'cursors': {
                    'current': '',
                    'first': '',
                    'last': '',
                    'previous': '',
                    'next': ''
                },
                'rows': [
                ]
            }
            for doc in dociter:
                results['rows'].append(doc)
                if len(results['rows']) >= (limit or 10):
                    break

            try:
                nextdoc = dociter.next()
                results['cursors']['next'] = ('{%s}:{doctype}:{docid}' % (order_by.lstrip('-') or 'doctype')).format(**nextdoc)
            except StopIteration:
                # Leave 'next' cursor as ''
                pass
            return results

    def search(self, text, doctype=None, **kwargs):
        if doctype:
            return self.client(doctype).search(text, doctype, **kwargs)
        else:
            empty_documents_map = {
                'documents': {
                    'metaData': {
                        'fields': []
                    },
                    'rows': []
                }
            }
            combined_response = {}
            successes = []
            procs = []

            for (doctype_rangestr, client) in self.search_mapping.iteritems():
                procs.append(self.pool.spawn(self._request, client, doctype_rangestr, text, **kwargs))
            
            gevent.joinall(procs)

            for response in (p.value for p in procs):
                if response is None or response.get('success') == False:
                    logging.warn("Constituent search failed.")
                    continue

                successes.append(response['success'])
                if response['success'] == True:
                    if not combined_response:
                        combined_response.update(empty_documents_map)

                    combined_documents = combined_response['documents']
                    
                    combined_fields = combined_documents['metaData']['fields']
                    fields = response['documents']['metaData']['fields']
                    for field in fields:
                        if field not in combined_fields:
                            combined_fields.append(field)
                    
                    combined_rows = combined_documents['rows']
                    rows = response['documents']['rows']
                    combined_rows.extend(rows)

            if any(successes):
                combined_response['success'] = True
            else:
                combined_response['success'] = False
                combined_response['error'] = "All dispatched search calls failed."

            return combined_response

    def _request(self, client, doctype_rangestr, text, **kwargs):
        return client.search(text, doctype_rangestr, **kwargs)


if __name__ == "__main__":
    import doctest
    doctest.testmod()
