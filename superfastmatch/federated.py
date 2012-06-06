# -*- coding: utf-8 -*-

import logging
import gevent
import gevent.pool
from copy import deepcopy
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
        """ `search_mapping`: maps doctype range strings (e.g. 1:2:7) to client objects."""
        self.search_mapping = dict(merge_doctype_mappings(client_mapping))
        self.pool = gevent.pool.Pool(len(self.search_mapping))

    def clients(self):
        return deepcopy(self.search_mapping)

    def client(self, doctype):
        try:
            doctype = int(doctype)
            if doctype not in self.client_mapping:
                raise Exception('No server mapped to doctype {doctype!r}. Mapped doctypes: {doctypes!r}'.format(doctype=doctype,
                                                                                                                doctypes=self.client_mapping.keys()))
            return self.client_mapping[doctype]
        except ValueError:
            if doctype not in self.search_mapping:
                raise Exception('No server mapped to doctype range {range!r}. Mapped ranged: {ranges!r}'.format(range=doctype,
                                                                                                                ranges=self.search_mapping.keys()))
            return self.search_mapping[doctype]

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
                'cursors': { 'current': '', 'first': '', 'last': '', 'previous': '', 'next': '' },
                'rows': []
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

                    if 'uuid' in response and 'uuid' not in combined_response:
                        combined_response['uuid'] = response['uuid']
                    if 'text' in response and 'text' not in combined_response:
                        combined_response['text'] = response['text']

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
