# -*- coding: utf-8 -*-

import logging
from collections import defaultdict
import gevent
import gevent.pool

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

class FederatedClient(object):
    """
    Implements a limited client interface to dispatch API calls to autonomous clients.
    Each doctype is mapped to a client object; multiple doctypes can be mapped to the
    same client.

    TODO:
      - Implement a DocumentIterator over this client that maintains inter-server 
        paging order.
      - Implement add() and delete()
      - Determine whether update_associations() should expose errors when the
        caller tries to associate between servers.
    """

    def __init__(self, client_mapping):
        """
        `client_mapping`: A dict mapping a doctype values to Client objects.
        """
        self.client_mapping = client_mapping
        self.search_mapping = dict(merge_doctype_mappings(client_mapping))
        self.pool = gevent.pool.Pool(len(self.search_mapping))

    def client(self, doctype):
        return self.client_mapping[doctype]

    def document(self, doctype, docid):
        client = self.client_mapping[doctype]
        return client.document(doctype, docid)

    def search(self, text, doctype=None, **kwargs):
        if doctype:
            client = self.client_mapping[doctype]
            return client.search(text, doctype, **kwargs)
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
