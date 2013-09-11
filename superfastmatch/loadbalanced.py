import datetime
import logging
import random

class LoadBalancedClient(object):
    """
    Dispatches calls to one of a list of superfastmatch.Client objects
    based on response times from previous calls.
    """


    def __init__(self, clients):
        self.clients = clients
        self.first_request = True
        self.search_times = [0] * len(self.clients)
        self.last_client = None

    def __repr__(self):
        return u"<LoadBalancedClient(numclients={0})>".format(len(self.clients))

    def new(self, doctype, text, defer=False, *args, **kwargs):
        def _new(client):
            return client.new(doctype, text, defer, *args, **kwargs)
        return self._balanced(_new)

    def add(self, doctype, docid, text, defer=False, *args, **kwargs):
        results = [client.add(doctype, docid, text, defer, *args, **kwargs)
                   for client in self.clients]
        for result in results:
            if result.get('success', False) == False:
                return result
        return results[0]

    def delete(self, doctype, docid, *args, **kwargs):
        results = [client.delete(doctype, docid, *args, **kwargs)
                   for client in self.clients]
        for result in results:
            if result.get('success', False) == False:
                return result
        return results[0]

    def get(self, doctype, docid):
        def _get(client):
            return client.get(doctype, docid)
        return self._balanced(_get)

    def document(self, doctype, docid):
        return self.get(doctype, docid)

    def documents(self, doctype=None, page=None, order_by=None, limit=None):
        def _documents(client):
            return client.documents(doctype, page, order_by, limit)
        return self._balanced(_documents)

    def search(self, text, doctype=None, **kwargs):
        def _search(client):
            return client.search(text, doctype, **kwargs)
        return self._balanced(_search)

    def queue(self):
        """
        Calls the queue method of the last client. The last client is used as
        the bellweather since it is the last server to receive each command.
        If the other servers were under higher load then they may take longer
        to complete. Obviously, the results of this method are only
        heuristicly descriptive of the load-balanced servers. For a holistic
        approach you need to query each individual server.
        """
        return self.clients[-1].queue()
    
    def _balanced(self, f):
        if self.first_request:
            client_index = random.randint(0, len(self.clients) - 1)
            self.first_request = False
        else:
            lowest = min(self.search_times)
            client_index = self.search_times.index(lowest)
        client = self.clients[client_index]
        t1 = datetime.datetime.now()
        result = f(client)
        self.last_client = client
        t2 = datetime.datetime.now()
        dur = t2 - t1
        self.search_times[client_index] = dur.total_seconds()
        logging.debug('Dispatched to {func} on client {clnt} which took {tm}'.format(func=f.__name__, clnt=client_index, tm=self.search_times[client_index]))
        return result
