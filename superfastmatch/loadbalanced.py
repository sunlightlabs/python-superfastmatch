import datetime

class LoadBalancedClient(object):
    """
    Dispatches calls to one of a list of superfastmatch.Client objects
    based on response times from previous calls.
    """


    def __init__(self, clients):
        self.clients = clients
        self.search_times = [0] * len(self.clients)
   
    def __repr__(self):
        return u"<LoadBalancedClient(numclients={0})>".format(len(self.clients))

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
        client = self._choose_client()
        return client.get(doctype, docid)

    def document(self, doctype, docid):
        return self.get(doctype, docid)

    def documents(self, doctype=None, page=None, order_by=None, limit=None):
        client = self._choose_client()
        return client.documents(doctype, page, order_by, limit)

    def search(self, text, doctype=None, **kwargs):
        client = self._choose_client()
        t1 = datetime.datetime.now()
        response = client.search(text, doctype, **kwargs)
        t2 = datetime.datetime.now()
        dur = t2 - t1
        client_index = self.clients.index(client)
        self.search_times[client_index] = dur.total_seconds()
        return response

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
    
    def _choose_client(self):
        lowest = min(self.search_times)
        client_index = self.search_times.index(lowest)
        return self.clients[client_index]
