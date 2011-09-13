""" Python library for interacting with SuperFastMatch server.
"""

__author__ = "James Turk (jturk@sunlightfoundation.com)"
__version__ = "0.1.0-dev"
__copyright__ = "Copyright (c) 2011 Sunlight Labs"
__license__ = "BSD"

import urllib
import httplib2

class SuperFastMatchError(Exception):
    """ Exception for SFM API errors """

class Client(object):


    def __init__(self, url='http://127.0.0.1:8080'):
        self.url = url
        self._http = httplib2.Http()


    def _apicall(self, method, path, params=''):
        if params:
            params = urllib.urlencode(params, doseq=True)
        uri = '%s%s' % (self.url, path)
        # copied from Donovan's example, not clear why it is needed
        headers = {'Expect': ''}

        resp, content = self._http.request(uri, method, params, headers)
        return content


    def add(self, doctype, docid, text, defer=False, **kwargs):
        method = 'POST' if defer else 'PUT'
        kwargs['text'] = text
        return self._apicall(method, '/document/%s/%s/' % (doctype, docid), kwargs)


    def delete(self, doctype, docid):
        return self._apicall('DELETE', '/document/%s/%s/' % (doctype, docid))


    def get(self, doctype, docid):
        return self._apicall('GET', '/document/%s/%s/' % (doctype, docid), {})


    def update_associations(self, doctype=None, doctype2=None):
        url = '/association/'
        if doctype:
            url = '%s/%s/' % (url, doctype)
        if doctype2:
            url = '%s/%s/' % (url, doctype2)
        return self._apicall('POST', url)


    def search(self, doctype=None):
        url = '/search/'
        if doctype:
            url = '%s/%s/' % (url, doctype)
        return self._apicall('POST', url)
