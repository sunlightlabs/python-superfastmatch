""" Python library for interacting with SuperFastMatch server.
"""

__author__ = "James Turk (jturk@sunlightfoundation.com)"
__version__ = "0.1.0-dev"
__copyright__ = "Copyright (c) 2011 Sunlight Labs"
__license__ = "BSD"

import urllib
import urlparse
import httplib
import httplib2
import json


class SuperFastMatchError(Exception):
    """ Exception for SFM API errors """


def ensure_sequence(arg):
    if hasattr(arg, 'strip'):
        return [arg]
    if hasattr(arg, '__getitem__'):
        return arg
    elif hasattr(arg, '__iter__'):
        return list(arg)
    else:
        return [arg]


class Client(object):


    def __init__(self, url='http://127.0.0.1:8080', parse_response=True,
                 username=None, password=None):
        self.url = url
        self._http = httplib2.Http()
        if username is not None and password is not None:
            self._http.add_credentials(username, password)
        self.parse_response = parse_response



    def _apicall(self, method, path, expected_status, params=''):
        expected_status = ensure_sequence(expected_status)

        if params:
            params = urllib.urlencode(params, doseq=True)
        uri = urlparse.urljoin(self.url, path)
        # copied from Donovan's example, not clear why it is needed
        headers = {'Expect': ''}

        resp, content = self._http.request(uri, method, params, headers)
        status = int(resp['status'])
        if status in expected_status:
            if self.parse_response == True:
                if resp['content-type'] in 'application/json':
                    obj = json.loads(content)
                    return obj
            return content
        else:
            tmpl = "Unexpected HTTP status. Expecting {0!r} but got {1!r} on {2!r}"
            msg = tmpl.format(str(expected_status), status, uri)
            raise SuperFastMatchError(msg)


    def add(self, doctype, docid, text, defer=False, **kwargs):
        method = 'POST' if defer else 'PUT'
        kwargs['text'] = text
        return self._apicall(method, '/document/%s/%s/' % (doctype, docid),
                             httplib.ACCEPTED, kwargs)


    def delete(self, doctype, docid):
        return self._apicall('DELETE', '/document/%s/%s/' % (doctype, docid),
                             [httplib.ACCEPTED, httplib.NOT_FOUND])


    def get(self, doctype, docid):
        return self._apicall('GET', '/document/%s/%s/' % (doctype, docid),
                             [httplib.OK, httplib.NOT_FOUND, httplib.NOT_MODIFIED],
                             {})


    def associations(self, doctype=None, page=None):
        url = '/association/'
        if doctype is not None:
            url = '%s%s' % (url, doctype)
        params = {}
        if page is not None:
            params['cursor'] = page
        return self._apicall('GET', url, httplib.OK, params)


    def update_associations(self, doctype=None, doctype2=None):
        url = '/association/'
        if doctype:
            url = '%s/%s/' % (url, doctype)
        if doctype2:
            url = '%s/%s/' % (url, doctype2)
        return self._apicall('POST', url, httplib.ACCEPTED)


    def document(self, doctype, docid):
        url = '/document/%s/%s' % (doctype, docid)
        return self._apicall('GET', url, [httplib.OK, httplib.NOT_MODIFIED, httplib.NOT_FOUND])


    def documents(self, doctype=None, page=None):
        url = '/document/'
        if doctype is not None:
            url = "%s%s" % (url, doctype)
        params = {}
        if page is not None:
            params['cursor'] = page
        return self._apicall('GET', url, httplib.OK, params)


    def search(self, text, doctype=None, **kwargs):
        url = '/search/'
        params = kwargs
        if text is not None:
            params['text'] = text
        if doctype:
            url = '%s/%s/' % (url, doctype)
            params['doctype'] = str(doctype)

        return self._apicall('POST', url, httplib.OK, params)
