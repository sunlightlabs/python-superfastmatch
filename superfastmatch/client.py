""" Python library for interacting with SuperFastMatch server.
"""

__author__ = "James Turk (jturk@sunlightfoundation.com)"
__version__ = "0.1.0-dev"
__copyright__ = "Copyright (c) 2011 Sunlight Labs"
__license__ = "BSD"

import logging
import urlparse
import httplib
import requests
import json
from .util import parse_doctype_range


log = logging.getLogger(__name__)


class SuperFastMatchError(Exception):
    """ Exception for SFM API errors """
    def __init__(self, msg, status, expected_status, response, *args, **kwargs):
        super(SuperFastMatchError, self).__init__(msg, *args, **kwargs)
        self.status = status
        self.expected_status = expected_status
        self.response = response


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


    def __init__(self, url='http://127.0.0.1:8080/', parse_response=True,
                 username=None, password=None, timeout=None):
        self.url = url
        if not self.url.endswith('/'):
            self.url += '/'
        self.timeout = timeout
        self.parse_response = parse_response

        auth = (username, password) if (username is not None and password is not None) else None
        self.requests = requests.session(auth=auth)
        self.requests.config['prefetch'] = True
        self.requests.config['keep_alive'] = True

    def __repr__(self):
        return u"<Client(url=%s)>" % (self.url, )

    def _apicall(self, method, path, expected_status, params=None):
        log.debug('_apicall({0}, {1}, ...'.format(method, path))
        params = params or {}
        expected_status = ensure_sequence(expected_status)

        if params:
            for (key, value) in params.iteritems():
                if isinstance(value, unicode):
                    params[key] = value.encode('utf-8')
        url = urlparse.urljoin(self.url, path)
        headers = {
            'Expect': None
        }
        response = self.requests.request(method, url,
                                         params=params if method == 'GET' else None,
                                         data=params if method in ('PUT', 'POST') else None,
                                         timeout=self.timeout,
                                         headers=headers)
        status = response.status_code
        content = response.content
        if status in expected_status:
            if self.parse_response == True:
                if response.headers.get('content-type', 'text/plain').startswith('application/json'):
                    obj = json.loads(content)
                    return obj
                else:
                    raise SuperFastMatchError("No Content-Type header in response",
                                              status, 200, (status, content))
            return content
        else:
            tmpl = "Unexpected HTTP status. Expecting {0!r} but got {1!r} on {2!r}"
            msg = tmpl.format(str(expected_status), status, url)
            raise SuperFastMatchError(msg, status, expected_status, (status, content))


    def add(self, doctype, docid, text, defer=False, **kwargs):
        method = 'POST' if defer else 'PUT'
        kwargs['text'] = text
        return self._apicall(method, 'document/%s/%s/' % (doctype, docid),
                             httplib.ACCEPTED, kwargs)


    def delete(self, doctype, docid):
        return self._apicall('DELETE', 'document/%s/%s/' % (doctype, docid),
                             [httplib.ACCEPTED, httplib.NOT_FOUND])


    def get(self, doctype, docid):
        return self._apicall('GET', 'document/%s/%s/' % (doctype, docid),
                             [httplib.OK, httplib.NOT_FOUND, httplib.NOT_MODIFIED],
                             {})


    def associations(self, doctype=None, page=None):
        url = 'association/'
        if doctype is not None:
            url = '%s%s' % (url, doctype)
        params = {}
        if page is not None:
            params['cursor'] = page
        return self._apicall('GET', url, httplib.OK, params)


    def update_associations(self, doctype=None, doctype2=None, skip_validation=False):
        url = 'associations/'
        if doctype:
            if not skip_validation:
                parse_doctype_range(doctype)
            url = '%s%s/' % (url, doctype)
        if doctype2:
            if not skip_validation:
                parse_doctype_range(doctype2)
            url = '%s%s/' % (url, doctype2)
        return self._apicall('POST', url, httplib.ACCEPTED)


    def document(self, doctype, docid):
        url = 'document/%s/%s/' % (doctype, docid)
        return self._apicall('GET', url, [httplib.OK, httplib.NOT_MODIFIED, httplib.NOT_FOUND])


    def documents(self, doctype=None, page=None, order_by=None, limit=None):
        url = 'document/'
        if doctype is not None:
            url = "%s%s/" % (url, doctype)
        params = {}
        if page is not None:
            params['cursor'] = page
        if order_by is not None:
            params['order_by'] = order_by
        if limit is not None:
            params['limit'] = limit
        return self._apicall('GET', url, httplib.OK, params)


    def search(self, text, doctype=None, **kwargs):
        uuid = kwargs.get('uuid')
        if uuid:
            url = 'search/{uuid}/'.format(uuid=uuid)
            return self._apicall('GET', url, httplib.OK, {})
        else:
            url = 'search/'
            params = kwargs
            if text is not None:
                params['text'] = text
            if doctype:
                url = '%s%s/' % (url, doctype)
                params['doctype'] = str(doctype)

            return self._apicall('POST', url, httplib.OK, params)


    def queue(self):
        return self._apicall('GET', 'queue/', httplib.OK)


