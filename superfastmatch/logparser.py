import logging
import dateutil.tz
import dateutil.parser
from parcon import Regex, Literal, SignificantLiteral, Translate, ParseException


class DataRecord(object):
    def __init__(self, **kwargs):
        for (k, v) in kwargs.items():
            setattr(self, k, v)
    def __repr__(self):
        return u"{clsname}({items!r})".format(clsname=self.__class__.__name__, items=sorted(self.__dict__.items()))

class LineRecord(DataRecord):
    @staticmethod
    def create((timestamp, event)):
        return LineRecord(timestamp=dateutil.parser.parse(timestamp), event=event)

class HTTPRecord(DataRecord):
    @staticmethod
    def create((host, port, method, uri, version, status)):
        constructors = { 'GET': GETRecord, 'POST': POSTRecord, 'PUT': PUTRecord, 'HEAD': HEADRecord, 'DELETE': DELETERecord }
        constructor = constructors.get(method, HTTPRecord)
        return constructor(host=host, port=int(port), method=method, uri=uri, version=version, status=int(status))

class GETRecord(HTTPRecord):
    pass

class POSTRecord(HTTPRecord):
    pass

class PUTRecord(HTTPRecord):
    pass

class HEADRecord(HTTPRecord):
    pass

class DELETERecord(HTTPRecord):
    pass

class ConnectRecord(DataRecord):
    @staticmethod
    def create((host, port)):
        return ConnectRecord(host=host, port=int(port))

class DisconnectRecord(DataRecord):
    @staticmethod
    def create((host, port)):
        return DisconnectRecord(host=host, port=int(port))

class StatusRecord(DataRecord):
    @staticmethod
    def create(msg):
        return StatusRecord(msg=msg)

class ServerStartRecord(DataRecord):
    @staticmethod
    def create(pid):
        return ServerStartRecord(pid=int(pid))

class ServerFinishRecord(DataRecord):
    @staticmethod
    def create(pid):
        return ServerFinishRecord(pid=int(pid))

RemoteHost = Regex("(?:\d{1,3}\.){3}\d{1,3}") + ":" + Regex("\d+")
Timestamp = Regex("\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{6}Z")
LogLevel = Literal("INFO") | Literal("DEBUG") | Literal("WARN") | Literal("NOTICE") | Literal("SYSTEM")
_ConnectEvent = Literal("connected: expr=") + RemoteHost
ConnectEvent = Translate(_ConnectEvent, ConnectRecord.create)
_DisconnectEvent = Literal("disconnecting: expr=") + RemoteHost
DisconnectEvent = Translate(_DisconnectEvent, DisconnectRecord.create)
_ServerStatus = (SignificantLiteral("Finished processing command queue")
                 | Regex("Finished processing \d+ items in command queue"))
ServerStatus = Translate(_ServerStatus, StatusRecord.create)
_DebugStatus = (SignificantLiteral("server stopped")
                | SignificantLiteral("closing the server socket")
                | SignificantLiteral("finishing the server")
                | Regex("listening server socket started: fd=\d+")
                | Regex("Posting initialisation finished in: \d+\.\d+ secs")
                | Regex("(Adding|Deleting) Document\(\d+,\d+\) Slots: (\d+:\d+ )+ Total: \d+")
                | Regex("starting the server: expr=((\d{1,}\.){3}\d{1,3})?:\d+")
                | Regex("server socket opened: expr=((\d{1,}\.){3}\d{1,3})?:\d+ timeout=\d+\.\d+"))
DebugStatus = Translate(_DebugStatus, StatusRecord.create)
URI = Regex("(/[-_a-zA-Z0-9?&%=:\.]*)+")
HTTPStatus = Regex("-?\d{3}")
HTTPVersion = Literal("HTTP/") + Regex("\d+\.\d+")
HTTPMethod = SignificantLiteral("GET") | SignificantLiteral("POST") | SignificantLiteral("PUT") | SignificantLiteral("HEAD") | SignificantLiteral("DELETE")
HTTPRequest = Translate(Literal("(") + RemoteHost + Literal("):") + HTTPMethod + URI + HTTPVersion + Literal(":") + HTTPStatus, HTTPRecord.create)
_ServerStart = Literal("================ [START]: pid=") + Regex("\d+")
ServerStart = Translate(_ServerStart, ServerStartRecord.create)
_ServerFinish = Literal("================ [FINISH]: pid=") + Regex("\d+")
ServerFinish = Translate(_ServerFinish, ServerFinishRecord.create)
Event = ConnectEvent | DisconnectEvent | HTTPRequest | ServerStart | ServerFinish | ServerStatus | DebugStatus
_Line = Timestamp + Literal(': [') + LogLevel + Literal(']: ') + Event
Line = Translate(_Line, LineRecord.create)


def exhaust(it):
    for n in it:
        pass

def _tests():
    """
    >>> l = Line.parse_string("2012-02-09T00:01:18.790750Z: [INFO]: Finished processing command queue")
    >>> isinstance(l.event, StatusRecord)
    True
    >>> l.event.msg
    'Finished processing command queue'

    >>> l = Line.parse_string("2012-02-09T00:01:20.643557Z: [INFO]: (10.104.39.239:40755): POST /document/1/3147 HTTP/1.1: 202")
    >>> isinstance(l.event, POSTRecord)
    True
    >>> l.timestamp.date()
    datetime.date(2012, 2, 9)
    >>> l.timestamp.time()
    datetime.time(0, 1, 20, 643557)
    >>> l.event.host
    '10.104.39.239'
    >>> l.event.port
    40755
    >>> l.event.status
    202
    >>> l.event.uri
    '/document/1/3147'

    >>> l = Line.parse_string("2012-02-23T16:42:04.517205Z: [INFO]: (216.59.106.66:12307): GET /document/1/?cursor=1%3A1%3A3389&limit=10 HTTP/1.1: 200")
    >>> isinstance(l.event, GETRecord)
    True
    >>> l.timestamp.date()
    datetime.date(2012, 2, 23)
    >>> l.timestamp.time()
    datetime.time(16, 42, 4, 517205)
    >>> l.event.host
    '216.59.106.66'
    >>> l.event.port
    12307
    >>> l.event.uri
    '/document/1/?cursor=1%3A1%3A3389&limit=10'
    >>> l.event.status
    200

    >>> l = Line.parse_string("2012-02-09T00:01:20.214572Z: [INFO]: connected: expr=10.104.39.239:40753")
    >>> isinstance(l.event, ConnectRecord)
    True
    >>> l.event.host
    '10.104.39.239'
    >>> l.event.port
    40753
    
    >>> l = Line.parse_string("2012-02-09T00:01:19.523097Z: [INFO]: disconnecting: expr=10.104.39.239:40751")
    >>> isinstance(l.event, DisconnectRecord)
    True
    >>> l.event.host
    '10.104.39.239'
    >>> l.event.port
    40751

    >>> l = Line.parse_string("2012-02-03T21:00:01.474668Z: [SYSTEM]: ================ [START]: pid=18022")
    >>> isinstance(l.event, ServerStartRecord)
    True
    >>> l.event.pid
    18022

    >>> l = Line.parse_string("2012-02-07T02:01:27.397123Z: [SYSTEM]: ================ [FINISH]: pid=25188")
    >>> isinstance(l.event, ServerFinishRecord)
    True
    >>> l.event.pid
    25188
    """
    pass


class LogIterator(object):
    """
    Iterates over the lines in a superfastmatch log, returning
    records split into their component parts.

    >>> from cStringIO import StringIO
    >>> logtext = u'''/bin/sh: 1: exec: ./superfastmatch: not found
    ... /bin/sh: 1: exec: superfastmatch: not found
    ... /bin/sh: 1: exec: superfastmatch: not found
    ... Error opening databases
    ... 2012-02-09T00:01:18.385780Z: [INFO]: connected: expr=10.104.39.239:40749
    ... 2012-02-09T00:01:18.385780Z: [INFO]: connected: expr=10.104.39.239:40749
    ... 2012-02-09T00:01:18.386113Z: [INFO]: (10.104.39.239:40749): POST /document/1/3144 HTTP/1.1: 202
    ... 2012-02-09T00:01:18.386168Z: [INFO]: disconnecting: expr=10.104.39.239:40749
    ... 2012-02-09T00:01:18.790750Z: [INFO]: Finished processing command queue
    ... 2012-02-09T00:01:19.522721Z: [INFO]: connected: expr=10.104.39.239:40751
    ... 2012-02-09T00:01:19.523037Z: [INFO]: (10.104.39.239:40751): POST /document/1/3145 HTTP/1.1: 202
    ... 2012-02-09T00:01:19.523097Z: [INFO]: disconnecting: expr=10.104.39.239:40751
    ... 2012-02-09T00:01:19.825838Z: [INFO]: Finished processing command queue
    ... 2012-02-09T00:01:20.214572Z: [INFO]: connected: expr=10.104.39.239:40753
    ... 2012-02-09T00:01:20.214936Z: [INFO]: (10.104.39.239:40753): POST /document/1/3146 HTTP/1.1: 202
    ... 2012-02-09T00:01:20.214996Z: [INFO]: disconnecting: expr=10.104.39.239:40753
    ... 2012-02-09T00:01:20.418406Z: [INFO]: Finished processing command queue
    ... 2012-02-09T00:01:20.643280Z: [INFO]: connected: expr=10.104.39.239:40755
    ... 2012-02-09T00:01:20.643557Z: [INFO]: (10.104.39.239:40755): POST /document/1/3147 HTTP/1.1: 202
    ... 2012-02-23T16:42:04.517205Z: [INFO]: (216.59.106.66:12307): GET /document/1/?cursor=1%3A1%3A3389&limit=10 HTTP/1.1: 200'''
    >>> logfile = StringIO(logtext)
    >>> records = LogIterator(logfile)
    >>> exhaust(records)
    >>> records.lines_processed
    16
    >>> records.lines_discarded
    4
    """

    def __init__(self, fileobj):
        self._fileobj = iter(fileobj)
        self.lines_processed = 0
        self.lines_discarded = 0

    def __iter__(self):
        return self

    def next(self):
        while True:
            try:
                text = self._fileobj.next()
                record = Line.parse_string(text)
                self.lines_processed += 1
                return record
            except ParseException:
                logging.debug("Unparseable line: {0}".format(text.strip('\r\n')))
                self.lines_discarded += 1


if __name__ == "__main__":
    import doctest
    doctest.testmod()

