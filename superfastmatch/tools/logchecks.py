"""
Tallies requests by type. Useful for guessing server load for past runs.
"""

import re
import logging
import dateutil.tz
import dateutil.parser
from argparse import ArgumentParser
from ..logparser import (LogIterator,
                         HTTPRecord, GETRecord, POSTRecord,
                         ConnectRecord, DisconnectRecord,
                         ServerStartRecord, ServerFinishRecord,
                         StatusRecord)


parser = ArgumentParser()
parser.add_argument('--begin', metavar='DATETIME', type=dateutil.parser.parse,
                    help='Begining of the time span to analyze.')
parser.add_argument('--end', metavar='DATETIME', type=dateutil.parser.parse,
                    help='End of the time span to analyze.')
parser.add_argument('--loglevel', metavar='LEVEL', default='WARN',
                    choices=('DEBUG', 'INFO', 'WARN', 'ERROR', 'CRTITICAL'), action='store',
                    help='Level of logging.')
parser.add_argument('--ignore-address', metavar='LEVEL', default=None,
                    help='Address to ignore requests from, usually 127.0.0.1')
parser.add_argument('logfile', metavar='LOGFILE', action='store',
                    help='Log file to analyze.')


DocumentURIPattern = re.compile('^/document/\d+/\d+/$')

def main():
    args = parser.parse_args()
    logging.basicConfig(level=getattr(logging, args.loglevel.upper()))

    get_requests = 0
    post_requests = 0
    search_requests = 0
    document_requests = 0
    enumeration_requests = 0
    connection_records = 0
    status_records = 0
    other_records = 0

    begin = args.begin or dateutil.parser.parse('9999-12-31T23:59:59.999999Z')
    end = args.end or dateutil.parser.parse('1970-01-01T00:00:00.00000Z')

    with file(args.logfile, 'r') as logfile:
        log_records = LogIterator(logfile)
        for record in log_records:
            if args.begin:
                if record.timestamp < args.begin:
                    continue
            elif record.timestamp < begin:
                begin = record.timestamp

            if args.end:
                if record.timestamp > args.end:
                    continue
            elif record.timestamp > end:
                end = record.timestamp

            if args.ignore_address and isinstance(record.event, HTTPRecord) and record.event.host == args.ignore_address:
                continue

            if isinstance(record.event, GETRecord):
                get_requests += 1
                if record.event.uri.startswith('/document/'):
                    if DocumentURIPattern.match(record.event.uri) is not None:
                        document_requests += 1
                    else:
                        enumeration_requests += 1

            elif isinstance(record.event, POSTRecord):
                post_requests += 1
                if record.event.uri == '/search/':
                    search_requests += 1

            elif isinstance(record.event, (ConnectRecord, DisconnectRecord)):
                connection_records += 1

            elif isinstance(record.event, (ServerStartRecord, ServerFinishRecord, StatusRecord)):
                status_records += 1

            else:
                other_records += 1

      
        minutes = max((end - begin).total_seconds() / 60, 0.00001)

        rowfmt = "{0!s: <25} {1!s: >15} {2!s: >7}"
        print rowfmt.format("Request Type", "Requests", "(per minute)")
        print rowfmt.format("GET", get_requests, get_requests / minutes)
        print rowfmt.format("GET /document/TYPE/ID/", document_requests, document_requests / minutes)
        print rowfmt.format("GET /document/?...", enumeration_requests, enumeration_requests / minutes)
        print rowfmt.format("POST", post_requests, post_requests / minutes)
        print rowfmt.format("POST /search/", search_requests, search_requests / minutes)
        print rowfmt.format("Other", other_records, '')
        print rowfmt.format("Discarded", log_records.lines_discarded, '')


if __name__ == "__main__":
    main()

