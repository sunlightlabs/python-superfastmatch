"""
This is the complement of the restore command. It reads
documents from a ZIP archive and POSTs them to a 
Superfastmatch server, possibly translating doctypes
in the process.
"""


import sys
import os
from argparse import ArgumentParser
from superfastmatch.client import Client
from superfastmatch.tools.routines import restore


def main():
    parser = ArgumentParser()
    parser.add_argument('--dryrun', default=False, action='store_true',
                        help='Don\'t actually restore the documents. Just run through the backup file.')
    parser.add_argument('--doctypes', metavar='MAPPING', action='store',
                        help='A string describing how to translate doctypes during the restore process.')
    parser.add_argument('--url', metavar='URL', type=str,
                        default='http://127.0.0.1:8080', action='store',
                        help='URL of the Superfastmatch server.')
    parser.add_argument('inpath', metavar='INPATH', action='store',
                        help='Backup file to read.')
    args = parser.parse_args()

    if os.path.exists(args.inpath) == False:
        print >>sys.stderr, "Unable to find {inpath}.".format(**vars(args))
        sys.exit(1)

    sfm = Client(args.url, parse_response=True)
    restore(sfm, args.url, args.inpath, doctype_mappingstr=args.doctypes, dryrun=args.dryrun)


if __name__ == "__main__":
    main()



