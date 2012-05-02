"""
This command is the complement of the restore command. It 
iterates over documents on the server, pickles them, then dumps
them into a series of approximately fixed-sized files and then
zips those files up with a small metadata file containing the 
number of documents, etc.
"""

import sys
import os
from argparse import ArgumentParser
from superfastmatch.client import Client
from superfastmatch.tools.routines import backup

def main():
    parser = ArgumentParser()
    parser.add_argument('--url', metavar='URL', type=str,
                        default='http://127.0.0.1:8080', action='store',
                        help='URL of the Superfastmatch server.')
    parser.add_argument('--overwrite', default=False, action='store_true',
                        help='Overwrite OUTFILE if it already exists.')
    parser.add_argument('--chunksize', metavar='BYTES', action='store', type=int, default=10000000,
                        help=('The approximate number of bytes (uncompressed) to store in each chunk. ' 
                              + 'Lower numbers trade performance for less memory usage. (default: 10M)'))
    parser.add_argument('doctypes', metavar='RANGE_STRING', action='store',
                        help='Range string of doctypes to backup, e.g. 1:4-7:10')
    parser.add_argument('outpath', metavar='OUTPATH', action='store',
                        help='File to write to.')
    args = parser.parse_args()

    if os.path.exists(args.outpath) and args.overwrite == False:
        print >>sys.stderr, "{outpath} already exists.".format(**vars(args))
        sys.exit(1)

    sfm = Client(args.url, parse_response=True)
    backup(sfm, args.outpath, args.doctypes, chunksize=args.chunksize)

if __name__ == "__main__":
    main()


