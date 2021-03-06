"""
A collection of routines for backing up and restoring documents 
in a superfastmatch server.

These have been separated from the modules that use them to allow
for relative imports.
"""

import os
import sys
try:
    import cPickle as pickle
except ImportError:
    import pickle
from tempfile import NamedTemporaryFile
from ..zipfile27 import ZipFile, ZIP_DEFLATED
from contextlib import closing
from copy import deepcopy
import progressbar
from ..util import ChunkedIterator, UnpicklerIterator, parse_doctype_range, parse_docid_range, SparseRange
from ..iterators import DocumentIterator


def prune_document(docmeta):
    """Removes the attributes that will be regenerated by the restore process.
    """
    ignored_attributes = ['characters', 'id']

    doc = deepcopy(docmeta)
    for attr in ignored_attributes:
        if attr in doc:
            del doc[attr]
    return doc


def backup(sfm, outpath, doctype_rangestr=None, chunksize=10000000):
    """
    Reusable routine for a backup tool.

    The term `chunksize` is used to refer to two different types of chunks. I'm sorry.
    One usage refers to the number of documents to retrieve from the server at once.
    The other usage refers to the size of the files stored insize the ZIP archive.
    """

    if doctype_rangestr is not None:
        # Just ensure that it's valid.
        parse_doctype_range(doctype_rangestr)

    docs = DocumentIterator(sfm,
                            order_by='docid',
                            doctype=doctype_rangestr,
                            chunksize=1000,
                            fetch_text=True)

    chunked_docs = ChunkedIterator(docs,
                                   chunksize=chunksize, # approx. 10 megabytes
                                   key=lambda d: len(d.get('text')))

    metadata = {
        'doctypes': set(),
        'doc_count': 0,
        'file_count': 0
    }

    with closing(ZipFile(outpath, 'w', compression=ZIP_DEFLATED, allowZip64=True)) as outfile:
        with NamedTemporaryFile(mode='wb') as metafile:
            for (file_number, docs_chunk) in enumerate(chunked_docs):
                if len(docs_chunk) == 0:
                    continue

                with NamedTemporaryFile(mode='wb') as docsfile:
                    for docmeta in docs_chunk:
                        if not docmeta:
                            print >>sys.stderr, "Dropped empty document."
                            continue
                        try:
                            doc = prune_document(docmeta)
                            pickle.dump(doc, docsfile, pickle.HIGHEST_PROTOCOL)
                            metadata['doctypes'].add(doc['doctype'])
                            metadata['doc_count'] += 1
                        except Exception as e:
                            print >>sys.stderr, str(e)

                    docsfile.flush()
                    print "Compressing backup chunk #{num} containing {chunksize} documents...".format(num=file_number, chunksize=len(docs_chunk))
                    outfile.write(docsfile.name, 'docs{num}'.format(num=file_number))
                    metadata['file_count'] += 1

            metadata['doctypes'] = list(metadata['doctypes'])
            print "Dumped {doc_count} documents spanning doctypes {doctypes}".format(**metadata)
            pickle.dump(metadata, metafile)
            metafile.flush()
            outfile.write(metafile.name, 'meta')

    print "Done."


def restore(sfm, inpath, docid_rangestr=None, doctype_mappingstr=None, dryrun=False):
    """
    Reads documents from a backup archive and posts them to a superfastmatch server.

    docid_rangestr is of the format 1-10,20,21 to import documents 1 through 10 and 20 and 21.

    doctype_mappingstr isof the format 10:11,11:10 to swap doctypes 10 and 11.

    TODO: The current implementation reads the entire archive regardless of the docid
    range specified. This could be sped up dramatically by taking that information into
    account but to do it right the archive format would need to be changed to include
    the doctype range of each docs## file. Low priority since this should be a rare
    task.
    """

    doctype_mappings = {}
    if doctype_mappingstr is not None:
        for mapping in doctype_mappingstr.split(','):
            mapping = mapping.strip()
            (src, dst) = mapping.split(':')
            src = int(src)
            dst = int(dst)
            doctype_mappings[src] = dst

    if doctype_mappings:
        print >>sys.stderr, "Remapping doctypes:"
        for (src, dst) in doctype_mappings.iteritems():
            print >>sys.stderr, "    {0} => {1}".format(src, dst)

    ignored_attributes = ['characters', 'id', 'defer']
    with closing(ZipFile(inpath, 'r')) as infile:
        with closing(infile.open('meta', 'r')) as metafile:
            metadata = pickle.load(metafile)

            docid_range = None
            if docid_rangestr is not None:
                docid_range = parse_docid_range(docid_rangestr)
                print >>sys.stderr, "Limiting import to {0}".format(docid_rangestr)

            progress = progressbar.ProgressBar(maxval=metadata['doc_count'],
                                               widgets=[
                                                   progressbar.widgets.AnimatedMarker(),
                                                   '  ',
                                                   progressbar.widgets.Counter(),
                                                   '/{0}  '.format(metadata['doc_count']),
                                                   progressbar.widgets.Percentage(),
                                                   '  ',
                                                   progressbar.widgets.ETA(),
                                               ])
            progress.start()
            doccounter = 0

            for file_number in range(0, metadata['file_count']):
                docsfile_name = 'docs{num}'.format(num=file_number)
                with closing(infile.open(docsfile_name, 'r')) as docsfile:
                    docloader = pickle.Unpickler(docsfile)
                    for doc in UnpicklerIterator(docloader):
                        if 'text' in doc and 'doctype' in doc and 'docid' in doc:
                            if docid_range is not None and doc['docid'] not in docid_range:
                                if doc['docid'] > docid_range.max:
                                    pass
                            else:
                                for attr in ignored_attributes:
                                    if doc.has_key(attr):
                                        del doc[attr]
                                new_doctype = doctype_mappings.get(doc['doctype'])
                                if new_doctype:
                                    doc['doctype'] = new_doctype
                                if dryrun == False:
                                    add_result = sfm.add(defer=True, **doc)
                                    if add_result['success'] == False:
                                        print >>sys.stderr, "Failed to restore document ({doctype}, {docid})".format(**doc)
                        elif 'doctype' in doc and 'docid' in doc:
                            print >>sys.stderr, "Document ({doctype}, {docid}) cannot be restored because it is missing a text attribute.".format(**doc)

                        elif 'text' in doc:
                            print >>sys.stderr, "Document with text '{snippet}...' cannot be restored because it is missing a doctype and/or docid attribute.".format(snippet=doc['text'][:40])

                        else:
                            print >>sys.stderr, "Cannot restore empty document (missing all of text, doctype, and docid attributes)."

                        doccounter += 1
                        progress.update(doccounter)
            progress.finish()
