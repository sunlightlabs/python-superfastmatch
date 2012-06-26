# Overview #

Python library for interacting with a superfastmatch server.

* `superfastmatch.client.Client`: Simple client for querying a single server.
* `superfastmatch.federated.Cient`: Client that spreads queries across multiple servers, sharding based on doctype.
* `superfastmatch.djangoclient.Client`: Subclass of `superfastmatch.client.Client` that is configured via `django.conf.settings.SUPERFASTMATCH`.
* `superfastmatch.iterators.DocumentIterator`: Iterates over all documents on the server via one of the above Client classes.
* `superfastmatch.iterators.FederatedDocumentIterator`: Iterates over the documents on multiple servers via a Client class for each server. This is similar to using a `DocumentIterator` over a `superfastmatch.federated.FederatedClient` but the time-space trade-offs differ.


`superfastmatch.djangoclient.from_django_conf` is a factory method that creates the appropriate client based on the structure of the configuration in `django.conf.settings.SUPERFASTMATCH`. Given the configuration below, `from_django_conf()` would create a `superfastmatch.client.Client` because the `default` key maps to a dict.

    SUPERFASTMATCH = {
        'default': {
            'url': 'http://localhost:8080/'
        }
    }

Specifying a list or tuple will yield a `superfastmatch.federated.FederatedClient`:

    SUPERFASTMATCH = {
        'default': [
            { 'doctypes': [1, 2],
              'url': 'http://localhost:8080'
            },
            { 'doctypes': [3],
              'url': 'http://otherhost:8080'
            }
        }
    }

# Tools #

This library comes with a backup tool and a corresponding restore tool. The backup tools iterates over documents on a superfastmatch server, pickles the portable attributes, and stores them in a zip file. The restore tool does the inverse operation, optionally allowing you to translate the stored doctypes (though not docids) in the process.

## `superfastmatch.tools.backup` ##
    python -m superfastmatch.tools.backup -h
    usage: backup.py [-h] [--url URL] [--overwrite] [--chunksize BYTES]
                     RANGE_STRING OUTPATH
    
    positional arguments:
      RANGE_STRING       Range string of doctypes to backup, e.g. 1:4-7:10
      OUTPATH            File to write to.
    
    optional arguments:
      -h, --help         show this help message and exit
      --url URL          URL of the Superfastmatch server.
      --overwrite        Overwrite OUTFILE if it already exists.
      --chunksize BYTES  The approximate number of bytes (uncompressed) to store
                         in each chunk. Lower numbers trade performance for less
                         memory usage. (default: 10M)



## `superfastmatch.tools.restore` ##
    python -m superfastmatch.tools.restore -h
    usage: restore.py [-h] [--dryrun] [--doctypes MAPPING] [--url URL] INPATH
    
    positional arguments:
      INPATH              Backup file to read.
    
    optional arguments:
      -h, --help          show this help message and exit
      --dryrun            Don't actually restore the documents. Just run through
                          the backup file.
      --doctypes MAPPING  A string describing how to translate doctypes during the
                          restore process.
      --url URL           URL of the Superfastmatch server.


