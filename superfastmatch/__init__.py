from .client import Client
from .federated import FederatedClient
from .client import SuperFastMatchError
try:
    from .djangoclient import from_django_conf, Client as DjangoClient
except ImportError:
    pass
from .iterators import DocumentIterator
from .util import parse_doctype_range
