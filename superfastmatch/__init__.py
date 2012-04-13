from .client import Client
from .federated import FederatedClient
from .client import SuperFastMatchError
try:
    from .djangoclient import Client as DjangoClient
except ImportError:
    pass
from .iterators import DocumentIterator
