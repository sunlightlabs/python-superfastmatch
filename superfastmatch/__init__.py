from client import Client
from client import SuperFastMatchError
try:
    from djangoclient import Client as DjangoClient
except ImportError:
    pass
