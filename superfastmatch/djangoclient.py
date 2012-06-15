import socket
import logging
import superfastmatch.client
from django.http import HttpResponse
from django.conf import settings

class Client(superfastmatch.client.Client):
    def __init__(self, confkey='default', *args, **kwargs):
        assert hasattr(settings, 'SUPERFASTMATCH'), "You must configure the Django Superfastmatch client."
        assert confkey in settings.SUPERFASTMATCH, "You must configure the '{0}' Django Superfastmatch client.".format(confkey)

        def copy_setting(key):
            if key not in kwargs and key in settings.SUPERFASTMATCH[confkey]:
                kwargs[key] = settings.SUPERFASTMATCH[confkey][key]

        copy_setting('url')
        copy_setting('username')
        copy_setting('password')
        copy_setting('parse_response')

        super(Client, self).__init__(*args, **kwargs)


def from_django_conf(confkey='default'):
    conf = settings.SUPERFASTMATCH[confkey]
    if isinstance(conf, dict):
        return Client(confkey)
    elif isinstance(conf, (list, tuple)):
        clients_by_url = dict()
        clients = dict()
        for subconf in conf:
            for doctype in subconf['doctypes']:
                c = clients_by_url.get(subconf['url'])
                if c is None:
                    c = superfastmatch.client.Client(subconf['url'],
                                                     parse_response=subconf.get('parse_response', True))
                    clients_by_url[subconf['url']] = c
                clients[doctype] = c

        if not clients:
            raise Exception('Django config for federated client {confkey} contained no valid configurations.'.format(confkey=confkey))
        return superfastmatch.federated.FederatedClient(clients)

if __name__ == "__main__":
    client = Client()

