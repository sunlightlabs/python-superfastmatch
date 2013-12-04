import superfastmatch.client
import superfastmatch.federated
import superfastmatch.loadbalanced
from django.conf import settings

__all__ = ['Client', 'from_django_conf']

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
        copy_setting('timeout')

        super(Client, self).__init__(*args, **kwargs)


def from_django_conf(confkey='default'):
    """
    Instantiates a superfastmatch client object based on the structure of the configuration specified.
    
    from_django_conf('default') uses the value of django.conf.settings.SUPERFASTMATCH['default']

    If the value is a dict, a basic client is returned. A list of dicts
    returns a federated client. In this case each dict is expected to
    have a key 'doctypes' that maps to a list of doctypes on that server.
    Finally, a tuple of dicts will yield a load balanced client. In 
    this case the servers are expected to contain exactly the same content.
    """

    conf = settings.SUPERFASTMATCH[confkey]
    if isinstance(conf, dict):
        return Client(confkey)
    elif isinstance(conf, list):
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
    elif isinstance(conf, tuple):
        clients = [superfastmatch.Client(url=params.get('url'), parse_response=params.get('parse_response')) for params in conf]
        return superfastmatch.loadbalanced.LoadBalancedClient(clients)

if __name__ == "__main__":
    client = Client()

