import superfastmatch.client
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

if __name__ == "__main__":
    client = Client()

