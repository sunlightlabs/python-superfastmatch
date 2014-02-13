import superfastmatch.client
from copy import copy
from superfastmatch.djangoclient import from_django_conf
from django.contrib.contenttypes.models import ContentType
from django.conf import settings


__all__ = ['DjangoModelMixin', 'index_document']


def index_document(client, doc, defer=True, failOnError=False):
    doctype = doc.sfm_doctype()
    docid = doc.sfm_docid()
    text = doc.sfm_text()
    attrs = doc.sfm_attributes()

    if doctype is None:
        logging.warn("Document is missing doctype, skipping.")
        return

    if docid is None:
        logging.warn("Document is missing docid, skipping.")
        return

    if text is None:
        logging.warn("You passed None as the text of the document, skipping.")
        return

    if u'text' in attrs:
        del attrs[u'text']
    if u'doctype' in attrs:
        del attrs[u'doctype']
    if u'docid' in attrs:
        del attrs[u'docid']
    if u'defer' in attrs:
        del attrs[u'defer']

    try:
        resp = client.add(doctype, docid, text, defer=defer, **attrs)
    except SuperFastMatchError as e:
        if failOnError:
            raise
        else:
            logging.error("Failed to index document: {}".format(unicode(e)))


class DjangoModelMixin(object):
    """
    Simple mix-in for indexing a model in superfastmatch.
    The main benefit of this mix-in is the integration it
    provides with the batch indexing management command.

    The mix-in allows for slight configuration. To modify
    it's behavior, set these class variables on your model:

        SFM_DjangoClientName: string, defaults to 'default'
        SFM_FailOnError: boolean, defaults to False
        SFM_UseDeferredAssociation: boolean, defaults to True

    You must implement the instance methods sfm_text and sfm_attributes.
    """

    def sfm_text(self):
        raise NotImplementedError("When you use the DjangoModelMixin you must implement sfm_text.")

    def sfm_attributes(self):
        raise NotImplementedError("When you use the DjangoModelMixin you must implement sfm_attributes.")

    def sfm_docid(self):
        return self.id

    @classmethod
    def sfm_doctype(cls):
        content_type = ContentType.objects.get_for_model(cls)
        return content_type.id

    def save(self, *args, **kwargs):
        sfm_client = from_django_conf(getattr(self, 'SFM_DjangoClientName', 'default'))
        index_document(sfm_client,
                       self,
                       defer=getattr(self, 'SFM_UseDeferredAssociation', True),
                       failOnError=getattr(self, 'SFM_FailOnError', False))
        super(DjangoModelMixin, self).save(*args, **kwargs)

    @classmethod
    def sfm_search(cls, query_text):
        sfm_client = from_django_conf(getattr(cls, 'SFM_DjangoClientName', 'default'))
        raw_results = sfm_client.search(doctype=cls.sfm_doctype(), text=query_text)
        rows = raw_results.get(u'documents', {}).get(u'rows', [])
        if rows:
            return cls.objects.filter(id__in=[doc['docid'] for doc in rows])
        else:
            return []

