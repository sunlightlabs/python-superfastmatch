
import sys

from pprint import pprint

import superfastmatch


def main(host, doctype):
    try:
        sfm = superfastmatch.Client(host, parse_response=True)
        results = sfm.documents(doctype)
        if results['success'] != True:
            print "Unable to enumerate documents in doctype {0}.".format(doctype)
            return

        documents = results['rows']
        if len(documents) == 0:
            print "No documents in doctype {0}.".format(doctype)
            return

        print "Going to delete {0} documents from doctype {1}".format(len(documents), doctype)
        print "Examples:"
        for doc in documents[:3]:
            pprint(doc, indent=4, width=40)
        print
        while True:
            response = raw_input("Continue? Yes or no: ")
            if response.lower() == "yes":
                break
            if response.lower() == "no":
                return

        for doc in documents:
            del_result = sfm.delete(doctype, doc['docid'])
            if del_result['success'] == True:
                print "Deleted ({0}, {1})".format(doctype, doc['docid'])
            else:
                print "Failed to delete ({0}, {1})".format(doctype, doc['docid'])

    except superfastmatch.SuperFastMatchError, e:
        print str(e)

def usage():
    print "USAGE: {0} <host> <doctype>".format(sys.argv[0])
    sys.exit(1)

if __name__ == "__main__":
    if len(sys.argv) == 3:
        host = sys.argv[1]
        doctype = sys.argv[2]
        
        try:
            doctype = int(doctype)
        except ValueError:
            print "<doctype> must be an integer, not '{0}'".format(doctype)
            usage()

        main(host, doctype)

    else:
        usage()

