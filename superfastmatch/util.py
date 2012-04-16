import stream

def parse_doctype_range(rangestr):
    """Return a list of the doctypes in the range specified expanded as a list 
    of integers. This is used to validate arguments. The actual range strings
    are passed onto the superfastmatch server.

    >>> parse_doctype_range('1-2:5:7-9')
    [1, 2, 5, 7, 8, 9]
    >>> parse_doctype_range('')
    >>> parse_doctype_range('1')
    [1]
    >>> parse_doctype_range('7-7')
    [7]
    """
    if not rangestr:
        raise Exception('Invalid doctype range ({0})'.format(rangestr))

    split_on_hyphen = lambda s: s.split('-')

    def expand(rng):
        if len(rng) == 1:
            return int(rng[0])
        elif len(rng) == 2:
            return range(int(rng[0]), int(rng[1]) + 1)
        else:
            raise Exception('Unrecognized range data type')

    return (stream.Stream(rangestr.split(':'))
            >> stream.map(split_on_hyphen)
            >> stream.map(expand)
            >> stream.flatten
            >> list)



