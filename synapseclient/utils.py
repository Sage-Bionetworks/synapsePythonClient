#!/usr/bin/env python2.7

# To debug this, python -m pdb myscript.py

import os, sys, urllib, urlparse, hashlib, re
import collections
import tempfile
import platform
from datetime import datetime as Datetime
from datetime import date as Date
from numbers import Number

UNIX_EPOCH = Datetime(1970, 1, 1, 0, 0)
GB = 2**30
MB = 2**20
KB = 2**10


def md5_for_file(filename, block_size=2**20):
    '''
    lifted this function from
    http://stackoverflow.com/questions/1131220/get-md5-hash-of-a-files-without-open-it-in-python
    '''
    md5 = hashlib.md5()
    f = open(filename,'rb')
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return(md5)


## download a remote file
## localFilePath can be None, in which case a temporary file is created
## returns a tuple (localFilePath, HTTPmsg), see urllib.urlretrieve
def download_file(url, localFilepath=None):
    if (localFilepath):
        dir = os.path.dirname(localFilepath)
        if not os.path.exists(dir):
            os.makedirs(dir)
    return urllib.urlretrieve(url, localFilepath)


# this could be made more robust
# see: http://tools.ietf.org/html/rfc6266
# and the python library http://pypi.python.org/pypi/rfc6266
def extract_filename(content_disposition):
    match = re.search('filename=([^ ]*)', content_disposition)
    return match.group(1) if match else 'filename'


def guess_object_type(obj):
    if isinstance(obj, basestring):
        if obj.startswith('syn'):
            return 'entity'
    elif 'entityType' in obj:
        return 'entity'
    elif 'contentSource' in obj:
        return 'evaluation'
    else:
        return 'entity'


def _get_from_members_items_or_properties(obj, key):
    try:
        if hasattr(obj, key):
            return obj.id
        if hasattr(obj, 'properties') and key in obj.properties:
            return obj.properties[key]
    except (KeyError, TypeError, AttributeError): pass
    try:
        if key in obj:
            return obj[key]
        elif 'properties' in obj and key in obj['properties']:
            return obj['properties'][key]
    except (KeyError, TypeError): pass
    return None

#TODO: what does this do on an unsaved Synapse Entity object?
def id_of(obj):
    """Try to figure out the synapse ID of the given object. Accepted input
    includes strings, entity objects, or entities represented by dictionaries"""
    if isinstance(obj, basestring):
        return obj
    if isinstance(obj, Number):
        return str(obj)
    result = _get_from_members_items_or_properties(obj, 'id')
    if result is None:
        raise Exception('Invalid parameters: couldn\'t find id of ' + str(obj))
    return result


def class_of(obj):
    """Return the class or type of the input object as a string"""
    if obj is None:
        return 'None'
    if hasattr(obj,'__class__'):
        return obj.__class__.__name__
    return str(type(obj))

def get_properties(entity):
    return entity.properties if hasattr(entity, 'properties') else entity


def get_entity_type(entity):
    return _get_from_members_items_or_properties(entity, 'entityType')


def is_url(s):
    """Return True if a string appears to be a valid URL."""
    if isinstance(s, basestring):
        try:
            url_parts = urlparse.urlsplit(s)
            ## looks like a Windows drive letter?
            if len(url_parts.scheme)==1 and url_parts.scheme.isalpha():
                return False
            if url_parts.scheme == 'file' and bool(url_parts.path):
                return True
            return bool(url_parts.scheme) and bool(url_parts.netloc)
        except Exception as e:
            return False
    return False


def as_url(s):
    """Try to convert input to a proper URL"""
    url_parts = urlparse.urlsplit(s)
    ## Windows drive letter?
    if len(url_parts.scheme)==1 and url_parts.scheme.isalpha():
        return 'file:///%s' % str(s)
    if url_parts.scheme:
        return url_parts.geturl()
    else:
        return 'file://%s' % str(s)


def file_url_to_path(url, verify_exists=False):
    parts = urlparse.urlsplit(url)
    if parts.scheme=='file' or parts.scheme=='':
        path = parts.path
        ## A windows file URL, for example file:///c:/WINDOWS/asdf.txt
        ## will get back a path of: /c:/WINDOWS/asdf.txt, which we need to fix by
        ## lopping off the leading slash character. Apparently, the Python developers
        ## think this is not a bug: http://bugs.python.org/issue7965
        if re.match(r'\/[A-Za-z]:', path):
            path = path[1:]
        if os.path.exists(path) or not verify_exists:
            return {
                'path': path,
                'files': [os.path.basename(path)],
                'cacheDir': os.path.dirname(path) }
    return {}

def is_synapse_entity(entity):
    if isinstance(entity, collections.Mapping):
        return 'entityType' in entity
    return False


def is_synapse_id(obj):
    """Returns a synapse ID, if the input is a synapse ID, otherwise returns None"""
    if isinstance(obj, basestring):
        m = re.match(r'(syn\d+)', obj)
        if m:
            return m.group(1)
    return None

def _is_date(dt):
    """
    Objects of class datetime.date and datetime.datetime will be recognized as dates
    """
    return isinstance(dt,Date) or isinstance(dt,Datetime)


def _to_list(value):
    """
    Convert the value (an iterable or a scalar value) to a list.
    """
    if isinstance(value, collections.Iterable) and not isinstance(value, basestring):
        return list(value)
    else:
        return [value]


def _to_iterable(value):
    """
    Convert the value (an iterable or a scalar value) to a list.
    """
    if isinstance(value, basestring):
        return (value,)
    if isinstance(value, collections.Iterable):
        return value
    return (value,)


def make_bogus_data_file(n=100, seed=12345):
    """Make a bogus data file for testing. File will contain 'n'
    random floating point numbers separated by commas. It is the
    caller's responsibility to remove the file when finished.
    """
    import random
    random.seed(seed)
    data = [random.gauss(mu=0.0, sigma=1.0) for i in range(n)]

    f = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
    try:
        f.write(", ".join((str(n) for n in data)))
        f.write("\n")
    finally:
        f.close()

    return f.name


def make_bogus_binary_file(n=1*MB, verbose=False):
    """Make a bogus binary data file for testing. It is the
    caller's responsibility to remove the file when finished.
    """
    if verbose:
        sys.stdout.write('writing bogus file')
    junk = os.urandom(min(n, 1*MB))
    with tempfile.NamedTemporaryFile(mode='wb', suffix=".dat", delete=False) as f:
        while n > 0:
            f.write(junk[0:min(n, 1*MB)])
            n -= min(n, 1*MB)
            if verbose:
                sys.stdout.write('.')
    if verbose:
        sys.stdout.write('\n')
    return f.name


## turns a datetime object into a unix epoch time expressed as a float
def to_unix_epoch_time(dt):
    """
    Convert either datetime.date or datetime.datetime objects to unix times
    (milliseconds since midnight Jan 1, 1970)
    """
    if type(dt) == Date:
        return (dt - UNIX_EPOCH.date()).total_seconds() * 1000
    return (dt - UNIX_EPOCH).total_seconds() * 1000


def from_unix_epoch_time(ms):
    """
    Return a datetime object given milliseconds since midnight Jan 1, 1970
    """
    ## utcfromtimestamp fails for negative values (dates before 1970-1-1) on windows
    ## so, here's a hack that enables ancient events, such as Chris's birthday be
    ## converted from ms since the unix epoch to higher level Datetime objects. Ha!
    if platform.system()=='Windows' and ms < 0:
        mirror_date = Datetime.utcfromtimestamp(abs(ms)/1000.0)
        return (UNIX_EPOCH - (mirror_date-UNIX_EPOCH))
    return Datetime.utcfromtimestamp(ms/1000.0)


def format_time_interval(seconds):
    periods = (
        ('year',        60*60*24*365),
        ('month',       60*60*24*30),
        ('day',         60*60*24),
        ('hour',        60*60),
        ('minute',      60),
        ('second',      1),)

    result=[]
    for period_name,period_seconds in periods:
        if seconds > period_seconds or period_name=='second':
            period_value, seconds = divmod(seconds, period_seconds)
            if period_value > 0 or period_name=='second':
                if period_value == 1:
                    result.append("%d %s" % (period_value, period_name))
                else:
                    result.append("%d %ss" % (period_value, period_name))
    return ", ".join(result)


## a helper method to find a particular used resource in an activity
## that matches a predicate
def _find_used(activity, predicate):
    for resource in activity['used']:
        if predicate(resource):
            return resource
    return None


def synapse_error_msg(ex):
    if isinstance(ex, basestring):
        return ex

    msg = '\n' + class_of(ex) + ': ' + str(ex) + '\n'

    if hasattr(ex, 'response'):
        response = ex.response
        try:
            synapse_error = response.json()
            msg += str(synapse_error['reason'])
        except Exception:
            msg += str(response.text)

    msg += '\n\n'

    return msg


def debug_response(response):
    """
    Given a response object (from the requests library), print debugging information
    """
    try:
        print '\n\n'
        print '\nREQUEST ' + '>' * 52
        print response.request.url, response.request.method
        print '  headers: ' + str(response.request.headers)
        if hasattr(response.request, 'body'):
            print '  body: ' + str(response.request.body)
        print '\nRESPONSE ' + '<' * 51
        print response
        print '  headers: ' + str(response.headers)
        try:
            print '  body: ' + str(response.json())
        except:
            print '  body: ' + str(response.text)
        print '-' * 60
        print '\n'
    except Exception as ex:
        print "Exception in debug_response: " + str(ex)
        print str(response)


BUFFER_SIZE = 8*KB

class Chunk(object):
    """
    """
    ## TODO implement seek and tell?

    def __init__(self, fileobj, size):
        self.fileobj = fileobj
        self.size = size
        self.position = 0
        self.closed = False

    def read(self, size=None):
        if size is None or size <= 0:
            size = self.size - self.position
        else:
            size = min(size, self.size - self.position)

        if self.closed or size <=0:
            return None

        self.position += size
        return self.fileobj.read(size)

    def mode(self):
        return self.fileobj.mode()

    def __len__(self):
        return self.size

    def __iter__(self):
        return self

    def next(self):
        if self.closed:
            raise StopIteration
        data = self.read(BUFFER_SIZE)
        if not data:
           raise StopIteration
        return data

    def close(self):
        self.closed = True


def chunks(fileobj, chunksize=5*MB):
    """
    Generate file-like objects from which chunksize bytes can be streamed.
    """
    remaining = os.stat(fileobj.name).st_size
    while remaining > 0:
        chunk = Chunk(fileobj, size=min(remaining, chunksize))
        remaining -= len(chunk)
        yield chunk


## http://code.activestate.com/recipes/576949/ (r3)
def itersubclasses(cls, _seen=None):
    """
    itersubclasses(cls)

    Generator over all subclasses of a given class, in depth first order.

    >>> list(itersubclasses(int)) == [bool]
    True
    >>> class A(object): pass
    >>> class B(A): pass
    >>> class C(A): pass
    >>> class D(B,C): pass
    >>> class E(D): pass
    >>> 
    >>> for cls in itersubclasses(A):
    ...     print(cls.__name__)
    B
    D
    E
    C
    >>> # get ALL (new-style) classes currently defined
    >>> [cls.__name__ for cls in itersubclasses(object)] #doctest: +ELLIPSIS
    ['type', ...'tuple', ...]
    """
    
    if not isinstance(cls, type):
        raise TypeError('itersubclasses must be called with '
                        'new-style classes, not %.100r' % cls)
    if _seen is None: _seen = set()
    try:
        subs = cls.__subclasses__()
    except TypeError: # fails only when cls is type
        subs = cls.__subclasses__(cls)
    for sub in subs:
        if sub not in _seen:
            _seen.add(sub)
            yield sub
            for sub in itersubclasses(sub, _seen):
                yield sub


def normalize_whitespace(s):
    """Strip the string and replace all whitespace sequences and other
    non-printable characters with a single space."""
    assert isinstance(s, str) or isinstance(s, unicode)
    return re.sub(r'[\x00-\x20\s]+', ' ', s.strip())


