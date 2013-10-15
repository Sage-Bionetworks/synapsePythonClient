"""
*****************
Utility Functions
*****************

Utility functions useful in the implementation and testing of the Synapse client.

~~~~~~~~~~~~~~~~~
Property Juggling
~~~~~~~~~~~~~~~~~

.. automethod:: synapseclient.utils.id_of
.. automethod:: synapseclient.utils.get_properties
.. automethod:: synapseclient.utils.is_url
.. automethod:: synapseclient.utils.as_url
.. automethod:: synapseclient.utils.is_synapse_id
.. automethod:: synapseclient.utils.to_unix_epoch_time
.. automethod:: synapseclient.utils.from_unix_epoch_time
.. automethod:: synapseclient.utils.format_time_interval

~~~~~~~~~~~~~
File Handling
~~~~~~~~~~~~~

.. automethod:: synapseclient.utils.md5_for_file
.. automethod:: synapseclient.utils.download_file
.. automethod:: synapseclient.utils.extract_filename
.. automethod:: synapseclient.utils.file_url_to_path
.. automethod:: synapseclient.utils.normalize_whitespace

~~~~~~~~
Chunking
~~~~~~~~

.. autoclass:: synapseclient.utils.Chunk
.. automethod:: synapseclient.utils.chunks
   
~~~~~~~
Testing
~~~~~~~

.. automethod:: synapseclient.utils.make_bogus_data_file
.. automethod:: synapseclient.utils.make_bogus_binary_file

"""

#!/usr/bin/env python2.7

import os, sys, urllib, urlparse, hashlib, re
import random
import collections
import tempfile
import platform
from datetime import datetime as Datetime
from datetime import date as Date
from numbers import Number
from synapseclient.exceptions import *

UNIX_EPOCH = Datetime(1970, 1, 1, 0, 0)
ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
GB = 2**30
MB = 2**20
KB = 2**10


def md5_for_file(filename, block_size=2**20):
    """
    Calculates the MD5 of the given file.  See `source <http://stackoverflow.com/questions/1131220/get-md5-hash-of-a-files-without-open-it-in-python>`_.
    
    :param filename:   The file to read in
    :param block_size: How much of the file to read in at once (bytes).
                       Defaults to 1 MB
    
    :returns: The MD5
    """
    
    md5 = hashlib.md5()
    f = open(filename,'rb')
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return(md5)


def download_file(url, localFilepath=None):
    """
    Downloads a remote file.
    
    :param localFilePath: May be None, in which case a temporary file is created
    
    :returns: A tuple (localFilePath, HTTPmsg).
              See `urllib.urlretrieve <http://docs.python.org/2/library/urllib.html#urllib.urlretrieve>`_
    """
    
    if (localFilepath):
        dir = os.path.dirname(localFilepath)
        if not os.path.exists(dir):
            os.makedirs(dir)
    return urllib.urlretrieve(url, localFilepath)


def extract_filename(content_disposition):
    """
    Extract a filename from an HTTP content-disposition header field.
    
    See `this memo <http://tools.ietf.org/html/rfc6266>`_ 
    and `this package <http://pypi.python.org/pypi/rfc6266>`_ 
    for cryptic details.
    """
    
    match = re.search('filename=([^ ]*)', content_disposition)
    return match.group(1) if match else 'filename'


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

## TODO: what does this do on an unsaved Synapse Entity object?
def id_of(obj):
    """
    Try to figure out the Synapse ID of the given object.  
    
    :param obj: May be a string, Entity object, or dictionary
    
    :returns: The ID or throws an exception
    """
    
    if isinstance(obj, basestring):
        return obj
    if isinstance(obj, Number):
        return str(obj)
    result = _get_from_members_items_or_properties(obj, 'id')
    if result is None:
        raise SynapseMalformedEntityError('Invalid parameters: couldn\'t find id of ' + str(obj))
    return result


def get_properties(entity):
    """Returns the dictionary of properties of the given Entity."""
    
    return entity.properties if hasattr(entity, 'properties') else entity


def is_url(s):
    """Return True if the string appears to be a valid URL."""
    
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
    """Tries to convert the input into a proper URL."""
    
    url_parts = urlparse.urlsplit(s)
    ## Windows drive letter?
    if len(url_parts.scheme)==1 and url_parts.scheme.isalpha():
        return 'file:///%s' % str(s)
    if url_parts.scheme:
        return url_parts.geturl()
    else:
        return 'file://%s' % str(s)


def guess_file_name(string):
    """Tries to derive a filename from an arbitrary string."""
    
    path = urlparse.urlparse(string).path
    path = normalize_path(path)
    tokens = filter(lambda x: x != '', path.split('/'))
    if len(tokens) > 0:
        return tokens[-1]
    
    # Try scrubbing the path of illegal characters
    if len(path) > 0:
        path = re.sub(r"[^a-zA-Z0-9_.+() -]", "", path)
    if len(path) > 0:
        return path
    raise ValueError("Could not derive a name from %s" % string)
    
    
def normalize_path(path):
    """Transforms a path into an absolute path with forward slashes only."""
    if path is None:
        return None
    return re.sub(r'\\', '/', os.path.abspath(path))

def file_url_to_path(url, verify_exists=False):
    """
    Convert a file URL to a path, handling some odd cases around Windows paths.

    :param url: a file URL
    :param verify_exists: If true, return an populated dict only if the
                          resulting file path exists on the local file system.

    :returns: a dict containing keys `path`, `files` and `cacheDir` or an empty
              dict if the URL is not a file URL.
    """
    
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


def is_synapse_id(obj):
    """Returns None iff the input is a Synapse ID."""
    
    if isinstance(obj, basestring):
        m = re.match(r'(syn\d+)', obj)
        if m:
            return m.group(1)
    return None

def _is_date(dt):
    """Objects of class datetime.date and datetime.datetime will be recognized as dates"""
    return isinstance(dt,Date) or isinstance(dt,Datetime)


def _to_list(value):
    """Convert the value (an iterable or a scalar value) to a list."""
    if isinstance(value, collections.Iterable) and not isinstance(value, basestring):
        return list(value)
    else:
        return [value]


def _to_iterable(value):
    """Convert the value (an iterable or a scalar value) to an iterable."""
    if isinstance(value, basestring):
        return (value,)
    if isinstance(value, collections.Iterable):
        return value
    return (value,)


def make_bogus_data_file(n=100, seed=None):
    """
    Makes a bogus data file for testing.  
    It is the caller's responsibility to clean up the file when finished.
    
    :param n:    How many random floating point numbers to be written into the file, separated by commas
    :param seed: Random seed for the random numbers
    
    :returns: The name of the file
    """
    
    if seed is not None:
        random.seed(seed)
    data = [random.gauss(mu=0.0, sigma=1.0) for i in range(n)]

    f = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
    try:
        f.write(", ".join((str(n) for n in data)))
        f.write("\n")
    finally:
        f.close()

    return f.name


def make_bogus_binary_file(n=1*MB):
    """
    Makes a bogus binary data file for testing. 
    It is the caller's responsibility to clean up the file when finished.
    
    :param n:       How many bytes to write
    
    :returns: The name of the file
    """
    
    junk = os.urandom(min(n, 1*MB))
    with tempfile.NamedTemporaryFile(mode='wb', suffix=".dat", delete=False) as f:
        while n > 0:
            f.write(junk[0:min(n, 1*MB)])
            n -= min(n, 1*MB)
    return f.name


def to_unix_epoch_time(dt):
    """
    Convert either `datetime.date or datetime.datetime objects 
    <http://docs.python.org/2/library/datetime.html>`_ to UNIX time.
    """
    
    if type(dt) == Date:
        return (dt - UNIX_EPOCH.date()).total_seconds() * 1000
    return (dt - UNIX_EPOCH).total_seconds() * 1000


def from_unix_epoch_time(ms):
    """Returns a Datetime object given milliseconds since midnight Jan 1, 1970."""
    
    # utcfromtimestamp() fails for negative values (dates before 1970-1-1) on Windows
    # so, here's a hack that enables ancient events, such as Chris's birthday to be
    # converted from milliseconds since the UNIX epoch to higher level Datetime objects. Ha!
    if platform.system()=='Windows' and ms < 0:
        mirror_date = Datetime.utcfromtimestamp(abs(ms)/1000.0)
        return (UNIX_EPOCH - (mirror_date-UNIX_EPOCH))
    return Datetime.utcfromtimestamp(ms/1000.0)


def format_time_interval(seconds):
    """Format a time interval given in seconds to a readable value, e.g. \"5 minutes, 37 seconds\"."""
    
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


def _find_used(activity, predicate):
    """Finds a particular used resource in an activity that matches a predicate."""
    
    for resource in activity['used']:
        if predicate(resource):
            return resource
    return None


BUFFER_SIZE = 8*KB

class Chunk(object):
    """
    A file-like object representing a fixed-size part of a larger file for use
    during chunked file uploading.
    """
    
    ## TODO: implement seek and tell?

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
    Given a file, generate `Chunk` objects from which `chunksize` bytes can be streamed.
    for use during chunked file uploading."""
    
    remaining = os.stat(fileobj.name).st_size
    while remaining > 0:
        chunk = Chunk(fileobj, size=min(remaining, chunksize))
        remaining -= len(chunk)
        yield chunk


def itersubclasses(cls, _seen=None):
    """
    http://code.activestate.com/recipes/576949/ (r3)
    
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
    """
    Strips the string and replace all whitespace sequences and other
    non-printable characters with a single space.
    """
    
    assert isinstance(s, str) or isinstance(s, unicode)
    return re.sub(r'[\x00-\x20\s]+', ' ', s.strip())


def _synapse_error_msg(ex):
    """
    Format a human readable error message
    """
    
    if isinstance(ex, basestring):
        return ex

    return '\n' + ex.__class__.__name__ + ': ' + str(ex) + '\n\n'


