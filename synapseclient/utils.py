"""
*****************
Utility Functions
*****************

Utility functions useful in the implementation and testing of the Synapse client.

"""
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals
from future.utils import implements_iterator
from builtins import str
import six

try:
    from urllib.parse import urlparse
    from urllib.parse import urlencode
    from urllib.parse import parse_qs
    from urllib.parse import urlunparse
    from urllib.parse import ParseResult
    from urllib.parse import urlsplit
except ImportError:
    from urlparse import urlparse
    from urllib import urlencode
    from urlparse import parse_qs
    from urlparse import urlunparse
    from urlparse import ParseResult
    from urlparse import urlsplit

try:
    import urllib.request, urllib.error
except ImportError:
    import urllib

import os, sys
import hashlib, re
import cgi
import errno
import math
import random
import requests
import collections
import tempfile
import platform
import functools
import threading
import warnings
from datetime import datetime as Datetime
from datetime import date as Date
from datetime import timedelta
from numbers import Number


UNIX_EPOCH = Datetime(1970, 1, 1, 0, 0)
ISO_FORMAT = "%Y-%m-%dT%H:%M:%S.000Z"
ISO_FORMAT_MICROS = "%Y-%m-%dT%H:%M:%S.%fZ"
GB = 2**30
MB = 2**20
KB = 2**10
BUFFER_SIZE = 8*KB


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

    :returns: localFilePath
    """

    f = None
    try:
        if localFilepath:
            dir = os.path.dirname(localFilepath)
            if not os.path.exists(dir):
                os.makedirs(dir)
            f = open(localFilepath, 'wb')
        else:
            f = tempfile.NamedTemporaryFile(delete=False)
            localFilepath = f.name

        r = requests.get(url, stream=True)
        toBeTransferred = float(r.headers['content-length'])
        for nChunks, chunk in enumerate(r.iter_content(chunk_size=1024*10)):
            if chunk:
                f.write(chunk)
                printTransferProgress(nChunks*1024*10, toBeTransferred)
    finally:
        if f:
            f.close()
            printTransferProgress(toBeTransferred, toBeTransferred)

    return localFilepath


def extract_filename(content_disposition_header, default_filename=None):
    """
    Extract a filename from an HTTP content-disposition header field.

    See `this memo <http://tools.ietf.org/html/rfc6266>`_
    and `this package <http://pypi.python.org/pypi/rfc6266>`_
    for cryptic details.
    """

    if not content_disposition_header:
        return default_filename
    value, params = cgi.parse_header(content_disposition_header)
    return params.get('filename', default_filename)


def extract_user_name(profile):
    """
    Extract a displayable user name from a user's profile
    """
    if 'userName' in profile and profile['userName']:
        return profile['userName']
    elif 'displayName' in profile and profile['displayName']:
        return profile['displayName']
    else:
        if 'firstName' in profile and profile['firstName'] and 'lastName' in profile and profile['lastName']:
            return profile['firstName'] + ' ' + profile['lastName']
        elif 'lastName' in profile and profile['lastName']:
            return profile['lastName']
        elif 'firstName' in profile and profile['firstName']:
            return profile['firstName']
        else:
            return str(profile.get('id', 'Unknown-user'))


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
    if isinstance(obj, six.string_types):
        return str(obj)
    if isinstance(obj, Number):
        return str(obj)
    result = _get_from_members_items_or_properties(obj, 'id')
    if result is None:
        result = _get_from_members_items_or_properties(obj, 'ownerId')
    if result is None:
        raise ValueError('Invalid parameters: couldn\'t find id of ' + str(obj))
    return result

def is_in_path(id, path):
    """Determines whether id is in the path as returned from /entity/{id}/path

    :param id: synapse id string
    :param path: object as returned from '/entity/{id}/path'

    :returns: True or False
    """
    return id in [item['id'] for item in path['path']]

def get_properties(entity):
    """Returns the dictionary of properties of the given Entity."""

    return entity.properties if hasattr(entity, 'properties') else entity


def is_url(s):
    """Return True if the string appears to be a valid URL."""
    if isinstance(s, six.string_types):
        try:
            url_parts = urlsplit(s)
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
    url_parts = urlsplit(s)
    ## Windows drive letter?
    if len(url_parts.scheme)==1 and url_parts.scheme.isalpha():
        return 'file:///%s' % str(s).replace("\\","/")
    if url_parts.scheme:
        return url_parts.geturl()
    else:
        return 'file://%s' % str(s)


def guess_file_name(string):
    """Tries to derive a filename from an arbitrary string."""
    path = normalize_path(urlparse(string).path)
    tokens = [x for x in path.split('/') if x != '']
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
    return re.sub(r'\\', '/', os.path.normcase(os.path.abspath(path)))


def equal_paths(path1, path2):
    """
    Compare file paths in a platform neutral way
    """
    return normalize_path(path1) == normalize_path(path2)


def file_url_to_path(url, verify_exists=False):
    """
    Convert a file URL to a path, handling some odd cases around Windows paths.

    :param url: a file URL
    :param verify_exists: If true, return an populated dict only if the
                          resulting file path exists on the local file system.

    :returns: a dict containing keys `path`, `files` and `cacheDir` or an empty
              dict if the URL is not a file URL.
    """
    parts = urlsplit(url)
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


def is_same_base_url(url1, url2):
    """Compares two urls to see if they are the same excluding up to the base path

    :param url1: a URL
    :param url2: a second URL

    :returns: Boolean
    """
    url1 = urlsplit(url1)
    url2 = urlsplit(url2)
    return (url1.scheme==url2.scheme and
            url1.hostname==url2.hostname)


def is_synapse_id(obj):
    """If the input is a Synapse ID return it, otherwise return None"""
    if isinstance(obj, six.string_types):
        m = re.match(r'(syn\d+)', obj)
        if m:
            return m.group(1)
    return None


def _is_date(dt):
    """Objects of class datetime.date and datetime.datetime will be recognized as dates"""
    return isinstance(dt,Date) or isinstance(dt,Datetime)


def _to_list(value):
    """Convert the value (an iterable or a scalar value) to a list."""
    if isinstance(value, collections.Iterable) and not isinstance(value, six.string_types):
        return list(value)
    else:
        return [value]


def _to_iterable(value):
    """Convert the value (an iterable or a scalar value) to an iterable."""
    if isinstance(value, six.string_types):
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

    f = tempfile.NamedTemporaryFile(mode="w", suffix=".txt", delete=False)
    try:
        f.write(", ".join(str(n) for n in data))
        f.write("\n")
    finally:
        f.close()

    return f.name


def make_bogus_binary_file(n=1*MB, filepath=None, printprogress=False):
    """
    Makes a bogus binary data file for testing.
    It is the caller's responsibility to clean up the file when finished.

    :param n:       How many bytes to write

    :returns: The name of the file
    """

    with open(filepath, 'wb') if filepath else tempfile.NamedTemporaryFile(mode='wb', suffix=".dat", delete=False) as f:
        if not filepath:
            filepath = f.name
        progress = 0
        remaining = n
        while remaining > 0:
            buff_size = int(min(remaining, 1*MB))
            f.write(os.urandom(buff_size))
            remaining -= buff_size
            if printprogress:
                progress += buff_size
                printTransferProgress(progress, n, 'Generated ', filepath)
        return filepath


def to_unix_epoch_time(dt):
    """
    Convert either `datetime.date or datetime.datetime objects
    <http://docs.python.org/2/library/datetime.html>`_ to UNIX time.
    """

    if type(dt) == Date:
        return (dt - UNIX_EPOCH.date()).total_seconds() * 1000
    return int((dt - UNIX_EPOCH).total_seconds() * 1000)


def to_unix_epoch_time_secs(dt):
    """
    Convert either `datetime.date or datetime.datetime objects 
    <http://docs.python.org/2/library/datetime.html>`_ to UNIX time.
    """

    if type(dt) == Date:
        return (dt - UNIX_EPOCH.date()).total_seconds()
    return (dt - UNIX_EPOCH).total_seconds()


def from_unix_epoch_time_secs(secs):
    """Returns a Datetime object given milliseconds since midnight Jan 1, 1970."""
    if isinstance(secs, six.string_types):
        secs = float(secs)

    # utcfromtimestamp() fails for negative values (dates before 1970-1-1) on Windows
    # so, here's a hack that enables ancient events, such as Chris's birthday to be
    # converted from milliseconds since the UNIX epoch to higher level Datetime objects. Ha!
    if platform.system()=='Windows' and secs < 0:
        mirror_date = Datetime.utcfromtimestamp(abs(secs))
        return (UNIX_EPOCH - (mirror_date-UNIX_EPOCH))
    return Datetime.utcfromtimestamp(secs)


def from_unix_epoch_time(ms):
    """Returns a Datetime object given milliseconds since midnight Jan 1, 1970."""

    if isinstance(ms, six.string_types):
        ms = float(ms)
    return from_unix_epoch_time_secs(ms/1000.0)


def datetime_to_iso(dt):
    ## Round microseconds to milliseconds (as expected by older clients)
    ## and add back the "Z" at the end.
    ## see: http://stackoverflow.com/questions/30266188/how-to-convert-date-string-to-iso8601-standard
    fmt = "{time.year:04}-{time.month:02}-{time.day:02}T{time.hour:02}:{time.minute:02}:{time.second:02}.{millisecond:03}{tz}"
    if dt.microsecond >= 999500:
        dt -= timedelta(microseconds=dt.microsecond)
        dt += timedelta(seconds=1)
    return fmt.format(time=dt, millisecond=int(round(dt.microsecond/1000.0)), tz="Z")


def iso_to_datetime(iso_time):
    return Datetime.strptime(iso_time, ISO_FORMAT_MICROS)


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
    assert isinstance(s, six.string_types)
    return re.sub(r'[\x00-\x20\s]+', ' ', s.strip())


def normalize_lines(s):
    assert isinstance(s, six.string_types)
    s2 = re.sub(r'[\t ]*\n[\t ]*', '\n', s.strip())
    return re.sub(r'[\t ]+', ' ', s2)


def _synapse_error_msg(ex):
    """
    Format a human readable error message
    """
    if isinstance(ex, six.string_types):
        return ex

    return '\n' + ex.__class__.__name__ + ': ' + str(ex) + '\n\n'


def _limit_and_offset(uri, limit=None, offset=None):
    """
    Set limit and/or offset query parameters of the given URI.
    """
    parts = urlparse(uri)
    query = parse_qs(parts.query)
    if limit is None:
        query.pop('limit', None)
    else:
        query['limit'] = limit
    if offset is None:
        query.pop('offset', None)
    else:
        query['offset'] = offset

    ## in Python 2, urllib expects encoded byte-strings
    if six.PY2:
        new_query = {}
        for k,v in query.items():
            if isinstance(v,list):
                v = [unicode(element).encode('utf-8') for element in v]
            elif isinstance(v,str):
                v = unicode(v).encode('utf-8')
            new_query[unicode(k).encode('utf-8')] = v
        query = new_query

    new_query_string = urlencode(query, doseq=True)
    return urlunparse(ParseResult(
        scheme=parts.scheme,
        netloc=parts.netloc,
        path=parts.path,
        params=parts.params,
        query=new_query_string,
        fragment=parts.fragment))


def query_limit_and_offset(query, hard_limit=1000):
    """
    Extract limit and offset from the end of a query string.

    :returns: A triple containing the query with limit and offset removed, the
              limit at most equal to the hard_limit, and the offset which
              defaults to 1
    """
    # Regex a lower-case string to simplify matching
    tempQueryStr = query.lower()
    regex = '\A(.*\s)(offset|limit)\s*(\d*\s*)\Z'

    # Continue to strip off and save the last limit/offset
    match = re.search(regex, tempQueryStr)
    options = {}
    while match is not None:
        options[match.group(2)] = int(match.group(3))
        tempQueryStr = match.group(1)
        match = re.search(regex, tempQueryStr)

    # Get a truncated version of the original query string (not in lower-case)
    query = query[:len(tempQueryStr)].strip()

    # Continue querying until the entire query has been fetched (or crash out)
    limit = min(options.get('limit',hard_limit), hard_limit)
    offset = options.get('offset',1)

    return query, limit, offset


def _extract_synapse_id_from_query(query):
    """
    An unfortunate hack to pull the synapse ID out of a table query of the
    form "select column1, column2 from syn12345 where...." needed to build
    URLs for table services.
    """
    m = re.search(r"from\s+(syn\d+)[^\s]", query, re.IGNORECASE)
    if m:
        return m.group(1)
    else:
        raise ValueError("Couldn't extract synapse ID from query: \"%s\"" % query)


#Derived from https://wiki.python.org/moin/PythonDecoratorLibrary#Memoize
def memoize(obj):
    cache = obj.cache = {}

    @functools.wraps(obj)
    def memoizer(*args, **kwargs):
        refresh = kwargs.pop('refresh', False)
        key = str(args) + str(kwargs)
        if refresh or key not in cache:
            cache[key] = obj(*args, **kwargs)
        return cache[key]
    return memoizer

def printTransferProgress(transferred, toBeTransferred, prefix = '', postfix='', isBytes=True):
    """Prints a progress bar

    :param transferred: a number of items/bytes completed
    :param toBeTransferred: total number of items/bytes when completed
    :param prefix: String printed before progress bar
    :param prefix: String printed after progress bar
    :param isBytes: A boolean indicating whether to convert bytes to kB, MB, GB etc.

    """
    barLength = 20 # Modify this to change the length of the progress bar
    status = ""
    if toBeTransferred<0:
        defaultToBeTransferred = (barLength*1*MB)
        if transferred > defaultToBeTransferred:
            progress = float(transferred % defaultToBeTransferred) / defaultToBeTransferred
        else:
            progress = float(transferred) / defaultToBeTransferred
    elif toBeTransferred==0:  #There is nothing to be transferred
        progress = 1
        status = "Done...\n"
    else:
        progress = float(transferred) / toBeTransferred
        if progress >= 1:
            progress = 1
            status = "Done...\n"
    block = int(round(barLength*progress))
    nbytes = humanizeBytes(transferred) if isBytes else transferred
    if toBeTransferred>0:
        outOf = "/%s" % (humanizeBytes(toBeTransferred) if isBytes else toBeTransferred)
        percentage = "%4.2f%%"%(progress*100)
    else:
        outOf = ""
        percentage = ""
    text = "\r%s [%s]%s     %s%s %s %s    " % (prefix,
                                               "#"*block + "-"*(barLength-block),
                                               percentage,
                                               nbytes, outOf,
                                               postfix, status)
    sys.stdout.write(text)
    sys.stdout.flush()


def humanizeBytes(bytes):
    bytes = float(bytes)
    units = ['bytes', 'kB', 'MB', 'GB', 'TB', 'PB', 'EB']
    for i, unit in enumerate(units):
        if bytes<1024:
            return '%3.1f%s' %(bytes, units[i])
        else:
            bytes /= 1024
    return 'Oops larger than Exabytes'


def touch(path, times=None):
    """
    Make sure a file exists. Update its access and modified times.
    """
    basedir = os.path.dirname(path)
    if not os.path.exists(basedir):
        try:
            os.makedirs(basedir)
        except OSError as err:
            ## alternate processes might be creating these at the same time
            if err.errno != errno.EEXIST:
                raise

    with open(path, 'a'):
        os.utime(path, times)
    return path


def _is_json(content_type):
    """detect if a content-type is JSON"""
    ## The value of Content-Type defined here:
    ## http://www.w3.org/Protocols/rfc2616/rfc2616-sec3.html#sec3.7
    return content_type.lower().strip().startswith('application/json') if content_type else False


def find_data_file_handle(bundle):
    """Return the fileHandle whose ID matches the dataFileHandleId in an entity bundle"""
    for fileHandle in bundle['fileHandles']:
        if fileHandle['id'] == bundle['entity']['dataFileHandleId']:
            return fileHandle
    return None


def unique_filename(path):
    """Returns a unique path by appending (n) for some number n to the end of the filename."""

    base, ext = os.path.splitext(path)
    counter = 0
    while os.path.exists(path):
        counter += 1
        path = base + ("(%d)" % counter) + ext

    return path


@implements_iterator
class threadsafe_iter:
    """Takes an iterator/generator and makes it thread-safe by
    serializing call to the `next` method of given iterator/generator.
    See: http://anandology.com/blog/using-iterators-and-generators/
    """
    def __init__(self, it):
        self.it = it
        self.lock = threading.Lock()

    def __iter__(self):
        return self

    def __next__(self):
        with self.lock:
            return next(self.it)

def threadsafe_generator(f):
    """A decorator that takes a generator function and makes it thread-safe.
    See: http://anandology.com/blog/using-iterators-and-generators/
    """
    def g(*a, **kw):
        return threadsafe_iter(f(*a, **kw))
    return g


def extract_prefix(keys):
    """
    Takes a list of strings and extracts a common prefix delimited by a dot,
    for example:
    >>> extract_prefix(["entity.bang", "entity.bar", "entity.bat"])
    entity.
    """
    prefixes = set()
    for key in keys:
        parts = key.split(".")
        if len(parts) > 1:
            prefixes.add(parts[0])
        else:
            return ""
    if len(prefixes) == 1:
        return prefixes.pop() + "."
    return ""

