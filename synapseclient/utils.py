#!/usr/bin/env python2.7

# To debug this, python -m pdb myscript.py

import os, urllib, urlparse, hashlib, re
import collections
import tempfile
import datetime
from datetime import datetime as Datetime
from datetime import date as Date
from numbers import Number


def computeMd5ForFile(filename, block_size=2**20):
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
def downloadFile(url, localFilepath=None):
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

def properties(entity):
    return entity.properties if hasattr(entity, 'properties') else entity


def entity_type(entity):
    return _get_from_members_items_or_properties(entity, 'entityType')


def is_url(s):
    """Return True if a string is a valid URL"""
    if isinstance(s, basestring):
        try:
            url_parts = urlparse.urlsplit(s)
            return bool(url_parts.scheme) and bool(url_parts.netloc)
        except Exception as e:
            return False
    return False


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


def make_bogus_data_file(n=100, seed=12345):
    """Make a bogus data file for testing. File will contain 'n'
    random floating point numbers separated by commas. It is the
    caller's responsibility to remove the file when finished.
    """
    import random
    random.seed(seed)
    data = [random.gauss(mu=0.0, sigma=1.0) for i in range(100)]

    try:
        f = tempfile.NamedTemporaryFile(suffix=".txt", delete=False)
        f.write(", ".join((str(n) for n in data)))
        f.write("\n")
    finally:
        f.close()

    return f.name


## turns a datetime object into a unix epoch time expressed as a float
def to_unix_epoch_time(dt):
    """
    Convert either datetime.date or datetime.datetime objects to unix times
    (milliseconds since midnight Jan 1, 1970)
    """
    if type(dt) == Date:
        dt = Datetime.combine(dt, datetime.time(0,0,0))
    return (dt - Datetime(1970, 1, 1)).total_seconds() * 1000


def from_unix_epoch_time(ms):
    """
    Return a datetime object given milliseconds since midnight Jan 1, 1970
    """
    return Datetime.utcfromtimestamp(ms/1000.0)


## a helper method to find a particular used resource in an activity
## that matches a predicate
def _findUsed(activity, predicate):
    for resource in activity['used']:
        if predicate(resource):
            return resource
    return None


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

