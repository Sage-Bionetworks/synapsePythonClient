#!/usr/bin/env python2.7

# To debug this, python -m pdb myscript.py

import os, urllib, urlparse, hashlib, re
import collections
import tempfile
import datetime
from datetime import datetime as Datetime
from datetime import date as Date


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
    return urllib.urlretrieve (url, localFilepath)


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


def id_of(obj):
    try:
        if 'id' in obj:
            return obj['id']
    except:
        pass
    return str(obj)


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


def _to_unix_epoch_time(dt):
    """
    Convert either datetime.date or datetime.datetime objects to unix times
    (milliseconds since midnight Jan 1, 1970)
    """
    if type(dt) == Date:
        dt = Datetime.combine(dt, datetime.time(0,0,0))
    return (dt - Datetime(1970, 1, 1)).total_seconds() * 1000

def _from_unix_epoch_time(ms):
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
