##
## Represent user-defined annotations on a synapse entity
## chris.bare@sagebase.org
############################################################
import collections
import re
import datetime
from datetime import datetime as Datetime
from datetime import date as Date


def _to_list(value):
    if isinstance(value, collections.Iterable) and not isinstance(value, str):
        return list(value)
    else:
        return [value]


## turns a datetime object into a unix epoch time expressed as a float
def _to_unix_epoch_time(dt):
    """
    Convert either datetime.date or datetime.datetime objects to unix times
    (milliseconds since midnight Jan 1, 1970)
    """
    if type(dt) == Date:
        dt = Datetime.combine(dt, datetime.time())
    return (dt - Datetime(1970, 1, 1)).total_seconds() * 1000

def _from_unix_epoch_time(ms):
    """
    Return a datetime object given milliseconds since midnight Jan 1, 1970
    """
    return Datetime.utcfromtimestamp(ms/1000.0)

def _is_date(dt):
    """
    Objects of class datetime.date and datetime.datetime will be recognized as date annotations
    """
    return isinstance(dt,Date) or isinstance(dt,Datetime)

def isSynapseAnnotations(annotations):
    """
    Test if the given object is a synapse-style annotations object,
    based on its keys
    """
    if not isinstance(annotations, collections.Mapping): return False
    annotations_keys = ['id', 'etag', 'creationDate', 'uri', 'stringAnnotations','longAnnotations','doubleAnnotations','dateAnnotations', 'blobAnnotations']
    return all([key in annotations_keys for key in annotations.keys()])


## convert the given dictionary into synapse-style annotations
def toSynapseAnnotations(annotations):
    if isSynapseAnnotations(annotations):
        return annotations
    synapseAnnos = {}
    for key, value in annotations.iteritems():
        if key in ['id', 'etag', 'blobAnnotations', 'creationDate', 'uri']:
            synapseAnnos[key] = value
        elif key in ['stringAnnotations','longAnnotations','doubleAnnotations','dateAnnotations'] and isinstance(value, collections.Mapping):
            synapseAnnos.setdefault(key, {}).update({k:_to_list(v) for k,v in value.iteritems()})
        else:
            elements = _to_list(value)
            if all((isinstance(elem, str) for elem in elements)):
                synapseAnnos.setdefault('stringAnnotations', {})[key] = elements
            elif all((isinstance(elem, int) or isinstance(elem, long) for elem in elements)):
                synapseAnnos.setdefault('longAnnotations', {})[key] = elements
            elif all((isinstance(elem, float) for elem in elements)):
                synapseAnnos.setdefault('doubleAnnotations', {})[key] = elements
            elif all((_is_date(elem) for elem in elements)):
                synapseAnnos.setdefault('dateAnnotations', {})[key] = [_to_unix_epoch_time(elem) for elem in elements]
            # elif all((isinstance(elem, ???) for elem in elements)):
            #     synapseAnnos.setdefault('blobAnnotations', {})[key] = [_to_unix_epoch_time(elem) for elem in elements]
            else:
                synapseAnnos.setdefault('stringAnnotations', {})[key] = [str(elem) for elem in elements]
    return synapseAnnos


## create Annotations object from synapse-style annotations
def fromSynapseAnnotations(annotations):
    """transform a dictionary in synapse annotations format to a simple flat dictionary"""
    ## flatten the raw annos to consolidate doubleAnnotations, longAnnotations,
    ## stringAnnotations and dateAnnotations into one dictionary
    annos = dict()
    for key, value in annotations.iteritems():
        if key=='dateAnnotations':
            for k,v in value.iteritems():
                #debug: print "%s=>%s\n" % (k,v,)
                annos.setdefault(k,[]).extend([_from_unix_epoch_time(float(t)) for t in v])
        elif key in ['stringAnnotations','longAnnotations','doubleAnnotations']:
            for k,v in value.iteritems():
                annos.setdefault(k,[]).extend(v)
        elif key=='blobAnnotations':
            pass ## TODO? blob annotations not supported
        else:
            annos[key] = value
    return annos


