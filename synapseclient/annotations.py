##
## Represent user-defined annotations on a synapse entity
## chris.bare@sagebase.org
############################################################
import collections
import re
from datetime import datetime, date

def _to_list(value):
    if isinstance(value, collections.Iterable) and not isinstance(value, str):
        return list(value)
    else:
        return [value]

## turns a datetime object into a unix epoch time expressed as a float
def _to_unix_epoch_time(dt):
    return (dt - datetime(1970, 1, 1)).total_seconds()

class Annotations(dict):
    """Arbitrary key/value annotations attached to a synapse entity"""
    def __init__(self, **kwargs):
        self.update(kwargs)

    ## convert this Annotations object to synapse-style annotations
    def toSynapseAnnotations(self):
        synapseAnnos = {}
        for key, value in self.iteritems():
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
                elif all((isinstance(elem, datetime) for elem in elements)):
                    synapseAnnos.setdefault('dateAnnotations', {})[key] = [_to_unix_epoch_time(elem) for elem in elements]
                else:
                    synapseAnnos.setdefault('stringAnnotations', {})[key] = [str(elem) for elem in elements]
        return synapseAnnos

    ## create Annotations object from synapse-style annotations
    @classmethod
    def fromSynapseAnnotations(cls, synapseAnnos):
        ## flatten the raw annos to consolidate doubleAnnotations, longAnnotations,
        ## stringAnnotations and dateAnnotations into one dictionary
        annos = cls()
        for key, value in synapseAnnos.iteritems():
            if key=='dateAnnotations':
                for k,v in value.iteritems():
                    ## debug: print "%s=>%s\n" % (k,v,)
                    annos.setdefault(k,[]).extend([datetime.utcfromtimestamp(float(t)) for t in v])
            elif key in ['stringAnnotations','longAnnotations','doubleAnnotations']:
                for k,v in value.iteritems():
                    annos.setdefault(k,[]).extend(v)
            elif key=='blobAnnotations':
                pass ## TODO? blob annotations not supported
            else:
                annos[key] = value
        return annos
