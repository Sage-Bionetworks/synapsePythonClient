"""
***********
Annotations
***********

Annotations are arbitrary metadata attached to Synapse entities. They can be
accessed like ordinary object properties or like dictionary keys::

    entity.my_annotation = 'This is one way to do it'
    entity['other_annotation'] = 'This is another'

Annotations can be given in the constructor for Synapse Entities::

    entity = File('data.xyz', parent=my_project, rating=9.1234)

Annotate the entity with location data::

    entity.lat_long = [47.627477, -122.332154]

Record when we collected the data::

    from datetime import datetime as Datetime
    entity.collection_date = Datetime.now()

See:

- :py:meth:`synapseclient.Synapse.getAnnotation`
- :py:meth:`synapseclient.Synapse.setAnnotation`

~~~~~~~~~~~~~~~~~~~~~~~
Annotating data sources
~~~~~~~~~~~~~~~~~~~~~~~

Data sources are best recorded using Synapse's `provenance <Activity.html>`_ tools.

~~~~~~~~~~~~~~~~~~~~~~
Implementation details
~~~~~~~~~~~~~~~~~~~~~~

In Synapse, entities have both properties and annotations. Properties are used by
the system, whereas annotations are completely user defined. In the Python client,
we try to present this situation as a normal object, with one set of properties.

For more on the implementation and a few gotchas, see the documentation on
:py:mod:`synapseclient.entity`.

See also:

- :py:class:`synapseclient.entity.Entity`
- :py:mod:`synapseclient.entity`

"""

import collections
from utils import to_unix_epoch_time, from_unix_epoch_time, _is_date, _to_list


def is_synapse_annotations(annotations):
    """Tests if the given object is a Synapse-style Annotations object."""
    
    if not isinstance(annotations, collections.Mapping): return False
    annotations_keys = ['id', 'etag', 'creationDate', 'uri', 'stringAnnotations','longAnnotations','doubleAnnotations','dateAnnotations', 'blobAnnotations']
    return all([key in annotations_keys for key in annotations.keys()])


def to_synapse_annotations(annotations):
    """Transforms a simple flat dictionary to a Synapse-style Annotation object."""
    
    if is_synapse_annotations(annotations):
        return annotations
    synapseAnnos = {}
    for key, value in annotations.iteritems():
        if key in ['id', 'etag', 'blobAnnotations', 'creationDate', 'uri']:
            synapseAnnos[key] = value
        elif key in ['stringAnnotations','longAnnotations','doubleAnnotations','dateAnnotations'] and isinstance(value, collections.Mapping):
            synapseAnnos.setdefault(key, {}).update({k:_to_list(v) for k,v in value.iteritems()})
        else:
            elements = _to_list(value)
            if all((isinstance(elem, basestring) for elem in elements)):
                synapseAnnos.setdefault('stringAnnotations', {})[key] = elements
            elif all((isinstance(elem, bool) for elem in elements)):
                synapseAnnos.setdefault('stringAnnotations', {})[key] = [str(element).lower() for element in elements]
            elif all((isinstance(elem, int) or isinstance(elem, long) for elem in elements)):
                synapseAnnos.setdefault('longAnnotations', {})[key] = elements
            elif all((isinstance(elem, float) for elem in elements)):
                synapseAnnos.setdefault('doubleAnnotations', {})[key] = elements
            elif all((_is_date(elem) for elem in elements)):
                synapseAnnos.setdefault('dateAnnotations', {})[key] = [to_unix_epoch_time(elem) for elem in elements]
            ## TODO: support blob annotations
            # elif all((isinstance(elem, ???) for elem in elements)):
            #     synapseAnnos.setdefault('blobAnnotations', {})[key] = [???(elem) for elem in elements]
            else:
                synapseAnnos.setdefault('stringAnnotations', {})[key] = [str(elem) for elem in elements]
    return synapseAnnos


def from_synapse_annotations(annotations):
    """Transforms a Synapse-style Annotation object to a simple flat dictionary."""
    
    # Flatten the raw annotations to consolidate doubleAnnotations, longAnnotations,
    # stringAnnotations and dateAnnotations into one dictionary
    annos = dict()
    for key, value in annotations.iteritems():
        if key=='dateAnnotations':
            for k,v in value.iteritems():
                # print "%s=>%s\n" % (k,v,)
                annos.setdefault(k,[]).extend([from_unix_epoch_time(float(t)) for t in v])
        elif key in ['stringAnnotations','longAnnotations','doubleAnnotations']:
            for k,v in value.iteritems():
                annos.setdefault(k,[]).extend(v)
        elif key=='blobAnnotations':
            pass ## TODO: blob annotations not supported
        else:
            annos[key] = value
    return annos


