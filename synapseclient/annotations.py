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
from exceptions import SynapseError


def is_synapse_annotations(annotations):
    """Tests if the given object is a Synapse-style Annotations object."""
    keys=['id', 'etag', 'creationDate', 'uri', 'stringAnnotations','longAnnotations','doubleAnnotations','dateAnnotations', 'blobAnnotations']
    if not isinstance(annotations, collections.Mapping): return False
    return all([key in keys for key in annotations.keys()])


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


def is_submission_status_annotations(annotations):
    """Tests if the given dictionary is in the form of annotations to submission status"""
    keys = ['objectId', 'scopeId', 'stringAnnos','longAnnos','doubleAnnos']
    if not isinstance(annotations, collections.Mapping): return False
    return all([key in keys for key in annotations.keys()])


def to_submission_status_annotations(annotations):
    """
    Converts a normal dictionary to the format used to annotate submission
    statuses, which is different from the format used to annotate entities.

    Example::

        from synapseclient.annotations import to_submission_status_annotations, from_submission_status_annotations
        from datetime import datetime as Datetime

        ## create a submission and get its status
        submission = syn.submit(evaluation, 'syn11111111')
        submission_status = syn.getSubmissionStatus(submission)

        ## add annotations
        submission_status.annotations = {'foo':'bar', 'shoe_size':12, 'IQ':12, 'timestamp':Datetime.now()}

        ## convert annotations
        submission_status.annotations = to_submission_status_annotations(submission_status.annotations)
        submission_status = syn.store(submission_status)


    Synapse categorizes these annotations by: stringAnnos, doubleAnnos,
    longAnnos. If date or blob annotations are supported, they are not
    `documented <http://rest.synapse.org/org/sagebionetworks/repo/model/annotation/Annotations.html>`_
    """
    if is_submission_status_annotations(annotations):
        return annotations
    synapseAnnos = {}
    for key, value in annotations.iteritems():
        if key in ['objectId', 'scopeId', 'stringAnnos','longAnnos','doubleAnnos']:
            synapseAnnos[key] = value
        elif isinstance(value, int) or isinstance(value, long):
            synapseAnnos.setdefault('longAnnos', []).append({ 'key':key, 'value':value })
        elif isinstance(value, float):
            synapseAnnos.setdefault('doubleAnnos', []).append({ 'key':key, 'value':value })
        elif isinstance(value, basestring):
            synapseAnnos.setdefault('stringAnnos', []).append({ 'key':key, 'value':value })
        elif _is_date(value):
            synapseAnnos.setdefault('longAnnos', []).append({ 'key':key, 'value':to_unix_epoch_time(value) })
        else:
            synapseAnnos.setdefault('stringAnnos', []).append({ 'key':key, 'value':unicode(value) })
    return synapseAnnos

def from_submission_status_annotations(annotations):
    """
    Convert back from submission status annotation format to a normal dictionary.

    Example::

        submission_status.annotations = from_submission_status_annotations(submission_status.annotations)
    """
    dictionary = {}
    for key, value in annotations.iteritems():
        if key in ['stringAnnos','longAnnos','doubleAnnos']:
            dictionary.update( { kvp['key']:kvp['value'] for kvp in value } )
        else:
            dictionary[key] = value
    return dictionary

def set_privacy(annotations, key, is_private=True, value_types=['longAnnos', 'doubleAnnos', 'stringAnnos']):
    if key in ['objectId', 'scopeId', 'stringAnnos','longAnnos','doubleAnnos']:
        raise SynapseError('End users shouldn\'t use the key "%s".')
    for value_type in value_types:
        kvps = annotations.get(value_type, None)
        if kvps:
            for kvp in kvps:
                if kvp['key'] == key:
                    kvp['isPrivate'] = is_private
                    return kvp
    raise KeyError('The key "%s" couldn\'t be found in the annotations.')
