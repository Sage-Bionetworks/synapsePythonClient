"""
***********
Annotations
***********

Annotations are arbitrary metadata attached to Synapse entities. They can be accessed like ordinary object properties
or like dictionary keys::

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

- :py:meth:`synapseclient.Synapse.getAnnotations`
- :py:meth:`synapseclient.Synapse.setAnnotations`

~~~~~~~~~~~~~~~~~~~~~~~
Annotating data sources
~~~~~~~~~~~~~~~~~~~~~~~

Data sources are best recorded using Synapse's `provenance <Activity.html>`_ tools.

~~~~~~~~~~~~~~~~~~~~~~
Implementation details
~~~~~~~~~~~~~~~~~~~~~~

In Synapse, entities have both properties and annotations. Properties are used by the system, whereas annotations are
completely user defined. In the Python client, we try to present this situation as a normal object, with one set of
properties.

For more on the implementation and a few gotchas, see the documentation on :py:mod:`synapseclient.entity`.

See also:

- :py:class:`synapseclient.entity.Entity`
- :py:mod:`synapseclient.entity`

"""

import collections
import warnings
from synapseclient.core.utils import to_unix_epoch_time, from_unix_epoch_time, is_date, to_list


def is_synapse_annotations(annotations):
    """Tests if the given object is a Synapse-style Annotations object."""
    keys = ['id', 'etag', 'creationDate', 'uri', 'stringAnnotations', 'longAnnotations', 'doubleAnnotations',
            'dateAnnotations', 'blobAnnotations']
    if not isinstance(annotations, collections.Mapping):
        return False
    return all([key in keys for key in annotations.keys()])


def to_synapse_annotations(annotations):
    """Transforms a simple flat dictionary to a Synapse-style Annotation object."""

    if is_synapse_annotations(annotations):
        return annotations
    synapseAnnos = {}
    for key in Annotations.system_properties:
        if hasattr(annotations, key):
            synapseAnnos[key] = getattr(annotations, key)
    for key, value in annotations.items():
        if key in ['id', 'etag', 'blobAnnotations', 'creationDate', 'uri']:
            synapseAnnos[key] = value
        elif key in ['stringAnnotations', 'longAnnotations', 'doubleAnnotations', 'dateAnnotations']\
                and isinstance(value, collections.Mapping):
            synapseAnnos.setdefault(key, {}).update({k: to_list(v) for k, v in value.items()})
        else:
            elements = to_list(value)
            if all((isinstance(elem, str) for elem in elements)):
                synapseAnnos.setdefault('stringAnnotations', {})[key] = elements
            elif all((isinstance(elem, bool) for elem in elements)):
                synapseAnnos.setdefault('stringAnnotations', {})[key] = [str(element).lower() for element in elements]
            elif all((isinstance(elem, int) for elem in elements)):
                synapseAnnos.setdefault('longAnnotations', {})[key] = elements
            elif all((isinstance(elem, float) for elem in elements)):
                synapseAnnos.setdefault('doubleAnnotations', {})[key] = elements
            elif all((is_date(elem) for elem in elements)):
                synapseAnnos.setdefault('dateAnnotations', {})[key] = [to_unix_epoch_time(elem) for elem in elements]
            # TODO: support blob annotations
            # elif all((isinstance(elem, ???) for elem in elements)):
            #     synapseAnnos.setdefault('blobAnnotations', {})[key] = [???(elem) for elem in elements]
            else:
                synapseAnnos.setdefault('stringAnnotations', {})[key] = [str(elem) for elem in elements]
    return synapseAnnos


def from_synapse_annotations(annotations):
    """Transforms a Synapse-style Annotation object to a simple flat dictionary."""

    def process_user_defined_annotations(kvps, annos, func):
        """
        for each annotation of a given class (date, string, double, ...), process the
        annotation with the given function and add it to the dict 'annos'.
        """
        for k, v in kvps.items():
            # don't overwrite system keys which won't be lists
            if k in Annotations.system_properties:
                warnings.warn('A user defined annotation, "%s", has the same name as a system defined annotation and'
                              ' will be dropped. Try syn._getRawAnnotations to get annotations in native Synapse'
                              ' format.' % k)
            else:
                annos.setdefault(k, []).extend([func(elem) for elem in v])

    # Flatten the raw annotations to consolidate doubleAnnotations, longAnnotations,
    # stringAnnotations and dateAnnotations into one dictionary
    annos = Annotations()
    for key, value in annotations.items():
        if key in Annotations.system_properties:
            setattr(annos, key, value)
        elif key == 'dateAnnotations':
            process_user_defined_annotations(value, annos, lambda x: from_unix_epoch_time(float(x)))
        elif key in ['stringAnnotations', 'longAnnotations']:
            process_user_defined_annotations(value, annos, lambda x: x)
        elif key == 'doubleAnnotations':
            process_user_defined_annotations(value, annos, lambda x: float(x))
        elif key == 'blobAnnotations':
            process_user_defined_annotations(value, annos, lambda x: x)
        else:
            warnings.warn('Unknown key in annotations response: %s' % key)
    return annos


def is_submission_status_annotations(annotations):
    """Tests if the given dictionary is in the form of annotations to submission status"""
    keys = ['objectId', 'scopeId', 'stringAnnos', 'longAnnos', 'doubleAnnos']
    if not isinstance(annotations, collections.Mapping):
        return False
    return all([key in keys for key in annotations.keys()])


def to_submission_status_annotations(annotations, is_private=True):
    """
    Converts a normal dictionary to the format used to annotate submission statuses, which is different from the format
    used to annotate entities.

    :param annotations: A normal Python dictionary whose values are strings, floats, ints or doubles

    :param is_private: Set privacy on all annotations at once. These can be set individually using
                       :py:func:`set_privacy`.

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


    Synapse categorizes these annotations by: stringAnnos, doubleAnnos, longAnnos.
    """
    if is_submission_status_annotations(annotations):
        return annotations
    synapseAnnos = {}
    for key, value in annotations.items():
        if key in ['objectId', 'scopeId', 'stringAnnos', 'longAnnos', 'doubleAnnos']:
            synapseAnnos[key] = value
        elif isinstance(value, bool):
            synapseAnnos.setdefault('stringAnnos', [])\
                .append({'key': key, 'value': str(value).lower(), 'isPrivate': is_private})
        elif isinstance(value, int):
            synapseAnnos.setdefault('longAnnos', [])\
                .append({'key': key, 'value': value, 'isPrivate': is_private})
        elif isinstance(value, float):
            synapseAnnos.setdefault('doubleAnnos', [])\
                .append({'key': key, 'value': value, 'isPrivate': is_private})
        elif isinstance(value, str):
            synapseAnnos.setdefault('stringAnnos', [])\
                .append({'key': key, 'value': value, 'isPrivate': is_private})
        elif is_date(value):
            synapseAnnos.setdefault('longAnnos', [])\
                .append({'key': key, 'value': to_unix_epoch_time(value), 'isPrivate': is_private})
        else:
            synapseAnnos.setdefault('stringAnnos', [])\
                .append({'key': key, 'value': str(value), 'isPrivate': is_private})
    return synapseAnnos


# TODO: this should accept a status object and return its annotations or an empty dict if there are none
def from_submission_status_annotations(annotations):
    """
    Convert back from submission status annotation format to a normal dictionary.

    Example::

        submission_status.annotations = from_submission_status_annotations(submission_status.annotations)
    """
    dictionary = {}
    for key, value in annotations.items():
        if key in ['stringAnnos', 'longAnnos']:
            dictionary.update({kvp['key']: kvp['value'] for kvp in value})
        elif key == 'doubleAnnos':
            dictionary.update({kvp['key']: float(kvp['value']) for kvp in value})
        else:
            dictionary[key] = value
    return dictionary


def set_privacy(annotations, key, is_private=True, value_types=['longAnnos', 'doubleAnnos', 'stringAnnos']):
    """
    Set privacy of individual annotations, where annotations are in the format used by Synapse SubmissionStatus objects.
    See the `Annotations documentation \
    <http://docs.synapse.org/rest/org/sagebionetworks/repo/model/annotation/Annotations.html>`_ and the docs regarding
    `querying annotations <http://docs.synapse.org/rest/GET/evaluation/submission/query.html>`_.

    :param annotations: Annotations that have already been converted to Synapse format using
                        :py:func:`to_submission_status_annotations`.
    :param key:         The key of the annotation whose privacy we're setting.
    :param is_private:  If False, the annotation will be visible to users with READ permission on the evaluation.
                        If True, the it will be visible only to users with READ_PRIVATE_SUBMISSION on the evaluation.
                        Note: Is this really correct???
    :param value_types: A list of the value types in which to search for the key. Defaults to all types
                        ['longAnnos', 'doubleAnnos', 'stringAnnos'].

    """
    for value_type in value_types:
        kvps = annotations.get(value_type, None)
        if kvps:
            for kvp in kvps:
                if kvp['key'] == key:
                    kvp['isPrivate'] = is_private
                    return kvp
    raise KeyError('The key "%s" couldn\'t be found in the annotations.' % key)


class Annotations(dict):
    """
    Represent Synapse Entity annotations as a flat dictionary with the system assigned properties id, etag, creationDate
    and uri as object attributes.
    """
    system_properties = ['id', 'etag', 'creationDate', 'uri']

    def __init__(self, *args, **kwargs):
        """
        Create an Annotations object taking key value pairs from a dictionary or from keyword arguments.
        System properties id, etag, creationDate and uri become attributes of the object.
        """
        # make sure all system properties exist
        for key in Annotations.system_properties:
            self.__dict__[key] = None

        for arg in args + (kwargs,):
            if isinstance(arg, collections.Mapping):
                for key in arg:
                    if key in Annotations.system_properties:
                        self.__dict__[key] = arg[key]
                    else:
                        self.__setitem__(key, arg[key])
            else:
                raise ValueError("Unrecognized argument to constructor of Annotations: %s" + str(arg))

    def __getattr__(self, key):
        return self[key]

    def __setattr__(self, key, value):
        if hasattr(self, key):
            return super(Annotations, self).__setattr__(key, value)
        else:
            return self.__setitem__(key, value)


