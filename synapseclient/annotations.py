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

- :py:meth:`synapseclient.Synapse.get_annotations`
- :py:meth:`synapseclient.Synapse.set_annotations`

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


~~~~~~~
Classes
~~~~~~~
.. autoclass:: synapseclient.annotations.Annotations
    :members:

    .. automethod:: __init__



"""

import collections

from .entity import Entity
from synapseclient.core.utils import to_unix_epoch_time, from_unix_epoch_time, is_date, to_list, id_of
import typing
import datetime


def _identity(x):
    return x


def raise_anno_type_error(anno_type: str):
    raise ValueError(f"Unknown type in annotations response: {anno_type}")


ANNO_TYPE_TO_FUNC: typing.Dict[str, typing.Callable[[str], typing.Union[str, int, float, datetime.datetime]]] = \
    collections.defaultdict(
        raise_anno_type_error,
        {
            'STRING': _identity,
            'BOOLEAN': lambda bool_str: bool_str == 'true',
            'LONG': int,
            'DOUBLE': float,
            'TIMESTAMP_MS': lambda time_str: from_unix_epoch_time(int(time_str))
        }
    )


def is_synapse_annotations(annotations: typing.Mapping):
    """Tests if the given object is a Synapse-style Annotations object."""
    if not isinstance(annotations, collections.abc.Mapping):
        return False
    return annotations.keys() >= {'id', 'etag', 'annotations'}


def _annotation_value_list_element_type(annotation_values: typing.List):
    if not annotation_values:
        raise ValueError("annotations value list can not be empty")

    first_element_type = type(annotation_values[0])

    if all(isinstance(x, first_element_type) for x in annotation_values):
        return first_element_type

    return object


def is_submission_status_annotations(annotations):
    """Tests if the given dictionary is in the form of annotations to submission status"""
    keys = ['objectId', 'scopeId', 'stringAnnos', 'longAnnos', 'doubleAnnos']
    if not isinstance(annotations, collections.abc.Mapping):
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
            synapseAnnos.setdefault('stringAnnos', []) \
                .append({'key': key, 'value': str(value).lower(), 'isPrivate': is_private})
        elif isinstance(value, int):
            synapseAnnos.setdefault('longAnnos', []) \
                .append({'key': key, 'value': value, 'isPrivate': is_private})
        elif isinstance(value, float):
            synapseAnnos.setdefault('doubleAnnos', []) \
                .append({'key': key, 'value': value, 'isPrivate': is_private})
        elif isinstance(value, str):
            synapseAnnos.setdefault('stringAnnos', []) \
                .append({'key': key, 'value': value, 'isPrivate': is_private})
        elif is_date(value):
            synapseAnnos.setdefault('longAnnos', []) \
                .append({'key': key, 'value': to_unix_epoch_time(value), 'isPrivate': is_private})
        else:
            synapseAnnos.setdefault('stringAnnos', []) \
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
     Represent Synapse Entity annotations as a flat dictionary with the system assigned properties id, etag
     as object attributes.
    """
    id: str
    etag: str

    def __init__(self, id: typing.Union[str, int, Entity], etag: str, values: typing.Dict = None, **kwargs):
        """
        Create an Annotations object taking key value pairs from a dictionary or from keyword arguments.
        System properties id, etag, creationDate and uri become attributes of the object.

        :param id:  A Synapse ID, a Synapse Entity object, a plain dictionary in which 'id' maps to a Synapse ID
        :param etag: etag of the Synapse Entity
        :param values:  (Optional) dictionary of values to be copied into annotations

        :param **kwargs: additional key-value pairs to be added as annotations

        Example::

            example1 = Annotations('syn123','40256475-6fb3-11ea-bb0a-9cb6d0d8d984', {'foo':'bar'})

            example2 = Annotations('syn123','40256475-6fb3-11ea-bb0a-9cb6d0d8d984', foo='bar')


            example3 = Annotations('syn123','40256475-6fb3-11ea-bb0a-9cb6d0d8d984')
            example3['foo'] = 'bar'

        """
        super().__init__()

        self.id = id
        self.etag = etag

        if values:
            self.update(values)
        if kwargs:
            self.update(kwargs)

    @property
    def id(self):
        return self._id

    @id.setter
    def id(self, value):
        if value is None:
            raise ValueError("id must not be None")
        self._id = id_of(value)

    @property
    def etag(self):
        return self._etag

    @etag.setter
    def etag(self, value):
        if value is None:
            raise ValueError("etag must not be None")
        self._etag = str(value)


def to_synapse_annotations(annotations: Annotations) -> typing.Dict[str, typing.Any]:
    """Transforms a simple flat dictionary to a Synapse-style Annotation object.
    https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/annotation/v2/Annotations.html
    """

    if is_synapse_annotations(annotations):
        return annotations
    synapse_annos = {}

    if not isinstance(annotations, Annotations):
        raise TypeError("annotations must be a synapseclient.Annotations object with 'id' and 'etag' attributes")

    synapse_annos['id'] = annotations.id
    synapse_annos['etag'] = annotations.etag

    synapse_annos['annotations'] = _convert_to_annotations_list(annotations)
    return synapse_annos


def _convert_to_annotations_list(annotations):
    nested_annos = {}
    for key, value in annotations.items():
        elements = to_list(value)
        element_cls = _annotation_value_list_element_type(elements)
        if issubclass(element_cls, str):
            nested_annos[key] = {'type': 'STRING',
                                 'value': elements}
        elif issubclass(element_cls, bool):
            nested_annos[key] = {'type': 'BOOLEAN',
                                 'value': ['true' if e else 'false' for e in elements]}
        elif issubclass(element_cls, int):
            nested_annos[key] = {'type': 'LONG',
                                 'value': [str(e) for e in elements]}
        elif issubclass(element_cls, float):
            nested_annos[key] = {'type': 'DOUBLE',
                                 'value': [str(e) for e in elements]}
        elif issubclass(element_cls, (datetime.date, datetime.datetime)):
            nested_annos[key] = {'type': 'TIMESTAMP_MS',
                                 'value': [str(to_unix_epoch_time(e)) for e in elements]}
        else:
            nested_annos[key] = {'type': 'STRING',
                                 'value': [str(e) for e in elements]}
    return nested_annos


def from_synapse_annotations(raw_annotations: typing.Dict[str, typing.Any]) -> Annotations:
    """Transforms a Synapse-style Annotation object to a simple flat dictionary."""
    if not is_synapse_annotations(raw_annotations):
        raise ValueError(
            'Unexpected format of annotations from Synapse. Must include keys: "id", "etag", and "annotations"')

    annos = Annotations(raw_annotations['id'], raw_annotations['etag'])
    for key, value_and_type in raw_annotations['annotations'].items():
        key: str
        conversion_func = ANNO_TYPE_TO_FUNC[value_and_type['type']]
        annos[key] = [conversion_func(v) for v in value_and_type['value']]

    return annos


def check_annotations_changed(bundle_annotations, new_annotations):
    converted_annos = _convert_to_annotations_list(new_annotations)
    return bundle_annotations['annotations'] != converted_annos


def convert_old_annotation_json(annotations):
    """Transforms a parsed JSON dictionary of old style annotations
    into a new style consistent with the entity bundle v2 format.

    This is intended to support some models that were saved as serialized
    entity bundle JSON (Submissions). we don't need to support newer
    types here e.g. BOOLEAN because they did not exist at the time
    that annotation JSON was saved in this form.
    """

    meta_keys = ('id', 'etag', 'creationDate', 'uri')

    type_mapping = {
        'doubleAnnotations': 'DOUBLE',
        'stringAnnotations': 'STRING',
        'longAnnotations': "LONG",
        'dateAnnotations': 'TIMESTAMP_MS',
    }

    annos_v1_keys = set(meta_keys) | set(type_mapping.keys())

    # blobAnnotations appear to be little/unused and there is no mapping defined here but if they
    # are present on the annos we should treat it as an old style annos dict
    annos_v1_keys.add('blobAnnotations')

    # if any keys in the annos dict are not consistent with an old style annotations then we treat
    # it as an annotations2 style dictionary that is not in need of any conversion
    if any(k not in annos_v1_keys for k in annotations.keys()):
        return annotations

    converted = {k: v for k, v in annotations.items() if k in meta_keys}
    converted_annos = converted['annotations'] = {}

    for old_type_key, converted_type in type_mapping.items():
        values = annotations.get(old_type_key)
        if values:
            for k, vs in values.items():
                converted_annos[k] = {
                    'type': converted_type,
                    'value': vs,
                }

    return converted
