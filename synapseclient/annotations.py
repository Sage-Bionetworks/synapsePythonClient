"""
# Annotations

Annotations are arbitrary metadata attached to Synapse entities.
They can be accessed like ordinary object properties or like dictionary keys:

```python
entity.my_annotation = 'This is one way to do it'
entity['other_annotation'] = 'This is another'
```

Annotations can be given in the constructor for Synapse Entities:

```python
entity = File('data.xyz', parent=my_project, rating=9.1234)
```

Annotate the entity with location data:

```python
entity.lat_long = [47.627477, -122.332154]
```

Record when we collected the data. **This will use the current timezone of the machine
running the code.**

```python
from datetime import datetime as Datetime
entity.collection_date = Datetime.now()
```

Record when we collected the data in UTC:

```python
from datetime import datetime as Datetime
entity.collection_date = Datetime.utcnow()
```

You may also use a Timezone aware datetime object like the following example. Using the
[pytz library](https://pypi.org/project/pytz/) is recommended for this purpose.:

```python
from datetime import datetime as Datetime, timezone as Timezone, timedelta as Timedelta

date = Datetime(2023, 12, 20, 8, 10, 0, tzinfo=Timezone(Timedelta(hours=-5)))
```

See:

- [synapseclient.Synapse.get_annotations][]
- [synapseclient.Synapse.set_annotations][]

## Annotating data sources

Data sources are best recorded using Synapse's
[Activity/Provenance][synapseclient.Activity] tools.

## Implementation details

In Synapse, entities have both properties and annotations. Properties are used by the
system, whereas annotations are completely user defined. In the Python client,
we try to present this situation as a normal object, with one set of properties.



See also:

- [Read more about Properties vs Annotations](../../explanations/properties_vs_annotations/)
- [synapseclient.entity.Entity][]

"""

import collections
import datetime
import typing

from synapseclient.core.utils import (
    from_unix_epoch_time,
    id_of,
    is_date,
    to_list,
    to_unix_epoch_time,
)

from .entity import Entity


def _identity(x):
    return x


def raise_anno_type_error(anno_type: str):
    raise ValueError(f"Unknown type in annotations response: {anno_type}")


ANNO_TYPE_TO_FUNC: typing.Dict[
    str, typing.Callable[[str], typing.Union[str, int, float, datetime.datetime]]
] = collections.defaultdict(
    raise_anno_type_error,
    {
        "STRING": _identity,
        "BOOLEAN": lambda bool_str: bool_str == "true",
        "LONG": int,
        "DOUBLE": float,
        "TIMESTAMP_MS": lambda time_str: from_unix_epoch_time(int(time_str)),
    },
)


def is_synapse_annotations(annotations: typing.Mapping) -> bool:
    """Tests if the given object is a Synapse-style Annotations object.

    Arguments:
        annotations: A key-value mapping that may or may not be a Synapse-style
        Annotations object.

    Returns:
        True if the given object is a Synapse-style Annotations object, False
        otherwise.
    """
    if not isinstance(annotations, collections.abc.Mapping):
        return False
    return annotations.keys() >= {"id", "etag", "annotations"}


def _annotation_value_list_element_type(annotation_values: typing.List):
    if not annotation_values:
        raise ValueError("annotations value list can not be empty")

    first_element_type = type(annotation_values[0])

    if all(isinstance(x, first_element_type) for x in annotation_values):
        return first_element_type

    return object


def is_submission_status_annotations(annotations: collections.abc.Mapping) -> bool:
    """Tests if the given dictionary is in the form of annotations to submission
    status.

    Arguments:
        annotations: A key-value mapping that may or may not be a submission status
        annotations object.

    Returns:
        True if the given object is a submission status annotations object, False
        otherwise.
    """
    keys = ["objectId", "scopeId", "stringAnnos", "longAnnos", "doubleAnnos"]
    if not isinstance(annotations, collections.abc.Mapping):
        return False
    return all([key in keys for key in annotations.keys()])


def to_submission_status_annotations(annotations, is_private=True):
    """
    Converts a normal dictionary to the format used to annotate submission statuses, which is different from the format
    used to annotate entities.

    Arguments:
        annotations: A normal Python dictionary whose values are strings, floats, ints or doubles.
        is_private: Set privacy on all annotations at once. These can be set individually using
                    [set_privacy][synapseclient.annotations.set_privacy].


    Example: Using this function
        Adding and converting annotations

            import synapseclient
            from synapseclient.annotations import to_submission_status_annotations
            from datetime import datetime as Datetime

            ## Initialize a Synapse object & authenticate
            syn = synapseclient.Synapse()
            syn.login()

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
        if key in ["objectId", "scopeId", "stringAnnos", "longAnnos", "doubleAnnos"]:
            synapseAnnos[key] = value
        elif isinstance(value, bool):
            synapseAnnos.setdefault("stringAnnos", []).append(
                {"key": key, "value": str(value).lower(), "isPrivate": is_private}
            )
        elif isinstance(value, int):
            synapseAnnos.setdefault("longAnnos", []).append(
                {"key": key, "value": value, "isPrivate": is_private}
            )
        elif isinstance(value, float):
            synapseAnnos.setdefault("doubleAnnos", []).append(
                {"key": key, "value": value, "isPrivate": is_private}
            )
        elif isinstance(value, str):
            synapseAnnos.setdefault("stringAnnos", []).append(
                {"key": key, "value": value, "isPrivate": is_private}
            )
        elif is_date(value):
            synapseAnnos.setdefault("longAnnos", []).append(
                {
                    "key": key,
                    "value": to_unix_epoch_time(value),
                    "isPrivate": is_private,
                }
            )
        else:
            synapseAnnos.setdefault("stringAnnos", []).append(
                {"key": key, "value": str(value), "isPrivate": is_private}
            )
    return synapseAnnos


# TODO: this should accept a status object and return its annotations or an empty dict if there are none
def from_submission_status_annotations(annotations) -> dict:
    """
    Convert back from submission status annotation format to a normal dictionary.

    Arguments:
        annotations: A dictionary in the format used to annotate submission statuses.

    Returns:
        A normal Python dictionary.

    Example: Using this function
        Converting from submission status annotations

            from synapseclient.annotations import from_submission_status_annotations

            submission_status.annotations = from_submission_status_annotations(submission_status.annotations)
    """
    dictionary = {}
    for key, value in annotations.items():
        if key in ["stringAnnos", "longAnnos"]:
            dictionary.update({kvp["key"]: kvp["value"] for kvp in value})
        elif key == "doubleAnnos":
            dictionary.update({kvp["key"]: float(kvp["value"]) for kvp in value})
        else:
            dictionary[key] = value
    return dictionary


def set_privacy(
    annotations,
    key,
    is_private=True,
    value_types=["longAnnos", "doubleAnnos", "stringAnnos"],
):
    """
    Set privacy of individual annotations, where annotations are in the format used by Synapse SubmissionStatus objects.
    See the [Annotations documentation](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/annotation/Annotations.html).

    Arguments:
        annotations: Annotations that have already been converted to Synapse format using
                        [to_submission_status_annotations][synapseclient.annotations.to_submission_status_annotations].
        key:         The key of the annotation whose privacy we're setting.
        is_private:  If False, the annotation will be visible to users with READ permission on the evaluation.
                        If True, the it will be visible only to users with READ_PRIVATE_SUBMISSION on the evaluation.
        value_types: A list of the value types in which to search for the key.

    """
    for value_type in value_types:
        kvps = annotations.get(value_type, None)
        if kvps:
            for kvp in kvps:
                if kvp["key"] == key:
                    kvp["isPrivate"] = is_private
                    return kvp
    raise KeyError('The key "%s" couldn\'t be found in the annotations.' % key)


class Annotations(dict):
    """
    Represent Synapse Entity annotations as a flat dictionary with the system assigned properties id, etag
    as object attributes.

    Attributes:
        id: Synapse ID of the Entity
        etag: Synapse etag of the Entity
        values: (Optional) dictionary of values to be copied into annotations
        **kwargs: additional key-value pairs to be added as annotations

    Example: Creating a few instances
        Creating and setting annotations

            from synapseclient import Annotations

            example1 = Annotations('syn123','40256475-6fb3-11ea-bb0a-9cb6d0d8d984', {'foo':'bar'})
            example2 = Annotations('syn123','40256475-6fb3-11ea-bb0a-9cb6d0d8d984', foo='bar')
            example3 = Annotations('syn123','40256475-6fb3-11ea-bb0a-9cb6d0d8d984')
            example3['foo'] = 'bar'
    """

    id: str
    etag: str

    def __init__(
        self,
        id: typing.Union[str, int, Entity],
        etag: str,
        values: typing.Dict = None,
        **kwargs,
    ):
        """
        Create an Annotations object taking key value pairs from a dictionary or from keyword arguments.
        System properties id, etag, creationDate and uri become attributes of the object.

        Attributes:
            id:  A Synapse ID, a Synapse Entity object, a plain dictionary in which 'id' maps to a Synapse ID
            etag: etag of the Synapse Entity
            values: (Optional) dictionary of values to be copied into annotations
            **kwargs: additional key-value pairs to be added as annotations

        Example: Creating a few instances
            Creating and setting annotations

                from synapseclient import Annotations

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
    """Transforms a simple flat dictionary to a Synapse-style Annotation object. See
    the [Synapse API](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/annotation/v2/Annotations.html)
    documentation for more information on Synapse-style Annotation objects.

    Arguments:
        annotations: A simple flat dictionary of annotations.

    Returns:
        A Synapse-style Annotation dict.
    """

    if is_synapse_annotations(annotations):
        return annotations
    synapse_annos = {}

    if not isinstance(annotations, Annotations):
        raise TypeError(
            "annotations must be a synapseclient.Annotations object with 'id' and"
            " 'etag' attributes"
        )

    synapse_annos["id"] = annotations.id
    synapse_annos["etag"] = annotations.etag

    synapse_annos["annotations"] = _convert_to_annotations_list(annotations)
    return synapse_annos


def _convert_to_annotations_list(annotations):
    nested_annos = {}
    for key, value in annotations.items():
        elements = to_list(value)
        element_cls = _annotation_value_list_element_type(elements)
        if issubclass(element_cls, str):
            nested_annos[key] = {"type": "STRING", "value": elements}
        elif issubclass(element_cls, bool):
            nested_annos[key] = {
                "type": "BOOLEAN",
                "value": ["true" if e else "false" for e in elements],
            }
        elif issubclass(element_cls, int):
            nested_annos[key] = {"type": "LONG", "value": [str(e) for e in elements]}
        elif issubclass(element_cls, float):
            nested_annos[key] = {"type": "DOUBLE", "value": [str(e) for e in elements]}
        elif issubclass(element_cls, (datetime.date, datetime.datetime)):
            nested_annos[key] = {
                "type": "TIMESTAMP_MS",
                "value": [str(to_unix_epoch_time(e)) for e in elements],
            }
        else:
            nested_annos[key] = {"type": "STRING", "value": [str(e) for e in elements]}
    return nested_annos


def from_synapse_annotations(
    raw_annotations: typing.Dict[str, typing.Any]
) -> Annotations:
    """Transforms a Synapse-style Annotation object to a simple flat dictionary.

    Arguments:
        raw_annotations: A Synapse-style Annotation dict.

    Returns:
        A simple flat dictionary of annotations.
    """
    if not is_synapse_annotations(raw_annotations):
        raise ValueError(
            'Unexpected format of annotations from Synapse. Must include keys: "id",'
            ' "etag", and "annotations"'
        )

    annos = Annotations(raw_annotations["id"], raw_annotations["etag"])
    for key, value_and_type in raw_annotations["annotations"].items():
        key: str
        conversion_func = ANNO_TYPE_TO_FUNC[value_and_type["type"]]
        annos[key] = [conversion_func(v) for v in value_and_type["value"]]

    return annos


def check_annotations_changed(bundle_annotations, new_annotations):
    converted_annos = _convert_to_annotations_list(new_annotations)
    return bundle_annotations["annotations"] != converted_annos


def convert_old_annotation_json(annotations):
    """Transforms a parsed JSON dictionary of old style annotations
    into a new style consistent with the entity bundle v2 format.

    This is intended to support some models that were saved as serialized
    entity bundle JSON (Submissions). we don't need to support newer
    types here e.g. BOOLEAN because they did not exist at the time
    that annotation JSON was saved in this form.

    Arguments:
        annotations: A parsed JSON dictionary of old style annotations.

    Returns:
        A v2 Annotation-style dictionary.
    """

    meta_keys = ("id", "etag", "creationDate", "uri")

    type_mapping = {
        "doubleAnnotations": "DOUBLE",
        "stringAnnotations": "STRING",
        "longAnnotations": "LONG",
        "dateAnnotations": "TIMESTAMP_MS",
    }

    annos_v1_keys = set(meta_keys) | set(type_mapping.keys())

    # blobAnnotations appear to be little/unused and there is no mapping defined here but if they
    # are present on the annos we should treat it as an old style annos dict
    annos_v1_keys.add("blobAnnotations")

    # if any keys in the annos dict are not consistent with an old style annotations then we treat
    # it as an annotations2 style dictionary that is not in need of any conversion
    if any(k not in annos_v1_keys for k in annotations.keys()):
        return annotations

    converted = {k: v for k, v in annotations.items() if k in meta_keys}
    converted_annos = converted["annotations"] = {}

    for old_type_key, converted_type in type_mapping.items():
        values = annotations.get(old_type_key)
        if values:
            for k, vs in values.items():
                converted_annos[k] = {
                    "type": converted_type,
                    "value": vs,
                }

    return converted
