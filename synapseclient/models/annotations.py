"""The required data for working with annotations in Synapse"""

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional, Union

from synapseclient import Synapse
from synapseclient.annotations import ANNO_TYPE_TO_FUNC
from synapseclient.api import set_annotations_async
from synapseclient.core.async_utils import async_to_sync
from synapseclient.models.protocols.annotations_protocol import (
    AnnotationsSynchronousProtocol,
)


@dataclass()
@async_to_sync
class Annotations(AnnotationsSynchronousProtocol):
    """Annotations that can be applied to a number of Synapse resources to provide
    additional information.

    Attributes:
        annotations: Additional metadata associated with the object. The key is the name
            of your desired annotations. The value is an object containing a list of
            string values (use empty list to represent no values for key) and the value
            type associated with all values in the list.
        id: ID of the object to which this annotation belongs. Not required if being
            used as a member variable on another class.
        etag: Etag of the object to which this annotation belongs. This field must match
            the current etag on the object. Not required if being used as a member
            variable on another class.
    """

    annotations: Union[
        Dict[
            str,
            Union[
                List[str],
                List[bool],
                List[float],
                List[int],
                List[date],
                List[datetime],
            ],
        ],
        None,
    ] = field(default_factory=dict)
    """Additional metadata associated with the object. The key is the name of your
    desired annotations. The value is an object containing a list of string values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list.
    """

    id: Optional[str] = None
    """ID of the object to which this annotation belongs. Not required if being used as
    a member variable on another class."""

    etag: Optional[str] = None
    """ Etag of the object to which this annotation belongs. To update an AnnotationV2,
    this field must match the current etag on the object. Not required if being used as
    a member variable on another class."""

    async def store_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Annotations":
        """Storing annotations to synapse.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The stored annotations.

        Raises:
            ValueError: If the id or etag are not set.
        """
        if self.id is None or self.etag is None:
            raise ValueError("id and etag are required to store annotations.")

        result = await set_annotations_async(
            annotations=self,
            synapse_client=synapse_client,
        )
        self.annotations = Annotations.from_dict(result)
        self.etag = result["etag"]
        Synapse.get_client(synapse_client=synapse_client).logger.debug(
            f"Annotations stored for {self.id}"
        )
        return self

    @classmethod
    def from_dict(
        cls, synapse_annotations: dict
    ) -> Union[
        Dict[
            str,
            Union[
                List[str],
                List[bool],
                List[float],
                List[int],
                List[date],
                List[datetime],
            ],
        ],
        None,
    ]:
        """Convert the annotations from the format the synapse rest API works in -
        to the format used by this class.

        Arguments:
            synapse_annotations: The annotations from the synapse rest API.

        Returns:
            The annotations in python class format or None.
        """
        if synapse_annotations is None:
            return None
        annotations = {}
        dict_to_convert = (
            synapse_annotations["annotations"]
            if "annotations" in synapse_annotations
            else synapse_annotations
        )
        for key in dict_to_convert:
            if isinstance(dict_to_convert[key], dict):
                conversion_func = ANNO_TYPE_TO_FUNC[dict_to_convert[key]["type"]]
                annotations[key] = [
                    conversion_func(v) for v in dict_to_convert[key]["value"]
                ]
            else:
                annotations[key] = dict_to_convert[key]

        return annotations
