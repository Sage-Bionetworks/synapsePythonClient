import asyncio

from datetime import datetime, date
from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from synapseclient.api import set_annotations
from opentelemetry import trace, context

from synapseclient import Synapse
from synapseclient.annotations import ANNO_TYPE_TO_FUNC


tracer = trace.get_tracer("synapseclient")


@dataclass()
class Annotations:
    """Annotations that can be applied to a number of Synapse resources to provide additional information."""

    annotations: Dict[
        str,
        Union[
            List[str], List[bool], List[float], List[int], List[date], List[datetime]
        ],
    ]
    """Additional metadata associated with the object. The key is the name of your
    desired annotations. The value is an object containing a list of string values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list
    """

    id: Optional[str] = None
    """ID of the object to which this annotation belongs. Not required if being used as
    a member variable on another class."""

    etag: Optional[str] = None
    """ Etag of the object to which this annotation belongs. To update an AnnotationV2,
    this field must match the current etag on the object. Not required if being used as
    a member variable on another class."""

    is_loaded: bool = False

    async def store(
        self,
        synapse_client: Optional[Synapse] = None,
    ):
        """Storing annotations to synapse."""
        # TODO: Validation that id and etag are present

        print(f"Storing annotations for id: {self.id}, etag: {self.etag}")
        with tracer.start_as_current_span(f"Annotation_store: {self.id}"):
            loop = asyncio.get_event_loop()
            current_context = context.get_current()
            result = await loop.run_in_executor(
                None,
                lambda: set_annotations(
                    annotations=self,
                    synapse_client=synapse_client,
                    opentelemetry_context=current_context,
                ),
            )
            print(f"annotations store for {self.id} complete")
            self.annotations = Annotations.convert_from_api_parameters(result)
            # TODO: From the returned call do we need to update anything in the root object?
        return self

    async def get(self):
        """Get the annotations from synapse."""
        print(f"Getting annotations for id: {self.id}, etag: {self.etag}")
        await asyncio.sleep(1)
        self.is_loaded = True

    @classmethod
    def convert_from_api_parameters(
        self, synapse_annotations: dict
    ) -> Dict[str, List[Union[str, bool, float, int, date, datetime]]]:
        """Convert the annotations from the format the synapse rest API works in -
        to the format used by this class.

        :param synapse_annotations: The annotations from the synapse rest API.
        :return: The annotations in python class format.
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
