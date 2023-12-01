import asyncio

from enum import Enum
from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from synapseclient.api import set_annotations
from opentelemetry import trace, context

from synapseclient import Synapse


tracer = trace.get_tracer("synapseclient")


class AnnotationsValueType(str, Enum):
    """The acceptable types that an annotation value can be."""

    STRING = "STRING"
    DOUBLE = "DOUBLE"
    LONG = "LONG"
    TIMESTAMP_MS = "TIMESTAMP_MS"
    BOOLEAN = "BOOLEAN"


@dataclass()
class AnnotationsValue:
    """A specific type of annotation and the values that are of that type."""

    type: AnnotationsValueType
    # TODO: What are all the python types we are going to accept here
    value: List[Union[str, bool]]


@dataclass()
class Annotations:
    """Annotations that can be applied to a number of Synapse resources to provide additional information."""

    annotations: Dict[str, AnnotationsValue]
    """ Additional metadata associated with the object. The key is the name of your
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
    ) -> Dict[str, AnnotationsValue]:
        """Convert the annotations from the synapse API to the model."""
        # TODO: This is not great logic and needs to be revisted. Ideally the annotations
        # TODO: returned as the same during a `.get` and `.store` call. Currently they are not
        # TODO: This also prevents us from using the annotations returned from a `.get` call to store them again.
        # TODO: Also there is difference in timestamp being transferred - The API is expecting milliseconds
        # TODO: But in most cases the python client is returning datetime.
        if synapse_annotations is None:
            return None
        annotations = {}
        dict_to_convert = (
            synapse_annotations["annotations"]
            if "annotations" in synapse_annotations
            else synapse_annotations
        )
        for key in dict_to_convert:
            # TODO: How can we determine which type is being used when it is not provided in the response from the python client.
            value = (
                dict_to_convert[key]["value"]
                if "value" in dict_to_convert[key]
                else dict_to_convert[key]
            )
            annotations[key] = AnnotationsValue(
                type=None,
                value=value,
            )
        return annotations
