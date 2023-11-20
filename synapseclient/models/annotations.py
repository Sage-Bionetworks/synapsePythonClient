import asyncio

from enum import Enum
from dataclasses import dataclass
from typing import Dict, List, Optional, Union
from synapseclient.api import set_annotations
from opentelemetry import trace

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
            await loop.run_in_executor(
                None,
                lambda: set_annotations(
                    annotations=self, synapse_client=synapse_client
                ),
            )
            print(f"annotations store for {self.id} complete")
            # TODO: From the returned call do we need to update anything in the root object?
        return self

    async def get(self):
        """Get the annotations from synapse."""
        print(f"Getting annotations for id: {self.id}, etag: {self.etag}")
        await asyncio.sleep(1)
        self.is_loaded = True
