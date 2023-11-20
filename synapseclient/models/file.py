import asyncio
from dataclasses import dataclass
from typing import Dict, Union
from opentelemetry import trace, context
from synapseclient.models import AnnotationsValue, Annotations

# import uuid

from synapseclient.entity import File as SynapseFile
from synapseclient import Synapse

from typing import Optional, TYPE_CHECKING


if TYPE_CHECKING:
    from synapseclient.models import Folder, Project


tracer = trace.get_tracer("synapseclient")


@dataclass()
class File:
    id: str
    """The unique immutable ID for this file. A new ID will be generated for new Files.
    Once issued, this ID is guaranteed to never change or be re-issued"""

    name: str
    """The name of this entity. Must be 256 characters or less.
    Names may only contain: letters, numbers, spaces, underscores, hyphens, periods,
    plus signs, apostrophes, and parentheses"""

    path: str
    # TODO - Should a file also have a folder, or a method that figures out the folder class?

    description: Optional[str] = None
    """The description of this file. Must be 1000 characters or less."""

    etag: Optional[str] = None
    created_on: Optional[str] = None
    modified_on: Optional[str] = None
    created_by: Optional[str] = None
    modified_by: Optional[str] = None
    parent_id: Optional[str] = None
    concrete_type: Optional[str] = None
    version_number: Optional[int] = None
    version_label: Optional[str] = None
    version_comment: Optional[str] = None
    is_latest_version: Optional[bool] = False
    data_file_handle_id: Optional[str] = None
    file_name_override: Optional[str] = None

    annotations: Optional[Dict[str, AnnotationsValue]] = None
    """Additional metadata associated with the folder. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list."""

    is_loaded: bool = False

    # TODO: How the parent is stored/referenced needs to be thought through
    async def store(
        self,
        parent: Union["Folder", "Project"],
        synapse_client: Optional[Synapse] = None,
    ):
        """Storing file to synapse."""
        with tracer.start_as_current_span(f"File_Store: {self.path}"):
            # TODO - We need to add in some validation before the store to verify we have enough
            # information to store the data

            # Call synapse
            loop = asyncio.get_event_loop()
            synapse_file = SynapseFile(path=self.path, name=self.name, parent=parent.id)
            # TODO: Propogating OTEL context is not working in this case
            entity = await loop.run_in_executor(
                None,
                lambda: Synapse()
                .get_client(synapse_client=synapse_client)
                .store(obj=synapse_file, opentelemetry_context=context.get_current()),
            )
            print(entity)
            self.id = entity.id
            self.etag = entity.etag

            print(f"Stored file {self.name}, id: {self.id}: {self.path}")

            if self.annotations:
                result = await Annotations(
                    id=self.id, etag=self.etag, annotations=self.annotations
                ).store(synapse_client=synapse_client)
                print(result)

            return self

    async def get(self):
        """Get metadata about the folder from synapse."""
        print(f"Getting file {self.name}")
        await asyncio.sleep(1)
        self.is_loaded = True
