import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Union
from typing import Optional, TYPE_CHECKING
from opentelemetry import trace, context

# import uuid

from synapseclient import Synapse
from synapseclient.entity import Folder as SynapseFolder
from synapseclient.models import File, Annotations, AnnotationsValue

if TYPE_CHECKING:
    from synapseclient.models import Project

tracer = trace.get_tracer("synapseclient")


@dataclass()
class Folder:
    """Folder is a hierarchical container for organizing data in Synapse."""

    id: str
    """The unique immutable ID for this folder. A new ID will be generated for new
    Folders. Once issued, this ID is guaranteed to never change or be re-issued"""

    name: str
    """The name of this folder. Must be 256 characters or less. Names may only contain:
    letters, numbers, spaces, underscores, hyphens, periods, plus signs, apostrophes,
    and parentheses"""

    # TODO: What are all of the things that could be the parent of a folder?
    parent_id: str
    """The ID of the Project or Folder that is the parent of this Folder."""

    description: Optional[str] = None
    etag: Optional[str] = None
    created_on: Optional[str] = None
    modified_on: Optional[str] = None
    created_by: Optional[str] = None
    modified_by: Optional[str] = None
    concrete_type: Optional[str] = None  # TODO - This is likely not needed

    files: Optional[List["File"]] = field(default_factory=list)
    """Files that exist within this folder."""

    folders: Optional[List["Folder"]] = field(default_factory=list)
    """Folders that exist within this folder."""

    annotations: Optional[Dict[str, AnnotationsValue]] = None
    """Additional metadata associated with the folder. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list."""

    is_loaded: bool = False

    # def __post_init__(self):
    #     # TODO - What is the best way to enforce this, basically we need a minimum amount
    #     # of information to be required such that we can save or load the data properly
    #     if not ((self.name is not None and self.parentId is not None) or self.id is not None):
    #         raise ValueError("Either name and parentId or id must be present")

    async def store(
        self,
        parent: Union["Folder", "Project"],
        synapse_client: Optional[Synapse] = None,
    ):
        """Storing folder and files to synapse."""
        with tracer.start_as_current_span(f"Folder_Store: {self.name}"):
            # TODO - We need to add in some validation before the store to verify we have enough
            # information to store the data

            # Call synapse
            loop = asyncio.get_event_loop()
            synapse_folder = SynapseFolder(self.name, parent=parent.id)
            # TODO: Propogating OTEL context is not working in this case
            entity = await loop.run_in_executor(
                None,
                lambda: Synapse()
                .get_client(synapse_client=synapse_client)
                .store(obj=synapse_folder, opentelemetry_context=context.get_current()),
            )
            print(entity)
            self.id = entity.id
            self.etag = entity.etag

            print(f"Stored folder {self.name}, id: {self.id}")

            tasks = []
            if self.files:
                tasks.extend(
                    file.store(parent=self, synapse_client=synapse_client)
                    for file in self.files
                )

            if self.folders:
                tasks.extend(
                    folder.store(parent=self, synapse_client=synapse_client)
                    for folder in self.folders
                )

            if self.annotations:
                tasks.append(
                    asyncio.create_task(
                        Annotations(
                            id=self.id, etag=self.etag, annotations=self.annotations
                        ).store(synapse_client=synapse_client)
                    )
                )

            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # TODO: Proper exception handling
                for result in results:
                    if isinstance(result, Folder):
                        print(f"Stored {result.name}")
                    elif isinstance(result, File):
                        print(f"Stored {result.name} at: {result.path}")
                    elif isinstance(result, Annotations):
                        print(
                            f"Stored annotations id: {result.id}, etag: {result.etag}"
                        )
                    else:
                        raise ValueError(f"Unknown type: {type(result)}")
            except Exception as ex:
                Synapse().get_client(synapse_client=synapse_client).logger.exception(ex)
                print("I hit an exception")

            print(f"Saved all files and folders in {self.name}")

            return self

    async def get(self):
        """Getting metadata about the folder from synapse."""
        # TODO - We will want to add some recursive logic to this for traversing child files/folders
        print(f"Loading folder {self.name}")
        await asyncio.sleep(1)
        self.is_loaded = True
        return self
