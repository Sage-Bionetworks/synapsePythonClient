import asyncio
from dataclasses import dataclass, field
from typing import List, Union
from opentelemetry import trace, context

# import uuid

from synapseclient.entity import Folder

from typing import Optional, TYPE_CHECKING

from .file import FileDataClass

# TODO - Is this an issue, is it needed??
if TYPE_CHECKING:
    from synapseclient import Synapse
    from .project import ProjectDataClass

MAX_CO_ROUTINES = 2

tracer = trace.get_tracer("synapseclient")


@dataclass()
class FolderDataClass:
    id: str
    name: str
    parentId: str
    synapse: "Synapse"  # TODO: How can we remove the need to pass this in?

    description: Optional[str] = None
    etag: Optional[str] = None
    createdOn: Optional[str] = None
    modifiedOn: Optional[str] = None
    createdBy: Optional[str] = None
    modifiedBy: Optional[str] = None
    concreteType: Optional[str] = None  # TODO - This is likely not needed
    # Files that exist within this folder
    files: Optional[List["FileDataClass"]] = field(default_factory=list)
    # Folders that exist within this folder
    folders: Optional[List["FolderDataClass"]] = field(default_factory=list)
    isLoaded: bool = False

    # def __post_init__(self):
    #     # TODO - What is the best way to enforce this, basically we need a minimum amount
    #     # of information to be required such that we can save or load the data properly
    #     if not ((self.name is not None and self.parentId is not None) or self.id is not None):
    #         raise ValueError("Either name and parentId or id must be present")

    async def store(
        self,
        parent: Union["FolderDataClass", "ProjectDataClass"],
    ):
        """Storing folder and files to synapse."""
        with tracer.start_as_current_span(f"Folder_Store: {self.name}"):
            # TODO - We need to add in some validation before the store to verify we have enough
            # information to store the data
            # print(f"Storing folder {self.name}")
            # await asyncio.sleep(1)

            # Call synapse
            loop = asyncio.get_event_loop()
            synapse_folder = Folder(self.name, parent=parent.id)
            # TODO: Propogating OTEL context is not working in this case
            entity = await loop.run_in_executor(
                None,
                lambda: self.synapse.store(
                    obj=synapse_folder, opentelemetry_context=context.get_current()
                ),
            )
            print(entity)
            self.id = entity.id

            # TODO - This is temporary, we need to generate a real id
            # self.id = uuid.uuid4()

            print(f"Stored folder {self.name}, id: {self.id}")

            tasks = []
            if self.files:
                tasks.extend(file.store(parent=self) for file in self.files)

            if self.folders:
                tasks.extend(folder.store(parent=self) for folder in self.folders)

            try:
                results = await asyncio.gather(*tasks, return_exceptions=True)

                # TODO: Proper exception handling
                for result in results:
                    if isinstance(result, FolderDataClass):
                        print(f"Stored {result.name}")
                    elif isinstance(result, FileDataClass):
                        print(f"Stored {result.name} at: {result.path}")
                    else:
                        raise ValueError(f"Unknown type: {type(result)}")
            except Exception as ex:
                self.synapse.logger.error(ex)
                print("I hit an exception")

            print(f"Saved all files and folders in {self.name}")

            return self

    async def get(self):
        """Getting metadata about the folder from synapse."""
        # TODO - We will want to add some recursive logic to this for traversing child files/folders
        print(f"Loading folder {self.name}")
        await asyncio.sleep(1)
        self.isLoaded = True
        return self
