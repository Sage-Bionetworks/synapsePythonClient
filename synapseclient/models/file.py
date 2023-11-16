import asyncio
from dataclasses import dataclass
from typing import Union
from opentelemetry import trace, context

# import uuid

from synapseclient.entity import File

from typing import Optional, TYPE_CHECKING


# TODO - Is this an issue, is it needed?
if TYPE_CHECKING:
    from synapseclient import Synapse
    from .folder import FolderDataClass
    from .project import ProjectDataClass


tracer = trace.get_tracer("synapseclient")


@dataclass()
class FileDataClass:
    id: str
    name: str
    path: str
    synapse: "Synapse"  # TODO: How can we remove the need to pass this in???
    # TODO - Should a file also have a folder, or a method that figures out the folder class?

    description: Optional[str] = None
    etag: Optional[str] = None
    createdOn: Optional[str] = None
    modifiedOn: Optional[str] = None
    createdBy: Optional[str] = None
    modifiedBy: Optional[str] = None
    parentId: Optional[str] = None
    concreteType: Optional[str] = None
    versionNumber: Optional[int] = None
    versionLabel: Optional[str] = None
    versionComment: Optional[str] = None
    isLatestVersion: Optional[bool] = False
    dataFileHandleId: Optional[str] = None
    fileNameOverride: Optional[str] = None

    isLoaded: bool = False

    # TODO: How the parent is stored/referenced needs to be thought through
    async def store(
        self,
        parent: Union["FolderDataClass", "ProjectDataClass"],
    ):
        """Storing file to synapse."""
        with tracer.start_as_current_span(f"File_Store: {self.path}"):
            # TODO - We need to add in some validation before the store to verify we have enough
            # information to store the data
            # print(f"Storing file {self.name}: {self.path}")
            # await asyncio.sleep(1)

            # Call synapse
            loop = asyncio.get_event_loop()
            synapse_file = File(path=self.path, name=self.name, parent=parent.id)
            # TODO: Propogating OTEL context is not working in this case
            entity = await loop.run_in_executor(
                None,
                lambda: self.synapse.store(
                    obj=synapse_file, opentelemetry_context=context.get_current()
                ),
            )
            print(entity)
            self.id = entity.id

            # TODO - This is temporary, we need to generate a real id
            # self.id = uuid.uuid4()

            print(f"Stored file {self.name}, id: {self.id}: {self.path}")

            return self

    async def get(self):
        """Get metadata about the folder from synapse."""
        print(f"Getting file {self.name}")
        await asyncio.sleep(1)
        self.isLoaded = True
