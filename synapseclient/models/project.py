import asyncio
from dataclasses import dataclass, field
from typing import List

# import uuid

from synapseclient.entity import Project
from opentelemetry import trace, context

from typing import Optional, TYPE_CHECKING

from .folder import FolderDataClass
from .file import FileDataClass

# TODO - Is this an issue, is it needed??
if TYPE_CHECKING:
    from synapseclient import Synapse

tracer = trace.get_tracer("synapseclient")

MAX_CO_ROUTINES = 2


@dataclass()
class ProjectDataClass:
    id: str
    name: str
    parentId: str  # TODO - Does a project have a parent?
    synapse: "Synapse"  # TODO: How can we remove the need to pass this in?

    description: Optional[str] = None
    etag: Optional[str] = None
    createdOn: Optional[str] = None
    modifiedOn: Optional[str] = None
    createdBy: Optional[str] = None
    modifiedBy: Optional[str] = None
    concreteType: Optional[str] = None
    alias: Optional[str] = None
    # Files at the root directory of the project
    files: Optional[List["FileDataClass"]] = field(default_factory=list)
    # Folder at the root directory of the project
    folders: Optional[List["FolderDataClass"]] = field(default_factory=list)
    isLoaded: bool = False

    # TODO: What if I don't handle queue size, but handle it in the HTTP REST API layer?
    # TODO: https://www.python-httpx.org/advanced/#pool-limit-configuration
    # TODO: Test out changing the underlying layer to httpx

    async def store(self):
        """Storing project, files, and folders to synapse."""
        with tracer.start_as_current_span(f"Project_Store: {self.name}"):
            # print(f"Storing project {self.name}")
            # await asyncio.sleep(1)

            # Call synapse
            loop = asyncio.get_event_loop()
            synapse_project = Project(self.name)
            # TODO: Propogating OTEL context is not working in this case
            entity = await loop.run_in_executor(
                None,
                lambda: self.synapse.store(
                    obj=synapse_project, opentelemetry_context=context.get_current()
                ),
            )
            print(entity)
            self.id = entity.id

            # # TODO - This is temporary, we need to generate a real id
            # self.id = uuid.uuid4()

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

            print(f"Saved all files and folders  in {self.name}")

            return self

    async def get(self):
        """Getting metadata about the project from synapse."""
        print(f"Loading project {self.name}")
        await asyncio.sleep(1)
        self.isLoaded = True
        return self
