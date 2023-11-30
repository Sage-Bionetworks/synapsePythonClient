import asyncio
from dataclasses import dataclass, field
from typing import List, Dict

# import uuid

from synapseclient.entity import Project as SynapseProject
from opentelemetry import trace, context

from typing import Optional

from synapseclient.models import Folder, File, Annotations, AnnotationsValue
from synapseclient import Synapse


tracer = trace.get_tracer("synapseclient")


@dataclass()
class Project:
    id: str
    """The unique immutable ID for this project. A new ID will be generated for new
    Projects. Once issued, this ID is guaranteed to never change or be re-issued"""

    name: str
    """The name of this project. Must be 256 characters or less. Names may only contain:
    letters, numbers, spaces, underscores, hyphens, periods, plus signs, apostrophes,
    and parentheses"""

    description: Optional[str] = None
    etag: Optional[str] = None
    created_on: Optional[str] = None
    modified_on: Optional[str] = None
    created_by: Optional[str] = None
    modified_by: Optional[str] = None
    concrete_type: Optional[str] = None
    alias: Optional[str] = None

    files: Optional[List["File"]] = field(default_factory=list)
    """Any files that are at the root directory of the project."""

    folders: Optional[List["Folder"]] = field(default_factory=list)
    """Any folders that are at the root directory of the project."""

    annotations: Optional[Dict[str, AnnotationsValue]] = None
    """Additional metadata associated with the folder. The key is the name of your
    desired annotations. The value is an object containing a list of values
    (use empty list to represent no values for key) and the value type associated with
    all values in the list."""

    is_loaded: bool = False

    # TODO: What if I don't handle queue size, but handle it in the HTTP REST API layer?
    # TODO: https://www.python-httpx.org/advanced/#pool-limit-configuration
    # TODO: Test out changing the underlying layer to httpx

    async def store(self, synapse_client: Optional[Synapse] = None):
        """Storing project, files, and folders to synapse."""

        with tracer.start_as_current_span(f"Project_Store: {self.name}"):
            # Call synapse
            loop = asyncio.get_event_loop()
            synapse_project = SynapseProject(self.name)
            # TODO: Propogating OTEL context is not working in this case
            entity = await loop.run_in_executor(
                None,
                lambda: Synapse()
                .get_client(synapse_client=synapse_client)
                .store(
                    obj=synapse_project, opentelemetry_context=context.get_current()
                ),
            )
            print(entity)
            self.id = entity.id
            self.etag = entity.etag

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

            print(f"Saved all files and folders  in {self.name}")

            return self

    async def get(self):
        """Getting metadata about the project from synapse."""
        print(f"Loading project {self.name}")
        await asyncio.sleep(1)
        self.is_loaded = True
        return self
