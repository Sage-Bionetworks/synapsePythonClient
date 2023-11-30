"""The purpose of this script is to demonstrate how to use the new OOP interface for projects.
The following actions are shown in this script:
1. Creating a project
2. Storing a folder to a project
3. Storing several files to a project
4. Storing several folders in a project
5. Getting metadata about a project
6. Updating the annotations in bulk for a number of folders and files
7. Deleting a project
"""
import asyncio
import os
from synapseclient.models import (
    File,
    Folder,
    Project,
    AnnotationsValueType,
    AnnotationsValue,
)
import synapseclient

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

trace.set_tracer_provider(
    TracerProvider(resource=Resource(attributes={SERVICE_NAME: "oop_project"}))
)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
tracer = trace.get_tracer("my_tracer")

syn = synapseclient.Synapse(debug=True)
syn.login()


def create_random_file(
    path: str,
) -> None:
    """Create a random file with random data.

    :param path: The path to create the file at.
    """
    with open(path, "wb") as f:
        f.write(os.urandom(1))


@tracer.start_as_current_span("Project")
async def store_project():
    # Creating annotations for my project ==================================================
    annotations_for_my_project = {
        "my_key_string": AnnotationsValue(
            type=AnnotationsValueType.STRING, value=["b", "a", "c"]
        ),
        "my_key_bool": AnnotationsValue(
            type=AnnotationsValueType.BOOLEAN, value=[False, False, False]
        ),
        "my_key_double": AnnotationsValue(
            type=AnnotationsValueType.DOUBLE, value=[1.2, 3.4, 5.6]
        ),
        "my_key_long": AnnotationsValue(
            type=AnnotationsValueType.LONG, value=[1, 2, 3]
        ),
        "my_key_timestamp": AnnotationsValue(
            type=AnnotationsValueType.TIMESTAMP_MS, value=[1701362964066, 1577862000000]
        ),
    }

    # Creating a project ==================================================================
    project = Project(
        name="bfauble_my_new_project_for_testing",
        annotations=annotations_for_my_project,
        description="This is a project with random data.",
    )

    project = await project.store()

    print(project)

    # Storing several files to a project ==================================================
    files_to_store = []
    for loop in range(1, 10):
        name_of_file = f"my_file_with_random_data_{loop}.txt"
        path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
        create_random_file(path_to_file)

        # Creating and uploading a file to a project =========================================
        file = File(
            path=path_to_file,
            name=name_of_file,
        )
        files_to_store.append(file)
    project.files = files_to_store
    project = await project.store()

    # Storing several folders in a project ==================================================
    folders_to_store = []
    for loop in range(1, 10):
        folder_to_store = Folder(
            name=f"my_new_folder_for_this_project_{loop}",
        )
        folders_to_store.append(folder_to_store)
    project.folders = folders_to_store
    project = await project.store()

    # Getting metadata about a project =====================================================
    project_copy = await Project(id=project.id).get(include_children=True)

    print(project_copy)
    for file in project_copy.files:
        print(f"File: {file.name}")

    for folder in project_copy.folders:
        print(f"Folder: {folder.name}")

    # Updating the annotations in bulk for a number of folders and files ==================
    new_annotations = {
        "my_new_key_string": AnnotationsValue(
            type=AnnotationsValueType.STRING, value=["b", "a", "c"]
        ),
    }

    for file in project_copy.files:
        file.annotations = new_annotations

    for folder in project_copy.folders:
        folder.annotations = new_annotations

    await project_copy.store()

    # Deleting a project ==================================================================
    project_to_delete = await Project(
        name="my_new_project_I_want_to_delete",
    ).store()

    await project_to_delete.delete()


asyncio.run(store_project())
