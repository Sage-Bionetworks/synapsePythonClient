"""The purpose of this script is to demonstrate how to use the new OOP interface for files.
The following actions are shown in this script:
1. Creating a file
2. Storing a file to a project
3. Storing a file to a folder
4. Getting metadata about a file
5. Downloading a file
6. Deleting a file
"""
import asyncio
import os

from synapseclient.models import (
    File,
    Folder,
)
from datetime import date, datetime, timedelta, timezone
import synapseclient

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

trace.set_tracer_provider(
    TracerProvider(resource=Resource(attributes={SERVICE_NAME: "oop_table"}))
)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
tracer = trace.get_tracer("my_tracer")

PROJECT_ID = "syn52948289"

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


@tracer.start_as_current_span("File")
async def store_file():
    # Creating annotations for my file ==================================================
    annotations_for_my_file = {
        "my_single_key_string": "a",
        "my_key_string": ["b", "a", "c"],
        "my_key_bool": [False, False, False],
        "my_key_double": [1.2, 3.4, 5.6],
        "my_key_long": [1, 2, 3],
        "my_key_date": [date.today(), date.today() - timedelta(days=1)],
        "my_key_datetime": [
            datetime.today(),
            datetime.today() - timedelta(days=1),
            datetime.now(tz=timezone(timedelta(hours=-5))),
            datetime(2023, 12, 7, 13, 0, 0, tzinfo=timezone(timedelta(hours=0))),
            datetime(2023, 12, 7, 13, 0, 0, tzinfo=timezone(timedelta(hours=-7))),
        ],
    }

    name_of_file = "my_file_with_random_data.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)

    # Creating and uploading a file to a project =========================================
    file = File(
        path=path_to_file,
        name=name_of_file,
        annotations=annotations_for_my_file,
        parent_id=PROJECT_ID,
        description="This is a file with random data.",
    )

    file = await file.store()

    print(file)

    # Updating and storing an annotation =================================================
    file_copy = await File(id=file.id).get()
    file_copy.annotations["my_key_string"] = ["new", "values", "here"]
    stored_file = await file_copy.store()
    print(stored_file)

    # Downloading a file =================================================================
    downloaded_file_copy = await File(id=file.id).get(
        download_location=os.path.expanduser("~/temp/myNewFolder")
    )

    print(downloaded_file_copy)

    # Get metadata about a file ==========================================================
    non_downloaded_file_copy = await File(id=file.id).get(
        download_file=False,
    )

    print(non_downloaded_file_copy)

    # Creating and uploading a file to a folder =========================================
    folder = await Folder(name="my_folder", parent_id=PROJECT_ID).store()

    file = File(
        path=path_to_file,
        name=name_of_file,
        annotations=annotations_for_my_file,
        parent_id=folder.id,
        description="This is a file with random data.",
    )

    file = await file.store()

    print(file)

    downloaded_file_copy = await File(id=file.id).get(
        download_location=os.path.expanduser("~/temp/myNewFolder")
    )

    print(downloaded_file_copy)

    non_downloaded_file_copy = await File(id=file.id).get(
        download_file=False,
    )

    print(non_downloaded_file_copy)

    # Uploading a file and then Deleting a file ==========================================
    name_of_file = "my_file_with_random_data_to_delete.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)

    file = await File(
        path=path_to_file,
        name=name_of_file,
        annotations=annotations_for_my_file,
        parent_id=PROJECT_ID,
        description="This is a file with random data I am going to delete.",
    ).store()

    await file.delete()


asyncio.run(store_file())
