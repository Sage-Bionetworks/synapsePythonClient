"""The purpose of this script is to demonstrate how to use the new OOP interface for folders.
The following actions are shown in this script:
1. Creating a folder
2. Storing a folder to a project
3. Storing several files to a folder
4. Storing several folders in a folder
5. Getting metadata about a folder
6. Updating the annotations in bulk for a number of folders and files
7. Deleting a folder
"""
import asyncio
import os
from synapseclient.models import (
    File,
    Folder,
)
import synapseclient
from datetime import date, datetime, timedelta, timezone

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


async def store_folder():
    # Creating annotations for my folder ==================================================
    annotations_for_my_folder = {
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

    # Creating a folder ==================================================================
    folder = Folder(
        name="my_new_folder_for_this_project",
        annotations=annotations_for_my_folder,
        parent_id=PROJECT_ID,
        description="This is a folder with random data.",
    )

    folder = await folder.store()

    print("Folder created:")
    print(folder)

    # Updating and storing an annotation =================================================
    folder_copy = await Folder(id=folder.id).get()
    folder_copy.annotations["my_key_string"] = ["new", "values", "here"]
    stored_folder = await folder_copy.store()
    print("Folder updated:")
    print(stored_folder)

    # Storing several files to a folder ==================================================
    files_to_store = []
    for loop in range(1, 10):
        name_of_file = f"my_file_with_random_data_{loop}.txt"
        path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
        create_random_file(path_to_file)

        file = File(
            path=path_to_file,
            name=name_of_file,
        )
        files_to_store.append(file)
    folder.files = files_to_store
    folder = await folder.store()

    # Storing several folders in a folder ==================================================
    folders_to_store = []
    for loop in range(1, 10):
        folder_to_store = Folder(
            name=f"my_new_folder_for_this_project_{loop}",
        )
        folders_to_store.append(folder_to_store)
    folder.folders = folders_to_store
    folder = await folder.store()

    # Getting metadata about a folder =====================================================
    folder_copy = await Folder(id=folder.id).get(include_children=True)

    print("Folder metadata:")
    print(folder_copy)
    for file in folder_copy.files:
        print(f"File: {file.name}")

    for folder in folder_copy.folders:
        print(f"Folder: {folder.name}")

    # Updating the annotations in bulk for a number of folders and files ==================
    new_annotations = {
        "my_new_key_string": ["b", "a", "c"],
    }

    for file in folder_copy.files:
        file.annotations = new_annotations

    for folder in folder_copy.folders:
        folder.annotations = new_annotations

    await folder_copy.store()

    # Deleting a folder ==================================================================
    folder_to_delete = await Folder(
        name="my_new_folder_for_this_project_I_want_to_delete",
        parent_id=PROJECT_ID,
    ).store()

    await folder_to_delete.delete()


asyncio.run(store_folder())
