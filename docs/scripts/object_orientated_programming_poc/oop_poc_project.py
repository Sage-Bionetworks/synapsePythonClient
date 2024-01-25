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
)
import synapseclient
from datetime import date, datetime, timedelta, timezone

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


async def store_project():
    # Creating annotations for my project ==================================================
    my_annotations = {
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

    # Creating a project =============================================================
    project = Project(
        name="my_new_project_for_testing",
        annotations=my_annotations,
        description="This is a project with random data.",
    )

    project = await project.store()

    print(project)

    # Updating and storing an annotation =============================================
    project_copy = await Project(id=project.id).get()
    project_copy.annotations["my_key_string"] = ["new", "values", "here"]
    stored_project = await project_copy.store()
    print(stored_project)

    # Storing several files to a project =============================================
    files_to_store = []
    for loop in range(1, 10):
        name_of_file = f"my_file_with_random_data_{loop}.txt"
        path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
        create_random_file(path_to_file)

        # Creating and uploading a file to a project =================================
        file = File(
            path=path_to_file,
            name=name_of_file,
            annotations=my_annotations,
        )
        files_to_store.append(file)
    project.files = files_to_store
    project = await project.store()

    # Storing several folders in a project ===========================================
    folders_to_store = []
    for loop in range(1, 10):
        folder_to_store = Folder(
            name=f"my_new_folder_for_this_project_{loop}",
            annotations=my_annotations,
        )
        folders_to_store.append(folder_to_store)
    project.files = []
    project.folders = folders_to_store
    project = await project.store()

    # Getting metadata about a project ===============================================
    project_copy = await Project(id=project.id).get(include_children=True)

    print(project_copy)
    for file in project_copy.files:
        print(f"File: {file.name}")

    for folder in project_copy.folders:
        print(f"Folder: {folder.name}")

    # Updating the annotations in bulk for a number of folders and files =============
    new_annotations = {
        "my_new_key_string": ["b", "a", "c"],
    }

    project_copy = await Project(id=project.id).get(include_children=True)

    for file in project_copy.files:
        file.annotations = new_annotations

    for folder in project_copy.folders:
        folder.annotations = new_annotations

    await project_copy.store()

    # Deleting a project =============================================================
    project_to_delete = await Project(
        name="my_new_project_I_want_to_delete",
    ).store()

    await project_to_delete.delete()


asyncio.run(store_project())
