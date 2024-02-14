"""
Expects that ~/temp exists and is a directory.

The purpose of this script is to demonstrate how to use the new OOP interface for projects.
The following actions are shown in this script:
1. Creating a project
2. Retrieving a project by id or name
3. Upserting data on a project
4. Storing several files to a project
5. Storing several folders in a project
6. Updating the annotations in bulk for a number of folders and files
7. Downloading an entire project structure to disk
8. Deleting a project
"""

import asyncio
import os
import uuid
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
    """Create a random file with random data."""
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
        "annotation_i_want_to_delete": "I want to delete this annotation",
    }

    # 1) Creating a project ==============================================================
    project = Project(
        name="my_new_project_for_testing",
        annotations=my_annotations,
        description="This is a project with random data.",
        alias="my_project_alias_" + str(uuid.uuid4()).replace("-", "_"),
    )

    project = await project.store()

    print(f"Project created with id: {project.id}")

    # 2) Retrieving a project by id or name ==============================================
    project = await Project(name="my_new_project_for_testing").get()
    print(f"Project retrieved by name: {project.name} with id: {project.id}")

    project = await Project(id=project.id).get()
    print(f"Project retrieved by id: {project.name} with id: {project.id}")

    # 3) Upserting data on a project =====================================================
    # When you have not already use `.store()` or `.get()` on a project any updates will
    # be a non-destructive upsert. This means that if the project does not exist it will
    # be created, if it does exist it will be updated.
    project = await Project(
        name="my_new_project_for_testing", description="my new description"
    ).store()
    print(f"Project description updated to {project.description}")

    # After the instance has interacted with Synapse any changes will be destructive,
    # meaning changes in the data will act as a replacement instead of an addition.
    print(f"Annotations before update: {project.annotations}")
    del project.annotations["annotation_i_want_to_delete"]
    project = await project.store()
    print(f"Annotations after update: {project.annotations}")

    # 4) Storing several files to a project ==============================================
    files_to_store = []
    for loop in range(1, 10):
        name_of_file = f"my_file_with_random_data_{loop}.txt"
        path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
        create_random_file(path_to_file)

        file = File(
            path=path_to_file,
            name=name_of_file,
            annotations=my_annotations,
        )
        files_to_store.append(file)
    project.files = files_to_store
    project = await project.store()

    # 5) Storing several folders in a project ============================================
    folders_to_store = []
    for loop in range(1, 10):
        folder_to_store = Folder(
            name=f"my_folder_for_this_project_{loop}",
            annotations=my_annotations,
        )
        folders_to_store.append(folder_to_store)
    project.folders = folders_to_store
    print(
        f"Storing project ({project.id}) with {len(project.folders)} folders and {len(project.files)} files"
    )
    project = await project.store()

    # 6) Updating the annotations in bulk for a number of folders and files ==============
    project_copy = await Project(id=project.id).sync_from_synapse(download_file=False)

    print(
        f"Found {len(project_copy.files)} files and {len(project_copy.folders)} folder at the root level for {project_copy.name} with id: {project_copy.id}"
    )

    new_annotations = {
        "my_new_key_string": ["b", "a", "c"],
    }

    for file in project_copy.files:
        file.annotations = new_annotations

    for folder in project_copy.folders:
        folder.annotations = new_annotations

    await project_copy.store()

    # 7) Downloading an entire project structure to disk =================================
    print(f"Downloading project ({project.id}) to ~/temp")
    await Project(id=project.id).sync_from_synapse(
        download_file=True, path="~/temp/recursiveDownload", recursive=True
    )

    # 8) Deleting a project ==============================================================
    project_to_delete = await Project(
        name="my_new_project_I_want_to_delete_" + str(uuid.uuid4()).replace("-", "_"),
    ).store()
    print(f"Project created with id: {project_to_delete.id}")

    await project_to_delete.delete()
    print(f"Project with id: {project_to_delete.id} deleted")


asyncio.run(store_project())
