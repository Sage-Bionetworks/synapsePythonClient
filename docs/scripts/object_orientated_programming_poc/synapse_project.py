"""The purpose of this script is to demonstrate how to use the new OOP interface for projects.
The following actions are shown in this script:
1. Creating a project
2. Getting metadata about a project
3. Storing several files to a project
4. Storing several folders in a project with a file in each folder
5. Updating the annotations in bulk for a number of folders and files
6. Syncing a project from Synapse to disk
7. Deleting a project

All steps also include setting a number of annotations for the objects.
"""

import os
import uuid
from datetime import datetime, timedelta, timezone

import synapseclient
from synapseclient.models import File, Folder, Project

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


# Creating annotations for my project ==================================================
# Annotations are plain dictionaries on the model. Values may be single items or lists.
my_annotations_dict = {
    "my_key_string": ["b", "a", "c"],
    "my_key_bool": [False, False, False],
    "my_key_double": [1.2, 3.4, 5.6],
    "my_key_long": [1, 2, 3],
    "my_key_timestamp": [
        datetime.today(),
        datetime.today() - timedelta(days=1),
        datetime.now(tz=timezone(timedelta(hours=-5))),
        datetime(2023, 12, 7, 13, 0, 0, tzinfo=timezone(timedelta(hours=0))),
        datetime(2023, 12, 7, 13, 0, 0, tzinfo=timezone(timedelta(hours=-7))),
    ],
}

# Creating a project =====================================================================
project = Project(
    name="my_new_project_for_testing_synapse_client",
    annotations=my_annotations_dict,
    description="This is a project with random data.",
)

my_stored_project = project.store()

print(my_stored_project)

# Getting metadata about a project =======================================================
my_project = Project(id=my_stored_project.id).get()
print(my_project)

# Storing several files to a project =====================================================
for loop in range(1, 10):
    name_of_file = f"my_file_with_random_data_{loop}.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)

    # Creating and uploading a file to a project =====================================
    # Setting the annotations directly on the model stores them with the file.
    file = File(
        path=path_to_file,
        name=name_of_file,
        parent_id=my_stored_project.id,
        annotations=my_annotations_dict,
    )
    file.store()

# Storing several folders to a project ===================================================
for loop in range(1, 10):
    # Creating and uploading a folder to a project ===================================
    folder = Folder(
        name=f"my_folder_{loop}",
        parent_id=my_stored_project.id,
        annotations=my_annotations_dict,
    )
    my_stored_folder = folder.store()

    # Adding a file to a folder ======================================================
    name_of_file = f"my_file_with_random_data_{uuid.uuid4()}.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)

    file = File(
        path=path_to_file,
        name=name_of_file,
        parent_id=my_stored_folder.id,
        annotations=my_annotations_dict,
    )
    file.store()

# Updating the annotations in bulk for a number of folders and files =====================
new_annotations = {
    "my_key_string": ["bbbbb", "aaaaa", "ccccc"],
}

# `sync_from_synapse` retrieves the project along with all of the folders and files
# under it. Setting `download_file=False` retrieves metadata only. Use `recursive=True`
# to also walk into nested folders.
project_copy = Project(id=my_stored_project.id).sync_from_synapse(download_file=False)

for file in project_copy.files:
    file.annotations = new_annotations
    file.store()

for folder in project_copy.folders:
    folder.annotations = new_annotations
    folder.store()

# Syncing a project from Synapse to disk =================================================
# This downloads all files and folders under the project and writes them to disk. A
# manifest TSV with the metadata for everything under the project is created alongside
# the downloaded files.
project_download_location = os.path.expanduser("~/my_synapse_project")
Project(id=my_stored_project.id).sync_from_synapse(
    download_file=True, path=project_download_location, recursive=True
)

# Creating and then deleting a project ===================================================
project = Project(
    name="my_new_project_for_testing_synapse_client_that_will_be_deleted",
    annotations=my_annotations_dict,
    description="This is a project with random data.",
)

my_stored_project = project.store()
my_stored_project.delete()
