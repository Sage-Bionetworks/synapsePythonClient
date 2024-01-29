"""The purpose of this script is to demonstrate how to use the current synapse interface for projects.
The following actions are shown in this script:
1. Creating a project
2. Getting metadata about a project
3. Storing several files to a project
4. Storing several folders in a project with a file in each folder
5. Updating the annotations in bulk for a number of folders and files
6. Using synapseutils to sync a project from and to synapse
7. Deleting a project

All steps also include setting a number of annotations for the objects.
"""
import os
import synapseclient
import synapseutils
import uuid

from synapseclient import Project, File, Annotations, Folder
from datetime import datetime, timedelta, timezone

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

my_stored_project: Project = syn.store(project)

print(my_stored_project)

# Getting metadata about a project =======================================================
my_project = syn.get(entity=my_stored_project.id)
print(my_project)

# Storing several files to a project =====================================================
for loop in range(1, 10):
    name_of_file = f"my_file_with_random_data_{loop}.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)

    # Creating and uploading a file to a project =====================================
    file = File(
        path=path_to_file,
        name=name_of_file,
        parent=my_stored_project.id,
    )
    my_stored_file = syn.store(obj=file)

    my_annotations = Annotations(
        id=my_stored_file.id,
        etag=my_stored_file.etag,
        **my_annotations_dict,
    )

    syn.set_annotations(annotations=my_annotations)

# Storing several folders to a project ===================================================
for loop in range(1, 10):
    # Creating and uploading a folder to a project ===================================
    folder = Folder(
        name=f"my_folder_{loop}",
        parent=my_stored_project.id,
    )

    my_stored_folder = syn.store(obj=folder)

    my_annotations = Annotations(
        id=my_stored_folder.id,
        etag=my_stored_folder.etag,
        **my_annotations_dict,
    )

    syn.set_annotations(annotations=my_annotations)

    # Adding a file to a folder ======================================================
    name_of_file = f"my_file_with_random_data_{uuid.uuid4()}.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)

    file = File(
        path=path_to_file,
        name=name_of_file,
        parent=my_stored_folder.id,
    )
    my_stored_file = syn.store(obj=file)

    my_annotations = Annotations(
        id=my_stored_file.id,
        etag=my_stored_file.etag,
        **my_annotations_dict,
    )

    syn.set_annotations(annotations=my_annotations)

# Updating the annotations in bulk for a number of folders and files =====================
new_annotations = {
    "my_key_string": ["bbbbb", "aaaaa", "ccccc"],
}

# Note: This `getChildren` function will only return the items that are directly
# under the `parent`. You would need to recursively call this function to get all
# of the children for all folders under the parent.
for child in syn.getChildren(
    parent=my_stored_project.id, includeTypes=["folder", "file"]
):
    is_folder = (
        "type" in child and child["type"] == "org.sagebionetworks.repo.model.Folder"
    )
    is_file = (
        "type" in child and child["type"] == "org.sagebionetworks.repo.model.FileEntity"
    )

    if is_folder:
        my_folder = syn.get(entity=child["id"])
        new_saved_annotations = syn.set_annotations(
            Annotations(id=child["id"], etag=my_folder.etag, **new_annotations)
        )
        print(new_saved_annotations)
    elif is_file:
        my_file = syn.get(entity=child["id"], downloadFile=False)
        new_saved_annotations = syn.set_annotations(
            Annotations(id=child["id"], etag=my_file.etag, **new_annotations)
        )
        print(new_saved_annotations)

# Using synapseutils to sync a project from and to synapse ===============================
# This `syncFromSynapse` will download all files and folders under the project.
# In addition it creates a manifest TSV file that contains the metadata for all
# of the files and folders under the project.
project_download_location = os.path.expanduser("~/my_synapse_project")
result = synapseutils.syncFromSynapse(
    syn=syn, entity=my_stored_project, path=project_download_location
)
print(result)

# This `syncToSynapse` will upload all files and folders under the project that
# are defined in the manifest TSV file.
# ---
# 12/08/2023 note: There is a bug in the `syncToSynapse` method if you are using
# multiple annotations for a single key. This will be fixed in the next few releases.
# Track https://sagebionetworks.jira.com/browse/SYNPY-1357 for more information.
synapseutils.syncToSynapse(
    syn,
    manifestFile=f"{project_download_location}/SYNAPSE_METADATA_MANIFEST.tsv",
    sendMessages=False,
)

# Creating and then deleting a project ===================================================
project = Project(
    name="my_new_project_for_testing_synapse_client_that_will_be_deleted",
    annotations=my_annotations_dict,
    description="This is a project with random data.",
)

my_stored_project: Project = syn.store(project)
syn.delete(obj=my_stored_project.id)
