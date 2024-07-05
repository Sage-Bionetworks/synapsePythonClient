"""
Handle running a few tests for benchmark download times from synapse and S3.

Uncomment/comment out the tests you want to/don't want to run.

This tests 3 different methods of downloading files from synapse and S3:
1. `synapseclient.Synapse.getChildren` - This method will traverse the entire synapse
    project and download all files and folders recursively.
2. `synapseutils.syncFromSynapse` - This uses the utility method to traverse the
    entire synapse project and download all files and folders recursively.
3. `aws s3 sync` - This uses the AWS CLI to sync all files and folders from S3.
"""

import os
import shutil
import subprocess  # nosec
from time import perf_counter

import synapseclient
import synapseutils
from synapseclient.models import Folder, Project

PARENT_PROJECT = "syn$FILL_ME_IN"
S3_BUCKET = "s3://$FILL_ME_IN"
S3_PROFILE = "$FILL_ME_IN"

# `uploadTestFiles.py` creates the following folders
FOLDER_10_FILES_10GIB = "download_benchmarking_10_files_10gib"
FOLDER_1_FILES_10GIB = "download_benchmarking_1_files_10gib"
FOLDER_10_FILES_1GIB = "download_benchmarking_10_files_1gib"
FOLDER_100_FILES_100MIB = "download_benchmarking_100_files_100mib"
FOLDER_10_FILES_100MIB = "download_benchmarking_10_files_100mib"
FOLDER_100_FILES_10MIB = "download_benchmarking_100_files_10mib"
FOLDER_1000_FILES_1MIB = "download_benchmarking_1000_files_1mib"


def excute_get_children_synapse_test(
    path: str, syn: synapseclient.Synapse, folder_id: str
) -> None:
    """This test uses the `synapseclient.Synapse.getChildren` method to download files
    for the entire synapse project. This will create the folder on disk and find the folders
    children recursively.


    """
    before = perf_counter()
    children_under_project = syn.getChildren(
        parent=folder_id, includeTypes=["file", "folder"]
    )

    def download_or_create_folder(
        entity: synapseclient.Entity, current_resolved_path: str
    ) -> None:
        is_folder = (
            "type" in entity
            and entity["type"] == "org.sagebionetworks.repo.model.Folder"
        )
        is_file = (
            "type" in entity
            and entity["type"] == "org.sagebionetworks.repo.model.FileEntity"
        )
        if is_folder:
            new_resolved_path = os.path.join(current_resolved_path, entity["name"])
            if not os.path.exists(new_resolved_path):
                os.mkdir(new_resolved_path)
            children_for_folder = syn.getChildren(
                parent=entity["id"], includeTypes=["file", "folder"]
            )

            for child_for_folder in children_for_folder:
                download_or_create_folder(
                    entity=child_for_folder,
                    current_resolved_path=new_resolved_path,
                )
        elif is_file:
            syn.get(
                entity=entity["id"],
                downloadFile=True,
                downloadLocation=current_resolved_path,
            )

    for entity in children_under_project:
        download_or_create_folder(entity=entity, current_resolved_path=path)

    print(f"\nTime to excute_get_children_synapse_test: {perf_counter() - before}")


def execute_synapseutils_sync_from_synapse_test(
    path: str, syn: synapseclient.Synapse, folder_id: str
) -> None:
    """Use the `synapseutils.syncFromSynapse` method to download files for the entire
    synapse project.

    """

    before = perf_counter()
    synapseutils.syncFromSynapse(syn=syn, entity=folder_id, path=path)

    print(f"\nTime to syncFromSynapse: {perf_counter() - before}")


def execute_sync_from_s3(path: str, key_in_bucket: str) -> None:
    """Executes the AWS CLI sync command."""
    time_before_sync = perf_counter()
    subprocess.run(
        [
            "aws",
            "s3",
            "sync",
            f"{S3_BUCKET}/{key_in_bucket}",
            path,
            "--profile",
            S3_PROFILE,
        ],
        check=False,
    )  # nosec

    print(f"\nTime to S3 sync: {perf_counter() - time_before_sync}")


def execute_test_suite(
    path: str, folder_name: str, syn: synapseclient.Synapse, project_id: str
) -> None:
    """Execute the test suite."""
    folder_id = Folder(name=folder_name, parent_id=project_id).get().id

    excute_get_children_synapse_test(path=path, syn=syn, folder_id=folder_id)
    shutil.rmtree(path)

    execute_synapseutils_sync_from_synapse_test(path=path, folder_id=folder_id, syn=syn)
    shutil.rmtree(path)

    # execute_sync_from_s3(path=path, key_in_bucket=project_name)
    # shutil.rmtree(path)


synapse = synapseclient.Synapse(debug=False)
root_path = os.path.expanduser("~/benchmarkingDownload")
if not os.path.exists(root_path):
    os.mkdir(root_path)

# Log-in with ~.synapseConfig `authToken`
synapse.login()
project_id = Project(id=PARENT_PROJECT).get().id

execute_test_suite(
    path=root_path,
    folder_name=FOLDER_1000_FILES_1MIB,
    syn=synapse,
    project_id=project_id,
)


# execute_test_suite(
#     path=root_path,
#     folder_name=FOLDER_100_FILES_10MIB,
#     syn=synapse,
#     project_id=project_id,
# )


# execute_test_suite(
#     path=root_path,
#     folder_name=FOLDER_10_FILES_100MIB,
#     syn=synapse,
#     project_id=project_id,
# )


# execute_test_suite(
#     path=root_path,
#     folder_name=FOLDER_100_FILES_100MIB,
#     syn=synapse,
#     project_id=project_id,
# )

# execute_test_suite(
#     path=root_path,
#     folder_name=FOLDER_10_FILES_1GIB,
#     syn=synapse,
#     project_id=project_id,
# )

# execute_test_suite(
#     path=root_path,
#     folder_name=FOLDER_1_FILES_10GIB,
#     syn=synapse,
#     project_id=project_id,
# )

# execute_test_suite(
#     path=root_path,
#     folder_name=FOLDER_10_FILES_10GIB,
#     syn=synapse,
#     project_id=project_id,
# )
