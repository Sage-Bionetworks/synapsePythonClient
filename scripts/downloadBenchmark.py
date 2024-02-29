"""
Handle running a few tests for benchmark download times from synapse and S3.

This tests 3 different methods of downloading files from synapse and S3:
1. `synapseclient.Synapse.getChildren` - This method will traverse the entire synapse
    project and download all files and folders recursively.
2. `synapseutils.syncFromSynapse` - This uses the utility method to traverse the
    entire synapse project and download all files and folders recursively.
3. `aws s3 sync` - This uses the AWS CLI to sync all files and folders from S3.
"""
import os
import shutil
from time import perf_counter
import synapseclient
import synapseutils
import subprocess  # nosec

S3_BUCKET = "s3://$FILL_ME_IN"
S3_PROFILE = "$FILL_ME_IN"

PROJECT_25_FILES_1MB = "download_benchmarking_25_files_1mb"
PROJECT_775_FILES_10MB = "download_benchmarking_775_files_10mb"
PROJECT_10_FILES_1GB = "download_benchmarking_10_files_1gb"
PROJECT_10_FILES_100GB = "download_benchmarking_10_files_100gb"


def excute_get_children_synapse_test(
    path: str, syn: synapseclient.Synapse, project_name: str
) -> None:
    """This test uses the `synapseclient.Synapse.getChildren` method to download files
    for the entire synapse project. This will create the folder on disk and find the folders
    children recursively.

    :param path: The path to download to.
    :param syn: The logged in synapse instance.
    :param project_name: The name of the project to download.
    """
    document_path = os.path.expanduser("~/")
    with open(
        os.path.join(document_path, "synapse_download_benchmarking.txt"), "a"
    ) as f:
        f.write(f"Started excute_get_children_synapse_test\n")
        f.close()
    before = perf_counter()
    parent_project_id = syn.store(synapseclient.Project(name=project_name)).id
    children_under_project = syn.getChildren(
        parent=parent_project_id, includeTypes=["file", "folder"]
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
                downloadLocation=os.path.join(current_resolved_path, entity["name"]),
            )

    for entity in children_under_project:
        download_or_create_folder(entity=entity, current_resolved_path=path)

    with open(
        os.path.join(document_path, "synapse_download_benchmarking.txt"), "a"
    ) as f:
        f.write(
            f"Time to excute_get_children_synapse_test: {perf_counter() - before}\n"
        )
        f.close()
    print(f"\nTime to excute_get_children_synapse_test: {perf_counter() - before}")


def execute_synapseutils_sync_from_synapse_test(
    path: str, syn: synapseclient.Synapse, project_name: str
) -> None:
    """Use the `synapseutils.syncFromSynapse` method to download files for the entire
    synapse project.

    :param path: The path to download to.
    :param syn: The logged in synapse instance.
    :param project_name: The name of the project to download.
    """
    document_path = os.path.expanduser("~/")
    with open(
        os.path.join(document_path, "synapse_download_benchmarking.txt"), "a"
    ) as f:
        f.write(f"\nStarted syncFromSynapse\n")
        f.close()
    before = perf_counter()
    project = syn.store(synapseclient.Project(name=project_name))
    synapseutils.syncFromSynapse(syn=syn, entity=project, path=path)

    with open(
        os.path.join(document_path, "synapse_download_benchmarking.txt"), "a"
    ) as f:
        f.write(f"\nTime to syncFromSynapse: {perf_counter() - before}\n")
        f.close()
    print(f"\nTime to syncFromSynapse: {perf_counter() - before}")


def execute_sync_from_s3(path: str, key_in_bucket: str) -> None:
    """Executes the AWS CLI sync command.

    :param path: The path to the root directory
    :param test_name: The name of the test to add to the span name
    """
    document_path = os.path.expanduser("~/")
    with open(
        os.path.join(document_path, "synapse_download_benchmarking.txt"), "a"
    ) as f:
        f.write(f"\nStarted S3 Sync\n")
        f.close()

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
        ]
    )  # nosec

    with open(
        os.path.join(document_path, "synapse_download_benchmarking.txt"), "a"
    ) as f:
        f.write(f"\nTime to S3 sync: {perf_counter() - time_before_sync}\n")
        f.close()
    print(f"\nTime to S3 sync: {perf_counter() - time_before_sync}")


def execute_test_suite(
    path: str, project_name: str, syn: synapseclient.Synapse
) -> None:
    """Execute the test suite.

    :param path: The path to download to.
    :param project_name: The name of the project to download.
    """
    excute_get_children_synapse_test(path=path, syn=syn, project_name=project_name)
    shutil.rmtree(path)

    execute_synapseutils_sync_from_synapse_test(
        path=path, project_name=project_name, syn=syn
    )
    shutil.rmtree(path)

    execute_sync_from_s3(path=path, key_in_bucket=project_name)
    shutil.rmtree(path)


synapse = synapseclient.Synapse(debug=False)
root_path = os.path.expanduser("~/benchmarkingDownload")
if not os.path.exists(root_path):
    os.mkdir(root_path)
# Log-in with ~.synapseConfig `authToken`
synapse.login()

document_path = os.path.expanduser("~/")
with open(os.path.join(document_path, "synapse_download_benchmarking.txt"), "a") as f:
    f.write(f"\nStarted Benchmarking: 25 Files - 1MB\n")
    f.close()
print("25 Files - 1MB")
# ## 25 Files - 1MB -----------------------------------------------------------------------

execute_test_suite(path=root_path, project_name=PROJECT_25_FILES_1MB, syn=synapse)

if not os.path.exists(root_path):
    os.mkdir(root_path)

with open(os.path.join(document_path, "synapse_download_benchmarking.txt"), "a") as f:
    f.write(f"\nStarted Benchmarking: 775 Files - 10MB\n")
    f.close()
print("775 Files - 10MB")
### 775 Files - 10MB ---------------------------------------------------------------------
execute_test_suite(path=root_path, project_name=PROJECT_775_FILES_10MB, syn=synapse)

if not os.path.exists(root_path):
    os.mkdir(root_path)


with open(os.path.join(document_path, "synapse_download_benchmarking.txt"), "a") as f:
    f.write(f"\nStarted Benchmarking: 10 Files - 1GB\n")
    f.close()
print("10 Files - 1GB")
## 10 Files - 1GB -----------------------------------------------------------------------
execute_test_suite(path=root_path, project_name=PROJECT_10_FILES_1GB, syn=synapse)

if not os.path.exists(root_path):
    os.mkdir(root_path)

with open(os.path.join(document_path, "synapse_download_benchmarking.txt"), "a") as f:
    f.write(f"\nStarted Benchmarking: 10 Files - 100GB\n")
    f.close()
print("10 Files - 100GB")
### 10 Files - 100GB ---------------------------------------------------------------------
execute_test_suite(path=root_path, project_name=PROJECT_10_FILES_100GB, syn=synapse)
