"""
Create some test files and upload them to Synapse and S3. This is used as the first step
for benchmarking downloads.
"""
import os
import shutil
from synapseclient.entity import Project
import synapseclient
import synapseutils
import subprocess  # nosec

PARENT_PROJECT = "syn$FILL_ME_IN"
S3_BUCKET = "s3://$FILL_ME_IN"
S3_PROFILE = "$FILL_ME_IN"

PROJECT_25_FILES_1MB = "download_benchmarking_25_files_1mb"
PROJECT_775_FILES_10MB = "download_benchmarking_775_files_10mb"
PROJECT_10_FILES_1GB = "download_benchmarking_10_files_1gb"
PROJECT_10_FILES_100GB = "download_benchmarking_10_files_100gb"


def create_folder_structure(
    path: str,
    depth_of_directory_tree: int,
    num_sub_directories: int,
    num_files_per_directory: int,
    total_size_of_files_mbytes: int,
) -> None:
    """Create a tree directory structure starting with `root/subdir`.

    Example:
        Input:
            depth_of_directory_tree = 1
            num_sub_directories = 1
            num_files_per_directory = 2
        Result:
            root/subdir1/file1.txt
            root/subdir1/file2.txt


        Input:
            depth_of_directory_tree = 1
            num_sub_directories = 2
            num_files_per_directory = 2
        Result:
            root/subdir1/file1.txt
            root/subdir1/file2.txt
            root/subdir2/file1.txt
            root/subdir2/file2.txt

    :param path: _description_
    :param depth_of_directory_tree: _description_
    :param num_sub_directories: _description_
    :param num_files_per_directory: _description_
    :param total_size_of_files_mbytes: _description_
    :return: _description_
    """
    # Calculate total number of files and size of each file
    total_dirs = sum(
        [num_sub_directories**i for i in range(1, depth_of_directory_tree + 1)]
    )
    total_files = total_dirs * num_files_per_directory
    total_size_of_files_bytes = total_size_of_files_mbytes * 1024 * 1024
    size_of_each_file_bytes = total_size_of_files_bytes // total_files

    print(f"total_directories: {total_dirs}")
    print(f"total_files: {total_files}")
    print(f"total_size_of_files_bytes: {total_size_of_files_bytes}")
    print(f"size_of_each_file_bits: {size_of_each_file_bytes}")

    def create_files_in_current_dir(path_to_create_files):
        for i in range(1, num_files_per_directory + 1):
            chunk_size = 1024  # size of each chunk in bytes
            num_chunks = size_of_each_file_bytes // chunk_size

            with open(f"{path_to_create_files}/file{i}.txt", "wb") as f:
                for _ in range(num_chunks):
                    f.write(os.urandom(chunk_size))

    def create_directories_in_current_dir(path_to_create_dirs, current_depth):
        if current_depth < depth_of_directory_tree:
            for i in range(1, num_sub_directories + 1):
                path = f"{path_to_create_dirs}/subdir{i}"
                os.makedirs(path, exist_ok=True)
                create_files_in_current_dir(path)
                new_depth = current_depth + 1
                create_directories_in_current_dir(path, new_depth)

    # Start creating directories and files
    root_dir = os.path.join(path, "root")
    os.makedirs(root_dir, exist_ok=True)
    create_directories_in_current_dir(root_dir, 0)
    return total_dirs, total_files, size_of_each_file_bytes


def execute_synapseutils_sync_from_synapse_test(
    path: str, syn: synapseclient.Synapse
) -> None:
    result = synapseutils.syncFromSynapse(syn=syn, entity=PARENT_PROJECT, path=path)
    print(result)

    synapseutils.syncToSynapse(
        syn,
        manifestFile=f"{path}/SYNAPSE_METADATA_MANIFEST.tsv",
        sendMessages=False,
    )


def sync_to_synapse(path: str, project_id: str, syn: synapseclient.Synapse) -> None:
    """Execute the test that uses synapseutils to sync all files/folders to synapse.

    :param path: The path to the root directory
    """
    manifest_path = f"{path}/benchmarking_manifest.tsv"
    with open(manifest_path, "w", encoding="utf-8") as _:
        pass

    synapseutils.generate_sync_manifest(
        syn,
        directory_path=path,
        parent_id=project_id,
        manifest_path=manifest_path,
    )

    # Write annotations to the manifest file -----------------------------------------
    # Open the `manifest_path` tab-delimited file and read its contents
    with open(manifest_path, "r") as file:
        lines = file.readlines()

    # Append 3 columns "annot1", "annot2", "annot3" to the header
    lines[0] = lines[0].strip() + "\tannot1\tannot2\tannot3\tannot4\tannot5\n"

    # Append the values to each line
    for i in range(1, len(lines)):
        lines[i] = lines[i].strip() + "\tvalue1\1\1.2\tFalse\t2020-01-01\n"

    # Write the modified contents back to the file
    with open(manifest_path, "w") as file:
        file.writelines(lines)
    # Finish writing annotations to the manifest file --------------------------------

    synapseutils.syncToSynapse(
        syn,
        manifestFile=manifest_path,
        sendMessages=False,
    )


def execute_sync_to_s3(path: str, key_in_bucket: str) -> None:
    """Executes the AWS CLI sync command. Expected to run last as this will delete local files.

    :param path: The path to the root directory
    :param test_name: The name of the test to add to the span name
    """

    subprocess.run(
        [
            "aws",
            "s3",
            "sync",
            path,
            f"{S3_BUCKET}/{key_in_bucket}",
            "--profile",
            S3_PROFILE,
        ]
    )  # nosec


def set_up_projects_one_time(path: str, syn: synapseclient.Synapse) -> None:
    create_folder_structure(
        path=path,
        depth_of_directory_tree=1,
        num_sub_directories=5,
        num_files_per_directory=5,
        total_size_of_files_mbytes=1,
    )
    # Set up the project:
    project_25_files_1MB = syn.store(obj=Project(name=PROJECT_25_FILES_1MB))
    sync_to_synapse(path=path, syn=syn, project_id=project_25_files_1MB.id)
    os.remove(f"{path}/benchmarking_manifest.tsv")
    execute_sync_to_s3(path=path, key_in_bucket=PROJECT_25_FILES_1MB)
    shutil.rmtree(path)

    create_folder_structure(
        path=path,
        depth_of_directory_tree=3,
        num_sub_directories=5,
        num_files_per_directory=5,
        total_size_of_files_mbytes=10,
    )
    project_775_files_10MB = syn.store(obj=Project(name=PROJECT_775_FILES_10MB))
    sync_to_synapse(path=path, syn=syn, project_id=project_775_files_10MB.id)
    os.remove(f"{path}/benchmarking_manifest.tsv")
    execute_sync_to_s3(path=path, key_in_bucket=PROJECT_775_FILES_10MB)
    shutil.rmtree(path)

    create_folder_structure(
        path=path,
        depth_of_directory_tree=1,
        num_sub_directories=1,
        num_files_per_directory=10,
        total_size_of_files_mbytes=1000,
    )
    project_10_files_1GB = syn.store(obj=Project(name=PROJECT_10_FILES_1GB))
    sync_to_synapse(path=path, syn=syn, project_id=project_10_files_1GB.id)
    os.remove(f"{path}/benchmarking_manifest.tsv")
    execute_sync_to_s3(path=path, key_in_bucket=PROJECT_10_FILES_1GB)
    shutil.rmtree(path)

    create_folder_structure(
        path=path,
        depth_of_directory_tree=1,
        num_sub_directories=1,
        num_files_per_directory=10,
        total_size_of_files_mbytes=100000,
    )
    project_10_files_100GB = syn.store(obj=Project(name=PROJECT_10_FILES_100GB))
    sync_to_synapse(path=path, syn=syn, project_id=project_10_files_100GB.id)
    os.remove(f"{path}/benchmarking_manifest.tsv")
    execute_sync_to_s3(path=path, key_in_bucket=PROJECT_10_FILES_100GB)
    shutil.rmtree(path)


synapse = synapseclient.Synapse(debug=False)
root_path = os.path.expanduser("~/benchmarkingDownload")
# Log-in with ~.synapseConfig `authToken`
synapse.login()

set_up_projects_one_time(path=root_path, syn=synapse)
