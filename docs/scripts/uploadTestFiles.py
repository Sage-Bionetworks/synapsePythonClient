"""
Create some test files and upload them to Synapse and S3. This is used as the first step
for benchmarking downloads.
"""

import asyncio
import os
import shutil
import subprocess  # nosec

import synapseclient
import synapseutils
from synapseclient.models import Folder, Project

PARENT_PROJECT = "syn$FILL_ME_IN"
S3_BUCKET = "s3://$FILL_ME_IN"
S3_PROFILE = "$FILL_ME_IN"


# Create a bunch of folders with known names to be used during the benchmarking
FOLDER_10_FILES_10GIB = "download_benchmarking_10_files_10gib"
FOLDER_1_FILES_10GIB = "download_benchmarking_1_files_10gib"
FOLDER_10_FILES_1GIB = "download_benchmarking_10_files_1gib"
FOLDER_100_FILES_100MIB = "download_benchmarking_100_files_100mib"
FOLDER_10_FILES_100MIB = "download_benchmarking_10_files_100mib"
FOLDER_100_FILES_10MIB = "download_benchmarking_100_files_10mib"
FOLDER_1000_FILES_1MIB = "download_benchmarking_1000_files_1mib"

MiB: int = 2**20


def create_folder_structure(
    path: str,
    depth_of_directory_tree: int,
    num_sub_directories: int,
    num_files_per_directory: int,
    total_size_of_files_mib: int,
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

    Arguments:
        path: The path to the root directory
        depth_of_directory_tree: The depth of the directory tree
        num_sub_directories: The number of subdirectories to create
        num_files_per_directory: The number of files to create in each directory
        total_size_of_files_mib: The total size of all files in MiB

    Returns:
        The total number of directories, total number of files, and the size of each
        file in bytes
    """
    # Calculate total number of files and size of each file
    total_dirs = sum(
        [num_sub_directories**i for i in range(1, depth_of_directory_tree + 1)]
    )
    total_files = total_dirs * num_files_per_directory
    total_size_of_files_bytes = total_size_of_files_mib * MiB
    size_of_each_file_bytes = total_size_of_files_bytes // total_files

    print(f"total_directories: {total_dirs}")
    print(f"total_files: {total_files}")
    print(f"total_size_of_files_bytes: {total_size_of_files_bytes}")
    print(f"size_of_each_file_bytes: {size_of_each_file_bytes}")

    def create_files_in_current_dir(path_to_create_files: str) -> None:
        for i in range(1, num_files_per_directory + 1):
            chunk_size = 1048576  # size of each chunk in bytes
            num_chunks = size_of_each_file_bytes // chunk_size
            filename = f"{path_to_create_files}/file{i}.txt"
            if (
                os.path.isfile(filename)
                and os.path.getsize(filename) == size_of_each_file_bytes
            ):
                with open(filename, "r+b") as f:
                    f.seek(0)
                    f.write(os.urandom(chunk_size))
            else:
                if os.path.isfile(filename):
                    os.remove(filename)
                with open(filename, "wb") as f:
                    for _ in range(num_chunks):
                        f.write(os.urandom(chunk_size))

    def create_directories_in_current_dir(
        path_to_create_dirs: str, current_depth: int
    ) -> None:
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


def sync_to_synapse(path: str, project_id: str, syn: synapseclient.Synapse) -> None:
    """Execute the test that uses synapseutils to sync all files/folders to synapse.

    Arguments:
        path: The path to the root directory
        project_id: The project ID to sync to
        syn: The logged in synapse instance

    Returns:
        None
    """
    manifest_path = f"{path}/benchmarking_manifest.tsv"
    with open(manifest_path, "w", encoding="utf-8") as f:
        pass

    synapseutils.generate_sync_manifest(
        syn,
        directory_path=path,
        parent_id=project_id,
        manifest_path=manifest_path,
    )

    synapseutils.syncToSynapse(
        syn,
        manifestFile=manifest_path,
        sendMessages=False,
    )


def execute_sync_to_s3(path: str, key_in_bucket: str) -> None:
    subprocess.run(
        f"aws s3 sync {path} {S3_BUCKET}/{key_in_bucket} --profile {S3_PROFILE}",
        shell=True,
        check=False,
    )  # nosec


async def set_up_folders_one_time(path: str, syn: synapseclient.Synapse) -> None:
    project = await Project(id=PARENT_PROJECT).get_async()

    depth = 1
    sub_directories = 1
    files_per_directory = 1000
    size_mib = 1024
    create_folder_structure(
        path=path,
        depth_of_directory_tree=depth,
        num_sub_directories=sub_directories,
        num_files_per_directory=files_per_directory,
        total_size_of_files_mib=size_mib,
    )

    folder = await Folder(
        name=FOLDER_1000_FILES_1MIB, parent_id=project.id
    ).store_async()
    sync_to_synapse(path=path, syn=syn, project_id=folder.id)
    os.remove(f"{path}/benchmarking_manifest.tsv")
    execute_sync_to_s3(path=path, key_in_bucket=FOLDER_1000_FILES_1MIB)
    shutil.rmtree(path)

    # depth = 1
    # sub_directories = 1
    # files_per_directory = 100
    # size_mib = 1024
    # create_folder_structure(
    #     path=path,
    #     depth_of_directory_tree=depth,
    #     num_sub_directories=sub_directories,
    #     num_files_per_directory=files_per_directory,
    #     total_size_of_files_mib=size_mib,
    # )

    # folder = await Folder(
    #     name=FOLDER_100_FILES_10MIB, parent_id=project.id
    # ).store_async()
    # sync_to_synapse(path=path, syn=syn, project_id=folder.id)
    # os.remove(f"{path}/benchmarking_manifest.tsv")
    # shutil.rmtree(path)

    # depth = 1
    # sub_directories = 1
    # files_per_directory = 10
    # size_mib = 1024
    # create_folder_structure(
    #     path=path,
    #     depth_of_directory_tree=depth,
    #     num_sub_directories=sub_directories,
    #     num_files_per_directory=files_per_directory,
    #     total_size_of_files_mib=size_mib,
    # )

    # folder = await Folder(
    #     name=FOLDER_10_FILES_100MIB, parent_id=project.id
    # ).store_async()
    # sync_to_synapse(path=path, syn=syn, project_id=folder.id)
    # os.remove(f"{path}/benchmarking_manifest.tsv")
    # shutil.rmtree(path)

    # depth = 1
    # sub_directories = 1
    # files_per_directory = 100
    # size_mib = 10240
    # create_folder_structure(
    #     path=path,
    #     depth_of_directory_tree=depth,
    #     num_sub_directories=sub_directories,
    #     num_files_per_directory=files_per_directory,
    #     total_size_of_files_mib=size_mib,
    # )

    # folder = await Folder(
    #     name=FOLDER_100_FILES_100MIB, parent_id=project.id
    # ).store_async()
    # sync_to_synapse(path=path, syn=syn, project_id=folder.id)
    # os.remove(f"{path}/benchmarking_manifest.tsv")
    # shutil.rmtree(path)

    # depth = 1
    # sub_directories = 1
    # files_per_directory = 10
    # size_mib = 10240
    # create_folder_structure(
    #     path=path,
    #     depth_of_directory_tree=depth,
    #     num_sub_directories=sub_directories,
    #     num_files_per_directory=files_per_directory,
    #     total_size_of_files_mib=size_mib,
    # )

    # folder = await Folder(name=FOLDER_10_FILES_1GIB, parent_id=project.id).store_async()
    # sync_to_synapse(path=path, syn=syn, project_id=folder.id)
    # os.remove(f"{path}/benchmarking_manifest.tsv")
    # shutil.rmtree(path)

    # depth = 1
    # sub_directories = 1
    # files_per_directory = 1
    # size_mib = 10240
    # create_folder_structure(
    #     path=path,
    #     depth_of_directory_tree=depth,
    #     num_sub_directories=sub_directories,
    #     num_files_per_directory=files_per_directory,
    #     total_size_of_files_mib=size_mib,
    # )

    # folder = await Folder(name=FOLDER_1_FILES_10GIB, parent_id=project.id).store_async()
    # sync_to_synapse(path=path, syn=syn, project_id=folder.id)
    # os.remove(f"{path}/benchmarking_manifest.tsv")
    # shutil.rmtree(path)

    # depth = 1
    # sub_directories = 1
    # files_per_directory = 10
    # size_mib = 102400
    # create_folder_structure(
    #     path=path,
    #     depth_of_directory_tree=depth,
    #     num_sub_directories=sub_directories,
    #     num_files_per_directory=files_per_directory,
    #     total_size_of_files_mib=size_mib,
    # )

    # folder = await Folder(
    #     name=FOLDER_10_FILES_10GIB, parent_id=project.id
    # ).store_async()
    # sync_to_synapse(path=path, syn=syn, project_id=folder.id)
    # os.remove(f"{path}/benchmarking_manifest.tsv")
    # shutil.rmtree(path)


synapse = synapseclient.Synapse(debug=False)
root_path = os.path.expanduser("~/benchmarkingDownload")
# Log-in with ~.synapseConfig `authToken`
synapse.login()

asyncio.run(set_up_folders_one_time(path=root_path, syn=synapse))
