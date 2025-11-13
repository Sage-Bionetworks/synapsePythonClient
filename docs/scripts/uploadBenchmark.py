"""
Handle running a few tests for benchmark upload times to synapse and S3. This has the ability
to create a directory and file structure, sync to synapse using synapseutils, sync to synapse
using os.walk, and sync to S3 using the AWS CLI.

For the Synapse tests we are also adding annotations to the uploaded files.

# For running this benchmarking script:
- Inside `execute_test_suite` there are a few tests to possibly run. Uncomment for
the test you want to run
- The cleanup script doesn't need to delete local files, but it can be changed to do so
- If you want to run a different file/folder size manually remove the folders from disk
or change the `delete_local` parameter to `True` in the `cleanup` function
- This script can be re-run with the same sets of files since it will clear the
syn.cache and synapse project before running the tests. It also updates the file MD5s.
- This script is not reccommended to run on a personal computer, it is best to run on a
service catalog EC2 instance. Mainly because this will purge your local Synapse cache.
"""

import asyncio
import datetime
import logging
import os
import shutil
import subprocess  # nosec
from time import perf_counter

from opentelemetry import trace

import synapseclient
import synapseutils
from synapseclient.annotations import Annotations
from synapseclient.entity import File as SynFile
from synapseclient.entity import Folder as SynFolder
from synapseclient.models import File, Folder, Project

# from opentelemetry.sdk.trace import TracerProvider
# from opentelemetry.sdk.trace.export import BatchSpanProcessor
# from opentelemetry.sdk.resources import SERVICE_NAME, Resource
# from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

# os.environ["OTEL_EXPORTER_OTLP_ENDPOINT"] = "https://ingest.us.signoz.cloud"
# os.environ["OTEL_EXPORTER_OTLP_HEADERS"] = "signoz-ingestion-key=<your key>"
# os.environ["OTEL_SERVICE_INSTANCE_ID"] = "local"

trace.set_tracer_provider(
    TracerProvider(resource=Resource(attributes={SERVICE_NAME: "upload_benchmarking"}))
)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
tracer = trace.get_tracer("my_tracer")

PARENT_PROJECT = "syn$FILL_ME_IN"
S3_BUCKET = "s3://$FILL_ME_IN"
S3_PROFILE = "$FILL_ME_IN"

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

    chunk_size = MiB  # size of each chunk in bytes

    def create_files_in_current_dir(path_to_create_files: str) -> None:
        for i in range(1, num_files_per_directory + 1):
            num_chunks = size_of_each_file_bytes // chunk_size
            filename = os.path.join(path_to_create_files, f"file{i}.txt")
            # when the file size is right, just modify the beginning to refresh the file
            if (
                os.path.isfile(filename)
                and os.path.getsize(filename) == size_of_each_file_bytes
            ):
                with open(filename, "r+b") as f:
                    f.seek(0)
                    f.write(os.urandom(chunk_size))
            # if the file doesn't exist or the size is wrong, create it from scratch
            else:
                if os.path.isfile(filename):
                    os.remove(filename)
                with open(filename, "wb") as f:
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


def cleanup(
    path: str,
    delete_synapse: bool = True,
    delete_s3: bool = False,
    delete_local: bool = True,
) -> None:
    """Cleanup data in synapse, local, and s3.

    Arguments:
        path: The path to the root directory
        delete_synapse: Whether to delete data in synapse
        delete_s3: Whether to delete data in S3
        delete_local: Whether to delete data locally
    """
    if delete_s3:
        subprocess.run(
            ["aws", "s3", "rm", S3_BUCKET, "--recursive", "--profile", S3_PROFILE]
        )  # nosec
    if delete_synapse:
        for child in syn.getChildren(PARENT_PROJECT, includeTypes=["folder", "file"]):
            syn.delete(child["id"])
        syn.cache.purge(after_date=datetime.datetime(2021, 1, 1))

    if delete_local and os.path.exists(path):
        shutil.rmtree(path)


def execute_synapseutils_test(
    path: str,
    test_name: str,
) -> None:
    """Execute the test that uses synapseutils to sync all files/folders to synapse.

    Arguments:
        path: The path to the root directory
        test_name: The name of the test to add to the span name
    """
    with tracer.start_as_current_span(f"synapseutils__{test_name}"):
        manifest_path = f"{path}/benchmarking_manifest.tsv"
        with open(manifest_path, "w", encoding="utf-8") as f:
            pass

        time_before_syncToSynapse = perf_counter()
        synapseutils.generate_sync_manifest(
            syn,
            directory_path=path,
            parent_id=PARENT_PROJECT,
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
            lines[i] = lines[i].strip() + "\tvalue1\t1\t1.2\ttrue\t2020-01-01\n"

        # Write the modified contents back to the file
        with open(manifest_path, "w") as file:
            file.writelines(lines)
        # Finish writing annotations to the manifest file --------------------------------

        time_before_syncToSynapse = perf_counter()
        synapseutils.syncToSynapse(
            syn,
            manifestFile=manifest_path,
            sendMessages=False,
        )

        print(
            f"\nTime to sync to Synapse: {perf_counter() - time_before_syncToSynapse}"
        )


def execute_walk_test(
    path: str,
    test_name: str,
) -> None:
    """Execute the test that uses os.walk to sync all files/folders to synapse.

    Arguments:
        path: The path to the root directory
        test_name: The name of the test to add to the span name
    """
    with tracer.start_as_current_span(f"manual_walk__{test_name}"):
        time_before_walking_tree = perf_counter()

        parents = {path: PARENT_PROJECT}
        saved_files = []
        saved_folders = []
        for directory_path, directory_names, file_names in os.walk(path):
            # Replicate the folders on Synapse
            for directory_name in directory_names:
                folder_path = os.path.join(directory_path, directory_name)
                parent_id = parents[directory_path]
                folder = SynFolder(
                    name=directory_name,
                    parent=parent_id,
                )
                # Store Synapse ID for sub-folders/files
                folder = syn.store(folder)
                saved_folders.append(folder)
                parents[folder_path] = folder["id"]

            # Replicate the files on Synapse
            for filename in file_names:
                filepath = os.path.join(directory_path, filename)
                file = SynFile(
                    path=filepath,
                    parent=parents[directory_path],
                )
                saved_file = syn.store(file)
                saved_files.append(saved_file)

                # Store annotations on the file ------------------------------------------
                syn.set_annotations(
                    annotations=Annotations(
                        id=saved_file.id,
                        etag=saved_file.etag,
                        **{
                            "annot1": "value1",
                            "annot2": 1,
                            "annot3": 1.2,
                            "annot4": True,
                            "annot5": "2020-01-01",
                        },
                    )
                )
                # Finish storing annotations on the file ---------------------------------
        print(
            f"\nTime to walk and sync tree: {perf_counter() - time_before_walking_tree}"
        )


def execute_walk_file_sequential(
    path: str,
    test_name: str,
) -> None:
    """Execute the test that uses os.walk to sync all files/folders to synapse.

    Arguments:
        path: The path to the root directory
        test_name: The name of the test to add to the span name
    """
    with tracer.start_as_current_span(f"manual_walk__{test_name}"):
        time_before_walking_tree = perf_counter()

        # Create descriptive log file name with timestamp
        timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        log_file_path = os.path.expanduser(
            f"~/upload_benchmark_{test_name}_{timestamp}.log"
        )
        with open(log_file_path, "a") as log_file:
            log_file.write(f"Test: {test_name}\n")
            start_time = datetime.datetime.now()
            log_file.write(f"Start time: {start_time.strftime('%Y-%m-%d %H:%M:%S')}\n")

        # Create a simple parent lookup
        parents = {path: PARENT_PROJECT}

        for directory_path, directory_names, file_names in os.walk(path):
            # Create folders on Synapse first
            for directory_name in directory_names:
                folder_path = os.path.join(directory_path, directory_name)
                parent_id = parents[directory_path]

                new_folder = Folder(name=directory_name, parent_id=parent_id)
                # Store each folder immediately and save its Synapse ID
                stored_folder = asyncio.run(new_folder.store_async())
                parents[folder_path] = stored_folder.id

            # Upload files one by one
            for filename in file_names:
                filepath = os.path.join(directory_path, filename)
                parent_id = parents[directory_path]

                new_file = File(
                    path=filepath,
                    parent_id=parent_id,
                    annotations={
                        "annot1": "value1",
                        "annot2": 1,
                        "annot3": 1.2,
                        "annot4": True,
                        "annot5": "2020-01-01",
                    },
                    description="This is a Test File",
                )
                # Upload this single file immediately
                asyncio.run(new_file.store_async())

        # Write end time and duration to log file
        with open(log_file_path, "a") as log_file:
            end_time = datetime.datetime.now()
            duration = perf_counter() - time_before_walking_tree
            log_file.write(f"End time: {end_time.strftime('%Y-%m-%d %H:%M:%S')}\n")
            log_file.write(f"Duration: {duration:.2f} seconds\n")
            log_file.write("-" * 50 + "\n")
        print(
            f"\nTime to walk and sync tree sequentially: {perf_counter() - time_before_walking_tree}"
        )


def execute_walk_test_oop(
    path: str,
    test_name: str,
) -> None:
    """Execute the test that uses os.walk to sync all files/folders to synapse.

    Arguments:
        path: The path to the root directory
        test_name: The name of the test to add to the span name
    """
    with tracer.start_as_current_span(f"manual_walk__{test_name}"):
        time_before_walking_tree = perf_counter()

        root_project = Project(id=PARENT_PROJECT)
        parents = {path: root_project}
        for directory_path, directory_names, file_names in os.walk(path):
            # Replicate the folders on Synapse
            for directory_name in directory_names:
                folder_path = os.path.join(directory_path, directory_name)
                parent_container = parents[directory_path]
                new_folder = Folder(name=directory_name)
                parent_container.folders.append(new_folder)
                parents[folder_path] = new_folder

            # Replicate the files on Synapse
            for filename in file_names:
                filepath = os.path.join(directory_path, filename)
                parent_container = parents[directory_path]
                new_file = File(
                    path=filepath,
                    annotations={
                        "annot1": "value1",
                        "annot2": 1,
                        "annot3": 1.2,
                        "annot4": True,
                        "annot5": "2020-01-01",
                    },
                    description="This is a Test File",
                )
                parent_container.files.append(new_file)
        asyncio.run(root_project.store_async())
        print(
            f"\nTime to walk and sync tree - OOP: {perf_counter() - time_before_walking_tree}"
        )


def execute_sync_to_s3(
    path: str,
    test_name: str,
) -> None:
    """Executes the AWS CLI sync command. Expected to run last as this will delete local files.

    :param path: The path to the root directory
    :param test_name: The name of the test to add to the span name
    """

    with tracer.start_as_current_span(f"s3_sync__{test_name}"):
        time_before_sync = perf_counter()
        subprocess.run(
            f"aws s3 sync {path} {S3_BUCKET} --profile {S3_PROFILE}",
            shell=True,
            check=False,
        )  # nosec
        print(f"\nTime to S3 sync: {perf_counter() - time_before_sync}")


def execute_test_suite(
    path: str,
    depth_of_directory_tree: int,
    num_sub_directories: int,
    num_files_per_directory: int,
    total_size_of_files_mib: int,
) -> None:
    """Execute the test suite.

    :param path: _description_
    :param depth_of_directory_tree: _description_
    :param num_sub_directories: _description_
    :param num_files_per_directory: _description_
    :param total_size_of_files_mib: _description_
    """
    # Cleanup can be changed to delete_local=True when we want to clear the files out
    # This can be kept as False to allow multiple tests with the same file/folder
    # structure to re-use the files on Disk.
    cleanup(path=path, delete_synapse=True, delete_s3=False, delete_local=False)
    _, total_files, _ = create_folder_structure(
        path=path,
        depth_of_directory_tree=depth_of_directory_tree,
        num_sub_directories=num_sub_directories,
        num_files_per_directory=num_files_per_directory,
        total_size_of_files_mib=total_size_of_files_mib,
    )

    if total_size_of_files_mib >= 1024:
        test_name = f"{total_files}_files_{total_size_of_files_mib // 1024}GiB"
    else:
        test_name = f"{total_files}_files_{total_size_of_files_mib}MiB"

    execute_walk_file_sequential(path, test_name)

    # execute_synapseutils_test(path, test_name)

    # execute_walk_test(path, test_name)

    # execute_walk_test_oop(path, test_name)

    # execute_sync_to_s3(path, test_name)


syn = synapseclient.Synapse(debug=True, http_timeout_seconds=600)
synapseclient.Synapse.enable_open_telemetry()
root_path = os.path.expanduser("~/benchmarking")

# Log-in with ~.synapseConfig `authToken`
syn.login()

print("25 Files - 25MiB")
# 25 Files - 25MiB -----------------------------------------------------------------------
depth = 1
sub_directories = 1
files_per_directory = 25
size_mib = 25

execute_test_suite(
    path=root_path,
    depth_of_directory_tree=depth,
    num_sub_directories=sub_directories,
    num_files_per_directory=files_per_directory,
    total_size_of_files_mib=size_mib,
)

# print("1 Files - 10MiB")
# ## 1 Files - 10MiB -----------------------------------------------------------------------
# depth = 1
# sub_directories = 1
# files_per_directory = 1
# size_mib = 10

# execute_test_suite(
#     path=root_path,
#     depth_of_directory_tree=depth,
#     num_sub_directories=sub_directories,
#     num_files_per_directory=files_per_directory,
#     total_size_of_files_mib=size_mib,
# )

# print("1000 Files - 1GiB")
# ## 1000 Files - 1GiB -----------------------------------------------------------------------
# depth = 1
# sub_directories = 1
# files_per_directory = 1000
# size_mib = 1024

# execute_test_suite(
#     path=root_path,
#     depth_of_directory_tree=depth,
#     num_sub_directories=sub_directories,
#     num_files_per_directory=files_per_directory,
#     total_size_of_files_mib=size_mib,
# )

# print("100 Files - 1GiB")
# ## 100 Files - 1GiB -----------------------------------------------------------------------
# depth = 1
# sub_directories = 1
# files_per_directory = 100
# size_mib = 1024

# execute_test_suite(
#     path=root_path,
#     depth_of_directory_tree=depth,
#     num_sub_directories=sub_directories,
#     num_files_per_directory=files_per_directory,
#     total_size_of_files_mib=size_mib,
# )

# print("10 Files - 1GiB")
# ## 10 Files - 1GiB -----------------------------------------------------------------------
# depth = 1
# sub_directories = 1
# files_per_directory = 10
# size_mib = 1024

# execute_test_suite(
#     path=root_path,
#     depth_of_directory_tree=depth,
#     num_sub_directories=sub_directories,
#     num_files_per_directory=files_per_directory,
#     total_size_of_files_mib=size_mib,
# )

# print("100 Files - 10GiB")
# ## 100 Files - 10GiB -----------------------------------------------------------------------
# depth = 1
# sub_directories = 1
# files_per_directory = 100
# size_mib = 10240

# execute_test_suite(
#     path=root_path,
#     depth_of_directory_tree=depth,
#     num_sub_directories=sub_directories,
#     num_files_per_directory=files_per_directory,
#     total_size_of_files_mib=size_mib,
# )

# print("10 Files - 10GiB")
# ## 10 Files - 10GiB -----------------------------------------------------------------------
# depth = 1
# sub_directories = 1
# files_per_directory = 10
# size_mib = 10240

# execute_test_suite(
#     path=root_path,
#     depth_of_directory_tree=depth,
#     num_sub_directories=sub_directories,
#     num_files_per_directory=files_per_directory,
#     total_size_of_files_mib=size_mib,
# )

# print("1 Files - 10GiB")
# ## 1 Files - 10GiB -----------------------------------------------------------------------
# depth = 1
# sub_directories = 1
# files_per_directory = 1
# size_mib = 10240

# execute_test_suite(
#     path=root_path,
#     depth_of_directory_tree=depth,
#     num_sub_directories=sub_directories,
#     num_files_per_directory=files_per_directory,
#     total_size_of_files_mib=size_mib,
# )

# print("10 Files - 100GiB")
# ## 10 Files - 100GiB -----------------------------------------------------------------------
# depth = 1
# sub_directories = 1
# files_per_directory = 10
# size_mib = 102400

# execute_test_suite(
#     path=root_path,
#     depth_of_directory_tree=depth,
#     num_sub_directories=sub_directories,
#     num_files_per_directory=files_per_directory,
#     total_size_of_files_mib=size_mib,
# )


# print("1 Files - 100GiB")
# ## 1 Files - 100GiB -----------------------------------------------------------------------
# depth = 1
# sub_directories = 1
# files_per_directory = 1
# size_mib = 102400

# execute_test_suite(
#     path=root_path,
#     depth_of_directory_tree=depth,
#     num_sub_directories=sub_directories,
#     num_files_per_directory=files_per_directory,
#     total_size_of_files_mib=size_mib,
# )

# print("45 File - 100GB")
# # 45 File - 100GB -----------------------------------------------------------------------
# depth = 1
# sub_directories = 1
# files_per_directory = 45
# size_mib = 45 * 100 * 1024

# execute_test_suite(
#     path=root_path,
#     depth_of_directory_tree=depth,
#     num_sub_directories=sub_directories,
#     num_files_per_directory=files_per_directory,
#     total_size_of_files_mib=size_mib,
# )
