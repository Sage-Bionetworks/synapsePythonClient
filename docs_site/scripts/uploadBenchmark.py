"""
Handle running a few tests for benchmark upload times to synapse and S3. This has the ability
to create a directory and file structure, sync to synapse using synapseutils, sync to synapse
using os.walk, and sync to S3 using the AWS CLI.

For the Synapse tests we are also adding annotations to the uploaded files.
"""
import os
import shutil
from time import perf_counter
from synapseclient.entity import File, Folder
from synapseclient.annotations import Annotations
import synapseclient
import synapseutils
import subprocess  # nosec

from opentelemetry import trace

# from opentelemetry.sdk.trace import TracerProvider
# from opentelemetry.sdk.trace.export import BatchSpanProcessor
# from opentelemetry.sdk.resources import SERVICE_NAME, Resource
# from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

# trace.set_tracer_provider(
#     TracerProvider(resource=Resource(attributes={SERVICE_NAME: "upload_benchmarking"}))
# )
# trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
tracer = trace.get_tracer("my_tracer")

PARENT_PROJECT = "syn$FILL_ME_IN"
S3_BUCKET = "s3://$FILL_ME_IN"
S3_PROFILE = "$FILL_ME_IN"


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


def cleanup(
    path: str,
    delete_synapse: bool = True,
    delete_s3: bool = False,
    delete_local: bool = True,
):
    """Cleanup data in synapse, local, and s3.

    :param path: _description_
    :param delete_synapse: _description_, defaults to True
    :param delete_s3: _description_, defaults to False
    """
    if delete_s3:
        subprocess.run(
            ["aws", "s3", "rm", S3_BUCKET, "--recursive", "--profile", S3_PROFILE]
        )  # nosec
    if delete_synapse:
        for child in syn.getChildren(PARENT_PROJECT, includeTypes=["folder"]):
            syn.delete(child["id"])

    if delete_local and os.path.exists(path):
        shutil.rmtree(path)


def execute_synapseutils_test(
    path: str,
    test_name: str,
) -> None:
    """Execute the test that uses synapseutils to sync all files/folders to synapse.

    :param path: The path to the root directory
    :param test_name: The name of the test to add to the span name
    """
    with tracer.start_as_current_span(f"synapseutils__{test_name}"):
        manifest_path = f"{path}/benchmarking_manifest.tsv"
        with open(manifest_path, "w", encoding="utf-8") as _:
            pass

        time_before_generate_sync_manifest = perf_counter()
        synapseutils.generate_sync_manifest(
            syn,
            directory_path=path,
            parent_id=PARENT_PROJECT,
            manifest_path=manifest_path,
        )
        print(
            f"\nTime to generate sync manifest: {perf_counter() - time_before_generate_sync_manifest}"
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
    cleanup(path=path, delete_synapse=True, delete_s3=False, delete_local=False)


def execute_walk_test(
    path: str,
    test_name: str,
) -> None:
    """Execute the test that uses os.walk to sync all files/folders to synapse.

    :param path: The path to the root directory
    :param test_name: The name of the test to add to the span name
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
                folder = Folder(
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
                file = File(
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
    cleanup(path=path, delete_synapse=True, delete_s3=False, delete_local=False)


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
            ["aws", "s3", "sync", path, S3_BUCKET, "--profile", S3_PROFILE]
        )  # nosec
        print(f"\nTime to S3 sync: {perf_counter() - time_before_sync}")

    cleanup(path=path, delete_synapse=False, delete_s3=True, delete_local=True)


def execute_test_suite(
    path: str,
    depth_of_directory_tree: int,
    num_sub_directories: int,
    num_files_per_directory: int,
    total_size_of_files_mbytes: int,
) -> None:
    """Execute the test suite.

    :param path: _description_
    :param depth_of_directory_tree: _description_
    :param num_sub_directories: _description_
    :param num_files_per_directory: _description_
    :param total_size_of_files_mbytes: _description_
    """
    _, total_files, _ = create_folder_structure(
        path=path,
        depth_of_directory_tree=depth_of_directory_tree,
        num_sub_directories=num_sub_directories,
        num_files_per_directory=num_files_per_directory,
        total_size_of_files_mbytes=total_size_of_files_mbytes,
    )
    test_name = f"{total_files}_files_{total_size_of_files_mbytes}MB"

    execute_synapseutils_test(path, test_name)

    execute_walk_test(path, test_name)

    execute_sync_to_s3(path, test_name)


syn = synapseclient.Synapse(debug=False)
root_path = os.path.expanduser("~/benchmarking")
# Log-in with ~.synapseConfig `authToken`
syn.login()

print("25 Files - 1MB")
## 25 Files - 1MB -----------------------------------------------------------------------
depth = 1
sub_directories = 5
files_per_directory = 5
size_mbytes = 1

execute_test_suite(
    path=root_path,
    depth_of_directory_tree=depth,
    num_sub_directories=sub_directories,
    num_files_per_directory=files_per_directory,
    total_size_of_files_mbytes=size_mbytes,
)

print("775 Files - 10MB")
### 775 Files - 10MB ---------------------------------------------------------------------
depth = 3
sub_directories = 5
files_per_directory = 5
size_mbytes = 10

execute_test_suite(
    path=root_path,
    depth_of_directory_tree=depth,
    num_sub_directories=sub_directories,
    num_files_per_directory=files_per_directory,
    total_size_of_files_mbytes=size_mbytes,
)

print("10 Files - 1GB")
## 10 Files - 1GB -----------------------------------------------------------------------
depth = 1
sub_directories = 1
files_per_directory = 10
size_mbytes = 1000

execute_test_suite(
    path=root_path,
    depth_of_directory_tree=depth,
    num_sub_directories=sub_directories,
    num_files_per_directory=files_per_directory,
    total_size_of_files_mbytes=size_mbytes,
)

print("10 Files - 100GB")
### 10 Files - 100GB ---------------------------------------------------------------------
depth = 1
sub_directories = 1
files_per_directory = 10
size_mbytes = 100000

execute_test_suite(
    path=root_path,
    depth_of_directory_tree=depth,
    num_sub_directories=sub_directories,
    num_files_per_directory=files_per_directory,
    total_size_of_files_mbytes=size_mbytes,
)
