"""
Expects that ~/temp exists and is a directory.

The purpose of this script is to demonstrate how to use the new OOP interface for files.
The following actions are shown in this script:
1. Creating a file
2. Storing a file
3. Storing a file in a sub-folder
4. Renaming a file
5. Downloading a file
6. Deleting a file
7. Copying a file
8. Storing an activity to a file
9. Retrieve an activity from a file
"""

import os
from datetime import date, datetime, timedelta, timezone

import synapseclient
from synapseclient.core import utils
from synapseclient.models import Activity, File, Folder, UsedEntity, UsedURL

PROJECT_ID = "syn52948289"

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


def store_file():
    # Cleanup synapse for previous runs - Does not delete local files/directories:
    try:
        Folder(name="file_script_folder", parent_id=PROJECT_ID).get().delete()
    except Exception:
        pass
    if not os.path.exists(os.path.expanduser("~/temp/myNewFolder")):
        os.mkdir(os.path.expanduser("~/temp/myNewFolder"))

    script_file_folder = Folder(name="file_script_folder", parent_id=PROJECT_ID).store()

    # Creating annotations for my file ==================================================
    annotations_for_my_file = {
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

    name_of_file = "file_script_my_file_with_random_data.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)

    # 1. Creating a file =================================================================
    file = File(
        path=path_to_file,
        annotations=annotations_for_my_file,
        parent_id=script_file_folder.id,
        description="This is a file with random data.",
    )

    # 2. Storing a file ==================================================================
    file = file.store()

    print(f"File created: ID: {file.id}, Parent ID: {file.parent_id}")

    name_of_file = "file_in_a_sub_folder.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)

    # 3. Storing a file to a sub-folder ==================================================
    script_sub_folder = Folder(
        name="file_script_sub_folder", parent_id=script_file_folder.id
    ).store()
    file_in_a_sub_folder = File(
        path=path_to_file,
        annotations=annotations_for_my_file,
        parent_id=script_sub_folder.id,
        description="This is a file with random data.",
    )
    file_in_a_sub_folder = file_in_a_sub_folder.store()

    print(
        f"File created in sub folder: ID: {file_in_a_sub_folder.id}, Parent ID: {file_in_a_sub_folder.parent_id}"
    )

    # 4. Renaming a file =================================================================
    name_of_file = "file_script_my_file_to_rename.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)

    # The name of the entity, and the name of the file is disjointed.
    # For example, the name of the file is "file_script_my_file_to_rename.txt"
    # and the name of the entity is "this_name_is_different"
    file: File = File(
        path=path_to_file,
        name="this_name_is_different",
        parent_id=script_file_folder.id,
    ).store()
    print(f"File created with name: {file.name}")
    print(f"The path of the file is: {file.path}")

    # You can change the name of the entity without changing the name of the file.
    file.name = "modified_name_attribute"
    file.store()
    print(f"File renamed to: {file.name}")

    # You can then change the name of the file that would be downloaded like:
    file.change_metadata(download_as="new_name_for_downloading.txt")
    print(f"File download values changed to: {file.file_handle.file_name}")

    # 5. Downloading a file ===============================================================
    # Downloading a file to a location has a default beahvior of "keep.both"
    downloaded_file = File(
        id=file.id, path=os.path.expanduser("~/temp/myNewFolder")
    ).get()
    print(f"Downloaded file: {downloaded_file.path}")

    # I can also specify how collisions are handled when downloading a file.
    # This will replace the file on disk if it already exists and is different (after).
    path_to_file = downloaded_file.path
    create_random_file(path_to_file)
    print(f"Before file md5: {utils.md5_for_file(path_to_file).hexdigest()}")
    downloaded_file = File(
        id=downloaded_file.id,
        path=os.path.expanduser("~/temp/myNewFolder"),
        if_collision="overwrite.local",
    ).get()
    print(f"After file md5: {utils.md5_for_file(path_to_file).hexdigest()}")

    # This will keep the file on disk (before), and no file is downloaded
    path_to_file = downloaded_file.path
    create_random_file(path_to_file)
    print(f"Before file md5: {utils.md5_for_file(path_to_file).hexdigest()}")
    downloaded_file = File(
        id=downloaded_file.id,
        path=os.path.expanduser("~/temp/myNewFolder"),
        if_collision="keep.local",
    ).get()
    print(f"After file md5: {utils.md5_for_file(path_to_file).hexdigest()}")

    # 6. Deleting a file =================================================================
    # Suppose I have a file that I want to delete.
    name_of_file = "file_to_delete.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)
    file_to_delete = File(path=path_to_file, parent_id=script_file_folder.id).store()
    file_to_delete.delete()

    # 7. Copying a file ===================================================================
    print(
        f"File I am going to copy: ID: {file_in_a_sub_folder.id}, Parent ID: {file_in_a_sub_folder.parent_id}"
    )
    new_sub_folder = Folder(
        name="sub_sub_folder", parent_id=script_file_folder.id
    ).store()
    copied_file_instance = file_in_a_sub_folder.copy(parent_id=new_sub_folder.id)
    print(
        f"File I copied: ID: {copied_file_instance.id}, Parent ID: {copied_file_instance.parent_id}"
    )

    # 8. Storing an activity to a file =====================================================
    activity = Activity(
        name="some_name",
        description="some_description",
        used=[
            UsedURL(name="example", url="https://www.synapse.org/"),
            UsedEntity(target_id="syn456", target_version_number=1),
        ],
        executed=[
            UsedURL(name="example", url="https://www.synapse.org/"),
            UsedEntity(target_id="syn789", target_version_number=1),
        ],
    )

    name_of_file = "file_with_an_activity.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)
    file_with_activity = File(
        path=path_to_file, parent_id=script_file_folder.id, activity=activity
    ).store()
    print(file_with_activity.activity)

    # 9. When I am retrieving that file later on I can get the activity like =============
    # By also specifying download_file=False, I can get the activity without downloading the file.
    new_file_with_activity_instance = File(
        id=file_with_activity.id, download_file=False
    ).get(include_activity=True)
    print(new_file_with_activity_instance.activity)


store_file()
