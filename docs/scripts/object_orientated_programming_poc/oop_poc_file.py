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
10. Bind a JSON schema to files and validate its contents
"""

import os
import time
from datetime import date, datetime, timedelta, timezone
from pprint import pprint

import synapseclient
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseNotFoundError
from synapseclient.models import Activity, File, Folder, UsedEntity, UsedURL

PROJECT_ID = "syn52948289"  # Replace with your own project ID
ORG_NAME = "MyUniqueOrgFile"
VERSION = "0.0.1"
SCHEMA_NAME = "test"
SCHEMA_URI = ORG_NAME + "-" + SCHEMA_NAME + "-" + VERSION

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


def try_delete_folder(folder_name: str, parent_id: str) -> None:
    """Simple try catch to delete a folder."""
    try:
        Folder(name=folder_name, parent_id=parent_id).get().delete()

    except Exception:
        pass


def try_delete_json_schema_from_file(file_path: str, parent_id: str) -> None:
    """Simple try catch to delete a json schema file."""
    try:
        js = syn.service("json_schema")
        test_folder = Folder(
            parent_id=PROJECT_ID, name="file_script_json_schema_folder"
        ).get()
        test_file = File(path=file_path, parent_id=test_folder.id).get()
        js.delete_json_schema_from_entity(test_file.id)
        time.sleep(2)
        print(js.get_json_schema_from_entity(test_file.id))
    except Exception as e:
        pass


def try_delete_registered_json_schema_from_org(schema_uri: str):
    """Simple try catch to delete a registered json schema from an organization."""
    js = syn.service("json_schema")
    try:
        js.delete_json_schema(schema_uri)
    except Exception as e:
        print(e)
        pass


def try_delete_organization(json_schema_org_name: str) -> None:
    """Simple try catch to delete a json schema organization."""
    try:
        js = syn.service("json_schema")
        all_org = js.list_organizations()
        for org in all_org:
            if org["name"] == json_schema_org_name:
                js.delete_organization(org["id"])
                break
    except Exception:
        pass


def create_or_retrieve_random_file_in_temp_folder(file_name: str) -> str:
    if not os.path.exists(os.path.expanduser("~/temp")):
        os.makedirs(os.path.expanduser("~/temp/testJSONSchemaFiles"), exist_ok=True)

    path_to_file = os.path.join(
        os.path.expanduser("~/temp/testJSONSchemaFiles"), file_name
    )
    return path_to_file


def cleanup_for_previous_runs_js_schema() -> File:
    """Cleanup for previous runs of the JSON schema."""
    path_to_file = create_or_retrieve_random_file_in_temp_folder(
        file_name="test_file.txt"
    )
    # Create a sub folder
    try:
        test_sub_folder = Folder(
            name="file_script_json_schema_folder", parent_id=PROJECT_ID
        ).get()
        try_delete_json_schema_from_file(
            file_path=path_to_file, parent_id=test_sub_folder.id
        )
        try_delete_registered_json_schema_from_org(schema_uri=SCHEMA_URI)
        try_delete_organization(json_schema_org_name=ORG_NAME)
        try_delete_folder(
            folder_name="file_script_json_schema_folder", parent_id=PROJECT_ID
        )
    except SynapseNotFoundError:
        pass

    test_sub_folder = Folder(
        name="file_script_json_schema_folder", parent_id=PROJECT_ID
    ).store()
    annotations = {"test_string": "child_value", "test_int": "invalid child str"}
    file = File(
        path=path_to_file, parent_id=test_sub_folder.id, annotations=annotations
    )
    file.store()

    return file


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

    # 11) Bind json schema to files and validate its contents =========================
    file = cleanup_for_previous_runs_js_schema()

    # Define a json schema organization and name
    title = "OOP Test Schema"

    # Set up an organization and create a JSON schema
    js = syn.service("json_schema")
    schema = {
        "$schema": "http://json-schema.org/draft-07/schema#",
        "$id": "https://example.com/schema/ooptest.json",
        "title": title,
        "type": "object",
        "properties": {
            "test_string": {"type": "string"},
            "test_int": {"type": "integer"},
            "test_derived_annos": {
                "description": "Derived annotation property",
                "type": "string",
                "const": "default value",
            },
        },
    }
    # Create an organization
    created_org = js.create_organization(ORG_NAME)
    print(
        f"Organization was created successfully. The name of the organization is: {ORG_NAME}, the id is: {created_org['id']}, created on: {created_org['createdOn']}, created by: {created_org['createdBy']}"
    )

    # Create a json schema
    test_org = js.JsonSchemaOrganization(ORG_NAME)
    created_schema = test_org.create_json_schema(schema, SCHEMA_NAME, VERSION)
    print(created_schema)

    # Bind JSON schema to the file
    bound_schema = file.bind_json_schema_to_entity(
        json_schema_uri=created_schema.uri, enable_derived_annos=True
    )
    json_schema_version_info = bound_schema.json_schema_version_info
    print("JSON schema was bound successfully. Please see details below:")
    pprint(vars(json_schema_version_info))

    # get the bound schema
    schema = file.get_json_schema_from_entity()
    print("JSON Schema was retrieved successfully. Please see details below:")
    pprint(vars(schema))

    # Validate the folder's contents against the schema
    time.sleep(2)
    validation_results = file.validate_entity_with_json_schema()
    print("Validation was completed. Please see details below:")
    pprint(vars(validation_results))


if __name__ == "__main__":
    store_file()
    print("Done with the OOP file POC script.")
