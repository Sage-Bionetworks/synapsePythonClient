"""
Expects that ~/temp exists and is a directory.

The purpose of this script is to demonstrate how to use the new OOP interface for folders.
The following actions are shown in this script:
1. Creating a folder
2. Storing a folder to a project
3. Storing several files to a folder
4. Storing several folders in a folder
5. Getting metadata about a folder and it's immediate children
6. Updating the annotations in bulk for a number of folders and files
7. Deleting a folder
8. Copying a folder
9. Moving a folder
10. Using sync_from_synapse to download the files and folders
11. Bind a JSON schema to a folder and validate its contents.
"""

import os
import time
from datetime import date, datetime, timedelta, timezone
from pprint import pprint

import synapseclient
from synapseclient.models import File, Folder

PROJECT_ID = "syn52948289"  # Replace with your own project ID
ORG_NAME = "MyUniqueOrg"
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


def try_delete_json_schema_from_entity(folder_name: str, parent_id: str) -> None:
    """Simple try catch to delete a json schema folder."""
    try:
        js = syn.service("json_schema")
        test_folder = Folder(name=folder_name, parent_id=parent_id).get()
        js.delete_json_schema_from_entity(test_folder.id)
    except Exception:
        pass


def try_delete_registered_json_schema_from_org(schema_uri: str):
    """Simple try catch to delete a registered json schema from an organization."""
    js = syn.service("json_schema")
    try:
        js.delete_json_schema(schema_uri)
    except Exception:
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


def store_folder():
    # Clean up synapse for previous runs:
    try_delete_folder("my_new_folder_for_this_project", PROJECT_ID)
    try_delete_folder("destination_for_copy", PROJECT_ID)
    try_delete_folder("my_new_folder_for_this_project_I_want_to_delete", PROJECT_ID)
    try_delete_json_schema_from_entity("test_folder", PROJECT_ID)
    try_delete_registered_json_schema_from_org(SCHEMA_URI)
    try_delete_organization(ORG_NAME)

    # Creating annotations for my folder ==================================================
    annotations_for_my_folder = {
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

    # 1) Creating a folder ===============================================================
    root_folder_for_my_project = Folder(
        name="my_new_folder_for_this_project",
        annotations=annotations_for_my_folder,
        parent_id=PROJECT_ID,
        description="This is a folder with random data.",
    )

    root_folder_for_my_project = root_folder_for_my_project.store()

    print(
        f"Folder created: {root_folder_for_my_project.name} with id: {root_folder_for_my_project.id}"
    )

    # 2) Updating and storing an annotation ==============================================
    new_folder_instance = Folder(id=root_folder_for_my_project.id).get()
    new_folder_instance.annotations["my_key_string"] = ["new", "values", "here"]
    stored_folder = new_folder_instance.store()
    print(f"Folder {stored_folder.name} updated with new annotations:")
    print(stored_folder.annotations)

    # 3) Storing several files to a folder ===============================================
    files_to_store = []
    for loop in range(1, 10):
        name_of_file = f"my_file_with_random_data_{loop}.txt"
        path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
        create_random_file(path_to_file)

        file = File(
            path=path_to_file,
            name=name_of_file,
        )
        files_to_store.append(file)
    root_folder_for_my_project.files = files_to_store
    root_folder_for_my_project = root_folder_for_my_project.store()

    # 4) Storing several folders in a folder =============================================
    folders_to_store = []
    for loop in range(1, 10):
        folder_to_store = Folder(
            name=f"my_new_folder_for_this_project_{loop}",
        )
        folders_to_store.append(folder_to_store)
    root_folder_for_my_project.folders = folders_to_store
    root_folder_for_my_project = root_folder_for_my_project.store()

    # 5) Getting metadata about a folder and it's immediate children =====================
    new_folder_instance = Folder(id=root_folder_for_my_project.id).sync_from_synapse(
        download_file=False, recursive=False
    )

    print(f"Synced folder {new_folder_instance.name} from synapse")
    for file in new_folder_instance.files:
        print(f"Found File in Synapse at: {new_folder_instance.name}/{file.name}")

    for folder in new_folder_instance.folders:
        print(f"Found Folder in Synapse at: {new_folder_instance.name}/{folder.name}")

    # 6) Updating the annotations in bulk for a number of folders and files ==============
    new_annotations = {
        "my_new_key_string": ["b", "a", "c"],
    }

    for file in new_folder_instance.files:
        file.annotations = new_annotations

    for folder in new_folder_instance.folders:
        folder.annotations = new_annotations

    new_folder_instance.store()

    # 7) Deleting a folder ===============================================================
    folder_to_delete = Folder(
        name="my_new_folder_for_this_project_I_want_to_delete",
        parent_id=PROJECT_ID,
    ).store()

    folder_to_delete.delete()

    # 8) Copying a folder ===============================================================
    destination_folder_to_copy_to = Folder(
        name="destination_for_copy", parent_id=PROJECT_ID
    ).store()
    coped_folder = root_folder_for_my_project.copy(
        parent_id=destination_folder_to_copy_to.id
    )

    print(
        f"Copied folder from {root_folder_for_my_project.id} to {coped_folder.id} in synapse"
    )

    # You'll also see all the files/folders were copied too
    for file in coped_folder.files:
        print(f"Found (copied) File in Synapse at: {coped_folder.name}/{file.name}")

    for folder in coped_folder.folders:
        print(f"Found (copied) Folder in Synapse at: {coped_folder.name}/{folder.name}")

    # 9) Moving a folder ===============================================================
    folder_i_am_going_to_move = Folder(
        name="folder_i_am_going_to_move", parent_id=PROJECT_ID
    ).store()
    current_parent_id = folder_i_am_going_to_move.parent_id
    folder_i_am_going_to_move.parent_id = destination_folder_to_copy_to.id
    folder_i_am_going_to_move.store()
    print(
        f"Moved folder from {current_parent_id} to {folder_i_am_going_to_move.parent_id}"
    )

    # # 10) Using sync_from_synapse to download the files and folders ======================
    # # This will download all the files and folders in the folder to the local file system
    path_to_download = os.path.expanduser("~/temp/recursiveDownload")
    if not os.path.exists(path_to_download):
        os.mkdir(path_to_download)
    root_folder_for_my_project.sync_from_synapse(path=path_to_download)

    # 11) Bind json schema to folders and validate its contents =========================
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
    all_org = js.list_organizations()
    for org in all_org:
        print(f"Found organization: {org['name']} with id: {org['id']}")
    created_org = js.create_organization(ORG_NAME)
    print(
        f"Organization was created successfully. The name of the organization is: {ORG_NAME}, the id is: {created_org['id']}, created on: {created_org['createdOn']}, created by: {created_org['createdBy']}"
    )

    test_org = js.JsonSchemaOrganization(ORG_NAME)
    created_schema = test_org.create_json_schema(schema, SCHEMA_NAME, VERSION)
    print(created_schema)

    test_folder = Folder(name="test_folder", parent_id=PROJECT_ID).store()

    # Bind a JSON schema to the folder
    bound_schema = test_folder.bind_json_schema_to_entity(
        json_schema_uri=created_schema.uri, enable_derived_annos=True
    )
    json_schema_version_info = bound_schema.json_schema_version_info
    print("JSON schema was bound successfully. Please see details below:")
    pprint(vars(json_schema_version_info))

    # get the bound schema
    schema = test_folder.get_json_schema_from_entity()
    print("JSON Schema was retrieved successfully. Please see details below:")
    pprint(vars(schema))

    # store annotations to the test folder
    test_folder.annotations = {
        "test_string": "example_value",
        "test_int": "invalid str",
    }
    test_folder.store()

    time.sleep(2)
    # Validate the folder's contents against the schema
    validation_results = test_folder.validate_entity_with_json_schema()
    print("Validation was completed. Please see details below:")
    pprint(vars(validation_results))

    # Now try adding a file to the folder
    if not os.path.exists(os.path.expanduser("~/temp")):
        os.makedirs(os.path.expanduser("~/temp/testJSONSchemaFiles"), exist_ok=True)

    name_of_file = "test_file.txt"
    path_to_file = os.path.join(
        os.path.expanduser("~/temp/testJSONSchemaFiles"), name_of_file
    )
    create_random_file(path_to_file)

    annotations = {"test_string": "child_value", "test_int": "invalid child str"}

    child_file = File(
        path=path_to_file, parent_id=test_folder.id, annotations=annotations
    )
    child_file = child_file.store()
    time.sleep(2)

    # Get the validation for all the children
    validation_statistics = test_folder.get_json_schema_validation_statistics()
    print(
        "Validation statistics were retrieved successfully. Please see details below:"
    )
    pprint(vars(validation_statistics))

    # Get the invalid validation results
    invalid_validation = test_folder.get_invalid_json_schema_validation()
    for child in invalid_validation:
        print("See details of validation results: ")
        pprint(vars(child))


if __name__ == "__main__":
    store_folder()
    print("Done with the OOP folder POC script.")
