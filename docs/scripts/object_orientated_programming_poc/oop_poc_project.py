"""
Expects that ~/temp exists and is a directory.

The purpose of this script is to demonstrate how to use the new OOP interface for projects.
The following actions are shown in this script:
1. Creating a project
2. Retrieving a project by id or name
3. Upserting data on a project
4. Storing several files to a project
5. Storing several folders in a project
6. Updating the annotations in bulk for a number of folders and files
7. Downloading an entire project structure to disk
8. Copy a project and all content to a new project
9. Deleting a project
10.Binding a JSON schema to a project and validating its contents
"""

import os
import time
import uuid
from datetime import date, datetime, timedelta, timezone
from pprint import pprint

import synapseclient
from synapseclient.core.exceptions import SynapseNotFoundError
from synapseclient.models import File, Folder, Project

syn = synapseclient.Synapse(debug=True)
syn.login()


JSON_SCHEMA_PROJECT_ID = "syn68258424"  # Replace with your own project ID
ORG_NAME = "MyUniqueOrgProjectName"  # Replace with your own organization name
VERSION = "0.0.1"
SCHEMA_NAME = "test"
SCHEMA_URI = ORG_NAME + "-" + SCHEMA_NAME + "-" + VERSION


def create_random_file(
    path: str,
) -> None:
    """Create a random file with random data."""
    with open(path, "wb") as f:
        f.write(os.urandom(1))


def try_delete_json_schema_from_project(test_project: Project) -> None:
    """Simple try catch to delete a json schema folder."""
    try:
        js = syn.service("json_schema")
        js.delete_json_schema_from_entity(test_project.id)
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


def store_project():
    # Creating annotations for my project ==================================================
    my_annotations = {
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
        "annotation_i_want_to_delete": "I want to delete this annotation",
    }

    # 1) Creating a project ==============================================================
    project = Project(
        name="my_new_project_for_testing",
        annotations=my_annotations,
        description="This is a project with random data.",
        alias="my_project_alias_" + str(uuid.uuid4()).replace("-", "_"),
    )

    project = project.store()

    print(f"Project created with id: {project.id}")

    # 2) Retrieving a project by id or name ==============================================
    project = Project(name="my_new_project_for_testing").get()
    print(f"Project retrieved by name: {project.name} with id: {project.id}")

    project = Project(id=project.id).get()
    print(f"Project retrieved by id: {project.name} with id: {project.id}")

    # 3) Upserting data on a project =====================================================
    # When you have not already use `.store()` or `.get()` on a project any updates will
    # be a non-destructive upsert. This means that if the project does not exist it will
    # be created, if it does exist it will be updated.
    project = Project(
        name="my_new_project_for_testing", description="my new description"
    ).store()
    print(f"Project description updated to {project.description}")

    # After the instance has interacted with Synapse any changes will be destructive,
    # meaning changes in the data will act as a replacement instead of an addition.
    print(f"Annotations before update: {project.annotations}")
    del project.annotations["annotation_i_want_to_delete"]
    project = project.store()
    print(f"Annotations after update: {project.annotations}")

    # 4) Storing several files to a project ==============================================
    files_to_store = []
    for loop in range(1, 10):
        name_of_file = f"my_file_with_random_data_{loop}.txt"
        path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
        create_random_file(path_to_file)

        file = File(
            path=path_to_file,
            name=name_of_file,
            annotations=my_annotations,
        )
        files_to_store.append(file)
    project.files = files_to_store
    project = project.store()

    # 5) Storing several folders in a project ============================================
    folders_to_store = []
    for loop in range(1, 10):
        folder_to_store = Folder(
            name=f"my_folder_for_this_project_{loop}",
            annotations=my_annotations,
        )
        folders_to_store.append(folder_to_store)
    project.folders = folders_to_store
    print(
        f"Storing project ({project.id}) with {len(project.folders)} folders and {len(project.files)} files"
    )
    project = project.store()

    # 6) Updating the annotations in bulk for a number of folders and files ==============
    project_copy = Project(id=project.id).sync_from_synapse(download_file=False)

    print(
        f"Found {len(project_copy.files)} files and {len(project_copy.folders)} folder at the root level for {project_copy.name} with id: {project_copy.id}"
    )

    new_annotations = {
        "my_new_key_string": ["b", "a", "c"],
    }

    for file in project_copy.files:
        file.annotations = new_annotations

    for folder in project_copy.folders:
        folder.annotations = new_annotations

    project_copy.store()

    # 7) Downloading an entire project structure to disk =================================
    print(f"Downloading project ({project.id}) to ~/temp")
    Project(id=project.id).sync_from_synapse(
        download_file=True, path="~/temp/recursiveDownload", recursive=True
    )

    # 8) Copy a project and all content to a new project =================================
    project_to_delete = Project(
        name="my_new_project_I_want_to_delete_" + str(uuid.uuid4()).replace("-", "_"),
    ).store()
    print(f"Project created with id: {project_to_delete.id}")

    project_to_delete = project.copy(destination_id=project_to_delete.id)
    print(
        f"Copied to new project, copied {len(project_to_delete.folders)} folders and {len(project_to_delete.files)} files"
    )

    # 9) Deleting a project ==============================================================
    project_to_delete.delete()
    print(f"Project with id: {project_to_delete.id} deleted")

    # 11) Bind json schema to projects and validate its contents =========================
    try:
        js_project = Project(name="I_want_to_test_json_schema_project").get()
    except SynapseNotFoundError:
        js_project = Project(name="I_want_to_test_json_schema_project").store()

    try_delete_json_schema_from_project(js_project)
    try_delete_registered_json_schema_from_org(SCHEMA_URI)
    try_delete_organization(ORG_NAME)

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

    created_org = js.create_organization(ORG_NAME)
    print(
        f"Organization was created successfully. The name of the organization is: {ORG_NAME}, the id is: {created_org['id']}, created on: {created_org['createdOn']}, created by: {created_org['createdBy']}"
    )

    test_org = js.JsonSchemaOrganization(ORG_NAME)
    created_schema = test_org.create_json_schema(schema, SCHEMA_NAME, VERSION)
    print(created_schema)

    # Bind a JSON schema to the folder
    bound_schema = js_project.bind_json_schema_to_entity(
        json_schema_uri=created_schema.uri, enable_derived_annos=True
    )
    json_schema_version_info = bound_schema.json_schema_version_info
    print("JSON schema was bound successfully. Please see details below:")
    pprint(vars(json_schema_version_info))

    # get the bound schema
    schema = js_project.get_json_schema_from_entity()
    print("JSON Schema was retrieved successfully. Please see details below:")
    pprint(vars(schema))

    # store annotations to the test project
    js_project.annotations = {
        "test_string": "example_value",
        "test_int": "invalid str",
    }
    js_project.store()

    time.sleep(2)
    # Validate the project's contents against the schema
    validation_results = js_project.validate_entity_with_json_schema()
    print("Validation was completed. Please see details below:")
    pprint(vars(validation_results))

    # Now try adding a file to the project
    if not os.path.exists(os.path.expanduser("~/temp")):
        os.makedirs(os.path.expanduser("~/temp/testJSONSchemaFiles"), exist_ok=True)

    name_of_file = "test_file.txt"
    path_to_file = os.path.join(
        os.path.expanduser("~/temp/testJSONSchemaFiles"), name_of_file
    )
    create_random_file(path_to_file)

    annotations = {"test_string": "child_value", "test_int": "invalid child str"}

    child_file = File(
        path=path_to_file, parent_id=js_project.id, annotations=annotations
    )
    child_file = child_file.store()
    time.sleep(2)

    # Get the validation for all the children
    validation_statistics = js_project.get_json_schema_validation_statistics()
    print(
        "Validation statistics were retrieved successfully. Please see details below:"
    )
    pprint(vars(validation_statistics))

    # Get the invalid validation results
    invalid_validation = js_project.get_invalid_json_schema_validation()
    for child in invalid_validation:
        print("See details of validation results: ")
        pprint(vars(child))


if __name__ == "__main__":
    store_project()
    print("Done with the OOP project POC script.")


store_project()
