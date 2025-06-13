import os
import time
from pprint import pprint

import synapseclient
from synapseclient.models import File, Folder

PROJECT_ID = ""  # Replace with your own project ID
ORG_NAME = "MyUniqueOrgForFolderTest"
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


def try_delete_json_schema_from_folder(folder_name: str, parent_id: str) -> None:
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


# Clean up any existing test data
try_delete_json_schema_from_folder("test_folder", PROJECT_ID)
try_delete_registered_json_schema_from_org(SCHEMA_URI)
try_delete_organization(ORG_NAME)

# Make sure the project exists
if not PROJECT_ID:
    raise ValueError(
        "Please set the PROJECT_ID variable to a valid Synapse project ID."
    )

# Start the script
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
bound_schema = test_folder.bind_schema(
    json_schema_uri=created_schema.uri, enable_derived_annos=True
)
json_schema_version_info = bound_schema.json_schema_version_info
print("JSON schema was bound successfully. Please see details below:")
pprint(vars(json_schema_version_info))

# get the bound schema
schema = test_folder.get_schema()
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
validation_results = test_folder.validate_schema()
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

child_file = File(path=path_to_file, parent_id=test_folder.id, annotations=annotations)
child_file = child_file.store()
time.sleep(2)

# Get the validation for all the children
validation_statistics = test_folder.get_schema_validation_statistics()
print("Validation statistics were retrieved successfully. Please see details below:")
pprint(vars(validation_statistics))

# Get the invalid validation results
invalid_validation = test_folder.get_invalid_validation()
for child in invalid_validation:
    print("See details of validation results: ")
    pprint(vars(child))
