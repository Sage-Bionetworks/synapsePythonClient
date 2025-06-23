import os
import time
from pprint import pprint

import synapseclient
from synapseclient.models import File, Folder

# 1. Set up Synapse Python client and retrieve project
syn = synapseclient.Synapse()
syn.login()


# Retrieve test project
PROJECT_ID = syn.findEntityId(
    name="My uniquely named project about Alzheimer's Disease 2"
)

# Create a test folder for JSON schema experiments
test_folder = Folder(name="clinical_data_folder", parent_id=PROJECT_ID).store()

# 2. Take a look at the constants and structure of the JSON schema
ORG_NAME = "myUniqueAlzheimersResearchOrgTurtorial"
VERSION = "0.0.1"
NEW_VERSION = "0.0.2"
SCHEMA_NAME = "clinicalObservations"

title = "Alzheimer's Clinical Observation Schema"
schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "https://example.com/schema/alzheimers_observation.json",
    "title": title,
    "type": "object",
    "properties": {
        "patient_id": {
            "type": "string",
            "description": "A unique identifier for the patient",
        },
        "cognitive_score": {
            "type": "integer",
            "description": "Quantitative cognitive function score",
        },
        "diagnosis_stage": {
            "type": "string",
            "description": "Stage of Alzheimer's diagnosis (e.g., Mild, Moderate, Severe)",
            "const": "Mild",  # Example constant for derived annotation
        },
    },
}

# 3. Try create test organization and json schema if they do not exist
js = syn.service("json_schema")
all_orgs = js.list_organizations()
for org in all_orgs:
    if org["name"] == ORG_NAME:
        print(f"Organization {ORG_NAME} already exists.")
        break
else:
    print(f"Creating organization {ORG_NAME}.")
    js.create_organization(ORG_NAME)

my_test_org = js.JsonSchemaOrganization(ORG_NAME)
test_schema = my_test_org.get_json_schema(SCHEMA_NAME)
if not test_schema:
    test_schema = my_test_org.create_json_schema(schema, SCHEMA_NAME, VERSION)

# If you want to make an update, you can re-register your schema with the organization:
updated_schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "https://example.com/schema/alzheimers_observation.json",
    "title": "my new title",
    "type": "object",
    "properties": {
        "patient_id": {
            "type": "string",
            "description": "A unique identifier for the patient",
        },
        "cognitive_score": {
            "type": "integer",
            "description": "Quantitative cognitive function score",
        },
        "updated_field": {
            "type": "string",
            "description": "Updated description for the field",
        },
    },
}
new_test_schema = my_test_org.create_json_schema(
    updated_schema, SCHEMA_NAME, NEW_VERSION
)


# 4. Bind the JSON schema to the folder
schema_uri = ORG_NAME + "-" + SCHEMA_NAME + "-" + VERSION
bound_schema = test_folder.bind_schema(
    json_schema_uri=schema_uri, enable_derived_annos=True
)
json_schema_version_info = bound_schema.json_schema_version_info
print("JSON schema was bound successfully. Please see details below:")
pprint(vars(json_schema_version_info))

# 5. Retrieve the Bound Schema
schema = test_folder.get_schema()
print("JSON Schema was retrieved successfully. Please see details below:")
pprint(vars(schema))

# 6. Add Invalid Annotations to the Folder and Store
test_folder.annotations = {
    "patient_id": "1234",
    "cognitive_score": "invalid str",
}
test_folder.store()

time.sleep(2)

validation_results = test_folder.validate_schema()
print("Validation was completed. Please see details below:")
pprint(vars(validation_results))

# 7. Create a File with Invalid Annotations and Upload It
# Then, view validation statistics and invalid validation results
if not os.path.exists(os.path.expanduser("~/temp")):
    os.makedirs(os.path.expanduser("~/temp/testJSONSchemaFiles"), exist_ok=True)

name_of_file = "test_file.txt"
path_to_file = os.path.join(
    os.path.expanduser("~/temp/testJSONSchemaFiles"), name_of_file
)


def create_random_file(
    path: str,
) -> None:
    """Create a random file with random data.

    :param path: The path to create the file at.
    """
    with open(path, "wb") as f:
        f.write(os.urandom(1))


create_random_file(path_to_file)

annotations = {"patient_id": "123456", "cognitive_score": "invalid child str"}

child_file = File(path=path_to_file, parent_id=test_folder.id, annotations=annotations)
child_file = child_file.store()
time.sleep(2)

validation_statistics = test_folder.get_schema_validation_statistics()
print("Validation statistics were retrieved successfully. Please see details below:")
pprint(vars(validation_statistics))

invalid_validation = invalid_results = test_folder.get_invalid_validation(
    synapse_client=syn
)
for child in invalid_validation:
    print("See details of validation results: ")
    pprint(vars(child))
