import time
from pprint import pprint

import synapseclient
from synapseclient.core.utils import make_bogus_data_file
from synapseclient.models import File, Folder

# 1. Set up Synapse Python client and retrieve project
syn = synapseclient.Synapse()
syn.login()

# Retrieve test project
PROJECT_ID = syn.findEntityId(
    name="My uniquely named project about Alzheimer's Disease"
)

# Create a test folder for JSON schema experiments
test_folder = Folder(name="clinical_data_folder", parent_id=PROJECT_ID).store()

# 2. Take a look at the constants and structure of the JSON schema
ORG_NAME = "myUniqueAlzheimersResearchOrgTutorial"
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
        syn.logger.info(f"Organization {ORG_NAME} already exists.")
        break
else:
    syn.logger.info(f"Creating organization {ORG_NAME}.")
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

try:
    new_test_schema = my_test_org.create_json_schema(
        updated_schema, SCHEMA_NAME, VERSION
    )
except synapseclient.core.exceptions.SynapseHTTPError as e:
    if e.response.status_code == 400 and "already exists" in e.response.text:
        syn.logger.warning(
            f"Schema {SCHEMA_NAME} already exists. Please switch to use a new version number."
        )
    else:
        raise e

# 4. Bind the JSON schema to the folder
schema_uri = ORG_NAME + "-" + SCHEMA_NAME + "-" + VERSION
bound_schema = test_folder.bind_schema(
    json_schema_uri=schema_uri, enable_derived_annotations=True
)
json_schema_version_info = bound_schema.json_schema_version_info
syn.logger.info("JSON schema was bound successfully. Please see details below:")
pprint(vars(json_schema_version_info))

# 5. Retrieve the Bound Schema
schema = test_folder.get_schema()
syn.logger.info("JSON Schema was retrieved successfully. Please see details below:")
pprint(vars(schema))

# 6. Add Invalid Annotations to the Folder and Store
test_folder.annotations = {
    "patient_id": "1234",
    "cognitive_score": "invalid str",
}
test_folder.store()

time.sleep(2)

validation_results = test_folder.validate_schema()
syn.logger.info("Validation was completed. Please see details below:")
pprint(vars(validation_results))

# 7. Create a File with Invalid Annotations and Upload It
# Then, view validation statistics and invalid validation results
path_to_file = make_bogus_data_file(n=5)

annotations = {"patient_id": "123456", "cognitive_score": "invalid child str"}

child_file = File(path=path_to_file, parent_id=test_folder.id, annotations=annotations)
child_file = child_file.store()
time.sleep(2)

validation_statistics = test_folder.get_schema_validation_statistics()
syn.logger.info(
    "Validation statistics were retrieved successfully. Please see details below:"
)
pprint(vars(validation_statistics))

invalid_validation = invalid_results = test_folder.get_invalid_validation()
for child in invalid_validation:
    syn.logger.info("See details of validation results: ")
    pprint(vars(child))
