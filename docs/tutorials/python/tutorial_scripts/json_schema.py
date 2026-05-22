# --8<-- [start:setup]
import time
from pprint import pprint

import synapseclient
from synapseclient.core.utils import make_bogus_data_file
from synapseclient.models import File, Folder, JSONSchema, Project, SchemaOrganization

# 1. Set up Synapse Python client
syn = synapseclient.Synapse()
syn.login()
# --8<-- [end:setup]

# 2. Take a look at the constants and structure of the JSON schema
# --8<-- [start:constants_and_schema]
# Replace your own project name here
PROJECT_ENT = Project(name="My uniquely named project about Alzheimer's Disease").get()
# Replace your own json schema organization name here
ORG_NAME = "myUniqueAlzheimersResearchOrgTutorial"
VERSION = "0.0.1"
NEW_VERSION = "0.0.2"

SCHEMA_NAME = "clinicalObservations"

title = "Alzheimer's Clinical Observation Schema"
schema_body = {
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
# --8<-- [end:constants_and_schema]

# 3. Try create test organization and json schema if they do not exist
# --8<-- [start:create_org_and_schema]
organization = SchemaOrganization(name=ORG_NAME)
try:
    organization.store()
except Exception as e:
    organization.get()

schemas = list(organization.get_json_schemas())
for schema in schemas:
    print(schema)

schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)
try:
    schema.get()
except Exception as e:
    schema.store(schema_body=schema_body, version=VERSION)

schema.get_body()
# --8<-- [end:create_org_and_schema]

# --8<-- [start:update_schema_version]
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
    schema.store(schema_body=updated_schema, version=VERSION)
except synapseclient.core.exceptions.SynapseHTTPError as e:
    if e.response.status_code == 400 and "already exists" in e.response.text:
        syn.logger.warning(
            f"Schema {SCHEMA_NAME} already exists. Please switch to use a new version number."
        )
    else:
        raise e

schema.store(schema_body=updated_schema, version=NEW_VERSION)
schema.get_body(version=VERSION)
# --8<-- [end:update_schema_version]
# 4. Bind the JSON schema to the folder

# --8<-- [start:bind_schema]
# Create a test folder for JSON schema experiments
test_folder = Folder(name="test_folder", parent_id=PROJECT_ENT.id).store()

bound_schema = test_folder.bind_schema(
    json_schema_uri=schema.uri, enable_derived_annotations=True
)
json_schema_version_info = bound_schema.json_schema_version_info
syn.logger.info("JSON schema was bound successfully. Please see details below:")
pprint(vars(json_schema_version_info))
# --8<-- [end:bind_schema]

# --8<-- [start:retrieve_bound_schema]
# 5. Retrieve the Bound Schema
schema = test_folder.get_schema()
syn.logger.info("JSON Schema was retrieved successfully. Please see details below:")
pprint(vars(schema))
# --8<-- [end:retrieve_bound_schema]

# --8<-- [start:add_invalid_annotations]
# 6. Add Invalid Annotations to the Folder and Store
test_folder.annotations = {
    "patient_id": "1234",
    "cognitive_score": "invalid str",
}
test_folder.store()
# --8<-- [end:add_invalid_annotations]

time.sleep(2)
# --8<-- [start:validate_folder]

validation_results = test_folder.validate_schema()
syn.logger.info("Validation was completed. Please see details below:")
# --8<-- [end:validate_folder]
pprint(vars(validation_results))

# --8<-- [start:create_file_with_invalid_annotations]
# 7. Create a File with Invalid Annotations and Upload It
# Then, view validation statistics and invalid validation results
path_to_file = make_bogus_data_file(n=5)

annotations = {"patient_id": "123456", "cognitive_score": "invalid child str"}

child_file = File(path=path_to_file, parent_id=test_folder.id, annotations=annotations)
# --8<-- [end:create_file_with_invalid_annotations]
child_file = child_file.store()
time.sleep(2)
# --8<-- [start:validation_statistics]

validation_statistics = test_folder.get_schema_validation_statistics()
syn.logger.info(
    "Validation statistics were retrieved successfully. Please see details below:"
)
# --8<-- [end:validation_statistics]
pprint(vars(validation_statistics))
# --8<-- [start:invalid_validation_details]

invalid_validation = invalid_results = test_folder.get_invalid_validation()
for child in invalid_validation:
    syn.logger.info("See details of validation results: ")
    # --8<-- [end:invalid_validation_details]
    pprint(vars(child))
