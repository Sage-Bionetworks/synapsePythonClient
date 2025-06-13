import os
import time
from pprint import pprint

import synapseclient
from synapseclient.models import File, Folder

# Step 1: Set up Synapse Python client
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


# Retrieve test project
PROJECT_ID = syn.findEntityId(
    name="My uniquely named project about Alzheimer's Disease"
)

# Create a test folder for JSON schema experiments
test_folder = Folder(name="clinical_data_folder", parent_id=PROJECT_ID).store()

# Step 2: Take a look at the constants and structure of the JSON schema
# For this example, a test organization and schema are already created
ORG_NAME = "myUniqueAlzheimersResearchOrgTurtorial"
VERSION = "0.0.1"
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

# Step 3: Retrieve test organization
js = syn.service("json_schema")
# Retrieve test organizations
my_test_org = js.JsonSchemaOrganization(ORG_NAME)

# Step 4: Bind the JSON schema to the folder
schema_uri = ORG_NAME + "-" + SCHEMA_NAME + "-" + VERSION
bound_schema = test_folder.bind_schema(
    json_schema_uri=schema_uri, enable_derived_annos=True
)
json_schema_version_info = bound_schema.json_schema_version_info
print("JSON schema was bound successfully. Please see details below:")
pprint(vars(json_schema_version_info))

# Step 5: Retrieve the Bound Schema
schema = test_folder.get_schema()
print("JSON Schema was retrieved successfully. Please see details below:")
pprint(vars(schema))

# Step 6: Add Invalid Annotations to the Folder and Store
test_folder.annotations = {
    "patient_id": "1234",
    "cognitive_score": "invalid str",
}
test_folder.store()

time.sleep(2)

# Step 7: Validate Folder Against the Schema
validation_results = test_folder.validate_schema()
print("Validation was completed. Please see details below:")
pprint(vars(validation_results))

# Step 8: Create a File with Invalid Annotations and Upload It
if not os.path.exists(os.path.expanduser("~/temp")):
    os.makedirs(os.path.expanduser("~/temp/testJSONSchemaFiles"), exist_ok=True)

name_of_file = "test_file.txt"
path_to_file = os.path.join(
    os.path.expanduser("~/temp/testJSONSchemaFiles"), name_of_file
)
create_random_file(path_to_file)

annotations = {"patient_id": "123456", "cognitive_score": "invalid child str"}

child_file = File(path=path_to_file, parent_id=test_folder.id, annotations=annotations)
child_file = child_file.store()
time.sleep(2)

# Step 9: View Validation Statistics
validation_statistics = test_folder.get_schema_validation_statistics()
print("Validation statistics were retrieved successfully. Please see details below:")
pprint(vars(validation_statistics))

# Step 10: View Invalid Validation Results
invalid_validation = test_folder.get_invalid_validation()
for child in invalid_validation:
    print("See details of validation results: ")
    pprint(vars(child))
