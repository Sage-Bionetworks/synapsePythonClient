from synapseclient import Synapse
from synapseclient.extensions.curator import (
    bind_jsonschema,
    generate_jsonschema,
    register_jsonschema,
)

# Path or URL to your data model (CSV or JSONLD format)
# Example: "path/to/my_data_model.csv" or "https://raw.githubusercontent.com/example.csv"
DATA_MODEL_SOURCE = "tests/unit/synapseclient/extensions/schema_files/example.model.csv"
# List of component names/data types to create schemas for, or None for all components/data types
# Example: ["Patient", "Biospecimen"] or None
DATA_TYPE = ["Patient"]
# Directory where JSON Schema files will be saved
OUTPUT_DIRECTORY = "temp"
# Path to a generated JSON Schema file for registration
SCHEMA_PATH = "temp/Patient.json"
# Your Synapse organization name for schema registration
ORGANIZATION_NAME = "my.organization"
# Name for the schema
SCHEMA_NAME = "patient.schema"
# Version number for the schema
SCHEMA_VERSION = "0.0.1"
# Synapse entity ID to bind the schema to (file, folder, etc.)
ENTITY_ID = "syn12345678"

syn = Synapse()
syn.login()

# Create JSON Schemas for multiple data types
schemas, file_paths = generate_jsonschema(
    data_model_source=DATA_MODEL_SOURCE,
    output=OUTPUT_DIRECTORY,
    data_types=DATA_TYPE,
    synapse_client=syn,
)

# Create JSON Schemas for all data types
schemas, file_paths = generate_jsonschema(
    data_model_source=DATA_MODEL_SOURCE,
    output=OUTPUT_DIRECTORY,
    synapse_client=syn,
)

# Specify path for JSON Schema
schemas, file_paths = generate_jsonschema(
    data_model_source=DATA_MODEL_SOURCE,
    data_types=DATA_TYPE,
    output="test.json",
    synapse_client=syn,
)

# Create JSON Schema in cwd
schemas, file_paths = generate_jsonschema(
    data_model_source=DATA_MODEL_SOURCE,
    data_types=DATA_TYPE,
    synapse_client=syn,
)

# Create JSON Schema using display names for both properties names and valid values
schemas, file_paths = generate_jsonschema(
    data_model_source=DATA_MODEL_SOURCE,
    data_types=DATA_TYPE,
    data_model_labels="display_label",
    synapse_client=syn,
)

# Register a JSON Schema to Synapse
json_schema = register_jsonschema(
    schema_path=SCHEMA_PATH,
    organization_name=ORGANIZATION_NAME,
    schema_name=SCHEMA_NAME,
    schema_version=SCHEMA_VERSION,
    synapse_client=syn,
)
print(f"Registered schema URI: {json_schema.uri}")

# Bind a JSON Schema to a Synapse entity
result = bind_jsonschema(
    entity_id=ENTITY_ID,
    json_schema_uri=json_schema.uri,
    enable_derived_annotations=True,
    synapse_client=syn,
)
print(f"Successfully bound schema to entity: {result}")
