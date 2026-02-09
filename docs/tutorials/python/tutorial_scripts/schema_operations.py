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

syn = Synapse()
syn.login()

schemas, file_paths = generate_jsonschema(
    data_model_source=DATA_MODEL_SOURCE,
    output=OUTPUT_DIRECTORY,
    data_types=DATA_TYPE,
    synapse_client=syn,
)

print(schemas[0])


# Create JSON Schemas for multiple data types
schemas, file_paths = generate_jsonschema(
    data_model_source=DATA_MODEL_SOURCE,
    output=OUTPUT_DIRECTORY,
    data_types=["Patient", "Biospecimen"],
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

# Register a JSON Schema to Synapse
schema_uri, message = register_jsonschema(
    schema_path="temp/Patient.json",  # Path to the generated JSON Schema file
    organization_name="my.organization",  # Your Synapse organization name
    schema_name="patient.schema",  # Name for the schema
    schema_version="0.0.1",  # Optional version number
    synapse_client=syn,
)
print(message)
print(f"Registered schema URI: {schema_uri}")

# Bind a JSON Schema to a Synapse entity
result = bind_jsonschema(
    entity_id="syn12345678",  # Replace with your entity ID (file, folder, etc.)
    json_schema_uri=schema_uri,  # URI from the registration step above
    enable_derived_annotations=True,  # Enable auto-population of metadata
    synapse_client=syn,
)
print(f"Successfully bound schema to entity: {result}")
