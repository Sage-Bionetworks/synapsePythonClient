from synapseclient import Synapse
from synapseclient.extensions.curator import generate_jsonschema

# Path or URL to your data model (CSV or JSONLD format)
# Example: "path/to/my_data_model.csv" or "https://raw.githubusercontent.com/example.csv"
DATA_MODEL_SOURCE = "tests/unit/synapseclient/extensions/schema_files/example.model.csv"
# List of component names/data types to create schemas for, or None for all components/data types
# Example: ["Patient", "Biospecimen"] or None
DATA_TYPE = ["Patient"]
# Directory where JSON Schema files will be saved
OUTPUT_DIRECTORY = "./"

syn = Synapse()
syn.login()

schemas, file_paths = generate_jsonschema(
    data_model_source=DATA_MODEL_SOURCE,
    output_directory=OUTPUT_DIRECTORY,
    data_type=DATA_TYPE,
    data_model_labels="class_label",
    synapse_client=syn,
)

print(schemas[0])
