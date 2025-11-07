from synapseclient import Synapse
from synapseclient.extensions.curator.schema_generation import generate_jsonschema

# Put the path/url of your data model here, either CSV or JSONLD format
DATA_MODEL_SOURCE = "tests/unit/synapseclient/extensions/schema_files/example.model.csv"
# Put the names of the datatypes in your data-model you want to create here
#  or None to create them all
DATA_TYPE = ["Patient"]
# Put the directory where you want the JSONSchema to generated at here
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
