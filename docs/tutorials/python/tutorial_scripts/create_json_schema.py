import logging

from synapseclient.extensions.curator.schema_generation import (
    DataModelGraph,
    DataModelGraphExplorer,
    DataModelParser,
    create_json_schema,
)

LOGGER = logging.Logger("create_json_schema")
# Put the path to your data model here, either CSV or JSONLD format
DATA_MODEL_PATH = "tests/unit/synapseclient/extensions/schema_files/example.model.csv"
# Put the name of the datatype in your data-model you want to create here
DATATYPE = "Patient"

data_model_parser = DataModelParser(path_to_data_model=DATA_MODEL_PATH, logger=LOGGER)
parsed_data_model = data_model_parser.parse_model()
data_model_grapher = DataModelGraph(parsed_data_model)
graph_data_model = data_model_grapher.graph
dmge = DataModelGraphExplorer(graph_data_model, logger=LOGGER)
json_schema = create_json_schema(
    dmge, datatype=DATATYPE, schema_name=DATATYPE, logger=LOGGER
)
print(json_schema)
