import os
from unittest.mock import Mock

import pytest

from synapseclient.extensions.curator.schema_generation import (
    DataModelCSVParser,
    DataModelGraph,
    DataModelGraphExplorer,
    DataModelJSONLDParser,
    DataModelParser,
    DataModelRelationships,
)

TESTS_DIR = os.path.dirname(os.path.abspath(__file__))
SCHEMA_FILES_DIR = os.path.join(TESTS_DIR, "schema_files")


class Helpers:
    @staticmethod
    def get_schema_file_path(filename):
        """Get path to schema files specifically"""
        return os.path.join(SCHEMA_FILES_DIR, filename)

    @staticmethod
    def get_data_model_graph_explorer(
        path=None, data_model_labels: str = "class_label"
    ):
        """Create DataModelGraphExplorer from schema file"""
        # commenting this now bc we dont want to have multiple instances
        if path is None:
            return

        fullpath = Helpers.get_schema_file_path(path)

        # Instantiate DataModelParser
        data_model_parser = DataModelParser(path_to_data_model=fullpath, logger=Mock())

        # Parse Model
        parsed_data_model = data_model_parser.parse_model()

        # Instantiate DataModelGraph
        data_model_grapher = DataModelGraph(
            parsed_data_model, data_model_labels=data_model_labels, logger=Mock()
        )

        # Generate graph
        graph_data_model = data_model_grapher.graph

        # Instantiate DataModelGraphExplorer
        DMGE = DataModelGraphExplorer(graph_data_model, logger=Mock())

        return DMGE


@pytest.fixture(scope="function")
def helpers():
    yield Helpers


@pytest.fixture(name="dmge", scope="function")
def DMGE(helpers: Helpers) -> DataModelGraphExplorer:
    """Fixture to instantiate a DataModelGraphExplorer object using the csv data model"""
    dmge = helpers.get_data_model_graph_explorer(path="data_models/example.model.csv")
    return dmge


@pytest.fixture(name="dmge_json_ld", scope="function")
def dmge_json_ld(helpers: Helpers) -> DataModelGraphExplorer:
    """Fixture to instantiate a DataModelGraphExplorer object using the jsonls data model"""
    dmge = helpers.get_data_model_graph_explorer(
        path="data_models_jsonld/example.model.jsonld"
    )
    return dmge


@pytest.fixture(name="dmr")
def fixture_dmr():
    "Returns a DataModelRelationships instance"
    return DataModelRelationships()


@pytest.fixture(name="dmp")
def fixture_dmp(helpers: Helpers) -> DataModelParser:
    "Returns a DataModelParser using the csv data model"
    data_model_path = helpers.get_schema_file_path("data_models/example.model.csv")
    dmp = DataModelParser(data_model_path, logger=Mock())
    return dmp


@pytest.fixture(name="dmp_jsonld")
def fixture_dmp_json_ld(helpers: Helpers) -> DataModelParser:
    "Returns a DataModelParser using the jsonld data model"
    data_model_path = helpers.get_schema_file_path(
        "data_models_jsonld/example.model.jsonld"
    )
    dmp = DataModelParser(data_model_path, logger=Mock())
    return dmp


@pytest.fixture(name="csv_dmp")
def fixture_csv_dmp() -> DataModelCSVParser:
    "Returns a DataModelCSVParser"
    dmp = DataModelCSVParser(logger=Mock())
    return dmp


@pytest.fixture(name="jsonld_dmp")
def fixture_jsonld_dmp() -> DataModelCSVParser:
    "Returns a DataModelJSONLDParser"
    dmp = DataModelJSONLDParser()
    return dmp
