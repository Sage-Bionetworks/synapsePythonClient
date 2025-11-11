import os
import sys
from unittest.mock import Mock

import pytest

from synapseclient.extensions.curator.schema_generation import (
    DataModelGraph,
    DataModelGraphExplorer,
    DataModelParser,
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
    """Fixture to instantiate a DataModelGraphExplorer object using the data model with column types"""
    dmge = helpers.get_data_model_graph_explorer(
        path="example.model.column_type_component.csv"
    )
    return dmge
