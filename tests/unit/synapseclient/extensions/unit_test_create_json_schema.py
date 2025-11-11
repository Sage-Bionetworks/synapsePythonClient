"""
This contains unit test for the crate_json_schema function, and its helper classes and functions.
The helper classes tested are JSONSchema, Node, GraphTraversalState,
"""

import json
import logging
import os
from typing import Any, Optional
from unittest.mock import Mock

import pytest
from jsonschema import Draft7Validator
from jsonschema.exceptions import ValidationError

from synapseclient.extensions.curator.schema_generation import (
    DataModelGraphExplorer,
    GraphTraversalState,
    JSONSchema,
    JSONSchemaFormat,
    JSONSchemaType,
    TraversalNode,
    _create_array_property,
    _create_enum_array_property,
    _create_enum_property,
    _create_simple_property,
    _get_validation_rule_based_fields,
    _set_conditional_dependencies,
    _set_property,
    _set_type_specific_keywords,
    _write_data_model,
    create_json_schema,
)

# pylint: disable=protected-access
# pylint: disable=too-many-arguments
# pylint: disable=too-many-positional-arguments


# Test data paths
TEST_DATA_BASE_PATH = "tests/unit/synapseclient/extensions"
SCHEMA_FILES_DIR = f"{TEST_DATA_BASE_PATH}/schema_files"
EXPECTED_SCHEMAS_DIR = f"{SCHEMA_FILES_DIR}/expected_jsonschemas"
JSON_INSTANCES_DIR = f"{SCHEMA_FILES_DIR}/json_instances"

# Schema file patterns
EXPECTED_SCHEMA_PATTERN = "{datatype}.schema.json"
EXPECTED_DISPLAY_NAMES_SCHEMA_PATTERN = "{datatype}.display_names_schema.json"
TEST_SCHEMA_PATTERN = "test.{datatype}.schema.json"
TEST_DISPLAY_NAMES_SCHEMA_PATTERN = "test.{datatype}.display_names_schema.json"


# Helper functions for path construction
def get_expected_schema_path(datatype: str, display_names: bool = False) -> str:
    """Get path to expected schema file"""
    pattern = (
        EXPECTED_DISPLAY_NAMES_SCHEMA_PATTERN
        if display_names
        else EXPECTED_SCHEMA_PATTERN
    )
    filename = f"expected.{pattern.format(datatype=datatype)}"
    return f"{EXPECTED_SCHEMAS_DIR}/{filename}"


def get_json_instance_path(filename: str) -> str:
    """Get path to JSON instance file"""
    return f"{JSON_INSTANCES_DIR}/{filename}"


def get_test_schema_path(
    test_directory: str, datatype: str, display_names: bool = False
) -> str:
    """Get path for generated test schema file"""
    pattern = (
        TEST_DISPLAY_NAMES_SCHEMA_PATTERN if display_names else TEST_SCHEMA_PATTERN
    )
    filename = pattern.format(datatype=datatype)
    return os.path.join(test_directory, filename)


@pytest.fixture(name="test_directory", scope="function")
def fixture_test_directory(tmp_path) -> str:
    """Returns a directory for creating test JSON Schemas in"""
    # pytest automatically handles cleanup
    return str(tmp_path)


@pytest.fixture(name="test_nodes")
def fixture_test_nodes(
    dmge: DataModelGraphExplorer,
) -> dict[str, TraversalNode]:
    """Yields dict of Nodes"""
    nodes = [
        "NoRules",
        "NoRulesNotRequired",
        "String",
        "StringNotRequired",
        "Enum",
        "EnumNotRequired",
        "InRange",
        "Regex",
        "Date",
        "URL",
        "List",
        "ListNotRequired",
        "ListEnum",
        "ListEnumNotRequired",
        "ListString",
        "ListInRange",
    ]
    nodes = {
        node: TraversalNode(node, "JSONSchemaComponent", dmge, logger=Mock())
        for node in nodes
    }
    return nodes


@pytest.fixture(name="test_nodes_column_types")
def fixture_test_nodes_column_types(
    dmge: DataModelGraphExplorer,
):
    """Yields dict of Nodes"""
    nodes = [
        "NoRules",
        "NoRulesNotRequired",
        "String",
        "StringNotRequired",
        "Enum",
        "EnumNotRequired",
        "InRange",
        "Regex",
        "Date",
        "URL",
        "List",
        "ListNotRequired",
        "ListEnum",
        "ListEnumNotRequired",
        "ListString",
        "ListInRange",
    ]
    nodes = {
        node: TraversalNode(node, "JSONSchemaComponent", dmge, logger=Mock())
        for node in nodes
    }
    return nodes


class TestJSONSchema:
    """Tests for JSONSchema class - validates JSON schema object creation and manipulation."""

    def test_init(self) -> None:
        """Test the JSONSchema.init method"""
        schema = JSONSchema()
        assert schema.schema_id == ""
        assert schema.title == ""
        assert schema.schema == "http://json-schema.org/draft-07/schema#"
        assert schema.type == "object"
        assert schema.description == "TBD"
        assert not schema.properties
        assert not schema.required
        assert not schema.all_of

    def test_as_json_schema_dict(self) -> None:
        """Test the JSONSchema.as_json_schema_dict method"""
        schema = JSONSchema()
        assert schema.as_json_schema_dict() == {
            "$id": "",
            "$schema": "http://json-schema.org/draft-07/schema#",
            "description": "TBD",
            "properties": {},
            "required": [],
            "title": "",
            "type": "object",
        }

    def test_add_required_property(self) -> None:
        """Test the JSONSchema.add_required_property method"""
        # GIVEN a JSONSchema instance
        schema = JSONSchema()
        # WHEN adding a required property
        schema.add_required_property("name1")
        # THEN that property should be retrievable
        assert schema.required == ["name1"]
        # WHEN adding a second required property
        schema.add_required_property("name2")
        # THEN both properties should be retrievable
        assert schema.required == ["name1", "name2"]

    def test_add_to_all_of_list(self) -> None:
        """Test the JSONSchema.add_to_all_of_list method"""
        # GIVEN a JSONSchema instance
        schema = JSONSchema()
        # WHEN adding a dict to the all of list
        schema.add_to_all_of_list({"if": {}, "then": {}})
        # THEN that dict should be retrievable
        assert schema.all_of == [{"if": {}, "then": {}}]
        # WHEN adding a second dict
        schema.add_to_all_of_list({"if2": {}, "then2": {}})
        # THEN both dicts should be retrievable
        assert schema.all_of == [{"if": {}, "then": {}}, {"if2": {}, "then2": {}}]

    def test_update_property(self) -> None:
        """
        Test JSONSchema.update_property method.

        Verifies that properties can be added and updated in the schema's
        properties dictionary, maintaining existing properties while adding new ones.
        """
        # GIVEN a JSONSchema instance
        schema = JSONSchema()
        # WHEN updating the properties dict
        schema.update_property({"name1": "property1"})
        # THEN that dict should be retrievable
        assert schema.properties == {"name1": "property1"}
        # WHEN updating the properties dict with a new key
        schema.update_property({"name2": "property2"})
        # THEN the new key and old key should be retrievable
        assert schema.properties == {"name1": "property1", "name2": "property2"}


@pytest.mark.parametrize(
    "node_name, expected_type, expected_is_array, expected_min, expected_max, expected_pattern, expected_format",
    [
        # Node with no columnType - all constraint fields should be None/False
        ("NoRules", None, False, None, None, None, None),
        # Node with columnType "string" - type is set to STRING via columnType
        ("String", JSONSchemaType.STRING, False, None, None, None, None),
        # Node with "list" validation rule - is_array is set to True, type remains None (no columnType)
        ("List", None, True, None, None, None, None),
        # Node with both "list" validation rules and columnType "string" - is_array is True, type is STRING
        ("ListString", JSONSchemaType.STRING, True, None, None, None, None),
        # Node with "inRange 50 100" validation rule and columnType "number" - min/max are set, type is NUMBER
        ("InRange", JSONSchemaType.NUMBER, False, 50, 100, None, None),
        # Node with "regex search [a-f]" validation rule and columnType "string" - pattern is set, type is STRING
        ("Regex", JSONSchemaType.STRING, False, None, None, "[a-f]", None),
        # Node with "date" validation rule and columnType "string" - format is set to DATE, type is STRING
        ("Date", JSONSchemaType.STRING, False, None, None, None, JSONSchemaFormat.DATE),
        # Node with "url" validation rule and columnType "string" - format is set to URI, type is STRING
        ("URL", JSONSchemaType.STRING, False, None, None, None, JSONSchemaFormat.URI),
    ],
    ids=["None", "String", "List", "ListString", "InRange", "Regex", "Date", "URI"],
)
def test_node_init(
    node_name: str,
    expected_type: Optional[JSONSchemaType],
    expected_is_array: bool,
    expected_min: Optional[float],
    expected_max: Optional[float],
    expected_pattern: Optional[str],
    expected_format: Optional[JSONSchemaFormat],
    test_nodes: dict[str, TraversalNode],
) -> None:
    """
    Tests for TraversalNode class initialization.

    Verifies that TraversalNode objects are correctly initialized with:
    - Types derived from columnType attribute in the data model
    - Validation constraints extracted from validation rules (format, pattern, min/max, array flag)
    - Proper combination of columnType and validation rule parsing

    The type property comes from the columnType field, while constraints
    come from parsing validation rules like "str", "inRange", "regex", etc.
    """
    node = test_nodes[node_name]
    assert node.type == expected_type
    assert node.format == expected_format
    assert node.is_array == expected_is_array
    assert node.minimum == expected_min
    assert node.maximum == expected_max
    assert node.pattern == expected_pattern


@pytest.mark.parametrize(
    "validation_rules, expected_is_array, expected_min, expected_max, expected_pattern, expected_format",
    [
        # If there are no validation rules, all fields should be None/False
        ([], False, None, None, None, None),
        # If there is a "str" validation rule, only format/constraints are set, no explicit type
        (["str"], False, None, None, None, None),
        # If there is a "list" validation rule, is_array is set to True
        (["list"], True, None, None, None, None),
        # If there are both "list" and "str" validation rules, is_array is True, other fields remain None
        (["list", "str"], True, None, None, None, None),
        # If there is an "inRange" rule, minimum and maximum are extracted and set
        (["inRange 50 100"], False, 50, 100, None, None),
        # If there is a "regex search" rule, the pattern is extracted and set
        (
            ["regex search [a-f]"],
            False,
            None,
            None,
            "[a-f]",
            None,
        ),
        # If there is a "date" rule, the format is set to JSONSchemaFormat.DATE
        (
            ["date"],
            False,
            None,
            None,
            None,
            JSONSchemaFormat.DATE,
        ),
        # If there is a "url" rule, the format is set to JSONSchemaFormat.URI
        (["url"], False, None, None, None, JSONSchemaFormat.URI),
    ],
    ids=["No rules", "String", "List", "ListString", "InRange", "Regex", "Date", "URL"],
)
def test_get_validation_rule_based_fields_no_explicit_type(
    validation_rules: list[str],
    expected_is_array: bool,
    expected_min: Optional[float],
    expected_max: Optional[float],
    expected_pattern: Optional[str],
    expected_format: Optional[JSONSchemaFormat],
) -> None:
    """
    Test for _get_validation_rule_based_fields
    Tests that output is expected based on the input validation rules
    """
    logger = Mock()
    (
        is_array,
        property_format,
        minimum,
        maximum,
        pattern,
    ) = _get_validation_rule_based_fields(validation_rules, logger)
    assert property_format == expected_format
    assert is_array == expected_is_array
    assert minimum == expected_min
    assert maximum == expected_max
    assert pattern == expected_pattern


@pytest.mark.parametrize(
    "validation_rules, expected_is_array, expected_min, expected_max, expected_pattern, expected_format",
    [
        (
            [],
            False,
            None,
            None,
            None,
            None,
        ),
        (
            ["str"],
            False,
            None,
            None,
            None,
            None,
        ),
        (
            ["list"],
            True,
            None,
            None,
            None,
            None,
        ),
        (
            ["inRange 50 100"],
            False,
            50,
            100,
            None,
            None,
        ),
        (
            ["regex search [a-f]"],
            False,
            None,
            None,
            "[a-f]",
            None,
        ),
        (
            ["date"],
            False,
            None,
            None,
            None,
            JSONSchemaFormat.DATE,
        ),
        (
            ["url"],
            False,
            None,
            None,
            None,
            JSONSchemaFormat.URI,
        ),
    ],
    ids=["No rules", "String", "List string", "InRange", "Regex", "Date", "URL"],
)
def test_get_validation_rule_based_fields_with_explicit_type(
    validation_rules: list[str],
    expected_is_array: bool,
    expected_min: Optional[float],
    expected_max: Optional[float],
    expected_pattern: Optional[str],
    expected_format: Optional[JSONSchemaFormat],
) -> None:
    """
    Test for _get_validation_rule_based_fields
    Tests that output is expected based on the input validation rules, and explicit type
    """
    logger = Mock()
    (
        is_array,
        property_format,
        minimum,
        maximum,
        pattern,
    ) = _get_validation_rule_based_fields(validation_rules, logger)
    assert property_format == expected_format
    assert is_array == expected_is_array
    assert minimum == expected_min
    assert maximum == expected_max
    assert pattern == expected_pattern


class TestGraphTraversalState:
    """Tests for GraphTraversalState class"""

    def test_init(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState.__init__"""
        # GIVEN a GraphTraversalState instance with 5 nodes
        gts = GraphTraversalState(dmge, "Patient", logger=Mock())
        # THEN the current_node, current_node_display_name, and first item in
        #  root dependencies should be "Component"
        assert gts.current_node.name == "Component"
        assert gts._root_dependencies[0] == "Component"
        assert gts.current_node.display_name == "Component"
        # THEN
        #  - root_dependencies should be 5 items long
        #  - nodes to process should be the same minus "Component"
        #  - _processed_nodes, _reverse_dependencies, and _valid_values_map should be empty
        assert gts._root_dependencies == [
            "Component",
            "Diagnosis",
            "PatientID",
            "Sex",
            "YearofBirth",
        ]
        assert gts._nodes_to_process == ["Diagnosis", "PatientID", "Sex", "YearofBirth"]
        assert not gts._processed_nodes
        assert not gts._reverse_dependencies
        assert not gts._valid_values_map

    def test_move_to_next_node(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState.move_to_next_node"""
        # GIVEN a GraphTraversalState instance with 2 nodes
        gts = GraphTraversalState(dmge, "Patient", logger=Mock())
        gts._nodes_to_process = ["YearofBirth"]
        # THEN the current_node should be "Component" and node to process has 1 node
        assert gts.current_node.name == "Component"
        assert gts.current_node.display_name == "Component"
        assert gts._nodes_to_process == ["YearofBirth"]
        # WHEN using move_to_next_node
        gts.move_to_next_node()
        # THEN the current_node should now be YearofBirth and no nodes to process
        assert gts.current_node.name == "YearofBirth"
        assert gts.current_node.display_name == "Year of Birth"
        assert not gts._nodes_to_process

    def test_are_nodes_remaining(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState.are_nodes_remaining"""
        # GIVEN a GraphTraversalState instance with 1 node
        gts = GraphTraversalState(dmge, "Patient", logger=Mock())
        gts._nodes_to_process = []
        # THEN there should be nodes_remaining
        assert gts.are_nodes_remaining()
        # WHEN using move_to_next_node
        gts.move_to_next_node()
        # THEN there should not be nodes_remaining
        assert not gts.are_nodes_remaining()

    def test_is_current_node_processed(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState.is_current_node_processed"""
        # GIVEN a GraphTraversalState instance
        gts = GraphTraversalState(dmge, "Patient", logger=Mock())
        # THEN the current node should not have been processed yet.
        assert not gts.is_current_node_processed()
        # WHEN adding a the current node to the processed list
        gts.update_processed_nodes_with_current_node()
        # THEN the current node should be listed as processed.
        assert gts.is_current_node_processed()

    def test_is_current_node_a_property(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState.is_current_node_a_property"""
        # GIVEN a GraphTraversalState instance where the first node is Component and second is Male
        gts = GraphTraversalState(dmge, "Patient", logger=Mock())
        gts._nodes_to_process = ["Male"]
        # THEN the current node should be a property
        assert gts.is_current_node_a_property()
        # WHEN using move_to_next_node
        gts.move_to_next_node()
        # THEN the current node should not be a property, as the Male node is a valid value
        assert not gts.is_current_node_a_property()

    def test_is_current_node_in_reverse_dependencies(
        self, dmge: DataModelGraphExplorer
    ) -> None:
        """Test GraphTraversalState.is_current_node_in_reverse_dependencies"""
        # GIVEN a GraphTraversalState instance where
        # - the first node is Component
        # - the second node is FamilyHistory
        # - FamilyHistory has a reverse dependency of Cancer
        gts = GraphTraversalState(dmge, "Patient", logger=Mock())
        gts._nodes_to_process = ["FamilyHistory"]
        gts._reverse_dependencies = {"FamilyHistory": ["Cancer"]}
        # THEN the current should not have reverse dependencies
        assert not gts.is_current_node_in_reverse_dependencies()
        # WHEN using move_to_next_node
        gts.move_to_next_node()
        # THEN the current node should have reverse dependencies
        assert gts.is_current_node_in_reverse_dependencies()

    def test_update_processed_nodes_with_current_node(
        self, dmge: DataModelGraphExplorer
    ) -> None:
        """Test GraphTraversalState.update_processed_nodes_with_current_node"""
        # GIVEN a GraphTraversalState instance
        gts = GraphTraversalState(dmge, "Patient", logger=Mock())
        # WHEN the node has been processed
        gts.update_processed_nodes_with_current_node()
        # THEN the node should be listed as processed
        assert gts._processed_nodes == ["Component"]

    def test_get_conditional_properties(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState.get_conditional_properties"""
        # GIVEN a GraphTraversalState instance where
        # - the first node is Component
        # - the second node is FamilyHistory
        # - FamilyHistory has a reverse dependency of Cancer
        # - Cancer is a valid value of Diagnosis
        gts = GraphTraversalState(dmge, "Patient", logger=Mock())
        gts._nodes_to_process = ["FamilyHistory"]
        gts._reverse_dependencies = {"FamilyHistory": ["Cancer"]}
        gts._valid_values_map = {"Cancer": ["Diagnosis"]}
        # WHEN using move_to_next_node
        gts.move_to_next_node()
        # THEN the current node should have conditional properties
        assert gts.get_conditional_properties() == [("Diagnosis", "Cancer")]

    def test_update_valid_values_map(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState._update_valid_values_map"""
        # GIVEN a GraphTraversalState instance
        gts = GraphTraversalState(dmge, "Patient", logger=Mock())
        # THEN the valid_values_map should be empty to start with
        assert not gts._valid_values_map
        # WHEN the map is updated with one node and two values
        gts._update_valid_values_map("Diagnosis", ["Healthy", "Cancer"])
        # THEN valid values map should have one entry for each valid value,
        #  with the node as the value
        assert gts._valid_values_map == {
            "Healthy": ["Diagnosis"],
            "Cancer": ["Diagnosis"],
        }

    def test_update_reverse_dependencies(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState._update_reverse_dependencies"""
        # GIVEN a GraphTraversalState instance
        gts = GraphTraversalState(dmge, "Patient", logger=Mock())
        # THEN the reverse_dependencies should be empty to start with
        assert not gts._reverse_dependencies
        # WHEN the map is updated with one node and two reverse_dependencies
        gts._update_reverse_dependencies("Cancer", ["CancerType", "FamilyHistory"])
        # THEN reverse_dependencies should have one entry for each valid value,
        #  with the node as the value
        assert gts._reverse_dependencies == {
            "CancerType": ["Cancer"],
            "FamilyHistory": ["Cancer"],
        }

    def test_update_nodes_to_process(self, dmge: DataModelGraphExplorer) -> None:
        """Test GraphTraversalState._update_nodes_to_process"""
        # GIVEN a GraphTraversalState instance with 5 nodes
        gts = GraphTraversalState(dmge, "Patient", logger=Mock())
        # THEN the GraphTraversalState should have 4 nodes in nodes_to_process
        assert len(gts._nodes_to_process) == 4
        # WHEN adding a node to nodes_to_process
        gts._update_nodes_to_process(["NewNode"])
        # THEN that node should be in nodes_to_process as the last item
        assert len(gts._nodes_to_process) == 5
        assert gts._nodes_to_process[4] == "NewNode"


@pytest.mark.parametrize(
    "datatype",
    [
        ("Biospecimen"),
        ("BulkRNA-seqAssay"),
        ("JSONSchemaComponent"),
        ("MockComponent"),
        ("MockFilename"),
        ("MockRDB"),
        ("Patient"),
    ],
    ids=[
        "Biospecimen",
        "BulkRNA-seqAssay",
        "JSONSchemaComponent",
        "MockComponent",
        "MockFilename",
        "MockRDB",
        "Patient",
    ],
)
def test_create_json_schema_with_class_label(
    dmge: DataModelGraphExplorer, datatype: str, test_directory: str
) -> None:
    """Tests for JSONSchemaGenerator.create_json_schema"""
    test_path = get_test_schema_path(test_directory, datatype)
    expected_path = get_expected_schema_path(datatype)
    logger = logging.getLogger(__name__)

    create_json_schema(
        dmge=dmge,
        datatype=datatype,
        schema_name=f"{datatype}_validation",
        schema_path=test_path,
        use_property_display_names=False,
        logger=logger,
    )
    with open(expected_path, encoding="utf-8") as file1, open(
        test_path, encoding="utf-8"
    ) as file2:
        expected_json = json.load(file1)
        test_json = json.load(file2)
    assert expected_json == test_json


@pytest.mark.parametrize(
    "datatype",
    [
        ("BulkRNA-seqAssay"),
        ("Patient"),
    ],
    ids=["BulkRNA-seqAssay", "Patient"],
)
def test_create_json_schema_with_display_names(
    dmge: DataModelGraphExplorer, datatype: str, test_directory: str
) -> None:
    """Tests for JSONSchemaGenerator.create_json_schema"""
    logger = logging.getLogger(__name__)
    test_path = get_test_schema_path(test_directory, datatype, display_names=True)
    expected_path = get_expected_schema_path(datatype, display_names=True)
    create_json_schema(
        dmge=dmge,
        datatype=datatype,
        schema_name=f"{datatype}_validation",
        schema_path=test_path,
        logger=logger,
    )
    with open(expected_path, encoding="utf-8") as file1, open(
        test_path, encoding="utf-8"
    ) as file2:
        expected_json = json.load(file1)
        test_json = json.load(file2)
    assert expected_json == test_json


def test_create_json_schema_with_no_column_type(
    dmge: DataModelGraphExplorer, test_directory: str
) -> None:
    """
    Tests for JSONSchemaGenerator.create_json_schema
    This tests where the data model does not have columnType attribute
    """
    datatype = "JSONSchemaComponent"
    test_path = get_test_schema_path(test_directory, datatype, display_names=True)
    expected_path = get_expected_schema_path(datatype)
    logger = logging.getLogger(__name__)
    create_json_schema(
        dmge=dmge,
        datatype=datatype,
        schema_name=f"{datatype}_validation",
        schema_path=test_path,
        use_property_display_names=False,
        logger=logger,
    )
    with open(expected_path, encoding="utf-8") as file1, open(
        test_path, encoding="utf-8"
    ) as file2:
        expected_json = json.load(file1)
        test_json = json.load(file2)
    assert expected_json == test_json


def test_create_json_schema_with_column_type(
    dmge: DataModelGraphExplorer, test_directory: str
) -> None:
    """
    Tests for JSONSchemaGenerator.create_json_schema
    This tests where the data model does have the columnType attribute
    """
    datatype = "JSONSchemaComponent"
    test_path = get_test_schema_path(test_directory, datatype, display_names=True)
    expected_path = get_expected_schema_path(datatype, display_names=True)

    logger = logging.getLogger(__name__)
    create_json_schema(
        dmge=dmge,
        datatype=datatype,
        schema_name=f"{datatype}_validation",
        schema_path=test_path,
        use_property_display_names=False,
        logger=logger,
    )
    with open(expected_path, encoding="utf-8") as file1, open(
        test_path, encoding="utf-8"
    ) as file2:
        expected_json = json.load(file1)
        test_json = json.load(file2)
    assert expected_json == test_json


@pytest.mark.parametrize(
    "instance_filename, datatype",
    [
        (
            "valid_biospecimen1.json",
            "Biospecimen",
        ),
        (
            "valid_bulk_rna1.json",
            "BulkRNA-seqAssay",
        ),
        (
            "valid_bulk_rna2.json",
            "BulkRNA-seqAssay",
        ),
        (
            "valid_patient1.json",
            "Patient",
        ),
        (
            "valid_patient2.json",
            "Patient",
        ),
    ],
    ids=[
        "Biospecimen",
        "BulkRNASeqAssay, FileFormat is BAM",
        "BulkRNASeqAssay, FileFormat is CRAM",
        "Patient, Diagnosis is Healthy",
        "Patient, Diagnosis is Cancer",
    ],
)
def test_validate_valid_instances(
    instance_filename: str,
    datatype: str,
) -> None:
    """Validates instances using expected JSON Schemas"""
    schema_path = get_expected_schema_path(datatype)
    instance_path = get_json_instance_path(instance_filename)

    with open(schema_path, encoding="utf-8") as schema_file:
        schema = json.load(schema_file)
    with open(instance_path, encoding="utf-8") as instance_file:
        instance = json.load(instance_file)
    validator = Draft7Validator(schema)
    validator.validate(instance)


@pytest.mark.parametrize(
    "instance_filename, datatype",
    [
        (
            "bulk_rna_missing_conditional_dependencies.json",
            "BulkRNA-seqAssay",
        ),
        (
            "patient_missing_conditional_dependencies.json",
            "Patient",
        ),
    ],
    ids=[
        "BulkRNA, FileFormat is CRAM, missing conditional dependencies",
        "Patient, Diagnosis is Cancer, missing conditional dependencies",
    ],
)
def test_validate_invalid_instances(
    instance_filename: str,
    datatype: str,
) -> None:
    """Raises a ValidationError validating invalid instances using expected JSON Schemas"""

    schema_path = get_expected_schema_path(datatype)
    instance_path = get_json_instance_path(instance_filename)

    with open(schema_path, encoding="utf-8") as schema_file:
        schema = json.load(schema_file)
    with open(instance_path, encoding="utf-8") as instance_file:
        instance = json.load(instance_file)
    validator = Draft7Validator(schema)
    with pytest.raises(ValidationError):
        validator.validate(instance)


def test_write_data_model_with_schema_path(test_directory: str) -> None:
    """Test for _write_data_model with the path provided."""
    schema_path = os.path.join(test_directory, "test_write_data_model1.json")
    logger = Mock()
    _write_data_model(json_schema_dict={}, schema_path=schema_path, logger=logger)
    assert os.path.exists(schema_path)


def test_write_data_model_with_name_and_jsonld_path(test_directory: str) -> None:
    """
    Test _write_data_model with a name and JSONLD path.

    When provided with a name and jsonld_path, the function should create
    a schema file with the format: "<jsonld_path_prefix>.<name>.schema.json"
    """
    json_ld_path = os.path.join(test_directory, "fake_model.jsonld")
    logger = Mock()
    schema_path = os.path.join(
        test_directory, "fake_model.test_write_data_model2.schema.json"
    )
    _write_data_model(
        json_schema_dict={},
        name="test_write_data_model2",
        jsonld_path=json_ld_path,
        logger=logger,
    )
    assert os.path.exists(schema_path)


def test_write_data_model_exception() -> None:
    """
    Test _write_data_model error handling.

    When neither schema_path nor (name + jsonld_path) are provided,
    the function should raise a ValueError.
    """
    with pytest.raises(ValueError):
        _write_data_model(json_schema_dict={}, logger=Mock())


@pytest.mark.parametrize(
    "reverse_dependencies, valid_values_map",
    [
        # If the input node has no reverse dependencies, nothing gets added
        ({"CancerType": []}, {}),
        # If the input node has reverse dependencies,
        #  but none of them are in the valid values map, nothing gets added
        ({"CancerType": ["Cancer"]}, {}),
    ],
    ids=[
        "No reverse dependencies",
        "No valid values",
    ],
)
def test_set_conditional_dependencies_nothing_added(
    reverse_dependencies: dict[str, list[str]],
    valid_values_map: dict[str, list[str]],
    dmge: DataModelGraphExplorer,
) -> None:
    """
    Tests for _set_conditional_dependencies
      were the schema doesn't change
    """
    json_schema = {"allOf": []}
    gts = GraphTraversalState(dmge, "Patient", logger=Mock())
    gts._reverse_dependencies = reverse_dependencies
    gts._valid_values_map = valid_values_map
    gts.current_node.name = "CancerType"
    gts.current_node.display_name = "Cancer Type"
    _set_conditional_dependencies(
        json_schema=json_schema, graph_state=gts, use_property_display_names=False
    )
    assert json_schema == {"allOf": []}


@pytest.mark.parametrize(
    "reverse_dependencies, valid_values_map, expected_schema",
    [
        (
            {"CancerType": ["Cancer"]},
            {"Cancer": ["Diagnosis"]},
            JSONSchema(
                all_of=[
                    {
                        "if": {"properties": {"Diagnosis": {"enum": ["Cancer"]}}},
                        "then": {
                            "properties": {"CancerType": {"not": {"type": "null"}}},
                            "required": ["CancerType"],
                        },
                    }
                ]
            ),
        ),
        (
            {"CancerType": ["Cancer"]},
            {"Cancer": ["Diagnosis1", "Diagnosis2"]},
            JSONSchema(
                all_of=[
                    {
                        "if": {"properties": {"Diagnosis1": {"enum": ["Cancer"]}}},
                        "then": {
                            "properties": {"CancerType": {"not": {"type": "null"}}},
                            "required": ["CancerType"],
                        },
                    },
                    {
                        "if": {"properties": {"Diagnosis2": {"enum": ["Cancer"]}}},
                        "then": {
                            "properties": {"CancerType": {"not": {"type": "null"}}},
                            "required": ["CancerType"],
                        },
                    },
                ]
            ),
        ),
        (
            {"CancerType": ["Cancer1", "Cancer2"]},
            {"Cancer1": ["Diagnosis1"], "Cancer2": ["Diagnosis2"]},
            JSONSchema(
                all_of=[
                    {
                        "if": {"properties": {"Diagnosis1": {"enum": ["Cancer1"]}}},
                        "then": {
                            "properties": {"CancerType": {"not": {"type": "null"}}},
                            "required": ["CancerType"],
                        },
                    },
                    {
                        "if": {"properties": {"Diagnosis2": {"enum": ["Cancer2"]}}},
                        "then": {
                            "properties": {"CancerType": {"not": {"type": "null"}}},
                            "required": ["CancerType"],
                        },
                    },
                ]
            ),
        ),
    ],
    ids=["one rev dep, one enum", "two rev deps, one enum", "two rev deps, two enums"],
)
def test_set_conditional_dependencies(
    reverse_dependencies: dict[str, list[str]],
    valid_values_map: dict[str, list[str]],
    expected_schema: JSONSchema,
    dmge: DataModelGraphExplorer,
) -> None:
    """Tests for _set_conditional_dependencies"""
    json_schema = JSONSchema()
    gts = GraphTraversalState(dmge, "Patient", logger=Mock())
    gts._reverse_dependencies = reverse_dependencies
    gts._valid_values_map = valid_values_map
    gts.current_node.name = "CancerType"
    gts.current_node.display_name = "Cancer Type"
    _set_conditional_dependencies(
        json_schema=json_schema, graph_state=gts, use_property_display_names=False
    )
    assert json_schema == expected_schema


@pytest.mark.parametrize(
    "node_name, expected_schema",
    [
        # Array with an enum
        (
            "ListEnum",
            JSONSchema(
                properties={
                    "ListEnum": {
                        "description": "TBD",
                        "title": "List Enum",
                        "oneOf": [
                            {
                                "type": "array",
                                "title": "array",
                                "items": {"enum": ["ab", "cd", "ef", "gh"]},
                            },
                        ],
                    }
                },
                required=["ListEnum"],
            ),
        ),
        # Array with an enum, required list should be empty
        (
            "ListEnumNotRequired",
            JSONSchema(
                properties={
                    "ListEnumNotRequired": {
                        "description": "TBD",
                        "title": "List Enum Not Required",
                        "oneOf": [
                            {
                                "type": "array",
                                "title": "array",
                                "items": {"enum": ["ab", "cd", "ef", "gh"]},
                            },
                            {"type": "null", "title": "null"},
                        ],
                    }
                },
                required=[],
            ),
        ),
        # Enum, not array
        (
            "Enum",
            JSONSchema(
                properties={
                    "Enum": {
                        "description": "TBD",
                        "title": "Enum",
                        "oneOf": [{"enum": ["ab", "cd", "ef", "gh"], "title": "enum"}],
                    }
                },
                required=["Enum"],
            ),
        ),
        #  Array not enum
        (
            "List",
            JSONSchema(
                properties={
                    "List": {
                        "oneOf": [
                            {"type": "array", "title": "array"},
                        ],
                        "description": "TBD",
                        "title": "List",
                    }
                },
                required=["List"],
            ),
        ),
        # Not array or enum
        (
            "String",
            JSONSchema(
                properties={
                    "String": {
                        "description": "TBD",
                        "type": "string",
                        "title": "String",
                    }
                },
                required=["String"],
            ),
        ),
    ],
    ids=["Array, enum", "Array, enum, not required", "Enum", "Array", "String"],
)
def test_set_property(
    node_name: str,
    expected_schema: dict[str, Any],
    test_nodes: dict[str, TraversalNode],
) -> None:
    """Tests for set_property"""
    schema = JSONSchema()
    _set_property(schema, test_nodes[node_name], use_property_display_names=False)
    assert schema == expected_schema


@pytest.mark.parametrize(
    "node_name, expected_schema, valid_values, invalid_values",
    [
        (
            "ListEnum",
            {
                "oneOf": [
                    {
                        "type": "array",
                        "title": "array",
                        "items": {"enum": ["ab", "cd", "ef", "gh"]},
                    }
                ],
            },
            [[], ["ab"]],
            [[None], ["x"], None],
        ),
        # If is_required is False, "{'type': 'null'}" is added to the oneOf list
        (
            "ListEnumNotRequired",
            {
                "oneOf": [
                    {
                        "type": "array",
                        "title": "array",
                        "items": {"enum": ["ab", "cd", "ef", "gh"]},
                    },
                    {"type": "null", "title": "null"},
                ],
            },
            [[], ["ab"], None],
            [[None], ["x"]],
        ),
    ],
    ids=["Required", "Not required"],
)
def test_create_enum_array_property(
    node_name: str,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
    test_nodes: dict[str, TraversalNode],
) -> None:
    """Test for _create_enum_array_property"""
    schema = _create_enum_array_property(test_nodes[node_name])
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": {"name": schema}, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


@pytest.mark.parametrize(
    "node_name, expected_schema, valid_values, invalid_values",
    [
        (
            "List",
            {"oneOf": [{"type": "array", "title": "array"}]},
            [[], [None], ["x"]],
            ["x", None],
        ),
        # If is_required is False, "{'type': 'null'}" is added to the oneOf list
        (
            "ListNotRequired",
            {
                "oneOf": [
                    {"type": "array", "title": "array"},
                    {"type": "null", "title": "null"},
                ],
            },
            [None, [], [None], ["x"]],
            ["x"],
        ),
        # If item_type is given, it is set in the schema
        (
            "ListString",
            {
                "oneOf": [
                    {"type": "array", "title": "array", "items": {"type": "string"}}
                ],
            },
            [[], ["x"]],
            [None, [None], [1]],
        ),
        # If property_data has range_min or range_max, they are set in the schema
        (
            "ListInRange",
            {
                "oneOf": [
                    {
                        "type": "array",
                        "title": "array",
                        "items": {"type": "number", "minimum": 50.0, "maximum": 100.0},
                    }
                ],
            },
            [[], [50]],
            [None, [None], [2], ["x"]],
        ),
    ],
    ids=[
        "Required, no item type",
        "Not required, no item type",
        "Required, string item type",
        "Required, integer item type",
    ],
)
def test_create_array_property(
    node_name: str,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
    test_nodes_column_types: dict[str, TraversalNode],
) -> None:
    """Test for _create_array_property"""
    schema = _create_array_property(test_nodes_column_types[node_name])
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": {"name": schema}, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


@pytest.mark.parametrize(
    "node_name, expected_schema, valid_values, invalid_values",
    [
        # If is_required is True, no type is added
        (
            "Enum",
            {"oneOf": [{"enum": ["ab", "cd", "ef", "gh"], "title": "enum"}]},
            ["ab"],
            [1, "x", None],
        ),
        # If is_required is False, "null" is added as a type
        (
            "EnumNotRequired",
            {
                "oneOf": [
                    {"enum": ["ab", "cd", "ef", "gh"], "title": "enum"},
                    {"type": "null", "title": "null"},
                ],
            },
            ["ab", None],
            [1, "x"],
        ),
    ],
    ids=["Required", "Not required"],
)
def test_create_enum_property(
    node_name: str,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
    test_nodes: dict[str, TraversalNode],
) -> None:
    """Test for _create_enum_property"""
    schema = _create_enum_property(test_nodes[node_name])
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": {"name": schema}, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


@pytest.mark.parametrize(
    "node_name, expected_schema, valid_values, invalid_values",
    [
        ("NoRulesNotRequired", {}, [None, 1, ""], []),
        # If property_type is given, it is added to the schema
        (
            "String",
            {"type": "string"},
            [""],
            [1, None],
        ),
        # If property_type is given, and is_required is False,
        # type is set to given property_type and "null"
        (
            "StringNotRequired",
            {
                "oneOf": [
                    {"type": "string", "title": "string"},
                    {"type": "null", "title": "null"},
                ],
            },
            [None, "x"],
            [1],
        ),
        # If is_required is True '"not": {"type":"null"}' is added to schema if
        # property_type is not given
        (
            "NoRules",
            {"not": {"type": "null"}},
            ["x", 1],
            [None],
        ),
        (
            "InRange",
            {
                "type": "number",
                "minimum": 50,
                "maximum": 100,
            },
            [50, 75, 100],
            [None, 0, 49, 101],
        ),
    ],
    ids=[
        "Not required, no type",
        "Required, string type",
        "Not required, string type",
        "Required, no type",
        "Required, number type",
    ],
)
def test_create_simple_property(
    node_name: str,
    expected_schema: dict[str, Any],
    valid_values: list[Any],
    invalid_values: list[Any],
    test_nodes_column_types: dict[str, TraversalNode],
) -> None:
    """Test for _create_simple_property"""
    schema = _create_simple_property(test_nodes_column_types[node_name])
    assert schema == expected_schema
    full_schema = {"type": "object", "properties": {"name": schema}, "required": []}
    validator = Draft7Validator(full_schema)
    for value in valid_values:
        validator.validate({"name": value})
    for value in invalid_values:
        with pytest.raises(ValidationError):
            validator.validate({"name": value})


@pytest.mark.parametrize(
    "node_name, expected_schema",
    [
        ("NoRules", {}),
        ("InRange", {"minimum": 50, "maximum": 100}),
        ("Regex", {"pattern": "[a-f]"}),
    ],
    ids=[
        "NoRules",
        "InRange",
        "Regex",
    ],
)
def test_set_type_specific_keywords(
    node_name: str,
    expected_schema: dict[str, Any],
    test_nodes: dict[str, TraversalNode],
) -> None:
    """Test for _set_type_specific_keywords"""
    schema = {}
    _set_type_specific_keywords(schema, test_nodes[node_name])
    assert schema == expected_schema
