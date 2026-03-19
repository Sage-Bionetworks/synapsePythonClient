"""Unit tests for file_based_metadata_task.py"""
from typing import Any

import pytest

from synapseclient.extensions.curator.file_based_metadata_task import (
    MAX_LIST_LENGTH,
    MAX_LIST_STRING_ITEM_SIZE,
    _create_columns_from_json_schema,
    _create_synapse_column_from_js_property,
    _get_column_type_from_js_property,
)
from synapseclient.models import Column, ColumnType


def test_create_columns_from_json_schema():
    schema = {
        "properties": {
            "string_col": {"type": "string"},
            "int_col": {"type": "integer"},
            "bool_col": {"type": "boolean"},
        }
    }
    expected = [
        Column(name="string_col", column_type=ColumnType.MEDIUMTEXT),
        Column(name="int_col", column_type=ColumnType.INTEGER),
        Column(name="bool_col", column_type=ColumnType.BOOLEAN),
    ]
    assert _create_columns_from_json_schema(schema) == expected


@pytest.mark.parametrize(
    "schema",
    [{}, {"properties": []}],
    ids=["empty schema", "properties is not a d ict"],
)
def test_create_columns_from_json_schema_exceptions(schema: dict[str, Any]):
    with pytest.raises(ValueError):
        _create_columns_from_json_schema(schema)


@pytest.mark.parametrize(
    "prop, name, expected_type, expected_max_size, expected_max_list_length",
    [
        (
            {"type": "array", "items": {"type": "string"}},
            "string_list_col",
            ColumnType.STRING_LIST,
            MAX_LIST_STRING_ITEM_SIZE,
            MAX_LIST_LENGTH,
        ),
        (
            {"type": "array", "items": {"type": "integer"}},
            "int_list_col",
            ColumnType.INTEGER_LIST,
            None,
            MAX_LIST_LENGTH,
        ),
        (
            {"type": "array", "items": {"type": "boolean"}},
            "bool_list_col",
            ColumnType.BOOLEAN_LIST,
            None,
            MAX_LIST_LENGTH,
        ),
        (
            {"type": "string"},
            "string_col",
            ColumnType.MEDIUMTEXT,
            None,
            None,
        ),
    ],
    ids=["string_list", "integer_list", "boolean_list", "string"],
)
def test_create_synapse_column_from_js_property(
    prop, name, expected_type, expected_max_size, expected_max_list_length
):
    result = _create_synapse_column_from_js_property(prop, name)
    assert isinstance(result, Column)
    assert result.name == name
    assert result.column_type == expected_type
    assert result.maximum_size == expected_max_size
    assert result.maximum_list_length == expected_max_list_length


@pytest.mark.parametrize(
    "prop, expected",
    [
        ({"enum": ["a", "b", "c"]}, ColumnType.MEDIUMTEXT),
        ({"type": "string"}, ColumnType.MEDIUMTEXT),
        ({"type": "integer"}, ColumnType.INTEGER),
        ({"type": "number"}, ColumnType.DOUBLE),
        ({"type": "boolean"}, ColumnType.BOOLEAN),
        ({"type": ["integer", "null"]}, ColumnType.INTEGER),
        ({"type": ["integer", "string"]}, ColumnType.MEDIUMTEXT),
        ({"type": "array", "items": {"type": "integer"}}, ColumnType.INTEGER_LIST),
        ({"oneOf": [{"type": "integer"}, {"type": "null"}]}, ColumnType.INTEGER),
        ({"type": "unknown"}, ColumnType.MEDIUMTEXT),
        ({}, ColumnType.MEDIUMTEXT),
    ],
    ids=[
        "enum_property",
        "type_string",
        "type_integer",
        "type_number",
        "type_boolean",
        "type_list_nullable",
        "type_list_multiple_types",
        "type_array",
        "one_of_list",
        "unknown_type",
        "empty_property",
    ],
)
def test_get_column_type_from_js_property(prop, expected):
    assert _get_column_type_from_js_property(prop) == expected
