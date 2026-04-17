"""Unit tests for the synapseclient.models.mixins.manifest module."""

import datetime
import os
import tempfile

import pytest

from synapseclient.models.mixins.manifest import (
    DEFAULT_GENERATED_MANIFEST_KEYS,
    MANIFEST_FILENAME,
    _convert_manifest_data_items_to_string_list,
    _convert_manifest_data_row_to_dict,
    _extract_entity_metadata_for_file,
    _get_entity_provenance_dict_for_file,
    _manifest_filename,
    _parse_manifest_value,
    _validate_manifest_required_fields,
    _write_manifest_data,
)


class TestManifestConstants:
    """Tests for manifest constants."""

    def test_manifest_filename_constant(self):
        """Test the MANIFEST_FILENAME constant."""
        assert MANIFEST_FILENAME == "SYNAPSE_METADATA_MANIFEST.tsv"

    def test_default_manifest_keys(self):
        """Test the DEFAULT_GENERATED_MANIFEST_KEYS constant."""
        expected_keys = [
            "path",
            "parent",
            "name",
            "id",
            "synapseStore",
            "contentType",
            "used",
            "executed",
            "activityName",
            "activityDescription",
        ]
        assert DEFAULT_GENERATED_MANIFEST_KEYS == expected_keys


class TestManifestFilename:
    """Tests for _manifest_filename function."""

    def test_manifest_filename(self):
        """Test generating manifest filename."""
        # GIVEN a path
        path = "/path/to/directory"

        # WHEN we generate the manifest filename
        result = _manifest_filename(path)

        # THEN it should be the path joined with MANIFEST_FILENAME
        assert result == os.path.join(path, MANIFEST_FILENAME)


class TestConvertManifestDataItemsToStringList:
    """Tests for _convert_manifest_data_items_to_string_list function."""

    def test_single_string(self):
        """Test converting a single string."""
        # GIVEN a list with a single string
        items = ["hello"]

        # WHEN we convert to string
        result = _convert_manifest_data_items_to_string_list(items)

        # THEN it should return the string directly
        assert result == "hello"

    def test_multiple_strings(self):
        """Test converting multiple strings."""
        # GIVEN a list with multiple strings
        items = ["a", "b", "c"]

        # WHEN we convert to string
        result = _convert_manifest_data_items_to_string_list(items)

        # THEN it should return a bracketed list
        assert result == "[a,b,c]"

    def test_string_with_comma(self):
        """Test converting a string with comma."""
        # GIVEN a single item with comma (no quotes needed for single item)
        items = ["hello,world"]

        # WHEN we convert to string
        result = _convert_manifest_data_items_to_string_list(items)

        # THEN it should return the string directly
        assert result == "hello,world"

    def test_multiple_strings_with_comma(self):
        """Test converting multiple strings where one has a comma."""
        # GIVEN multiple strings where one contains commas
        items = ["string,with,commas", "string without commas"]

        # WHEN we convert to string
        result = _convert_manifest_data_items_to_string_list(items)

        # THEN the comma-containing string should be quoted
        assert result == '["string,with,commas",string without commas]'

    def test_datetime(self):
        """Test converting a datetime."""
        # GIVEN a datetime value
        dt = datetime.datetime(2020, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)

        # WHEN we convert to string
        result = _convert_manifest_data_items_to_string_list([dt])

        # THEN it should return ISO format
        assert result == "2020-01-01T00:00:00Z"

    def test_multiple_datetimes(self):
        """Test converting multiple datetimes."""
        # GIVEN multiple datetime values
        dt1 = datetime.datetime(2020, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)
        dt2 = datetime.datetime(2021, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)

        # WHEN we convert to string
        result = _convert_manifest_data_items_to_string_list([dt1, dt2])

        # THEN it should return a bracketed list of ISO dates
        assert result == "[2020-01-01T00:00:00Z,2021-01-01T00:00:00Z]"

    def test_boolean_true(self):
        """Test converting True."""
        # GIVEN a True value
        items = [True]

        # WHEN we convert to string
        result = _convert_manifest_data_items_to_string_list(items)

        # THEN it should return "True"
        assert result == "True"

    def test_boolean_false(self):
        """Test converting False."""
        # GIVEN a False value
        items = [False]

        # WHEN we convert to string
        result = _convert_manifest_data_items_to_string_list(items)

        # THEN it should return "False"
        assert result == "False"

    def test_integer(self):
        """Test converting an integer."""
        # GIVEN an integer value
        items = [1]

        # WHEN we convert to string
        result = _convert_manifest_data_items_to_string_list(items)

        # THEN it should return the string representation
        assert result == "1"

    def test_float(self):
        """Test converting a float."""
        # GIVEN a float value
        items = [1.5]

        # WHEN we convert to string
        result = _convert_manifest_data_items_to_string_list(items)

        # THEN it should return the string representation
        assert result == "1.5"

    def test_empty_list(self):
        """Test converting an empty list."""
        # GIVEN an empty list
        items = []

        # WHEN we convert to string
        result = _convert_manifest_data_items_to_string_list(items)

        # THEN it should return an empty string
        assert result == ""


class TestConvertManifestDataRowToDict:
    """Tests for _convert_manifest_data_row_to_dict function."""

    def test_simple_row(self):
        """Test converting a simple row."""
        # GIVEN a row with simple values
        row = {"path": "/path/to/file", "name": "file.txt"}
        keys = ["path", "name"]

        # WHEN we convert it
        result = _convert_manifest_data_row_to_dict(row, keys)

        # THEN it should return the same values
        assert result == {"path": "/path/to/file", "name": "file.txt"}

    def test_row_with_list(self):
        """Test converting a row with a list value."""
        # GIVEN a row with a list value
        row = {"annotations": ["a", "b", "c"]}
        keys = ["annotations"]

        # WHEN we convert it
        result = _convert_manifest_data_row_to_dict(row, keys)

        # THEN the list should be converted to a string
        assert result == {"annotations": "[a,b,c]"}

    def test_missing_key(self):
        """Test converting a row with a missing key."""
        # GIVEN a row missing a key
        row = {"path": "/path/to/file"}
        keys = ["path", "name"]

        # WHEN we convert it
        result = _convert_manifest_data_row_to_dict(row, keys)

        # THEN the missing key should be empty string
        assert result == {"path": "/path/to/file", "name": ""}


class TestParseManifestValue:
    """Tests for _parse_manifest_value function."""

    def test_simple_string(self):
        """Test parsing a simple string."""
        assert _parse_manifest_value("hello") == "hello"

    def test_list_syntax(self):
        """Test parsing list syntax."""
        assert _parse_manifest_value("[a,b,c]") == ["a", "b", "c"]

    def test_list_with_quoted_string(self):
        """Test parsing list with quoted string containing comma."""
        result = _parse_manifest_value('["hello,world",other]')
        assert result == ["hello,world", "other"]

    def test_boolean_true(self):
        """Test parsing 'true' string."""
        assert _parse_manifest_value("true") is True
        assert _parse_manifest_value("True") is True
        assert _parse_manifest_value("TRUE") is True

    def test_boolean_false(self):
        """Test parsing 'false' string."""
        assert _parse_manifest_value("false") is False
        assert _parse_manifest_value("False") is False
        assert _parse_manifest_value("FALSE") is False

    def test_integer(self):
        """Test parsing an integer string."""
        assert _parse_manifest_value("123") == 123

    def test_float(self):
        """Test parsing a float string."""
        assert _parse_manifest_value("1.5") == 1.5

    def test_non_numeric_string(self):
        """Test that non-numeric strings stay as strings."""
        assert _parse_manifest_value("hello123") == "hello123"


class TestWriteManifestData:
    """Tests for _write_manifest_data function."""

    def test_write_simple_manifest(self):
        """Test writing a simple manifest file."""
        # GIVEN simple data
        keys = ["path", "name", "id"]
        data = [
            {"path": "/path/to/file1.txt", "name": "file1.txt", "id": "syn123"},
            {"path": "/path/to/file2.txt", "name": "file2.txt", "id": "syn456"},
        ]

        # WHEN we write it to a temp file
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".tsv") as f:
            filename = f.name

        try:
            _write_manifest_data(filename, keys, data)

            # THEN the file should contain the expected content
            with open(filename, "r") as f:
                content = f.read()

            lines = content.strip().split("\n")
            assert len(lines) == 3  # header + 2 data rows
            assert lines[0] == "path\tname\tid"
            assert lines[1] == "/path/to/file1.txt\tfile1.txt\tsyn123"
            assert lines[2] == "/path/to/file2.txt\tfile2.txt\tsyn456"
        finally:
            os.unlink(filename)


class TestValidateManifestRequiredFields:
    """Tests for _validate_manifest_required_fields function."""

    def test_valid_manifest(self):
        """Test validating a valid manifest file."""
        # GIVEN a valid manifest file
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".tsv") as f:
            f.write("path\tparent\n")
            f.write(f"{f.name}\tsyn123\n")
            filename = f.name

        try:
            # Create the file referenced in path column
            with open(filename, "a") as f:
                pass  # File already exists

            # WHEN we validate it
            is_valid, errors = _validate_manifest_required_fields(filename)

            # THEN it should be valid
            assert is_valid is True
            assert errors == []
        finally:
            os.unlink(filename)

    def test_missing_file(self):
        """Test validating a non-existent manifest file."""
        # WHEN we validate a non-existent file
        is_valid, errors = _validate_manifest_required_fields("/nonexistent/file.tsv")

        # THEN it should be invalid
        assert is_valid is False
        assert len(errors) == 1
        assert "not found" in errors[0]

    def test_missing_required_field(self):
        """Test validating a manifest missing a required field."""
        # GIVEN a manifest missing the 'parent' field
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".tsv") as f:
            f.write("path\tname\n")
            f.write("/path/to/file.txt\tfile.txt\n")
            filename = f.name

        try:
            # WHEN we validate it
            is_valid, errors = _validate_manifest_required_fields(filename)

            # THEN it should be invalid
            assert is_valid is False
            assert any("parent" in e for e in errors)
        finally:
            os.unlink(filename)

    def test_empty_path(self):
        """Test validating a manifest with empty path."""
        # GIVEN a manifest with empty path
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".tsv") as f:
            f.write("path\tparent\n")
            f.write("\tsyn123\n")
            filename = f.name

        try:
            # WHEN we validate it
            is_valid, errors = _validate_manifest_required_fields(filename)

            # THEN it should be invalid
            assert is_valid is False
            assert any("'path' is empty" in e for e in errors)
        finally:
            os.unlink(filename)

    def test_invalid_parent_id(self):
        """Test validating a manifest with invalid parent ID."""
        # GIVEN a manifest with invalid parent ID
        with tempfile.NamedTemporaryFile(mode="w", delete=False, suffix=".tsv") as f:
            f.write("path\tparent\n")
            f.write(f"{f.name}\tinvalid_parent\n")
            filename = f.name

        try:
            # WHEN we validate it
            is_valid, errors = _validate_manifest_required_fields(filename)

            # THEN it should be invalid
            assert is_valid is False
            assert any("not a valid Synapse ID" in e for e in errors)
        finally:
            os.unlink(filename)


class TestExtractEntityMetadataForFile:
    """Tests for _extract_entity_metadata_for_file function."""

    def test_extract_basic_metadata(self):
        """Test extracting basic file metadata."""

        # GIVEN a mock File object
        class MockFile:
            def __init__(self):
                self.parent_id = "syn123"
                self.path = "/path/to/file.txt"
                self.name = "file.txt"
                self.id = "syn456"
                self.synapse_store = True
                self.content_type = "text/plain"
                self.annotations = None
                self.activity = None

        file = MockFile()

        # WHEN we extract metadata
        keys, data = _extract_entity_metadata_for_file([file])

        # THEN we should get the expected data
        assert "path" in keys
        assert "parent" in keys
        assert "name" in keys
        assert "id" in keys
        assert len(data) == 1
        assert data[0]["path"] == "/path/to/file.txt"
        assert data[0]["parent"] == "syn123"
        assert data[0]["name"] == "file.txt"
        assert data[0]["id"] == "syn456"

    def test_extract_with_annotations(self):
        """Test extracting metadata with annotations."""

        # GIVEN a mock File object with annotations
        class MockFile:
            def __init__(self):
                self.parent_id = "syn123"
                self.path = "/path/to/file.txt"
                self.name = "file.txt"
                self.id = "syn456"
                self.synapse_store = True
                self.content_type = "text/plain"
                self.annotations = {"study": ["Study1"], "dataType": ["RNA-seq"]}
                self.activity = None

        file = MockFile()

        # WHEN we extract metadata
        keys, data = _extract_entity_metadata_for_file([file])

        # THEN annotation keys should be included
        assert "study" in keys
        assert "dataType" in keys
        assert data[0]["study"] == ["Study1"]
        assert data[0]["dataType"] == ["RNA-seq"]


class TestGetEntityProvenanceDictForFile:
    """Tests for _get_entity_provenance_dict_for_file function."""

    def test_no_activity(self):
        """Test extracting provenance when there is no activity."""

        # GIVEN a mock File object with no activity
        class MockFile:
            def __init__(self):
                self.activity = None

        file = MockFile()

        # WHEN we extract provenance
        result = _get_entity_provenance_dict_for_file(file)

        # THEN we should get an empty dict
        assert result == {}

    def test_with_activity(self):
        """Test extracting provenance when there is an activity."""

        # GIVEN mock objects
        class MockUsedEntity:
            def format_for_manifest(self):
                return "syn789"

        class MockActivity:
            def __init__(self):
                self.name = "Analysis"
                self.description = "Processing data"
                self.used = [MockUsedEntity()]
                self.executed = []

        class MockFile:
            def __init__(self):
                self.activity = MockActivity()

        file = MockFile()

        # WHEN we extract provenance
        result = _get_entity_provenance_dict_for_file(file)

        # THEN we should get the expected dict
        assert result["activityName"] == "Analysis"
        assert result["activityDescription"] == "Processing data"
        assert result["used"] == "syn789"
        assert result["executed"] == ""
