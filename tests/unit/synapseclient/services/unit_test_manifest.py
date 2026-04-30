import csv
import datetime
import os
import tempfile

import pytest

from synapseclient.models import Activity, File
from synapseclient.models.activity import UsedEntity, UsedURL
from synapseclient.models.services.manifest import (
    MANIFEST_CSV_FILENAME,
    _convert_manifest_data_items_to_string_list,
    _convert_manifest_data_row_to_dict,
    _extract_entity_metadata_for_manifest_csv,
    _get_entity_provenance_dict_for_manifest,
    _manifest_csv_filename,
    _write_manifest_data_csv,
    generate_manifest_csv,
)


class TestManifestCsvFilename:
    """Tests for the _manifest_csv_filename helper."""

    def test_plain_directory(self) -> None:
        # GIVEN a plain absolute path
        # WHEN _manifest_csv_filename is called
        result = _manifest_csv_filename("/tmp/mydir")

        # THEN it joins the path with the manifest filename
        assert result == os.path.join("/tmp/mydir", MANIFEST_CSV_FILENAME)

    def test_tilde_is_expanded(self) -> None:
        # GIVEN a path starting with ~
        # WHEN _manifest_csv_filename is called
        result = _manifest_csv_filename("~/mydir")

        # THEN ~ is expanded to the user's home directory
        assert result == os.path.join(
            os.path.expanduser("~/mydir"), MANIFEST_CSV_FILENAME
        )
        assert "~" not in result

    def test_filename_is_manifest_csv(self) -> None:
        # GIVEN any directory path
        # WHEN _manifest_csv_filename is called
        result = _manifest_csv_filename("/some/path")

        # THEN the basename of the result is MANIFEST_CSV_FILENAME
        assert os.path.basename(result) == MANIFEST_CSV_FILENAME


class TestGenerateManifestCsv:
    """Tests for the generate_manifest_csv and related helper functions."""

    def _make_file(
        self,
        syn_id: str = "syn123",
        name: str = "file.txt",
        path: str = "/data/file.txt",
        parent_id: str = "syn456",
        content_type: str = "text/plain",
        synapse_store: bool = True,
        annotations: dict = None,
        activity: Activity = None,
    ) -> File:
        f = File(
            id=syn_id,
            name=name,
            path=path,
            parent_id=parent_id,
            content_type=content_type,
            synapse_store=synapse_store,
        )
        if annotations:
            f.annotations = annotations
        if activity:
            f.activity = activity
        return f

    def test_extract_entity_metadata_includes_annotations_and_activity(self) -> None:
        # GIVEN a File entity with provenance
        activity = Activity(
            name="My Pipeline",
            description="Run analysis",
            used=[UsedEntity(target_id="syn111", target_version_number=1)],
            executed=[UsedURL(url="https://github.com/example/pipeline")],
        )
        f = self._make_file(
            activity=activity, annotations={"tissue": ["brain"], "count": [42]}
        )

        # WHEN metadata is extracted
        keys, data = _extract_entity_metadata_for_manifest_csv([f])

        # THEN provenance keys are present in the column list
        assert {
            "used",
            "executed",
            "activityName",
            "activityDescription",
            "tissue",
            "count",
        }.issubset(keys)

        assert data[0]["parentId"] == "syn456"
        assert data[0]["ID"] == "syn123"
        assert data[0]["path"] == "/data/file.txt"
        assert data[0]["name"] == "file.txt"
        assert data[0]["activityName"] == "My Pipeline"
        assert data[0]["activityDescription"] == "Run analysis"
        assert data[0]["used"] == "syn111.1"
        assert data[0]["executed"] == "https://github.com/example/pipeline"
        assert data[0]["tissue"] == "brain"
        assert data[0]["count"] == "42"

    def test_generate_manifest_csv_data_items_are_converted_to_strings(self) -> None:
        # GIVEN a File with a name containing a comma and mixed-type annotations
        f = self._make_file(
            name="a, b, c",
            path="/data/file.txt",
            annotations={
                "single_str": "hello",
                "multi_str": ["a", "b", "c"],
                "str_with_comma": ["hello,world", "plain text"],
                "booleans": [True, False],
                "integers": [1],
                "floats": [1.0],
                "single_dt": [
                    datetime.datetime(2020, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc)
                ],
                "multi_dt": [
                    datetime.datetime(
                        2020, 1, 1, 0, 0, 0, tzinfo=datetime.timezone.utc
                    ),
                    datetime.datetime(
                        2021, 6, 15, 12, 30, 0, tzinfo=datetime.timezone.utc
                    ),
                ],
            },
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            # WHEN generate_manifest_csv is called
            generate_manifest_csv(all_files=[f], path=tmpdir)
            manifest_path = os.path.join(tmpdir, "manifest.csv")
            content = open(manifest_path, encoding="utf8").read()
            with open(manifest_path, newline="", encoding="utf8") as fp:
                row = next(csv.DictReader(fp))

        assert '"a, b, c"' in content
        assert row["single_str"] == "hello"
        assert row["multi_str"] == "[a,b,c]"
        assert row["str_with_comma"] == '["hello,world",plain text]'
        assert row["booleans"] == "[True,False]"
        assert row["integers"] == "1"
        assert row["floats"] == "1.0"
        assert row["single_dt"] == "2020-01-01T00:00:00Z"
        assert row["multi_dt"] == "[2020-01-01T00:00:00Z,2021-06-15T12:30:00Z]"

    def test_generate_manifest_csv_with_only_header_row(self) -> None:
        # GIVEN an empty file list
        with tempfile.TemporaryDirectory() as tmpdir:
            # WHEN generate_manifest_csv is called with no files
            generate_manifest_csv(all_files=[], path=tmpdir)

            # THEN the manifest.csv file is created with only the header row and no data rows
            manifest_path = os.path.join(tmpdir, "manifest.csv")
            with open(manifest_path, newline="", encoding="utf8") as fp:
                reader = csv.DictReader(fp)
                rows = list(reader)
                assert reader.fieldnames == [
                    "path",
                    "parentId",
                    "name",
                    "ID",
                    "synapseStore",
                    "contentType",
                    "used",
                    "executed",
                    "activityName",
                    "activityDescription",
                ]
            assert rows == []

    def test_generate_manifest_csv_with_path_None_raises_ValueError(self) -> None:
        # GIVEN an empty file list
        with tempfile.TemporaryDirectory() as tmpdir:
            # WHEN generate_manifest_csv is called with path=None
            with pytest.raises(
                ValueError,
                match="The path argument is required to generate a manifest.csv file.",
            ):
                generate_manifest_csv(all_files=[], path=None)

    def test_generate_manifest_csv_quotes_values_with_commas(self) -> None:
        # GIVEN a File whose name contains a comma
        f = self._make_file(name="file, extra.txt", path="/tmp/file, extra.txt")

        with tempfile.TemporaryDirectory() as tmpdir:
            generate_manifest_csv(all_files=[f], path=tmpdir)
            manifest_path = os.path.join(tmpdir, "manifest.csv")
            content = open(manifest_path, encoding="utf8").read()
        # THEN the comma-containing value is quoted in the CSV
        assert '"file, extra.txt"' in content


class TestWriteManifestDataCsv:
    """Tests for the _write_manifest_data_csv helper."""

    def test_writes_header_and_rows(self) -> None:
        # GIVEN keys and one row of data
        keys = ["path", "parentId", "name"]
        data = [{"path": "/data/f.txt", "parentId": "syn1", "name": "f.txt"}]

        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, "manifest.csv")
            # WHEN _write_manifest_data_csv is called
            _write_manifest_data_csv(filename, keys, data)

            with open(filename, newline="", encoding="utf8") as fp:
                rows = list(csv.DictReader(fp))

        # THEN header and row values are written correctly
        assert len(rows) == 1
        assert rows[0]["path"] == "/data/f.txt"
        assert rows[0]["parentId"] == "syn1"
        assert rows[0]["name"] == "f.txt"

    def test_missing_keys_use_empty_string(self) -> None:
        # GIVEN a row missing the "name" key
        keys = ["path", "parentId", "name"]
        data = [{"path": "/data/f.txt", "parentId": "syn1"}]

        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, "manifest.csv")
            _write_manifest_data_csv(filename, keys, data)

            with open(filename, newline="", encoding="utf8") as fp:
                rows = list(csv.DictReader(fp))

        # THEN the missing field is written as an empty string
        assert rows[0]["name"] == ""

    def test_extra_keys_in_row_are_ignored(self) -> None:
        # GIVEN a row with a key not in the fieldnames list
        keys = ["path", "name"]
        data = [{"path": "/data/f.txt", "name": "f.txt", "extra": "ignored"}]

        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, "manifest.csv")
            # WHEN _write_manifest_data_csv is called
            # THEN no exception is raised and only declared keys appear
            _write_manifest_data_csv(filename, keys, data)

            with open(filename, newline="", encoding="utf8") as fp:
                reader = csv.DictReader(fp)
                rows = list(reader)
                assert "extra" not in reader.fieldnames

        assert rows[0]["path"] == "/data/f.txt"

    def test_values_with_commas_are_quoted(self) -> None:
        # GIVEN a value that contains a comma
        keys = ["name", "parentId"]
        data = [{"name": "file, with comma.txt", "parentId": "syn1"}]

        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, "manifest.csv")
            _write_manifest_data_csv(filename, keys, data)
            content = open(filename, encoding="utf8").read()

            with open(filename, newline="", encoding="utf8") as fp:
                rows = list(csv.DictReader(fp))

        # THEN the comma-containing value is quoted in the raw CSV
        assert '"file, with comma.txt"' in content
        # AND reads back correctly
        assert rows[0]["name"] == "file, with comma.txt"

    def test_empty_data_writes_header_only(self) -> None:
        # GIVEN no data rows
        keys = ["path", "parentId", "name"]

        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, "manifest.csv")
            _write_manifest_data_csv(filename, keys, [])

            with open(filename, newline="", encoding="utf8") as fp:
                reader = csv.DictReader(fp)
                rows = list(reader)
                header = reader.fieldnames

        # THEN the file exists with only the header
        assert rows == []
        assert header == keys

    def test_unicode_values_are_written_correctly(self) -> None:
        # GIVEN a value with non-ASCII characters
        keys = ["name", "parentId"]
        data = [{"name": "données_été.txt", "parentId": "syn1"}]

        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, "manifest.csv")
            _write_manifest_data_csv(filename, keys, data)

            with open(filename, newline="", encoding="utf8") as fp:
                rows = list(csv.DictReader(fp))

        # THEN unicode characters round-trip correctly
        assert rows[0]["name"] == "données_été.txt"

    def test_multiple_rows_written_in_order(self) -> None:
        # GIVEN multiple rows
        keys = ["name", "parentId"]
        data = [
            {"name": "a.txt", "parentId": "syn1"},
            {"name": "b.txt", "parentId": "syn2"},
            {"name": "c.txt", "parentId": "syn3"},
        ]

        with tempfile.TemporaryDirectory() as tmpdir:
            filename = os.path.join(tmpdir, "manifest.csv")
            _write_manifest_data_csv(filename, keys, data)

            with open(filename, newline="", encoding="utf8") as fp:
                rows = list(csv.DictReader(fp))

        # THEN all rows are present and in order
        assert len(rows) == 3
        assert [r["name"] for r in rows] == ["a.txt", "b.txt", "c.txt"]


class TestGetEntityProvenanceDictForManifest:
    """Tests for _get_entity_provenance_dict_for_manifest."""

    def _make_file_with_activity(self, activity: Activity = None) -> File:
        f = File(id="syn1", name="f.txt", path="/f.txt", parent_id="syn2")
        if activity:
            f.activity = activity
        return f

    def test_returns_empty_dict_when_no_activity(self) -> None:
        f = self._make_file_with_activity()
        result = _get_entity_provenance_dict_for_manifest(f)
        assert result == {}

    def test_returns_all_provenance_keys_with_activity(self) -> None:
        activity = Activity(
            name="Pipeline",
            description="Runs analysis",
            used=[UsedEntity(target_id="syn10", target_version_number=2)],
            executed=[UsedURL(url="https://github.com/example/run")],
        )
        f = self._make_file_with_activity(activity)

        result = _get_entity_provenance_dict_for_manifest(f)
        assert result["used"] == "syn10.2"
        assert result["executed"] == "https://github.com/example/run"
        assert result["activityName"] == "Pipeline"
        assert result["activityDescription"] == "Runs analysis"

    def test_activity_name_and_description_default_to_empty_string(self) -> None:
        activity = Activity(name=None, description=None)
        f = self._make_file_with_activity(activity)

        result = _get_entity_provenance_dict_for_manifest(f)
        assert result["activityName"] == ""
        assert result["activityDescription"] == ""

    def test_empty_used_and_executed_lists(self) -> None:
        activity = Activity(name="minimal", used=[], executed=[])
        f = self._make_file_with_activity(activity)

        result = _get_entity_provenance_dict_for_manifest(f)

        assert result["activityName"] == "minimal"
        assert result["used"] == ""
        assert result["executed"] == ""

    def test_multiple_used_and_executed_are_semicolon_joined(self) -> None:
        # GIVEN an activity with multiple used and executed entries
        activity = Activity(
            name="multi",
            used=[
                UsedEntity(target_id="syn1", target_version_number=1),
                UsedEntity(target_id="syn2", target_version_number=3),
            ],
            executed=[
                UsedURL(url="https://github.com/a"),
                UsedURL(url="https://github.com/b"),
            ],
        )
        f = self._make_file_with_activity(activity)

        result = _get_entity_provenance_dict_for_manifest(f)

        assert result["activityName"] == "multi"
        assert result["used"] == "syn1.1;syn2.3"
        assert result["executed"] == "https://github.com/a;https://github.com/b"


_UTC = datetime.timezone.utc


class TestConvertManifestDataItemsToStringList:
    """Tests for _convert_manifest_data_items_to_string_list."""

    @pytest.mark.parametrize(
        "items,expected",
        [
            ([], ""),
            (["hello"], "hello"),
            # single item with comma is NOT quoted — quoting only applies in multi-item lists
            (["hello,world"], "hello,world"),
            (["a", "b", "c"], "[a,b,c]"),
            (["hello,world", "plain"], '["hello,world",plain]'),
            ([True], "True"),
            ([True, False], "[True,False]"),
            ([42], "42"),
            ([1, 2, 3], "[1,2,3]"),
            ([1.5], "1.5"),
            (
                [datetime.datetime(2020, 1, 1, tzinfo=_UTC)],
                "2020-01-01T00:00:00Z",
            ),
            (
                [
                    datetime.datetime(2020, 1, 1, tzinfo=_UTC),
                    datetime.datetime(2021, 6, 15, 12, 30, tzinfo=_UTC),
                ],
                "[2020-01-01T00:00:00Z,2021-06-15T12:30:00Z]",
            ),
        ],
    )
    def test_converts_items(self, items: list, expected: str) -> None:
        assert _convert_manifest_data_items_to_string_list(items) == expected


class TestConvertManifestDataRowToDict:
    """Tests for _convert_manifest_data_row_to_dict."""

    def test_all_keys_present_passes_through(self) -> None:
        row = {"path": "/f.txt", "parentId": "syn1", "name": "f.txt"}
        keys = ["path", "parentId", "name"]

        result = _convert_manifest_data_row_to_dict(row, keys)

        assert result == {"path": "/f.txt", "parentId": "syn1", "name": "f.txt"}

    def test_missing_key_defaults_to_empty_string(self) -> None:
        row = {"path": "/f.txt", "parentId": "syn1"}
        keys = ["path", "parentId", "name"]

        result = _convert_manifest_data_row_to_dict(row, keys)

        assert result["name"] == ""

    def test_list_value_converted_to_string(self) -> None:
        row = {"tags": ["a", "b", "c"]}
        keys = ["tags"]

        result = _convert_manifest_data_row_to_dict(row, keys)

        assert result["tags"] == "[a,b,c]"

    def test_extra_keys_in_row_are_not_included_in_output(self) -> None:
        row = {"path": "/f.txt", "extra": "ignored"}
        keys = ["path"]

        result = _convert_manifest_data_row_to_dict(row, keys)

        assert "extra" not in result
        assert result == {"path": "/f.txt"}
