import csv
import datetime
import os
import tempfile

from synapseclient.models import Activity, FailureStrategy, File, Folder
from synapseclient.models.activity import UsedEntity, UsedURL
from synapseclient.models.services.manifest import (
    MANIFEST_CSV_FILENAME,
    _extract_entity_metadata_for_manifest_csv,
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

    def test_generate_manifest_csv_skips_when_no_files(self) -> None:
        # GIVEN an empty file list
        with tempfile.TemporaryDirectory() as tmpdir:
            # WHEN generate_manifest_csv is called with no files
            generate_manifest_csv(all_files=[], path=tmpdir)

            # THEN no manifest.csv is created
            assert not os.path.exists(os.path.join(tmpdir, "manifest.csv"))

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
