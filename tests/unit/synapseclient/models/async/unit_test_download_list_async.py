"""Unit tests for DownloadList helper methods."""

import csv
from pathlib import Path

import pytest

from synapseclient.core.exceptions import SynapseError
from synapseclient.models.download_list import DownloadList


class TestReadManifestRows:
    """Tests for DownloadList._read_manifest_rows."""

    def _write_csv(self, tmp_path: Path, header: list[str], rows: list[dict]) -> str:
        path = str(tmp_path / "manifest.csv")
        with open(path, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=header)
            writer.writeheader()
            writer.writerows(rows)
        return path

    @pytest.mark.parametrize(
        "csv_content, expected_columns, expected_row_count, row_checks",
        [
            pytest.param(
                "ID,versionNumber,name\nsyn111,1,file_a.txt\nsyn222,3,file_b.txt\n",
                ["ID", "versionNumber", "name"],
                2,
                {0: {"ID": "syn111", "versionNumber": "1"}, 1: {"ID": "syn222"}},
                id="standard_manifest",
            ),
            pytest.param(
                "ID,versionNumber\n",
                ["ID", "versionNumber"],
                0,
                {},
                id="headers_only_no_rows",
            ),
            pytest.param(
                "",
                None,
                0,
                {},
                id="empty_file",
            ),
            pytest.param(
                "ID\nsyn999\n",
                ["ID"],
                1,
                {0: {"ID": "syn999"}},
                id="single_column",
            ),
            pytest.param(
                "ID,versionNumber\nsyn123,\n",
                ["ID", "versionNumber"],
                1,
                {0: {"ID": "syn123", "versionNumber": ""}},
                id="empty_string_values_preserved",
            ),
            pytest.param(
                'ID,name\nsyn123,"file, with comma.txt"\n',
                ["ID", "name"],
                1,
                {0: {"name": "file, with comma.txt"}},
                id="quoted_field_with_comma",
            ),
        ],
    )
    def test_read_manifest_rows(
        self,
        tmp_path: Path,
        csv_content: str,
        expected_columns: list[str],
        expected_row_count: int,
        row_checks: dict[int, dict[str, str]],
    ) -> None:
        """_read_manifest_rows returns correct columns and rows for various CSV shapes."""
        # GIVEN a CSV file with the specified content
        path = str(tmp_path / "manifest.csv")
        with open(path, "w", newline="") as f:
            f.write(csv_content)

        # WHEN I read the manifest
        columns, rows = DownloadList._read_manifest_rows(path)

        # THEN columns and row count match expectations
        assert columns == expected_columns
        assert len(rows) == expected_row_count

        # AND specific cell values match
        for row_idx, expected_values in row_checks.items():
            for key, value in expected_values.items():
                assert rows[row_idx][key] == value

    def test_many_rows(self, tmp_path: Path) -> None:
        """Reading a manifest with many rows returns all of them."""
        # GIVEN a CSV with 500 rows
        header = ["ID", "versionNumber"]
        data = [{"ID": f"syn{i}", "versionNumber": str(i)} for i in range(500)]
        path = self._write_csv(tmp_path, header, data)

        # WHEN I read the manifest
        columns, rows = DownloadList._read_manifest_rows(path)

        # THEN all 500 rows are returned
        assert columns == header
        assert len(rows) == 500
        assert rows[0]["ID"] == "syn0"
        assert rows[499]["ID"] == "syn499"


class TestValidateAndExtendColumns:
    """Tests for DownloadList._validate_and_extend_columns."""

    @pytest.mark.parametrize(
        "columns, expected",
        [
            pytest.param(
                ["ID", "versionNumber"],
                ["ID", "versionNumber", "path", "error"],
                id="standard_columns",
            ),
            pytest.param(
                ["ID"],
                ["ID", "path", "error"],
                id="single_column",
            ),
            pytest.param(
                ["ID", "versionNumber", "name", "createdBy"],
                ["ID", "versionNumber", "name", "createdBy", "path", "error"],
                id="many_columns",
            ),
        ],
    )
    def test_appends_path_and_error(
        self, columns: list[str], expected: list[str]
    ) -> None:
        """Valid columns are returned with path and error appended."""
        assert DownloadList._validate_and_extend_columns(columns) == expected

    def test_none_columns_raises(self) -> None:
        """None columns (empty manifest) raises SynapseError."""
        with pytest.raises(SynapseError, match="no headers"):
            DownloadList._validate_and_extend_columns(None)

    @pytest.mark.parametrize(
        "columns",
        [
            pytest.param(["ID", "path"], id="contains_path"),
            pytest.param(["ID", "error"], id="contains_error"),
            pytest.param(["path", "error"], id="contains_both"),
        ],
    )
    def test_reserved_column_names_raise(self, columns: list[str]) -> None:
        """Columns containing reserved names 'path' or 'error' raise SynapseError."""
        with pytest.raises(SynapseError, match="reserved column names"):
            DownloadList._validate_and_extend_columns(columns)
