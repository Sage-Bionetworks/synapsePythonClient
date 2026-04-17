"""Unit tests for synapseclient.models.services.migration and migration_types (sync and async)."""
import asyncio
import csv
import json
import os
import sqlite3
import tempfile
from dataclasses import fields
from typing import Any, Dict
from unittest import mock
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import SynapseError
from synapseclient.models.services.migration import (
    BATCH_SIZE,
    DEFAULT_PART_SIZE,
    _check_file_handle_exists,
    _check_indexed,
    _confirm_migration,
    _create_new_file_version_async,
    _ensure_schema,
    _escape_column_name,
    _execute_migration_async,
    _get_default_db_path,
    _get_file_migration_status,
    _get_part_size,
    _get_table_file_handle_rows_async,
    _get_version_numbers_async,
    _index_container_async,
    _index_entity_async,
    _index_file_entity_async,
    _index_table_entity_async,
    _insert_file_migration,
    _insert_table_file_migration,
    _join_column_names,
    _mark_container_indexed,
    _migrate_file_version_async,
    _migrate_item_async,
    _migrate_table_attached_file_async,
    _prepare_migration_db,
    _query_migration_batch,
    _record_indexing_error,
    _retrieve_index_settings,
    _update_migration_database,
    _verify_storage_location_ownership_async,
    index_files_for_migration_async,
    migrate_indexed_files_async,
    track_migration_results_async,
)
from synapseclient.models.services.migration_types import (
    IndexingError,
    MigrationError,
    MigrationKey,
    MigrationResult,
    MigrationSettings,
    MigrationStatus,
    MigrationType,
)

# =============================================================================
# Fixtures
# =============================================================================

MODULE = "synapseclient.models.services.migration"


@pytest.fixture
def in_memory_db():
    """Return an in-memory SQLite connection with schema applied."""
    conn = sqlite3.connect(":memory:")
    cursor = conn.cursor()
    _ensure_schema(cursor)
    conn.commit()
    yield conn, cursor
    conn.close()


@pytest.fixture
def db_file():
    """Return a path to a temporary SQLite file with schema applied."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    conn = sqlite3.connect(path)
    try:
        cursor = conn.cursor()
        _ensure_schema(cursor)
        conn.commit()
    finally:
        conn.close()
    yield path
    os.unlink(path)


@pytest.fixture
def db_file_with_settings():
    """A temp db file with MigrationSettings already populated."""
    fd, path = tempfile.mkstemp(suffix=".db")
    os.close(fd)
    settings = MigrationSettings(
        root_id="syn1",
        dest_storage_location_id="99",
        source_storage_location_ids=[],
        file_version_strategy="new",
        include_table_files=False,
    )
    conn = sqlite3.connect(path)
    try:
        cursor = conn.cursor()
        _ensure_schema(cursor)
        cursor.execute(
            "INSERT INTO migration_settings (settings) VALUES (?)",
            (json.dumps(settings.to_dict()),),
        )
        conn.commit()
    finally:
        conn.close()
    yield path, settings
    os.unlink(path)


def _populate_db(db_path: str) -> None:
    """Insert sample rows into a migration database for MigrationResult tests."""
    rows = [
        # (id, type, version, row_id, col_id, parent_id, status, exception, from_sl, from_fh, to_fh, file_size)
        (
            "syn1",
            MigrationType.PROJECT.value,
            None,
            None,
            None,
            None,
            MigrationStatus.INDEXED.value,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "syn2",
            MigrationType.FOLDER.value,
            None,
            None,
            None,
            "syn1",
            MigrationStatus.INDEXED.value,
            None,
            None,
            None,
            None,
            None,
        ),
        (
            "syn3",
            MigrationType.FILE.value,
            1,
            None,
            None,
            "syn1",
            MigrationStatus.MIGRATED.value,
            None,
            "10",
            "fh_a",
            "fh_b",
            1024,
        ),
        (
            "syn4",
            MigrationType.TABLE_ATTACHED_FILE.value,
            2,
            5,
            7,
            "syn1",
            MigrationStatus.MIGRATED.value,
            None,
            "10",
            "fh_c",
            "fh_d",
            512,
        ),
        (
            "syn5",
            MigrationType.FILE.value,
            3,
            None,
            None,
            "syn1",
            MigrationStatus.ERRORED.value,
            "boom",
            None,
            None,
            None,
            None,
        ),
        (
            "syn6",
            MigrationType.FILE.value,
            4,
            None,
            None,
            "syn1",
            MigrationStatus.INDEXED.value,
            None,
            "10",
            "fh_e",
            None,
            256,
        ),
        (
            "syn7",
            MigrationType.FILE.value,
            5,
            None,
            None,
            "syn1",
            MigrationStatus.ALREADY_MIGRATED.value,
            None,
            "20",
            "fh_f",
            None,
            128,
        ),
    ]
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        _ensure_schema(cursor)
        cursor.executemany(
            """
            INSERT INTO migrations (id, type, version, row_id, col_id, parent_id, status, exception,
                from_storage_location_id, from_file_handle_id, to_file_handle_id, file_size)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            rows,
        )
        conn.commit()


@pytest.fixture
def result_db():
    with tempfile.NamedTemporaryFile(suffix=".db", delete=False) as f:
        path = f.name
    _populate_db(path)
    yield path
    os.unlink(path)


def _make_mock_client():
    client = MagicMock()
    client.rest_get_async = AsyncMock()
    client.rest_put_async = AsyncMock()
    client.logger = MagicMock()
    client._get_parallel_file_transfer_semaphore.return_value = asyncio.Semaphore(10)
    return client


def _make_file_handle(
    concrete_type=None, storage_location_id="10", content_size=1024, fh_id="fh1"
):
    fh = MagicMock()
    fh.concrete_type = concrete_type or concrete_types.S3_FILE_HANDLE
    fh.storage_location_id = storage_location_id
    fh.content_size = content_size
    fh.id = fh_id
    return fh


def _make_entity(
    entity_id="syn3", version_number=1, file_handle=None, data_file_handle_id="fh1"
):
    entity = MagicMock()
    entity.id = entity_id
    entity.version_number = version_number
    entity.file_handle = file_handle or _make_file_handle()
    entity.data_file_handle_id = data_file_handle_id
    entity.dataFileHandleId = data_file_handle_id
    entity.store_async = AsyncMock()
    return entity


async def _aiter(*items):
    """Helper: yield items from an async generator."""
    for item in items:
        yield item


# =============================================================================
# migration_types.py – MigrationStatus
# =============================================================================


class TestMigrationStatus:
    def test_values(self):
        assert MigrationStatus.INDEXED.value == 1
        assert MigrationStatus.MIGRATED.value == 2
        assert MigrationStatus.ALREADY_MIGRATED.value == 3
        assert MigrationStatus.ERRORED.value == 4

    def test_names(self):
        assert MigrationStatus(1).name == "INDEXED"
        assert MigrationStatus(2).name == "MIGRATED"
        assert MigrationStatus(3).name == "ALREADY_MIGRATED"
        assert MigrationStatus(4).name == "ERRORED"


# =============================================================================
# migration_types.py – MigrationType
# =============================================================================


class TestMigrationType:
    def test_values(self):
        assert MigrationType.PROJECT.value == 1
        assert MigrationType.FOLDER.value == 2
        assert MigrationType.FILE.value == 3
        assert MigrationType.TABLE_ATTACHED_FILE.value == 4

    @pytest.mark.parametrize(
        "concrete_type,expected",
        [
            (concrete_types.PROJECT_ENTITY, MigrationType.PROJECT),
            (concrete_types.FOLDER_ENTITY, MigrationType.FOLDER),
            (concrete_types.FILE_ENTITY, MigrationType.FILE),
            (concrete_types.TABLE_ENTITY, MigrationType.TABLE_ATTACHED_FILE),
        ],
    )
    def test_from_concrete_type(self, concrete_type, expected):
        assert MigrationType.from_concrete_type(concrete_type) == expected

    def test_from_concrete_type_unknown_raises(self):
        with pytest.raises(ValueError, match="Unhandled concrete type"):
            MigrationType.from_concrete_type("org.sagebionetworks.repo.model.Unknown")


# =============================================================================
# migration_types.py – MigrationKey
# =============================================================================


class TestMigrationKey:
    def test_equality_same(self):
        k1 = MigrationKey("syn1", MigrationType.FILE, version=2)
        k2 = MigrationKey("syn1", MigrationType.FILE, version=2)
        assert k1 == k2

    def test_equality_different_version(self):
        k1 = MigrationKey("syn1", MigrationType.FILE, version=1)
        k2 = MigrationKey("syn1", MigrationType.FILE, version=2)
        assert k1 != k2

    def test_equality_different_id(self):
        k1 = MigrationKey("syn1", MigrationType.FILE)
        k2 = MigrationKey("syn2", MigrationType.FILE)
        assert k1 != k2

    def test_equality_different_type(self):
        k1 = MigrationKey("syn1", MigrationType.FILE)
        k2 = MigrationKey("syn1", MigrationType.TABLE_ATTACHED_FILE)
        assert k1 != k2

    def test_equality_with_row_col(self):
        k1 = MigrationKey("syn1", MigrationType.TABLE_ATTACHED_FILE, row_id=1, col_id=2)
        k2 = MigrationKey("syn1", MigrationType.TABLE_ATTACHED_FILE, row_id=1, col_id=2)
        assert k1 == k2

    def test_not_equal_to_other_type(self):
        k = MigrationKey("syn1", MigrationType.FILE)
        assert k != "not a key"

    def test_hashable_usable_in_set(self):
        k1 = MigrationKey("syn1", MigrationType.FILE, version=1)
        k2 = MigrationKey("syn1", MigrationType.FILE, version=1)
        k3 = MigrationKey("syn2", MigrationType.FILE, version=1)
        s = {k1, k2, k3}
        assert len(s) == 2

    def test_default_optional_fields_are_none(self):
        k = MigrationKey("syn1", MigrationType.FOLDER)
        assert k.version is None
        assert k.row_id is None
        assert k.col_id is None


# =============================================================================
# migration_types.py – MigrationSettings
# =============================================================================


class TestMigrationSettings:
    def _make_settings(self, **kwargs):
        defaults = dict(
            root_id="syn1",
            dest_storage_location_id="123",
            source_storage_location_ids=["10", "20"],
            file_version_strategy="new",
            include_table_files=False,
        )
        defaults.update(kwargs)
        return MigrationSettings(**defaults)

    def test_to_dict_round_trip(self):
        s = self._make_settings()
        d = s.to_dict()
        assert d["root_id"] == "syn1"
        assert d["dest_storage_location_id"] == "123"
        assert d["source_storage_location_ids"] == ["10", "20"]
        assert d["file_version_strategy"] == "new"
        assert d["include_table_files"] == 0

    def test_to_dict_include_table_files_true(self):
        s = self._make_settings(include_table_files=True)
        assert s.to_dict()["include_table_files"] == 1

    def test_from_dict(self):
        d = {
            "root_id": "syn5",
            "dest_storage_location_id": "99",
            "source_storage_location_ids": ["5"],
            "file_version_strategy": "all",
            "include_table_files": 1,
        }
        s = MigrationSettings.from_dict(d)
        assert s.root_id == "syn5"
        assert s.dest_storage_location_id == "99"
        assert s.source_storage_location_ids == ["5"]
        assert s.file_version_strategy == "all"
        assert s.include_table_files is True

    def test_from_dict_int_false(self):
        d = {
            "root_id": "syn5",
            "dest_storage_location_id": "99",
            "include_table_files": 0,
        }
        s = MigrationSettings.from_dict(d)
        assert s.include_table_files is False

    def test_from_dict_missing_optional_fields(self):
        d = {"root_id": "syn1", "dest_storage_location_id": "5"}
        s = MigrationSettings.from_dict(d)
        assert s.source_storage_location_ids == []
        assert s.file_version_strategy == "new"
        assert s.include_table_files is False

    def test_verify_migration_settings_matching(self):
        s = self._make_settings()
        # Should not raise
        s.verify_migration_settings(s, "/tmp/test.db")

    @pytest.mark.parametrize(
        "field_name,bad_value",
        [
            ("root_id", "syn999"),
            ("dest_storage_location_id", "9999"),
            ("file_version_strategy", "all"),
            ("include_table_files", True),
        ],
    )
    def test_verify_migration_settings_mismatch_raises(self, field_name, bad_value):
        existing = self._make_settings()
        current_kwargs = {field_name: bad_value}
        current = self._make_settings(**current_kwargs)
        with pytest.raises(ValueError, match="Index parameter does not match"):
            current.verify_migration_settings(existing, "/tmp/test.db")


# =============================================================================
# migration_types.py – IndexingError
# =============================================================================


class TestIndexingError:
    def test_attributes(self):
        err = IndexingError("syn42", concrete_types.FILE_ENTITY)
        assert err.entity_id == "syn42"
        assert err.concrete_type == concrete_types.FILE_ENTITY

    def test_is_exception(self):
        assert issubclass(IndexingError, Exception)


# =============================================================================
# migration_types.py – MigrationError
# =============================================================================


class TestMigrationError:
    def test_basic_message(self):
        key = MigrationKey("syn1", MigrationType.FILE)
        err = MigrationError(key, from_file_handle_id="fh1")
        assert "syn1" in str(err)
        assert err.key is key
        assert err.from_file_handle_id == "fh1"
        assert err.to_file_handle_id is None

    def test_with_cause(self):
        key = MigrationKey("syn1", MigrationType.FILE)
        cause = RuntimeError("network failure")
        err = MigrationError(key, from_file_handle_id="fh1", cause=cause)
        assert "network failure" in str(err)

    def test_with_to_handle(self):
        key = MigrationKey("syn1", MigrationType.FILE)
        err = MigrationError(key, from_file_handle_id="fh1", to_file_handle_id="fh2")
        assert err.to_file_handle_id == "fh2"

    def test_is_exception(self):
        assert issubclass(MigrationError, Exception)


# =============================================================================
# migration_types.py – MigrationResult
# =============================================================================


class TestMigrationResult:
    def test_get_counts_by_status(self, result_db):
        result = MigrationResult(db_path=result_db)
        counts = result.get_counts_by_status()
        # Containers (PROJECT, FOLDER) are excluded from counts
        assert counts["MIGRATED"] == 2
        assert counts["ERRORED"] == 1
        assert counts["INDEXED"] == 1
        assert counts["ALREADY_MIGRATED"] == 1

    def test_counts_by_status_property(self, result_db):
        result = MigrationResult(db_path=result_db)
        assert result.counts_by_status == result.get_counts_by_status()

    def test_get_migrations_returns_only_file_and_table(self, result_db):
        result = MigrationResult(db_path=result_db)
        migrations = list(result.get_migrations())
        types = {m["type"] for m in migrations}
        assert types <= {"file", "table"}

    def test_get_migrations_file_entry(self, result_db):
        result = MigrationResult(db_path=result_db)
        migrations = list(result.get_migrations())
        file_migrations = [m for m in migrations if m["id"] == "syn3"]
        assert len(file_migrations) == 1
        m = file_migrations[0]
        assert m["type"] == "file"
        assert m["version"] == 1
        assert m["status"] == "MIGRATED"
        assert m["from_file_handle_id"] == "fh_a"
        assert m["to_file_handle_id"] == "fh_b"

    def test_get_migrations_table_entry(self, result_db):
        result = MigrationResult(db_path=result_db)
        migrations = list(result.get_migrations())
        table_migrations = [m for m in migrations if m["id"] == "syn4"]
        assert len(table_migrations) == 1
        m = table_migrations[0]
        assert m["type"] == "table"
        assert m["row_id"] == 5

    def test_get_migrations_error_entry(self, result_db):
        result = MigrationResult(db_path=result_db)
        migrations = list(result.get_migrations())
        errored = [m for m in migrations if m["status"] == "ERRORED"]
        assert len(errored) == 1
        assert errored[0]["exception"] == "boom"

    def test_get_migrations_col_name_resolved_via_client(self, result_db):
        mock_client = mock.MagicMock()
        mock_client.restGET.return_value = {"name": "my_col"}
        result = MigrationResult(db_path=result_db, synapse_client=mock_client)
        migrations = list(result.get_migrations())
        table_m = [m for m in migrations if m["type"] == "table"][0]
        assert table_m["col_name"] == "my_col"

    def test_as_csv(self, result_db):
        result = MigrationResult(db_path=result_db)
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False, mode="w") as f:
            csv_path = f.name
        try:
            result.as_csv(csv_path)
            with open(csv_path, newline="") as f:
                reader = csv.DictReader(f)
                rows = list(reader)
            ids = {r["id"] for r in rows}
            # Should include file and table-attached entries, not containers
            assert "syn3" in ids
            assert "syn4" in ids
            assert "syn1" not in ids  # PROJECT
            assert "syn2" not in ids  # FOLDER
            assert "id" in reader.fieldnames
            assert "status" in reader.fieldnames
        finally:
            os.unlink(csv_path)

    @pytest.mark.asyncio
    async def test_get_counts_by_status_async(self, result_db):
        result = MigrationResult(db_path=result_db)
        counts = await result.get_counts_by_status_async()
        assert counts["MIGRATED"] == 2

    @pytest.mark.asyncio
    async def test_get_migrations_async(self, result_db):
        result = MigrationResult(db_path=result_db)
        migrations = await result.get_migrations_async()
        assert isinstance(migrations, list)
        assert len(migrations) > 0

    @pytest.mark.asyncio
    async def test_as_csv_async(self, result_db):
        result = MigrationResult(db_path=result_db)
        with tempfile.NamedTemporaryFile(suffix=".csv", delete=False) as f:
            csv_path = f.name
        try:
            await result.as_csv_async(csv_path)
            assert os.path.exists(csv_path)
            with open(csv_path) as f:
                content = f.read()
            assert "id" in content
        finally:
            os.unlink(csv_path)


# =============================================================================
# migration.py – pure helper functions
# =============================================================================


class TestGetDefaultDbPath:
    def test_returns_path_with_entity_id(self):
        path = _get_default_db_path("syn123")
        assert "migration_syn123.db" in path
        assert os.path.exists(os.path.dirname(path))


class TestEscapeColumnName:
    def test_plain_string(self):
        assert _escape_column_name("my_col") == '"my_col"'

    def test_dict_with_name_key(self):
        assert _escape_column_name({"name": "col_name"}) == '"col_name"'

    def test_escapes_double_quotes(self):
        assert _escape_column_name('col"name') == '"col""name"'

    def test_dict_escapes_double_quotes(self):
        assert _escape_column_name({"name": 'a"b'}) == '"a""b"'


class TestJoinColumnNames:
    def test_single(self):
        assert _join_column_names(["col1"]) == '"col1"'

    def test_multiple(self):
        result = _join_column_names(["a", "b", "c"])
        assert result == '"a","b","c"'

    def test_dict_columns(self):
        cols = [{"name": "x"}, {"name": "y"}]
        assert _join_column_names(cols) == '"x","y"'


class TestGetPartSize:
    def test_small_file_uses_default(self):
        size = 1 * 1024 * 1024  # 1 MB
        assert _get_part_size(size) == DEFAULT_PART_SIZE

    def test_large_file_exceeds_default(self):
        from synapseclient.core.upload.multipart_upload import MAX_NUMBER_OF_PARTS

        # File so large that default part size would require too many parts
        size = DEFAULT_PART_SIZE * MAX_NUMBER_OF_PARTS + 1
        part_size = _get_part_size(size)
        assert part_size > DEFAULT_PART_SIZE


class TestGetFileMigrationStatus:
    def _make_handle(self, concrete_type, storage_location_id):
        handle = mock.MagicMock()
        handle.concrete_type = concrete_type
        handle.storage_location_id = storage_location_id
        return handle

    def test_non_s3_handle_returns_none(self):
        handle = self._make_handle(
            "org.sagebionetworks.repo.model.file.ExternalFileHandle", "10"
        )
        result = _get_file_migration_status(handle, [], "20")
        assert result is None

    def test_already_at_destination_returns_already_migrated(self):
        handle = self._make_handle(concrete_types.S3_FILE_HANDLE, "20")
        result = _get_file_migration_status(handle, [], "20")
        assert result == MigrationStatus.ALREADY_MIGRATED.value

    def test_no_source_filter_returns_indexed(self):
        handle = self._make_handle(concrete_types.S3_FILE_HANDLE, "10")
        result = _get_file_migration_status(handle, [], "20")
        assert result == MigrationStatus.INDEXED.value

    def test_source_filter_match_returns_indexed(self):
        handle = self._make_handle(concrete_types.S3_FILE_HANDLE, "10")
        result = _get_file_migration_status(handle, ["10", "11"], "20")
        assert result == MigrationStatus.INDEXED.value

    def test_source_filter_no_match_returns_none(self):
        handle = self._make_handle(concrete_types.S3_FILE_HANDLE, "99")
        result = _get_file_migration_status(handle, ["10", "11"], "20")
        assert result is None


# =============================================================================
# migration.py – database helper functions
# =============================================================================


class TestEnsureSchema:
    def test_creates_migrations_table(self, in_memory_db):
        conn, cursor = in_memory_db
        tables = cursor.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall()
        table_names = {t[0] for t in tables}
        assert "migrations" in table_names
        assert "migration_settings" in table_names

    def test_idempotent(self, in_memory_db):
        conn, cursor = in_memory_db
        # Running again should not raise
        _ensure_schema(cursor)


class TestCheckIndexed:
    def test_not_indexed(self, in_memory_db):
        conn, cursor = in_memory_db
        assert _check_indexed(cursor, "syn999", synapse_client=MagicMock()) is False

    def test_indexed(self, in_memory_db):
        conn, cursor = in_memory_db
        cursor.execute(
            "INSERT INTO migrations (id, type, status) VALUES (?, ?, ?)",
            ("syn1", MigrationType.FILE.value, MigrationStatus.INDEXED.value),
        )
        conn.commit()
        assert _check_indexed(cursor, "syn1", synapse_client=MagicMock()) is True


class TestMarkContainerIndexed:
    def test_inserts_row(self, in_memory_db):
        conn, cursor = in_memory_db
        # Callers always pass migration_type as .value (int)
        _mark_container_indexed(cursor, "syn10", MigrationType.FOLDER.value, "syn1")
        conn.commit()
        row = cursor.execute(
            "SELECT id, type, parent_id, status FROM migrations WHERE id = 'syn10'"
        ).fetchone()
        assert row is not None
        assert row[1] == MigrationType.FOLDER.value
        assert row[2] == "syn1"
        assert row[3] == MigrationStatus.INDEXED.value

    def test_check_indexed_prevents_double_insert(self, in_memory_db):
        """In practice, _check_indexed guards against re-indexing containers.
        After marking a container indexed, _check_indexed should return True."""
        conn, cursor = in_memory_db
        _mark_container_indexed(cursor, "syn10", MigrationType.FOLDER.value, "syn1")
        conn.commit()
        assert _check_indexed(cursor, "syn10", synapse_client=MagicMock()) is True


class TestRecordIndexingError:
    def test_inserts_error_row(self, in_memory_db):
        conn, cursor = in_memory_db
        _record_indexing_error(
            cursor, "syn11", MigrationType.FILE.value, "syn1", "Traceback..."
        )
        conn.commit()
        row = cursor.execute(
            "SELECT status, exception FROM migrations WHERE id='syn11'"
        ).fetchone()
        assert row[0] == MigrationStatus.ERRORED.value
        assert row[1] == "Traceback..."


class TestInsertFileMigration:
    def test_inserts_row(self, in_memory_db):
        conn, cursor = in_memory_db
        insert_values = [
            (
                "syn3",
                MigrationType.FILE.value,
                1,
                "syn1",
                "10",
                "fh_a",
                1024,
                MigrationStatus.INDEXED.value,
            ),
        ]
        _insert_file_migration(cursor, insert_values)
        conn.commit()
        row = cursor.execute(
            "SELECT id, type, version, from_file_handle_id FROM migrations WHERE id='syn3'"
        ).fetchone()
        assert row is not None
        assert row[2] == 1
        assert row[3] == "fh_a"

    def test_inserts_multiple(self, in_memory_db):
        conn, cursor = in_memory_db
        insert_values = [
            (
                "syn3",
                MigrationType.FILE.value,
                1,
                "syn1",
                "10",
                "fh_a",
                1024,
                MigrationStatus.INDEXED.value,
            ),
            (
                "syn4",
                MigrationType.FILE.value,
                2,
                "syn1",
                "10",
                "fh_b",
                2048,
                MigrationStatus.INDEXED.value,
            ),
        ]
        _insert_file_migration(cursor, insert_values)
        conn.commit()
        count = cursor.execute("SELECT count(*) FROM migrations").fetchone()[0]
        assert count == 2


class TestInsertTableFileMigration:
    def test_inserts_row(self, in_memory_db):
        conn, cursor = in_memory_db
        insert_values = [
            (
                "syn5",
                MigrationType.TABLE_ATTACHED_FILE.value,
                1,
                2,
                3,
                "syn1",
                "10",
                "fh_x",
                512,
                MigrationStatus.INDEXED.value,
            ),
        ]
        _insert_table_file_migration(cursor, insert_values)
        conn.commit()
        row = cursor.execute(
            "SELECT id, row_id, col_id FROM migrations WHERE id='syn5'"
        ).fetchone()
        assert row is not None
        assert row[1] == 1
        assert row[2] == 2

    def test_ignore_on_duplicate(self, in_memory_db):
        conn, cursor = in_memory_db
        insert_values = [
            (
                "syn5",
                MigrationType.TABLE_ATTACHED_FILE.value,
                1,
                2,
                3,
                "syn1",
                "10",
                "fh_x",
                512,
                MigrationStatus.INDEXED.value,
            ),
            (
                "syn5",
                MigrationType.TABLE_ATTACHED_FILE.value,
                1,
                2,
                3,
                "syn1",
                "10",
                "fh_x",
                512,
                MigrationStatus.INDEXED.value,
            ),
        ]
        _insert_table_file_migration(cursor, insert_values)
        conn.commit()
        count = cursor.execute(
            "SELECT count(*) FROM migrations WHERE id='syn5'"
        ).fetchone()[0]
        assert count == 1


class TestRetrieveIndexSettings:
    def test_returns_none_when_empty(self, in_memory_db):
        conn, cursor = in_memory_db
        assert _retrieve_index_settings(cursor) is None

    def test_returns_settings_when_present(self, in_memory_db):
        conn, cursor = in_memory_db
        settings = MigrationSettings(
            root_id="syn1",
            dest_storage_location_id="99",
            source_storage_location_ids=["5"],
            file_version_strategy="all",
            include_table_files=True,
        )
        cursor.execute(
            "INSERT INTO migration_settings (settings) VALUES (?)",
            (json.dumps(settings.to_dict()),),
        )
        conn.commit()
        retrieved = _retrieve_index_settings(cursor)
        assert retrieved.root_id == "syn1"
        assert retrieved.dest_storage_location_id == "99"
        assert retrieved.include_table_files is True


class TestPrepareMigrationDb:
    def test_inserts_settings_on_first_run(self, in_memory_db):
        conn, cursor = in_memory_db
        _prepare_migration_db(
            conn=conn,
            cursor=cursor,
            db_path=":memory:",
            root_id="syn1",
            dest_storage_location_id="99",
            source_storage_location_ids=["5"],
            file_version_strategy="new",
            include_table_files=False,
        )
        retrieved = _retrieve_index_settings(cursor)
        assert retrieved is not None
        assert retrieved.root_id == "syn1"

    def test_no_error_on_matching_settings(self, in_memory_db):
        conn, cursor = in_memory_db
        kwargs = dict(
            conn=conn,
            cursor=cursor,
            db_path=":memory:",
            root_id="syn1",
            dest_storage_location_id="99",
            source_storage_location_ids=["5"],
            file_version_strategy="new",
            include_table_files=False,
        )
        _prepare_migration_db(**kwargs)
        # Should not raise on second call with same settings
        _prepare_migration_db(**kwargs)

    def test_raises_on_mismatched_settings(self, in_memory_db):
        conn, cursor = in_memory_db
        _prepare_migration_db(
            conn=conn,
            cursor=cursor,
            db_path=":memory:",
            root_id="syn1",
            dest_storage_location_id="99",
            source_storage_location_ids=[],
            file_version_strategy="new",
            include_table_files=False,
        )
        with pytest.raises(ValueError, match="Index parameter does not match"):
            _prepare_migration_db(
                conn=conn,
                cursor=cursor,
                db_path=":memory:",
                root_id="syn_different",  # changed
                dest_storage_location_id="99",
                source_storage_location_ids=[],
                file_version_strategy="new",
                include_table_files=False,
            )


class TestCheckFileHandleExists:
    def test_returns_none_when_not_found(self, in_memory_db):
        conn, cursor = in_memory_db
        assert _check_file_handle_exists(cursor, "fh_missing") is None

    def test_returns_to_handle_when_found(self, in_memory_db):
        conn, cursor = in_memory_db
        cursor.execute(
            """INSERT INTO migrations (id, type, status, from_file_handle_id, to_file_handle_id)
               VALUES (?, ?, ?, ?, ?)""",
            (
                "syn1",
                MigrationType.FILE.value,
                MigrationStatus.MIGRATED.value,
                "fh_a",
                "fh_b",
            ),
        )
        conn.commit()
        assert _check_file_handle_exists(cursor, "fh_a") == "fh_b"

    def test_returns_none_when_to_handle_is_null(self, in_memory_db):
        conn, cursor = in_memory_db
        cursor.execute(
            """INSERT INTO migrations (id, type, status, from_file_handle_id)
               VALUES (?, ?, ?, ?)""",
            ("syn1", MigrationType.FILE.value, MigrationStatus.INDEXED.value, "fh_a"),
        )
        conn.commit()
        assert _check_file_handle_exists(cursor, "fh_a") is None


class TestUpdateMigrationDatabase:
    def _insert_indexed_file(self, cursor, entity_id="syn1", version=1):
        cursor.execute(
            """INSERT INTO migrations (id, type, version, status, from_file_handle_id)
               VALUES (?, ?, ?, ?, ?)""",
            (
                entity_id,
                MigrationType.FILE.value,
                version,
                MigrationStatus.INDEXED.value,
                "fh_src",
            ),
        )

    def test_updates_to_migrated(self, in_memory_db):
        conn, cursor = in_memory_db
        self._insert_indexed_file(cursor)
        conn.commit()
        key = MigrationKey("syn1", MigrationType.FILE, version=1)
        # Callers always pass status as .value (int)
        _update_migration_database(
            conn, cursor, key, "fh_dest", MigrationStatus.MIGRATED.value
        )
        row = cursor.execute(
            "SELECT status, to_file_handle_id FROM migrations WHERE id='syn1'"
        ).fetchone()
        assert row[0] == MigrationStatus.MIGRATED.value
        assert row[1] == "fh_dest"

    def test_stores_exception_traceback(self, in_memory_db):
        conn, cursor = in_memory_db
        self._insert_indexed_file(cursor)
        conn.commit()
        key = MigrationKey("syn1", MigrationType.FILE, version=1)
        cause = RuntimeError("disk full")
        _update_migration_database(
            conn, cursor, key, None, MigrationStatus.ERRORED.value, exception=cause
        )
        row = cursor.execute(
            "SELECT status, exception FROM migrations WHERE id='syn1'"
        ).fetchone()
        assert row[0] == MigrationStatus.ERRORED.value
        assert "disk full" in row[1]


class TestConfirmMigration:
    def test_force_returns_true(self, in_memory_db):
        conn, cursor = in_memory_db
        assert _confirm_migration(cursor, "99", force=True) is True

    def test_no_items_returns_false(self, in_memory_db):
        conn, cursor = in_memory_db
        assert (
            _confirm_migration(cursor, "99", force=False, synapse_client=MagicMock())
            is False
        )

    def test_non_tty_returns_false_without_input(self, in_memory_db):
        conn, cursor = in_memory_db
        cursor.execute(
            "INSERT INTO migrations (id, type, status) VALUES (?, ?, ?)",
            ("syn1", MigrationType.FILE.value, MigrationStatus.INDEXED.value),
        )
        conn.commit()
        with mock.patch("sys.stdout") as mock_stdout:
            mock_stdout.isatty.return_value = False
            result = _confirm_migration(
                cursor, "99", force=False, synapse_client=MagicMock()
            )
        assert result is False

    def test_tty_yes_returns_true(self, in_memory_db):
        conn, cursor = in_memory_db
        cursor.execute(
            "INSERT INTO migrations (id, type, status) VALUES (?, ?, ?)",
            ("syn1", MigrationType.FILE.value, MigrationStatus.INDEXED.value),
        )
        conn.commit()
        with mock.patch("sys.stdout") as mock_stdout, mock.patch(
            "builtins.input", return_value="y"
        ):
            mock_stdout.isatty.return_value = True
            result = _confirm_migration(cursor, "99", force=False)
        assert result is True

    def test_tty_no_returns_false(self, in_memory_db):
        conn, cursor = in_memory_db
        cursor.execute(
            "INSERT INTO migrations (id, type, status) VALUES (?, ?, ?)",
            ("syn1", MigrationType.FILE.value, MigrationStatus.INDEXED.value),
        )
        conn.commit()
        with mock.patch("sys.stdout") as mock_stdout, mock.patch(
            "builtins.input", return_value="n"
        ):
            mock_stdout.isatty.return_value = True
            result = _confirm_migration(cursor, "99", force=False)
        assert result is False


class TestQueryMigrationBatch:
    def _insert_indexed(
        self,
        cursor,
        entity_id,
        migration_type,
        version=None,
        row_id=None,
        col_id=None,
        from_fh="fh_x",
    ):
        cursor.execute(
            """INSERT INTO migrations (id, type, version, row_id, col_id, status, from_file_handle_id)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                entity_id,
                migration_type.value,
                version,
                row_id,
                col_id,
                MigrationStatus.INDEXED.value,
                from_fh,
            ),
        )

    def test_returns_forward_progress(self, in_memory_db):
        conn, cursor = in_memory_db
        self._insert_indexed(
            cursor, "syn2", MigrationType.FILE, version=1, from_fh="fh_1"
        )
        self._insert_indexed(
            cursor, "syn3", MigrationType.FILE, version=1, from_fh="fh_2"
        )
        conn.commit()

        start_key = MigrationKey("", MigrationType.FILE)
        results = _query_migration_batch(cursor, start_key, set(), set(), limit=10)
        ids = [r["id"] for r in results]
        assert "syn2" in ids
        assert "syn3" in ids

    def test_excludes_pending_file_handles(self, in_memory_db):
        conn, cursor = in_memory_db
        self._insert_indexed(
            cursor, "syn2", MigrationType.FILE, version=1, from_fh="fh_pending"
        )
        self._insert_indexed(
            cursor, "syn3", MigrationType.FILE, version=1, from_fh="fh_ok"
        )
        conn.commit()

        start_key = MigrationKey("", MigrationType.FILE)
        results = _query_migration_batch(
            cursor, start_key, {"fh_pending"}, set(), limit=10
        )
        ids = [r["id"] for r in results]
        assert "syn2" not in ids
        assert "syn3" in ids

    def test_respects_limit(self, in_memory_db):
        conn, cursor = in_memory_db
        for i in range(5):
            self._insert_indexed(
                cursor, f"syn{i+10}", MigrationType.FILE, version=1, from_fh=f"fh_{i}"
            )
        conn.commit()

        start_key = MigrationKey("", MigrationType.FILE)
        results = _query_migration_batch(cursor, start_key, set(), set(), limit=2)
        assert len(results) <= 2


# =============================================================================
# _verify_storage_location_ownership_async
# =============================================================================


class TestVerifyStorageLocationOwnershipAsync:
    @pytest.mark.asyncio
    async def test_success(self):
        client = _make_mock_client()
        mock_get = AsyncMock(return_value={"storageLocationId": "99"})
        with patch(f"{MODULE}.get_storage_location_setting", mock_get):
            # Should not raise
            await _verify_storage_location_ownership_async("99", synapse_client=client)
            mock_get.assert_awaited_once_with(
                storage_location_id="99", synapse_client=client
            )

    @pytest.mark.asyncio
    async def test_synapse_error_raises_value_error(self):
        client = _make_mock_client()
        mock_get = AsyncMock(side_effect=SynapseError("forbidden"))
        with patch(f"{MODULE}.get_storage_location_setting", mock_get):
            with pytest.raises(ValueError, match="Unable to verify ownership"):
                await _verify_storage_location_ownership_async(
                    "99", synapse_client=client
                )


# =============================================================================
# _get_version_numbers_async
# =============================================================================


class TestGetVersionNumbersAsync:
    @pytest.mark.asyncio
    async def test_yields_version_numbers(self):
        client = _make_mock_client()
        pages = [{"versionNumber": 3}, {"versionNumber": 2}, {"versionNumber": 1}]

        async def _mock_paginated(path, *, synapse_client):
            for p in pages:
                yield p

        with patch(f"{MODULE}.rest_get_paginated_async", _mock_paginated):
            versions = [v async for v in _get_version_numbers_async("syn1", client)]

        assert versions == [3, 2, 1]

    @pytest.mark.asyncio
    async def test_empty_yields_nothing(self):
        client = _make_mock_client()

        async def _mock_paginated(path, *, synapse_client):
            return
            yield  # make it an async generator

        with patch(f"{MODULE}.rest_get_paginated_async", _mock_paginated):
            versions = [v async for v in _get_version_numbers_async("syn1", client)]

        assert versions == []


# =============================================================================
# index_files_for_migration_async – validation
# =============================================================================


class TestIndexFilesForMigrationAsyncValidation:
    @pytest.mark.asyncio
    async def test_invalid_file_version_strategy_raises(self):
        client = _make_mock_client()
        with patch(f"{MODULE}.Synapse.get_client", return_value=client):
            with pytest.raises(ValueError, match="Invalid file_version_strategy"):
                await index_files_for_migration_async(
                    entity="syn1",
                    dest_storage_location_id="99",
                    file_version_strategy="bogus",
                    synapse_client=client,
                )

    @pytest.mark.asyncio
    async def test_skip_strategy_with_no_table_files_raises(self):
        client = _make_mock_client()
        with patch(f"{MODULE}.Synapse.get_client", return_value=client):
            with pytest.raises(ValueError, match="nothing to migrate"):
                await index_files_for_migration_async(
                    entity="syn1",
                    dest_storage_location_id="99",
                    file_version_strategy="skip",
                    include_table_files=False,
                    synapse_client=client,
                )

    @pytest.mark.asyncio
    async def test_ownership_failure_raises(self):
        client = _make_mock_client()
        client.rest_get_async.side_effect = SynapseError("forbidden")

        with patch(f"{MODULE}.Synapse.get_client", return_value=client), patch(
            f"{MODULE}.utils.id_of", return_value="syn1"
        ):
            with pytest.raises(ValueError, match="Unable to verify ownership"):
                await index_files_for_migration_async(
                    entity="syn1",
                    dest_storage_location_id="99",
                    synapse_client=client,
                )

    @pytest.mark.asyncio
    async def test_successful_indexing_returns_migration_result(self):
        client = _make_mock_client()
        entity = _make_entity("syn3")

        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            with (
                patch(f"{MODULE}.Synapse.get_client", return_value=client),
                patch(f"{MODULE}.utils.id_of", return_value="syn3"),
                patch(
                    f"{MODULE}._verify_storage_location_ownership_async",
                    new=AsyncMock(),
                ),
                patch(f"{MODULE}._index_entity_async", new=AsyncMock()),
            ):
                result = await index_files_for_migration_async(
                    entity=entity,
                    dest_storage_location_id="99",
                    db_path=db_path,
                    synapse_client=client,
                )
        finally:
            os.unlink(db_path)

        assert isinstance(result, MigrationResult)
        assert result.db_path == db_path

    @pytest.mark.asyncio
    async def test_indexing_error_is_reraised(self):
        client = _make_mock_client()
        entity = _make_entity("syn3")
        underlying = RuntimeError("network down")
        indexing_err = IndexingError("syn3", concrete_types.FILE_ENTITY)
        indexing_err.__cause__ = underlying

        fd, db_path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            with (
                patch(f"{MODULE}.Synapse.get_client", return_value=client),
                patch(f"{MODULE}.utils.id_of", return_value="syn3"),
                patch(
                    f"{MODULE}._verify_storage_location_ownership_async",
                    new=AsyncMock(),
                ),
                patch(f"{MODULE}._index_entity_async", side_effect=indexing_err),
            ):
                with pytest.raises(RuntimeError, match="network down"):
                    await index_files_for_migration_async(
                        entity=entity,
                        dest_storage_location_id="99",
                        db_path=db_path,
                        synapse_client=client,
                    )
        finally:
            os.unlink(db_path)


# =============================================================================
# _index_entity_async
# =============================================================================


class TestIndexEntityAsync:
    def _common_kwargs(self, conn, cursor, client, entity_id="syn3"):
        return dict(
            conn=conn,
            cursor=cursor,
            entity=entity_id,
            parent_id="syn1",
            dest_storage_location_id="99",
            source_storage_location_ids=[],
            file_version_strategy="new",
            include_table_files=False,
            continue_on_error=False,
            synapse_client=client,
        )

    def _mock_entity_type(self, concrete_type):
        et = MagicMock()
        et.type = concrete_type
        return et

    @pytest.mark.asyncio
    async def test_routes_file_entity(self, in_memory_db):
        conn, cursor = in_memory_db
        client = _make_mock_client()

        with (
            patch(f"{MODULE}.utils.id_of", return_value="syn3"),
            patch(
                f"{MODULE}.get_entity_type",
                new=AsyncMock(
                    return_value=self._mock_entity_type(concrete_types.FILE_ENTITY)
                ),
            ),
            patch(
                f"{MODULE}._index_file_entity_async", new=AsyncMock()
            ) as mock_index_file,
        ):
            await _index_entity_async(**self._common_kwargs(conn, cursor, client))

        mock_index_file.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_file_entity_when_strategy_is_skip(self, in_memory_db):
        conn, cursor = in_memory_db
        client = _make_mock_client()
        kwargs = self._common_kwargs(conn, cursor, client)
        kwargs["file_version_strategy"] = "skip"

        with (
            patch(f"{MODULE}.utils.id_of", return_value="syn3"),
            patch(
                f"{MODULE}.get_entity_type",
                new=AsyncMock(
                    return_value=self._mock_entity_type(concrete_types.FILE_ENTITY)
                ),
            ),
            patch(
                f"{MODULE}._index_file_entity_async", new=AsyncMock()
            ) as mock_index_file,
        ):
            await _index_entity_async(**kwargs)

        mock_index_file.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_routes_table_entity_when_include_tables(self, in_memory_db):
        conn, cursor = in_memory_db
        client = _make_mock_client()
        kwargs = self._common_kwargs(conn, cursor, client)
        kwargs["include_table_files"] = True

        with (
            patch(f"{MODULE}.utils.id_of", return_value="syn5"),
            patch(
                f"{MODULE}.get_entity_type",
                new=AsyncMock(
                    return_value=self._mock_entity_type(concrete_types.TABLE_ENTITY)
                ),
            ),
            patch(
                f"{MODULE}._index_table_entity_async", new=AsyncMock()
            ) as mock_index_table,
        ):
            await _index_entity_async(**kwargs)

        mock_index_table.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_table_entity_when_include_tables_false(self, in_memory_db):
        conn, cursor = in_memory_db
        client = _make_mock_client()

        with (
            patch(f"{MODULE}.utils.id_of", return_value="syn5"),
            patch(
                f"{MODULE}.get_entity_type",
                new=AsyncMock(
                    return_value=self._mock_entity_type(concrete_types.TABLE_ENTITY)
                ),
            ),
            patch(
                f"{MODULE}._index_table_entity_async", new=AsyncMock()
            ) as mock_index_table,
        ):
            await _index_entity_async(**self._common_kwargs(conn, cursor, client))

        mock_index_table.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_routes_folder_entity(self, in_memory_db):
        conn, cursor = in_memory_db
        client = _make_mock_client()

        with (
            patch(f"{MODULE}.utils.id_of", return_value="syn2"),
            patch(
                f"{MODULE}.get_entity_type",
                new=AsyncMock(
                    return_value=self._mock_entity_type(concrete_types.FOLDER_ENTITY)
                ),
            ),
            patch(
                f"{MODULE}._index_container_async", new=AsyncMock()
            ) as mock_container,
        ):
            await _index_entity_async(**self._common_kwargs(conn, cursor, client))

        mock_container.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_routes_project_entity(self, in_memory_db):
        conn, cursor = in_memory_db
        client = _make_mock_client()

        with (
            patch(f"{MODULE}.utils.id_of", return_value="syn1"),
            patch(
                f"{MODULE}.get_entity_type",
                new=AsyncMock(
                    return_value=self._mock_entity_type(concrete_types.PROJECT_ENTITY)
                ),
            ),
            patch(
                f"{MODULE}._index_container_async", new=AsyncMock()
            ) as mock_container,
        ):
            await _index_entity_async(**self._common_kwargs(conn, cursor, client))

        mock_container.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_skips_already_indexed_entity(self, in_memory_db):
        conn, cursor = in_memory_db
        client = _make_mock_client()
        # Pre-insert the entity as indexed
        cursor.execute(
            "INSERT INTO migrations (id, type, status) VALUES (?, ?, ?)",
            ("syn3", MigrationType.FILE.value, MigrationStatus.INDEXED.value),
        )
        conn.commit()

        with (
            patch(f"{MODULE}.utils.id_of", return_value="syn3"),
            patch(
                f"{MODULE}.get_entity_type",
                new=AsyncMock(
                    return_value=self._mock_entity_type(concrete_types.FILE_ENTITY)
                ),
            ),
            patch(
                f"{MODULE}._index_file_entity_async", new=AsyncMock()
            ) as mock_index_file,
        ):
            await _index_entity_async(**self._common_kwargs(conn, cursor, client))

        mock_index_file.assert_not_awaited()

    @pytest.mark.asyncio
    async def test_error_without_continue_raises_indexing_error(self, in_memory_db):
        conn, cursor = in_memory_db
        client = _make_mock_client()

        with (
            patch(f"{MODULE}.utils.id_of", return_value="syn3"),
            patch(
                f"{MODULE}.get_entity_type",
                new=AsyncMock(
                    return_value=self._mock_entity_type(concrete_types.FILE_ENTITY)
                ),
            ),
            patch(
                f"{MODULE}._index_file_entity_async", side_effect=RuntimeError("boom")
            ),
        ):
            with pytest.raises(IndexingError):
                await _index_entity_async(**self._common_kwargs(conn, cursor, client))

    @pytest.mark.asyncio
    async def test_error_with_continue_records_error(self, in_memory_db):
        conn, cursor = in_memory_db
        client = _make_mock_client()
        kwargs = self._common_kwargs(conn, cursor, client)
        kwargs["continue_on_error"] = True

        with (
            patch(f"{MODULE}.utils.id_of", return_value="syn3"),
            patch(
                f"{MODULE}.get_entity_type",
                new=AsyncMock(
                    return_value=self._mock_entity_type(concrete_types.FILE_ENTITY)
                ),
            ),
            patch(
                f"{MODULE}._index_file_entity_async",
                side_effect=RuntimeError("transient"),
            ),
        ):
            # Should not raise
            await _index_entity_async(**kwargs)

        row = cursor.execute("SELECT status FROM migrations WHERE id='syn3'").fetchone()
        assert row[0] == MigrationStatus.ERRORED.value


# =============================================================================
# _index_file_entity_async
# =============================================================================


class TestIndexFileEntityAsync:
    def _make_cursor(self):
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        _ensure_schema(cursor)
        conn.commit()
        return conn, cursor

    @pytest.mark.asyncio
    async def test_new_strategy_inserts_with_none_version(self):
        conn, cursor = self._make_cursor()
        client = _make_mock_client()
        fh = _make_file_handle(storage_location_id="10")
        entity = _make_entity("syn3", file_handle=fh)

        with patch(f"{MODULE}.utils.id_of", return_value="syn3"):
            await _index_file_entity_async(
                cursor=cursor,
                entity=entity,
                parent_id="syn1",
                dest_storage_location_id="99",
                source_storage_location_ids=[],
                file_version_strategy="new",
                synapse_client=client,
            )

        conn.commit()
        row = cursor.execute(
            "SELECT id, version, status FROM migrations WHERE id='syn3'"
        ).fetchone()
        assert row is not None
        assert row[1] is None  # new strategy → version=None
        assert row[2] == MigrationStatus.INDEXED.value

    @pytest.mark.asyncio
    async def test_latest_strategy_inserts_with_version_number(self):
        conn, cursor = self._make_cursor()
        client = _make_mock_client()
        fh = _make_file_handle(storage_location_id="10")
        entity = _make_entity("syn3", version_number=5, file_handle=fh)

        with patch(f"{MODULE}.utils.id_of", return_value="syn3"):
            await _index_file_entity_async(
                cursor=cursor,
                entity=entity,
                parent_id="syn1",
                dest_storage_location_id="99",
                source_storage_location_ids=[],
                file_version_strategy="latest",
                synapse_client=client,
            )

        conn.commit()
        row = cursor.execute(
            "SELECT version FROM migrations WHERE id='syn3'"
        ).fetchone()
        assert row[0] == 5

    @pytest.mark.asyncio
    async def test_all_strategy_inserts_each_version(self):
        conn, cursor = self._make_cursor()
        client = _make_mock_client()
        fh = _make_file_handle(storage_location_id="10")
        entity = _make_entity("syn3", file_handle=fh)

        async def _mock_versions(entity_id, syn_client):
            for v in [1, 2, 3]:
                yield v

        with (
            patch(f"{MODULE}.utils.id_of", return_value="syn3"),
            patch(f"{MODULE}._get_version_numbers_async", _mock_versions),
            patch(f"{MODULE}.get_async", new=AsyncMock(return_value=entity)),
        ):
            await _index_file_entity_async(
                cursor=cursor,
                entity=entity,
                parent_id="syn1",
                dest_storage_location_id="99",
                source_storage_location_ids=[],
                file_version_strategy="all",
                synapse_client=client,
            )

        conn.commit()
        count = cursor.execute(
            "SELECT count(*) FROM migrations WHERE id='syn3'"
        ).fetchone()[0]
        assert count == 3

    @pytest.mark.asyncio
    async def test_already_migrated_file_skipped(self):
        conn, cursor = self._make_cursor()
        client = _make_mock_client()
        # storage_location_id matches dest → ALREADY_MIGRATED → should still insert
        fh = _make_file_handle(storage_location_id="99")
        entity = _make_entity("syn3", file_handle=fh)

        with patch(f"{MODULE}.utils.id_of", return_value="syn3"):
            await _index_file_entity_async(
                cursor=cursor,
                entity=entity,
                parent_id="syn1",
                dest_storage_location_id="99",
                source_storage_location_ids=[],
                file_version_strategy="new",
                synapse_client=client,
            )

        conn.commit()
        row = cursor.execute("SELECT status FROM migrations WHERE id='syn3'").fetchone()
        assert row[0] == MigrationStatus.ALREADY_MIGRATED.value

    @pytest.mark.asyncio
    async def test_source_filter_excludes_non_matching(self):
        conn, cursor = self._make_cursor()
        client = _make_mock_client()
        fh = _make_file_handle(storage_location_id="99")  # not in source list
        entity = _make_entity("syn3", file_handle=fh)

        with patch(f"{MODULE}.utils.id_of", return_value="syn3"):
            await _index_file_entity_async(
                cursor=cursor,
                entity=entity,
                parent_id="syn1",
                dest_storage_location_id="20",
                source_storage_location_ids=["10"],  # "99" not in list
                file_version_strategy="new",
                synapse_client=client,
            )

        conn.commit()
        count = cursor.execute(
            "SELECT count(*) FROM migrations WHERE id='syn3'"
        ).fetchone()[0]
        assert count == 0


# =============================================================================
# _get_table_file_handle_rows_async
# =============================================================================


class TestGetTableFileHandleRowsAsync:
    @pytest.mark.asyncio
    async def test_no_file_handle_columns_yields_nothing(self):
        client = _make_mock_client()
        col = MagicMock()
        col.column_type = "STRING"  # not FILEHANDLEID

        with patch(f"{MODULE}.get_columns", new=AsyncMock(return_value=[col])):
            rows = [
                r
                async for r in _get_table_file_handle_rows_async(
                    "syn5", synapse_client=client
                )
            ]

        assert rows == []

    @pytest.mark.asyncio
    async def test_file_handle_columns_yields_rows(self):
        client = _make_mock_client()
        col = MagicMock()
        col.column_type = "FILEHANDLEID"
        col.id = "col_42"

        fh = _make_file_handle()

        # Row: [row_id, row_version, file_handle_id]
        mock_results = MagicMock()
        mock_results.iterrows.return_value = iter([(0, [1, 2, "fh_abc"])])

        mock_table_instance = MagicMock()
        mock_table_instance.query_async = AsyncMock(return_value=mock_results)
        mock_table_class = MagicMock(return_value=mock_table_instance)

        with (
            patch(f"{MODULE}.get_columns", new=AsyncMock(return_value=[col])),
            patch("synapseclient.models.Table", mock_table_class),
            patch(
                f"{MODULE}.get_file_handle_for_download_async",
                new=AsyncMock(return_value={"fileHandle": fh}),
            ),
        ):
            rows = [
                r
                async for r in _get_table_file_handle_rows_async(
                    "syn5", synapse_client=client
                )
            ]

        assert len(rows) == 1
        row_id, row_version, file_handles = rows[0]
        assert row_id == 1
        assert row_version == 2
        assert "col_42" in file_handles


# =============================================================================
# _index_table_entity_async
# =============================================================================


class TestIndexTableEntityAsync:
    @pytest.mark.asyncio
    async def test_inserts_table_file_entries(self):
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        _ensure_schema(cursor)
        conn.commit()

        client = _make_mock_client()
        fh = _make_file_handle(storage_location_id="10", fh_id="fh_t1")

        async def _mock_rows(entity_id, *, synapse_client):
            yield 1, 2, {"col_7": fh}

        with patch(f"{MODULE}._get_table_file_handle_rows_async", _mock_rows):
            await _index_table_entity_async(
                cursor=cursor,
                entity_id="syn5",
                parent_id="syn1",
                dest_storage_location_id="99",
                source_storage_location_ids=[],
                synapse_client=client,
            )

        conn.commit()
        row = cursor.execute(
            "SELECT id, type, row_id, col_id FROM migrations WHERE id='syn5'"
        ).fetchone()
        assert row is not None
        assert row[1] == MigrationType.TABLE_ATTACHED_FILE.value
        assert row[2] == 1  # row_id
        assert row[3] == "col_7"  # col_id

    @pytest.mark.asyncio
    async def test_skips_non_s3_file_handles(self):
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        _ensure_schema(cursor)
        conn.commit()

        client = _make_mock_client()
        fh = _make_file_handle(
            concrete_type="org.sagebionetworks.repo.model.file.ExternalFileHandle",
            storage_location_id="10",
        )

        async def _mock_rows(entity_id, *, synapse_client):
            yield 1, 2, {"col_7": fh}

        with patch(f"{MODULE}._get_table_file_handle_rows_async", _mock_rows):
            await _index_table_entity_async(
                cursor=cursor,
                entity_id="syn5",
                parent_id="syn1",
                dest_storage_location_id="99",
                source_storage_location_ids=[],
                synapse_client=client,
            )

        conn.commit()
        count = cursor.execute("SELECT count(*) FROM migrations").fetchone()[0]
        assert count == 0


# =============================================================================
# _index_container_async
# =============================================================================


class TestIndexContainerAsync:
    @pytest.mark.asyncio
    async def test_indexes_children_and_marks_container(self):
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        _ensure_schema(cursor)
        conn.commit()

        client = _make_mock_client()
        child_entity = _make_entity("syn3")

        et = MagicMock()
        et.type = concrete_types.PROJECT_ENTITY

        async def _mock_get_children(parent, include_types, synapse_client):
            yield {"id": "syn3"}

        with (
            patch(f"{MODULE}.get_entity_type", new=AsyncMock(return_value=et)),
            patch(f"{MODULE}.get_children", _mock_get_children),
            patch(f"{MODULE}.get_async", new=AsyncMock(return_value=child_entity)),
            patch(f"{MODULE}._index_entity_async", new=AsyncMock()) as mock_index,
        ):
            await _index_container_async(
                conn=conn,
                cursor=cursor,
                entity_id="syn1",
                parent_id=None,
                dest_storage_location_id="99",
                source_storage_location_ids=[],
                file_version_strategy="new",
                include_table_files=False,
                continue_on_error=False,
                synapse_client=client,
            )

        mock_index.assert_awaited_once()
        # Container should be marked as indexed
        row = cursor.execute("SELECT id FROM migrations WHERE id='syn1'").fetchone()
        assert row is not None

    @pytest.mark.asyncio
    async def test_includes_table_type_when_flag_set(self):
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        _ensure_schema(cursor)
        conn.commit()

        client = _make_mock_client()
        et = MagicMock()
        et.type = concrete_types.FOLDER_ENTITY

        captured_types = []

        async def _mock_get_children(parent, include_types, synapse_client):
            captured_types.extend(include_types)
            return
            yield  # empty generator

        with (
            patch(f"{MODULE}.get_entity_type", new=AsyncMock(return_value=et)),
            patch(f"{MODULE}.get_children", _mock_get_children),
        ):
            await _index_container_async(
                conn=conn,
                cursor=cursor,
                entity_id="syn2",
                parent_id="syn1",
                dest_storage_location_id="99",
                source_storage_location_ids=[],
                file_version_strategy="new",
                include_table_files=True,
                continue_on_error=False,
                synapse_client=client,
            )

        assert "table" in captured_types

    @pytest.mark.asyncio
    async def test_excludes_file_types_when_strategy_is_skip(self):
        conn = sqlite3.connect(":memory:")
        cursor = conn.cursor()
        _ensure_schema(cursor)
        conn.commit()

        client = _make_mock_client()
        et = MagicMock()
        et.type = concrete_types.FOLDER_ENTITY

        captured_types = []

        async def _mock_get_children(parent, include_types, synapse_client):
            captured_types.extend(include_types)
            return
            yield

        with (
            patch(f"{MODULE}.get_entity_type", new=AsyncMock(return_value=et)),
            patch(f"{MODULE}.get_children", _mock_get_children),
        ):
            await _index_container_async(
                conn=conn,
                cursor=cursor,
                entity_id="syn2",
                parent_id="syn1",
                dest_storage_location_id="99",
                source_storage_location_ids=[],
                file_version_strategy="skip",
                include_table_files=True,
                continue_on_error=False,
                synapse_client=client,
            )

        assert "file" not in captured_types
        assert "folder" not in captured_types


# =============================================================================
# _migrate_item_async
# =============================================================================


class TestMigrateItemAsync:
    @pytest.mark.asyncio
    async def test_copies_file_handle_and_creates_new_version(self):
        client = _make_mock_client()
        key = MigrationKey("syn3", MigrationType.FILE, version=None)
        semaphore = asyncio.Semaphore(10)

        with (
            patch(
                f"{MODULE}.multipart_copy_async", new=AsyncMock(return_value="fh_new")
            ),
            patch(
                f"{MODULE}._create_new_file_version_async", new=AsyncMock()
            ) as mock_create,
        ):
            result = await _migrate_item_async(
                key=key,
                from_file_handle_id="fh_old",
                to_file_handle_id=None,
                file_size=1024,
                dest_storage_location_id="99",
                semaphore=semaphore,
                synapse_client=client,
            )

        assert result["to_file_handle_id"] == "fh_new"
        assert result["from_file_handle_id"] == "fh_old"
        mock_create.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_reuses_existing_file_handle(self):
        client = _make_mock_client()
        key = MigrationKey("syn3", MigrationType.FILE, version=None)
        semaphore = asyncio.Semaphore(10)

        with (
            patch(f"{MODULE}.multipart_copy_async", new=AsyncMock()) as mock_copy,
            patch(f"{MODULE}._create_new_file_version_async", new=AsyncMock()),
        ):
            result = await _migrate_item_async(
                key=key,
                from_file_handle_id="fh_old",
                to_file_handle_id="fh_existing",  # already copied
                file_size=1024,
                dest_storage_location_id="99",
                semaphore=semaphore,
                synapse_client=client,
            )

        mock_copy.assert_not_awaited()
        assert result["to_file_handle_id"] == "fh_existing"

    @pytest.mark.asyncio
    async def test_migrates_versioned_file(self):
        client = _make_mock_client()
        key = MigrationKey("syn3", MigrationType.FILE, version=2)
        semaphore = asyncio.Semaphore(10)

        with (
            patch(
                f"{MODULE}.multipart_copy_async", new=AsyncMock(return_value="fh_new")
            ),
            patch(
                f"{MODULE}._migrate_file_version_async", new=AsyncMock()
            ) as mock_migrate_ver,
        ):
            await _migrate_item_async(
                key=key,
                from_file_handle_id="fh_old",
                to_file_handle_id=None,
                file_size=1024,
                dest_storage_location_id="99",
                semaphore=semaphore,
                synapse_client=client,
            )

        mock_migrate_ver.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_migrates_table_attached_file(self):
        client = _make_mock_client()
        key = MigrationKey(
            "syn5", MigrationType.TABLE_ATTACHED_FILE, row_id=1, col_id=2
        )
        semaphore = asyncio.Semaphore(10)

        with (
            patch(
                f"{MODULE}.multipart_copy_async", new=AsyncMock(return_value="fh_new")
            ),
            patch(
                f"{MODULE}._migrate_table_attached_file_async", new=AsyncMock()
            ) as mock_table,
        ):
            await _migrate_item_async(
                key=key,
                from_file_handle_id="fh_old",
                to_file_handle_id=None,
                file_size=512,
                dest_storage_location_id="99",
                semaphore=semaphore,
                synapse_client=client,
            )

        mock_table.assert_awaited_once()

    @pytest.mark.asyncio
    async def test_exception_wrapped_as_migration_error(self):
        client = _make_mock_client()
        key = MigrationKey("syn3", MigrationType.FILE, version=None)
        semaphore = asyncio.Semaphore(10)

        with patch(
            f"{MODULE}.multipart_copy_async", side_effect=RuntimeError("S3 error")
        ):
            with pytest.raises(MigrationError) as exc_info:
                await _migrate_item_async(
                    key=key,
                    from_file_handle_id="fh_old",
                    to_file_handle_id=None,
                    file_size=1024,
                    dest_storage_location_id="99",
                    semaphore=semaphore,
                    synapse_client=client,
                )

        assert exc_info.value.key is key
        assert "S3 error" in str(exc_info.value)


# =============================================================================
# _create_new_file_version_async
# =============================================================================


class TestCreateNewFileVersionAsync:
    @pytest.mark.asyncio
    async def test_sets_file_handle_and_stores(self):
        client = _make_mock_client()
        entity = _make_entity("syn3")

        with (
            patch(f"{MODULE}.Synapse.get_client", return_value=client),
            patch(f"{MODULE}.get_async", new=AsyncMock(return_value=entity)),
        ):
            await _create_new_file_version_async(
                entity_id="syn3",
                to_file_handle_id="fh_new",
                synapse_client=client,
            )
        assert entity.data_file_handle_id == "fh_new"
        entity.store_async.assert_awaited_once()


# =============================================================================
# _migrate_file_version_async
# =============================================================================


class TestMigrateFileVersionAsync:
    @pytest.mark.asyncio
    async def test_calls_rest_put_with_correct_payload(self):
        client = _make_mock_client()

        with patch(f"{MODULE}.Synapse.get_client", return_value=client):
            await _migrate_file_version_async(
                entity_id="syn3",
                version=2,
                from_file_handle_id="fh_old",
                to_file_handle_id="fh_new",
                synapse_client=client,
            )

        client.rest_put_async.assert_awaited_once()
        call_args = client.rest_put_async.call_args
        assert call_args[0][0] == "/entity/syn3/version/2/filehandle"
        body = json.loads(call_args[1]["body"])
        assert body["oldFileHandleId"] == "fh_old"
        assert body["newFileHandleId"] == "fh_new"


# =============================================================================
# _migrate_table_attached_file_async
# =============================================================================


class TestMigrateTableAttachedFileAsync:
    @pytest.mark.asyncio
    async def test_sends_transaction(self):
        client = _make_mock_client()
        key = MigrationKey(
            "syn5", MigrationType.TABLE_ATTACHED_FILE, row_id=7, col_id=3
        )

        mock_transaction = MagicMock()
        mock_transaction.send_job_and_wait_async = AsyncMock()

        with patch(f"{MODULE}.TableUpdateTransaction", return_value=mock_transaction):
            await _migrate_table_attached_file_async(
                key=key,
                to_file_handle_id="fh_new",
                synapse_client=client,
            )

        mock_transaction.send_job_and_wait_async.assert_awaited_once()


# =============================================================================
# track_migration_results_async
# =============================================================================


class TestTrackMigrationResultsAsync:
    def _make_db(self, from_fh="fh_src", entity_id="syn3", version=1):
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        cursor = conn.cursor()
        _ensure_schema(cursor)
        cursor.execute(
            """INSERT INTO migrations (id, type, version, status, from_file_handle_id)
               VALUES (?, ?, ?, ?, ?)""",
            (
                entity_id,
                MigrationType.FILE.value,
                version,
                MigrationStatus.INDEXED.value,
                from_fh,
            ),
        )
        conn.commit()
        return conn, cursor

    @pytest.mark.asyncio
    async def test_successful_task_marks_migrated(self):
        conn, cursor = self._make_db()
        key = MigrationKey("syn3", MigrationType.FILE, version=1)
        from_fh = "fh_src"

        async def _successful_migrate():
            return {
                "key": key,
                "from_file_handle_id": from_fh,
                "to_file_handle_id": "fh_dst",
            }

        task = asyncio.create_task(_successful_migrate())
        await asyncio.sleep(0)  # let it complete

        pending_fh = {from_fh}
        completed_fh = set()
        pending_keys = {key}

        await track_migration_results_async(
            conn=conn,
            cursor=cursor,
            active_tasks={task},
            pending_file_handles=pending_fh,
            completed_file_handles=completed_fh,
            pending_keys=pending_keys,
            return_when=asyncio.ALL_COMPLETED,
            continue_on_error=False,
        )

        row = cursor.execute(
            "SELECT status, to_file_handle_id FROM migrations WHERE id='syn3'"
        ).fetchone()
        assert row[0] == MigrationStatus.MIGRATED.value
        assert row[1] == "fh_dst"
        assert from_fh in completed_fh
        assert key not in pending_keys

    @pytest.mark.asyncio
    async def test_failed_task_marks_errored(self):
        conn, cursor = self._make_db()
        key = MigrationKey("syn3", MigrationType.FILE, version=1)
        from_fh = "fh_src"
        inner_error = RuntimeError("network")

        async def _failing_migrate():
            err = MigrationError(key, from_fh)
            err.__cause__ = inner_error
            raise err

        task = asyncio.create_task(_failing_migrate())
        await asyncio.sleep(0)

        pending_fh = {from_fh}
        completed_fh = set()
        pending_keys = {key}

        await track_migration_results_async(
            conn=conn,
            cursor=cursor,
            active_tasks={task},
            pending_file_handles=pending_fh,
            completed_file_handles=completed_fh,
            pending_keys=pending_keys,
            return_when=asyncio.ALL_COMPLETED,
            continue_on_error=True,  # don't re-raise
        )

        row = cursor.execute("SELECT status FROM migrations WHERE id='syn3'").fetchone()
        assert row[0] == MigrationStatus.ERRORED.value
        assert from_fh in completed_fh

    @pytest.mark.asyncio
    async def test_failed_task_reraises_when_not_continue_on_error(self):
        conn, cursor = self._make_db()
        key = MigrationKey("syn3", MigrationType.FILE, version=1)
        from_fh = "fh_src"
        inner_error = RuntimeError("critical failure")

        async def _failing_migrate():
            err = MigrationError(key, from_fh)
            err.__cause__ = inner_error
            raise err

        task = asyncio.create_task(_failing_migrate())
        await asyncio.sleep(0)

        with pytest.raises(RuntimeError, match="critical failure"):
            await track_migration_results_async(
                conn=conn,
                cursor=cursor,
                active_tasks={task},
                pending_file_handles={from_fh},
                completed_file_handles=set(),
                pending_keys={key},
                return_when=asyncio.ALL_COMPLETED,
                continue_on_error=False,
            )


# =============================================================================
# migrate_indexed_files_async
# =============================================================================


class TestMigrateIndexedFilesAsync:
    @pytest.mark.asyncio
    async def test_raises_if_no_settings_in_db(self):
        fd, path = tempfile.mkstemp(suffix=".db")
        os.close(fd)
        try:
            conn = sqlite3.connect(path)
            try:
                cursor = conn.cursor()
                _ensure_schema(cursor)
                conn.commit()
            finally:
                conn.close()

            client = _make_mock_client()
            with patch(f"{MODULE}.Synapse.get_client", return_value=client):
                with pytest.raises(
                    ValueError, match="Unable to retrieve existing index settings"
                ):
                    await migrate_indexed_files_async(
                        db_path=path, synapse_client=client
                    )
        finally:
            os.unlink(path)

    @pytest.mark.asyncio
    async def test_returns_none_when_migration_not_confirmed(
        self, db_file_with_settings
    ):
        path, _ = db_file_with_settings
        # Add an indexed row so there's something to confirm
        conn = sqlite3.connect(path)
        try:
            cursor = conn.cursor()
            cursor.execute(
                "INSERT INTO migrations (id, type, status) VALUES (?, ?, ?)",
                ("syn3", MigrationType.FILE.value, MigrationStatus.INDEXED.value),
            )
            conn.commit()
        finally:
            conn.close()

        client = _make_mock_client()
        with (
            patch(f"{MODULE}.Synapse.get_client", return_value=client),
            patch("sys.stdout") as mock_stdout,
        ):
            mock_stdout.isatty.return_value = False
            result = await migrate_indexed_files_async(
                db_path=path,
                force=False,
                synapse_client=client,
            )

        assert result is None

    @pytest.mark.asyncio
    async def test_returns_migration_result_on_success(self, db_file_with_settings):
        path, _ = db_file_with_settings
        client = _make_mock_client()

        with (
            patch(f"{MODULE}.Synapse.get_client", return_value=client),
            patch(f"{MODULE}._execute_migration_async", new=AsyncMock()),
        ):
            result = await migrate_indexed_files_async(
                db_path=path,
                force=True,
                synapse_client=client,
            )

        assert isinstance(result, MigrationResult)
        assert result.db_path == path


# =============================================================================
# _execute_migration_async
# =============================================================================


class TestExecuteMigrationAsync:
    def _make_db_with_indexed_file(self, from_fh="fh_src", entity_id="syn3", version=1):
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        cursor = conn.cursor()
        _ensure_schema(cursor)
        cursor.execute(
            """INSERT INTO migrations (id, type, version, status, from_file_handle_id, file_size)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (
                entity_id,
                MigrationType.FILE.value,
                version,
                MigrationStatus.INDEXED.value,
                from_fh,
                1024,
            ),
        )
        conn.commit()
        return conn, cursor

    @pytest.mark.asyncio
    async def test_migrates_single_item(self):
        conn, cursor = self._make_db_with_indexed_file()
        client = _make_mock_client()

        key = MigrationKey("syn3", MigrationType.FILE, version=1)

        async def _mock_migrate_item(
            key,
            from_file_handle_id,
            to_file_handle_id,
            file_size,
            dest_storage_location_id,
            semaphore,
            *,
            synapse_client,
        ):
            return {
                "key": key,
                "from_file_handle_id": from_file_handle_id,
                "to_file_handle_id": "fh_dst",
            }

        with patch(f"{MODULE}._migrate_item_async", _mock_migrate_item):
            await _execute_migration_async(
                conn=conn,
                cursor=cursor,
                dest_storage_location_id="99",
                create_table_snapshots=False,
                continue_on_error=False,
                synapse_client=client,
            )

        row = cursor.execute("SELECT status FROM migrations WHERE id='syn3'").fetchone()
        assert row[0] == MigrationStatus.MIGRATED.value

    @pytest.mark.asyncio
    async def test_empty_db_completes_without_error(self):
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        cursor = conn.cursor()
        _ensure_schema(cursor)
        conn.commit()

        client = _make_mock_client()

        await _execute_migration_async(
            conn=conn,
            cursor=cursor,
            dest_storage_location_id="99",
            create_table_snapshots=False,
            continue_on_error=False,
            synapse_client=client,
        )

    @pytest.mark.asyncio
    async def test_continue_on_error_records_failure(self):
        conn, cursor = self._make_db_with_indexed_file()
        client = _make_mock_client()

        key = MigrationKey("syn3", MigrationType.FILE, version=1)

        async def _failing_migrate(
            key,
            from_file_handle_id,
            to_file_handle_id,
            file_size,
            dest_storage_location_id,
            semaphore,
            *,
            synapse_client,
        ):
            err = MigrationError(key, from_file_handle_id)
            err.__cause__ = RuntimeError("disk full")
            raise err

        with patch(f"{MODULE}._migrate_item_async", _failing_migrate):
            await _execute_migration_async(
                conn=conn,
                cursor=cursor,
                dest_storage_location_id="99",
                create_table_snapshots=False,
                continue_on_error=True,
                synapse_client=client,
            )

        row = cursor.execute("SELECT status FROM migrations WHERE id='syn3'").fetchone()
        assert row[0] == MigrationStatus.ERRORED.value
