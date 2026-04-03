"""
Asynchronous service for indexing, and migrating entities between storage locations.

This module provides native async implementations of the indexing and migration functionality
"""

import asyncio
import collections.abc
import json
import os
import sqlite3
import sys
import tempfile
import traceback
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Dict,
    List,
    Optional,
    Set,
    Tuple,
    Union,
)

from synapseclient import Synapse
from synapseclient.api import get_entity_type, rest_get_paginated_async
from synapseclient.api.entity_services import (
    get_children,
    update_entity_file_handle_version,
)
from synapseclient.api.file_services import get_file_handle_for_download_async
from synapseclient.api.storage_location_services import get_storage_location_setting
from synapseclient.api.table_services import get_columns
from synapseclient.core import utils
from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import SynapseError
from synapseclient.core.upload.multipart_upload import MAX_NUMBER_OF_PARTS
from synapseclient.core.upload.multipart_upload_async import multipart_copy_async
from synapseclient.core.utils import test_import_sqlite3
from synapseclient.entity import Entity

if TYPE_CHECKING:
    from synapseclient.models import Table

from synapseclient.models.table_components import (
    AppendableRowSetRequest,
    PartialRow,
    PartialRowSet,
    TableUpdateTransaction,
)
from synapseclient.operations import FileOptions, get_async

from .migration_types import (
    IndexingError,
    MigrationError,
    MigrationKey,
    MigrationResult,
    MigrationSettings,
    MigrationStatus,
    MigrationType,
)

# Default part size for multipart copy (100 MB)
# we use a much larger default part size for part copies than we would for part uploads.
# with part copies the data transfer is within AWS so don't need to concern ourselves
# with upload failures of the actual bytes.
# this value aligns with what some AWS client libraries use e.g.
# https://github.com/aws/aws-sdk-java/blob/57ed2e4bd57e08f316bf5c6c71f6fd82a27fa240/aws-java-sdk-s3/src/main/java/com/amazonaws/services/s3/transfer/TransferManagerConfiguration.java#L46
DEFAULT_PART_SIZE = 100 * utils.MB

# Batch size for database operations so the batch operations are chunked.
BATCH_SIZE = 500

# Maximum concurrent file copy.
MAX_CONCURRENT_FILE_COPIES = max(int(Synapse().max_threads / 2), 1)


# =============================================================================
# Indexing Helper Functions
# =============================================================================
async def _verify_storage_location_ownership_async(
    storage_location_id: int,
    *,
    synapse_client: Optional[Synapse] = None,
) -> None:
    """Verify the user owns the destination storage location.
    Only the creator of the storage location can can retrieve it by its id.

    Arguments:
        storage_location_id: The storage location ID to verify.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.

    Raises:
        ValueError: If the user does not own the storage location.
    """
    try:
        await get_storage_location_setting(
            storage_location_id=storage_location_id,
            synapse_client=synapse_client,
        )
    except SynapseError:
        raise ValueError(
            f"Unable to verify ownership of storage location {storage_location_id}. "
            f"You must be the creator of the destination storage location."
        )


def _get_default_db_path(entity_id: str) -> str:
    """Generate a default temp database path for migration tracking.

    Arguments:
        entity_id: The Synapse entity ID being migrated.

    Returns:
        Path to a SQLite database file in a temp directory.
    """
    temp_dir = tempfile.mkdtemp(prefix="synapse_migration_")
    return os.path.join(temp_dir, f"migration_{entity_id}.db")


async def _get_version_numbers_async(
    entity_id: str,
    synapse_client: Optional[Synapse] = None,
) -> AsyncGenerator[int, None]:
    """Get all version numbers for an entity.

    Arguments:
        entity_id: The entity ID.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.

    Yields:
        Version numbers.
    """
    async for version_info in rest_get_paginated_async(
        f"/entity/{entity_id}/version", synapse_client=synapse_client
    ):
        yield version_info["versionNumber"]


def _escape_column_name(column: Union[str, collections.abc.Mapping]) -> str:
    """Escape a column name for use in a Synapse table query statement.
    Arguments:
        column: A string column name or a dictionary with a 'name' key.
    Returns:
        Escaped column name wrapped in double quotes.
    """
    from synapseclient.models import Column

    col_name = (
        column["name"]
        if isinstance(column, collections.abc.Mapping)
        else column.name
        if isinstance(column, Column)
        else str(column)
    )
    escaped_name = col_name.replace('"', '""')
    return f'"{escaped_name}"'


def _join_column_names(columns: List[Any]) -> str:
    """Join column names into a comma-delimited list for table queries.
    Arguments:
        columns: A list of column names or column objects with 'name' keys.
    Returns:
        Comma-separated string of escaped column names.
    """
    return ",".join(_escape_column_name(c) for c in columns)


def _check_indexed(
    cursor: sqlite3.Cursor,
    entity_id: str,
    synapse_client: Optional[Synapse] = None,
) -> bool:
    """Check if an entity has already been indexed.
    If so, it can skip reindexing it.

    Arguments:
        cursor: The cursor object from the connection to the SQLite database.
        entity_id: The entity ID to check.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.

    Returns:
        True if the entity is already indexed.
    """
    indexed_row = cursor.execute(
        "select 1 from migrations where id = ?", (entity_id,)
    ).fetchone()

    if indexed_row:
        synapse_client.logger.debug(f"{entity_id} already indexed, skipping")
        return True

    synapse_client.logger.debug(f"{entity_id} not yet indexed, indexing now")
    return False


# =============================================================================
# Database Helper Functions
# =============================================================================
def _ensure_schema(cursor: sqlite3.Cursor) -> None:
    """Ensure the SQLite database has the required schema.

    Arguments:
        cursor: The cursor object from the connection to the SQLite database.
    """
    # migration_settings table
    # A table to store parameters used to create the index.
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS migration_settings (settings TEXT NOT NULL)"
    )

    # Migrations table
    # The representation of migratable file handles is flat including both file entities
    # and table attached files, so not all columns are applicable to both. row id and col id
    # are only used by table attached files.
    cursor.execute(
        """
        CREATE TABLE IF NOT EXISTS migrations (
            id TEXT NOT NULL,
            type INTEGER NOT NULL,
            version INTEGER NULL,
            row_id INTEGER NULL,
            col_id INTEGER NULL,
            parent_id NULL,
            status INTEGER NOT NULL,
            exception TEXT NULL,
            from_storage_location_id NULL,
            from_file_handle_id TEXT NULL,
            to_file_handle_id TEXT NULL,
            file_size INTEGER NULL,
            PRIMARY KEY (id, type, row_id, col_id, version)
        )
        """
    )

    # Index the status column for faster status-based lookups
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_status ON migrations(status)")
    # Index the from_file_handle_id and to_file_handle_id columns for faster file handle-based lookups
    # This is used to see if there is already a migrated copy of a file handle before doing a copy
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS ix_file_handle_ids "
        "ON migrations(from_file_handle_id, to_file_handle_id)"
    )


def _prepare_migration_db(
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    db_path: str,
    root_id: str,
    dest_storage_location_id: str,
    source_storage_location_ids: List[str],
    file_version_strategy: str,
    include_table_files: bool,
) -> None:
    """Prepare the migration database by checking the migration settings for the given parameters.
    This is a guardrail: it binds a given SQLite index settings to the specific entity and migration options it was created with, enabling safe resumption and preventing mismatched reuse.

    Arguments:
        conn: The connection to the SQLite database.
        cursor: The cursor to the SQLite database.
        db_path: Path to the SQLite database file.
        root_id: The root entity ID being migrated.
        dest_storage_location_id: Destination storage location ID.
        source_storage_location_ids: List of source storage location IDs to filter.
        file_version_strategy: Strategy for handling file versions.
        include_table_files: Whether to include table-attached files.
    """
    current_settings = MigrationSettings(
        root_id=root_id,
        dest_storage_location_id=dest_storage_location_id,
        source_storage_location_ids=source_storage_location_ids,
        file_version_strategy=file_version_strategy,
        include_table_files=include_table_files,
    )
    existing_settings = _retrieve_index_settings(cursor)

    if existing_settings:
        current_settings.verify_migration_settings(existing_settings, db_path)
    else:
        cursor.execute(
            "INSERT INTO migration_settings (settings) VALUES (?)",
            (json.dumps(current_settings.to_dict()),),
        )

    conn.commit()


def _retrieve_index_settings(cursor: sqlite3.Cursor) -> Optional[MigrationSettings]:
    """Retrieve index settings from the database as a MigrationSettings instance.

    Arguments:
        cursor: The cursor object from the connection to the SQLite database.

    Returns:
        MigrationSettings if a row exists, None otherwise.
    """
    row = cursor.execute("SELECT settings FROM migration_settings").fetchone()
    if row:
        return MigrationSettings.from_dict(json.loads(row[0]))
    return None


def _insert_file_migration(
    cursor: sqlite3.Cursor,
    insert_values: List[
        Tuple[str, str, Optional[int], Optional[str], int, str, int, MigrationStatus]
    ],
) -> None:
    """Insert a file migration entry to the migrations database.

    Arguments:
        cursor: The cursor object from the connection to the SQLite database.
        insert_values: List of tuples containing the file migration data.
    """
    cursor.executemany(
        """
            insert into migrations (
                id,
                type,
                version,
                parent_id,
                from_storage_location_id,
                from_file_handle_id,
                file_size,
                status
            ) values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        insert_values,
    )


def _insert_table_file_migration(
    cursor: sqlite3.Cursor,
    insert_values: List[
        Tuple[str, str, Optional[int], Optional[str], int, str, int, MigrationStatus]
    ],
) -> None:
    """Insert a table-attached file migration entry.

    Arguments:
        cursor: The cursor object from the connection to the SQLite database.
        insert_values: List of tuples containing the table-attached file migration data.
    """
    cursor.executemany(
        """
            INSERT OR IGNORE INTO migrations (
                id, type, row_id, col_id, version, parent_id,
                from_storage_location_id, from_file_handle_id,
                file_size, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
        insert_values,
    )


def _mark_container_indexed(
    cursor: sqlite3.Cursor,
    entity_id: str,
    migration_type: MigrationType,
    parent_id: Optional[str],
) -> None:
    """Mark a container (Project or Folder) as indexed.

    Arguments:
        cursor: The cursor object from the connection to the SQLite database.
        entity_id: The Synapse ID of the container entity.
        migration_type: The MigrationType of the container.
        parent_id: The Synapse ID of the parent entity.
    """
    cursor.execute(
        "INSERT OR IGNORE INTO migrations (id, type, parent_id, status) VALUES (?, ?, ?, ?)",
        [entity_id, migration_type, parent_id, MigrationStatus.INDEXED.value],
    )


def _record_indexing_error(
    cursor: sqlite3.Cursor,
    entity_id: str,
    migration_type: MigrationType,
    parent_id: Optional[str],
    tb_str: str,
) -> None:
    """Record an indexing error in the database.

    Arguments:
        cursor: The cursor object from the connection to the SQLite database.
        entity_id: The Synapse ID of the entity that failed.
        migration_type: The MigrationType of the entity.
        parent_id: The Synapse ID of the parent entity.
        tb_str: The traceback string.
    """
    cursor.execute(
        """
            insert into migrations (
                id,
                type,
                parent_id,
                status,
                exception
            ) values (?, ?, ?, ?, ?)
        """,
        (
            entity_id,
            migration_type,
            parent_id,
            MigrationStatus.ERRORED.value,
            tb_str,
        ),
    )


# =============================================================================
# Migration Helper Functions
# =============================================================================
def _check_file_handle_exists(
    cursor: sqlite3.Cursor, from_file_handle_id: str
) -> Optional[str]:
    """Check if a file handle has already been copied.

    Arguments:
        cursor: The cursor object from the connection to the SQLite database.
        from_file_handle_id: The source file handle ID.

    Returns:
        The destination file handle ID if found, None otherwise.
    """
    row = cursor.execute(
        "SELECT to_file_handle_id FROM migrations WHERE from_file_handle_id = ? AND to_file_handle_id IS NOT NULL",
        (from_file_handle_id,),
    ).fetchone()
    return row[0] if row else None


def _query_migration_batch(
    cursor: sqlite3.Cursor,
    last_key: MigrationKey,
    pending_file_handle_ids: Set[str],
    completed_file_handle_ids: Set[str],
    limit: int,
) -> List[Dict[str, Any]]:
    """Query the next batch of items to migrate.

    This matches the original synapseutils query logic:
    - Forward progress through entities ordered by id, type, row_id, col_id, version
    - Backtracking to pick up files with completed file handles that were skipped

    Arguments:
        cursor: The cursor object from the connection to the SQLite database.
        last_key: The last processed MigrationKey.
        pending_file_handle_ids: Set of file handle IDs currently being processed.
        completed_file_handles: Set of file handles already completed.
        limit: Maximum number of items to return.

    Returns:
        List of migration entries as dictionaries.
    """
    query_kwargs = {
        "indexed_status": MigrationStatus.INDEXED.value,
        "id": last_key.id,
        "file_type": MigrationType.FILE.value,
        "table_type": MigrationType.TABLE_ATTACHED_FILE.value,
        "version": last_key.version,
        "row_id": last_key.row_id,
        "col_id": last_key.col_id,
        "limit": limit,
    }

    # Build the IN clauses for file handles
    pending = "('" + "','".join(pending_file_handle_ids) + "')"
    completed = "('" + "','".join(completed_file_handle_ids) + "')"

    # Query the next batch of items to migrate.
    # 1. Forward progress: entities after the current position
    # 2. Backtracking: entities before current position that share completed file handles
    results = cursor.execute(
        f"""
            SELECT
                id,
                type,
                version,
                row_id,
                col_id,
                from_file_handle_id,
                file_size
            FROM migrations
            WHERE
                status = :indexed_status
                AND (
                    (
                        ((id > :id AND type IN (:file_type, :table_type))
                        OR (id = :id AND type = :file_type AND version IS NOT NULL AND version > :version)
                        OR (id = :id AND type = :table_type AND (row_id > :row_id OR (row_id = :row_id AND col_id > :col_id))))
                        AND from_file_handle_id NOT IN {pending}
                    ) OR
                    (
                        id <= :id
                        AND from_file_handle_id IN {completed}
                    )
                )
            ORDER BY
                id,
                type,
                row_id,
                col_id,
                version
            LIMIT :limit
            """,  # noqa
        query_kwargs,
    )

    batch = []
    for row in results:
        batch.append(
            {
                "id": row[0],
                "type": row[1],
                "version": row[2],
                "row_id": row[3],
                "col_id": row[4],
                "from_file_handle_id": row[5],
                "file_size": row[6],
            }
        )
    return batch


def _update_migration_database(
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    key: MigrationKey,
    to_file_handle_id: str,
    status: MigrationStatus,
    exception: Optional[Exception] = None,
) -> None:
    """Update a migration database record as successful or errored.

    Arguments:
        conn: The connection to the SQLite database.
        cursor: The cursor object from the connection to the SQLite database.
        key: The migration key.
        to_file_handle_id: The destination file handle ID.
        status: The migration status.
        exception: The exception that occurred.
    """
    tb_str = (
        "".join(
            traceback.format_exception(
                type(exception), exception, exception.__traceback__
            )
        )
        if exception
        else None
    )

    update_sql = """
        UPDATE migrations SET
            status = ?,
            to_file_handle_id = ?,
            exception = ?
        WHERE
            id = ?
            AND type = ?
    """
    update_args = [status, to_file_handle_id, tb_str, key.id, key.type.value]
    for arg in ("version", "row_id", "col_id"):
        arg_value = getattr(key, arg)
        if arg_value is not None:
            update_sql += "and {} = ?\n".format(arg)
            update_args.append(arg_value)
        else:
            update_sql += "and {} is null\n".format(arg)

    cursor.execute(update_sql, tuple(update_args))
    conn.commit()


def _confirm_migration(
    cursor: sqlite3.Cursor,
    dest_storage_location_id: str,
    force: bool = False,
    *,
    synapse_client: Optional[Synapse] = None,
) -> bool:
    """Confirm migration with user if in interactive mode.

    Arguments:
        cursor: The cursor object from the connection to the SQLite database.
        dest_storage_location_id: Destination storage location ID.
        force: If running in an interactive shell, migration requires an interactice confirmation.
            This can be bypassed by using the force=True option. Defaults to False.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.

    Returns:
        True if migration should proceed, False otherwise.
    """

    if force:
        return True

    count = cursor.execute(
        "SELECT count(*) FROM migrations WHERE status = ?",
        (MigrationStatus.INDEXED.value,),
    ).fetchone()[0]

    if count == 0:
        synapse_client.logger.info("No items for migration.")
        return False

    if sys.stdout.isatty():
        user_input = input(
            f"{count} items for migration to {dest_storage_location_id}. Proceed? (y/n)? "
        )
        return user_input.strip().lower() == "y"
    else:
        synapse_client.logger.info(
            f"{count} items for migration. "
            "force option not used, and console input not available to confirm migration, aborting. "
            "Use the force option or run from an interactive shell to proceed with migration."
        )
        return False


def _get_part_size(file_size: int) -> int:
    """Calculate the part size for multipart copy.

    Arguments:
        file_size: The file size in bytes.

    Returns:
        The part size in bytes.
    """
    import math

    # Ensure we don't exceed max parts
    min_part_size = math.ceil(file_size / MAX_NUMBER_OF_PARTS)
    return max(DEFAULT_PART_SIZE, min_part_size)


def _get_file_migration_status(
    file_handle: Dict[str, Any],
    source_storage_location_ids: List[str],
    dest_storage_location_id: str,
) -> Optional[MigrationStatus]:
    """
    Determine whether a file should be included in the migrations database
    and return its migration status.

    Only S3 file handles are considered for migration. Other handle types
    (e.g., external URLs) are ignored.

    A file is included according to the following rules:
    - If the file is already stored in the destination location, it is included
      and marked as ALREADY_MIGRATED.
    - If `source_storage_location_ids` is provided, the file's current storage
      location must be in that list to be included.
    - If `source_storage_location_ids` is empty, all files not already at the
      destination are included.

    Args:
        file_handle: File handle metadata.
        source_storage_location_ids: Storage location IDs that qualify as
            migration sources. If empty, all source locations are considered.
        dest_storage_location_id: Destination storage location ID.

    Returns:
        MigrationStatus enum (ALREADY_MIGRATED, INDEXED) if the file should be included in the migrations database, or
        None if the file should not be included in the migrations database.
    """
    # Only S3 file handles can be migrated
    if file_handle.concrete_type != concrete_types.S3_FILE_HANDLE:
        return None

    current_storage_location_id = str(file_handle.storage_location_id)

    if current_storage_location_id == dest_storage_location_id:
        return MigrationStatus.ALREADY_MIGRATED.value

    if source_storage_location_ids:
        if current_storage_location_id not in source_storage_location_ids:
            return None

    return MigrationStatus.INDEXED.value


# =============================================================================
# Indexing Functions
# =============================================================================
async def index_files_for_migration_async(
    entity: Entity,
    dest_storage_location_id: str,
    db_path: Optional[str] = None,
    *,
    source_storage_location_ids: Optional[List[str]] = [],
    file_version_strategy: str = "new",
    include_table_files: bool = False,
    continue_on_error: bool = False,
    synapse_client: Optional[Synapse] = None,
) -> MigrationResult:
    """Index files for migration to a new storage location.

    This is the first step in migrating files to a new storage location. This function itself does not modify the given entity but only update the migrations and migration_settings tables in the SQLite database.
    After indexing, use `migrate_indexed_files_async` to perform the actual migration.

    Arguments:
        entity: The Synapse entity to migrate (Project, Folder, File, or Table). If it is a container (a Project or Folder), its contents will be recursively indexed.
        dest_storage_location_id: The destination storage location ID.
        db_path: A path on disk where the SQLite index database will be created. Must be on a volume with enough space for metadata of all indexed contents. If not provided, a temporary directory will be created and the path will be returned in the MigrationResult object.
        source_storage_location_ids: Optional list of source storage location IDs that will be migrated. If provided, files outside of one of the listed storage locations will not be indexed for migration. If not provided, then all files not already in the destination storage location will be indexed for migrated.
        file_version_strategy: Strategy to migrate file versions: "new", "all", "latest", "skip".
            - `new`: will create a new version of file entities in the new storage location, leaving existing versions unchanged
            - `all`: all existing versions will be migrated in place to the new storage location
            - `latest`: the latest version will be migrated in place to the new storage location
            - `skip`: skip migrating file entities. use this e.g. if wanting to e.g. migrate table attached files in a container while leaving the files unchanged

        include_table_files: Whether to include files attached to tables. If False (default) then e.g. only
            file entities in the container will be migrated and tables will be untouched.
        continue_on_error: Whether any errors encountered while indexing an entity will be raised
                            or instead just recorded in the index while allowing the index creation
                            to continue. Defaults to False.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.

    Returns:
        A MigrationResult object that can be used to inspect the contents of the index or output the index to a CSV for manual inspection.

    Raises:
        ValueError: If the file_version_strategy is invalid or if skipping both file entities and table attached files.
    """
    test_import_sqlite3()
    client = Synapse.get_client(synapse_client=synapse_client)

    # Validate parameters
    valid_file_version_strategy = {"new", "all", "latest", "skip"}
    if file_version_strategy not in valid_file_version_strategy:
        raise ValueError(
            f"Invalid file_version_strategy: {file_version_strategy}, "
            f"must be one of {valid_file_version_strategy}"
        )

    if file_version_strategy == "skip" and not include_table_files:
        raise ValueError(
            "Skipping both file entities and table attached files, nothing to migrate"
        )

    # Verify ownership
    await _verify_storage_location_ownership_async(
        storage_location_id=dest_storage_location_id,
        synapse_client=client,
    )

    entity_id = entity.id

    # Create database path if not provided
    if db_path is None:
        db_path = _get_default_db_path(entity_id)

    # Initialize database
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        _ensure_schema(cursor)
        _prepare_migration_db(
            conn=conn,
            cursor=cursor,
            db_path=db_path,
            root_id=entity_id,
            dest_storage_location_id=dest_storage_location_id,
            source_storage_location_ids=source_storage_location_ids,
            file_version_strategy=file_version_strategy,
            include_table_files=include_table_files,
        )
    try:
        await _index_entity_async(
            conn=conn,
            cursor=cursor,
            entity=entity,
            parent_id=None,
            dest_storage_location_id=dest_storage_location_id,
            source_storage_location_ids=source_storage_location_ids,
            file_version_strategy=file_version_strategy,
            include_table_files=include_table_files,
            continue_on_error=continue_on_error,
            synapse_client=client,
        )
    except IndexingError as ex:
        client.logger.exception(
            f"Aborted due to failure to index entity {ex.entity_id} of type {ex.concrete_type}. "
            "Use continue_on_error=True to skip individual failures."
        )
        raise ex.__cause__

    return MigrationResult(db_path=db_path, synapse_client=client)


# =============================================================================
# Indexing Implementation
# =============================================================================
async def _index_entity_async(
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    entity: Entity,
    parent_id: Optional[str],
    dest_storage_location_id: str,
    source_storage_location_ids: List[str],
    file_version_strategy: str,
    include_table_files: bool,
    continue_on_error: bool,
    *,
    synapse_client: Optional[Synapse] = None,
) -> None:
    """Recursively index an entity and its children into migrations database.

    Arguments:
        conn: The connection to the SQLite database.
        cursor: The cursor object from the connection to the SQLite database.
        entity: The Synapse entity object.
        parent_id: The parent entity Synapse ID.
        dest_storage_location_id: Destination storage location ID.
        source_storage_location_ids: List of source storage locations.
        file_version_strategy: Strategy for file versions.
        include_table_files: Whether to include table-attached files.
        continue_on_error: Whether to continue on errors.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.
    """
    entity_id = utils.id_of(entity)
    retrieved_entity = await get_entity_type(
        entity_id=entity_id, synapse_client=synapse_client
    )
    concrete_type = retrieved_entity.type

    # Check if already indexed
    is_indexed = _check_indexed(cursor, entity_id, synapse_client)
    try:
        if not is_indexed:
            if concrete_type == concrete_types.FILE_ENTITY:
                if file_version_strategy != "skip":
                    await _index_file_entity_async(
                        cursor=cursor,
                        entity=entity,
                        parent_id=parent_id,
                        dest_storage_location_id=dest_storage_location_id,
                        source_storage_location_ids=source_storage_location_ids,
                        file_version_strategy=file_version_strategy,
                        synapse_client=synapse_client,
                    )

            elif concrete_type == concrete_types.TABLE_ENTITY:
                if include_table_files:
                    await _index_table_entity_async(
                        cursor=cursor,
                        entity_id=entity_id,
                        parent_id=parent_id,
                        dest_storage_location_id=dest_storage_location_id,
                        source_storage_location_ids=source_storage_location_ids,
                        synapse_client=synapse_client,
                    )

            elif concrete_type in (
                concrete_types.FOLDER_ENTITY,
                concrete_types.PROJECT_ENTITY,
            ):
                await _index_container_async(
                    conn=conn,
                    cursor=cursor,
                    entity_id=entity_id,
                    parent_id=parent_id,
                    dest_storage_location_id=dest_storage_location_id,
                    source_storage_location_ids=source_storage_location_ids,
                    file_version_strategy=file_version_strategy,
                    include_table_files=include_table_files,
                    continue_on_error=continue_on_error,
                    synapse_client=synapse_client,
                )
        conn.commit()

    except IndexingError:
        # this is a recursive function, we don't need to log the error at every level so just
        # pass up exceptions of this type that wrap the underlying exception and indicate
        # that they were already logged
        raise
    except Exception as ex:
        if continue_on_error:
            synapse_client.logger.warning(f"Error indexing entity {entity_id}: {ex}")
            tb_str = "".join(traceback.format_exception(type(ex), ex, ex.__traceback__))
            migration_type = MigrationType.from_concrete_type(concrete_type).value
            _record_indexing_error(cursor, entity_id, migration_type, parent_id, tb_str)
        else:
            raise IndexingError(entity_id, concrete_type) from ex


async def _index_file_entity_async(
    cursor: sqlite3.Cursor,
    entity: Entity,
    parent_id: Optional[str],
    dest_storage_location_id: str,
    source_storage_location_ids: List[str],
    file_version_strategy: str,
    *,
    synapse_client: Optional[Synapse] = None,
) -> None:
    """Index a file entity for migration.

    Arguments:
        cursor: The cursor object from the connection to the SQLite database.
        entity: The Synapse entity object, a File.
        parent_id: The parent entity Synapse ID.
        dest_storage_location_id: Destination storage location ID.
        source_storage_location_ids: List of source storage locations.
        file_version_strategy: Strategy for file versions.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.
    """
    entity_id = utils.id_of(entity)
    synapse_client.logger.info(f"Indexing file entity {entity_id}")

    entity_versions: List[Tuple[Any, Optional[int]]] = []

    if file_version_strategy == "new":
        entity_versions.append((entity, None))

    elif file_version_strategy == "all":
        async for version in _get_version_numbers_async(entity_id, synapse_client):
            entity = await get_async(
                synapse_id=entity_id,
                file_options=FileOptions(download_file=False),
                synapse_client=synapse_client,
            )
            entity_versions.append((entity, version))

    elif file_version_strategy == "latest":
        entity_versions.append((entity, entity.version_number))

    insert_values = []
    for entity, version in entity_versions:
        status = _get_file_migration_status(
            entity.file_handle, source_storage_location_ids, dest_storage_location_id
        )
        if status:
            insert_values.append(
                (
                    entity_id,
                    MigrationType.FILE.value,
                    version,
                    parent_id,
                    entity.file_handle.storage_location_id,
                    entity.data_file_handle_id,
                    entity.file_handle.content_size,
                    status,
                )
            )
    if insert_values:
        _insert_file_migration(cursor, insert_values)


async def _get_table_file_handle_rows_async(
    entity_id: str,
    *,
    synapse_client: Optional[Synapse] = None,
) -> List[Tuple[int, int, Dict[str, Any]]]:
    """Get the table file handle rows for a given entity.

    Arguments:
        entity_id: The table entity ID.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.

    Returns:
        A list of tuples containing the row ID, row version, and file handles.
    """
    from synapseclient.models import Table
    from synapseclient.models.file import FileHandle

    columns = await get_columns(table_id=entity_id, synapse_client=synapse_client)
    file_handle_columns = [c for c in columns if c.column_type == "FILEHANDLEID"]

    if file_handle_columns:
        file_column_select = _join_column_names(file_handle_columns)
        results = await Table(id=entity_id).query_async(
            query=f"select {file_column_select} from {entity_id}",
            include_row_id_and_row_version=True,
            synapse_client=synapse_client,
        )
        for _, row in results.iterrows():
            file_handles = {}
            # first two cols are row id and row version, rest are file handle ids from our query
            row_id, row_version = row[:2]

            file_handle_ids = row[2:]
            for i, file_handle_id in enumerate(file_handle_ids):
                if file_handle_id:
                    col_id = file_handle_columns[i].id

                    response = await get_file_handle_for_download_async(
                        file_handle_id=file_handle_id,
                        synapse_id=entity_id,
                        entity_type="TableEntity",
                        synapse_client=synapse_client,
                    )
                    file_handle = FileHandle().fill_from_dict(response["fileHandle"])
                    file_handles[col_id] = file_handle
            yield row_id, row_version, file_handles


async def _index_table_entity_async(
    cursor: sqlite3.Cursor,
    entity_id: str,
    parent_id: Optional[str],
    dest_storage_location_id: str,
    source_storage_location_ids: List[str],
    *,
    synapse_client: Optional[Synapse] = None,
) -> None:
    """Index a table entity's file attachments for migration.

    Arguments:
        cursor: The cursor object from the connection to the SQLite database.
        entity_id: The Synapse ID of the table entity.
        parent_id: The parent entity Synapse ID.
        dest_storage_location_id: Destination storage location ID.
        source_storage_location_ids: List of source storage locations to filter.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.
    """
    synapse_client.logger.info(f"Indexing table entity {entity_id}")
    insert_values = []
    async for row_id, row_version, file_handles in _get_table_file_handle_rows_async(
        entity_id=entity_id, synapse_client=synapse_client
    ):
        for col_id, file_handle in file_handles.items():
            status = _get_file_migration_status(
                file_handle, source_storage_location_ids, dest_storage_location_id
            )
            if status:
                insert_values.append(
                    (
                        entity_id,
                        MigrationType.TABLE_ATTACHED_FILE.value,
                        row_id,
                        col_id,
                        row_version,
                        parent_id,
                        file_handle.storage_location_id,
                        file_handle.id,
                        file_handle.content_size,
                        status,
                    )
                )
                if len(insert_values) % BATCH_SIZE == 0:
                    _insert_table_file_migration(cursor, insert_values)
                    insert_values.clear()
    if insert_values:
        _insert_table_file_migration(cursor, insert_values)


async def _index_container_async(
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    entity_id: str,
    parent_id: Optional[str],
    dest_storage_location_id: str,
    source_storage_location_ids: List[str],
    file_version_strategy: str,
    include_table_files: bool,
    continue_on_error: bool,
    *,
    synapse_client: Optional[Synapse] = None,
) -> None:
    """Index a container (Project or Folder) and its children.

    Arguments:
        conn: The connection to the SQLite database.
        cursor: The cursor object from the connection to the SQLite database.
        entity_id: The Synapse ID of the entity, a Project or Folder.
        parent_id: The Synapse ID of the parent entity.
        dest_storage_location_id: Destination storage location ID.
        source_storage_location_ids: List of source storage locations to filter.
        file_version_strategy: Strategy for file versions.
        include_table_files: Whether to include table-attached files.
        continue_on_error: Whether to continue on errors.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.
    """
    retrieved_entity = await get_entity_type(
        entity_id=entity_id, synapse_client=synapse_client
    )
    concrete_type = retrieved_entity.type
    synapse_client.logger.info(
        f'Indexing {concrete_type[concrete_type.rindex(".") + 1 :]} {entity_id}'
    )

    # Determine included types
    include_types = []
    if file_version_strategy != "skip":
        include_types.extend(["folder", "file"])
    if include_table_files:
        include_types.append("table")

    # Get children using the async API
    children = []
    async for child in get_children(
        parent=entity_id,
        include_types=include_types,
        synapse_client=synapse_client,
    ):
        children.append(child)

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_FILE_COPIES)

    async def index_child(child: Dict[str, Any]) -> None:
        async with semaphore:
            child_entity = await get_async(
                synapse_id=child["id"], synapse_client=synapse_client
            )

            await _index_entity_async(
                conn=conn,
                cursor=cursor,
                entity=child_entity,
                parent_id=entity_id,
                dest_storage_location_id=dest_storage_location_id,
                source_storage_location_ids=source_storage_location_ids,
                file_version_strategy=file_version_strategy,
                include_table_files=include_table_files,
                continue_on_error=continue_on_error,
                synapse_client=synapse_client,
            )

    # Process children with as_completed for progress tracking
    tasks = [asyncio.create_task(index_child(child)) for child in children]
    for task in asyncio.as_completed(tasks):
        await task

    # Mark container as indexed
    migration_type = (
        MigrationType.PROJECT.value
        if concrete_type == concrete_types.PROJECT_ENTITY
        else MigrationType.FOLDER.value
    )
    _mark_container_indexed(cursor, entity_id, migration_type, parent_id)


# =============================================================================
# Migration Functions
# =============================================================================
async def _migrate_item_async(
    key: MigrationKey,
    from_file_handle_id: str,
    to_file_handle_id: Optional[str],
    file_size: int,
    dest_storage_location_id: str,
    semaphore: asyncio.Semaphore,
    *,
    synapse_client: Optional[Synapse] = None,
) -> Dict[str, Any]:
    """Migrate a single item.

    Arguments:
        key: The migration key.
        from_file_handle_id: The source file handle ID.
        to_file_handle_id: The destination file handle ID (if already copied).
        file_size: File size in bytes.
        dest_storage_location_id: The destination storage location ID.
        semaphore: The concurrency semaphore.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.

    Returns:
        Dictionary with the key, from_file_handle_id, and to_file_handle_id.
    """
    async with semaphore:
        try:
            # copy to a new file handle if we haven't already
            if not to_file_handle_id:
                source_association = {
                    "fileHandleId": from_file_handle_id,
                    "associateObjectId": key.id,
                    "associateObjectType": (
                        "FileEntity"
                        if key.type == MigrationType.FILE
                        else "TableEntity"
                    ),
                }

                to_file_handle_id = await multipart_copy_async(
                    synapse_client,
                    source_association,
                    storage_location_id=dest_storage_location_id,
                    part_size=_get_part_size(file_size),
                )
            # Update entity with new file handle
            if key.type == MigrationType.FILE:
                if key.version is None:
                    await _create_new_file_version_async(
                        entity_id=key.id,
                        to_file_handle_id=to_file_handle_id,
                        synapse_client=synapse_client,
                    )
                else:
                    await _migrate_file_version_async(
                        entity_id=key.id,
                        version=key.version,
                        from_file_handle_id=from_file_handle_id,
                        to_file_handle_id=to_file_handle_id,
                        synapse_client=synapse_client,
                    )
            elif key.type == MigrationType.TABLE_ATTACHED_FILE:
                await _migrate_table_attached_file_async(
                    key=key,
                    to_file_handle_id=to_file_handle_id,
                    synapse_client=synapse_client,
                )

            return {
                "key": key,
                "from_file_handle_id": from_file_handle_id,
                "to_file_handle_id": to_file_handle_id,
            }

        except Exception as ex:
            raise MigrationError(
                key, from_file_handle_id, to_file_handle_id, cause=ex
            ) from ex


async def _create_new_file_version_async(
    entity_id: str,
    to_file_handle_id: str,
    *,
    synapse_client: Optional[Synapse] = None,
) -> None:
    """Create a new version of a file entity with the new file handle.

    Arguments:
        entity_id: The file entity ID.
        to_file_handle_id: The new file handle ID.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.
    """
    synapse_client.logger.info(f"Creating new version for file entity {entity_id}")

    entity = await get_async(
        synapse_id=entity_id,
        file_options=FileOptions(download_file=False),
        synapse_client=synapse_client,
    )
    entity.data_file_handle_id = to_file_handle_id
    await entity.store_async(synapse_client=synapse_client)


async def _migrate_file_version_async(
    entity_id: str,
    version: int,
    from_file_handle_id: str,
    to_file_handle_id: str,
    *,
    synapse_client: Optional[Synapse] = None,
) -> None:
    """Migrate/update an existing file version with a new file handle.

    Arguments:
        entity_id: The Synapse ID of the entity.
        version: The version number of the entity.
        from_file_handle_id: The original file handle ID.
        to_file_handle_id: The new file handle ID.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.
    """
    await update_entity_file_handle_version(
        entity_id=entity_id,
        version=version,
        old_file_handle_id=from_file_handle_id,
        new_file_handle_id=to_file_handle_id,
        synapse_client=synapse_client,
    )


async def _migrate_table_attached_file_async(
    key: MigrationKey,
    to_file_handle_id: str,
    *,
    synapse_client: Optional[Synapse] = None,
) -> None:
    """Migrate/update a table attached file with a new file handle.

    Arguments:
        key: The migration key.
        to_file_handle_id: The new file handle ID.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.
    """
    partial_row = PartialRow(
        row_id=str(key.row_id),
        values=[{"key": str(key.col_id), "value": to_file_handle_id}],
    )
    partial_row_set = PartialRowSet(
        table_id=key.id,
        rows=[partial_row],
    )
    appendable_request = AppendableRowSetRequest(
        entity_id=key.id,
        to_append=partial_row_set,
    )
    transaction = TableUpdateTransaction(
        entity_id=key.id,
        changes=[appendable_request],
    )
    await transaction.send_job_and_wait_async(synapse_client=synapse_client)


async def track_migration_results_async(
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    active_tasks: Set[asyncio.Task],
    pending_file_handles: Set[str],
    completed_file_handles: Set[str],
    pending_keys: Set[MigrationKey],
    return_when: asyncio.Future[asyncio.Task],
    continue_on_error: bool,
) -> None:
    """Track the results of the migration tasks.

    Arguments:
        conn: The connection to the SQLite database.
        cursor: The cursor object from the connection to the SQLite database.
        pending_file_handles: The set of pending file handles.
        completed_file_handles: The set of completed file handles.
        active_tasks: The set of active migration tasks.
        pending_keys: The set of pending migration keys.
        return_when: The return when condition for the asyncio.wait.
        continue_on_error: Whether to continue on errors.

    Returns:
        None
    """
    done, _ = await asyncio.wait(
        active_tasks,
        return_when=return_when,
    )
    active_tasks -= done
    for completed_task in done:
        to_file_handle_id = None
        ex = None
        try:
            result = completed_task.result()
            key = result["key"]
            from_file_handle_id = result["from_file_handle_id"]
            to_file_handle_id = result["to_file_handle_id"]
            status = MigrationStatus.MIGRATED.value
            completed_file_handles.add(from_file_handle_id)

        except MigrationError as migration_error:
            key = migration_error.key
            from_file_handle_id = migration_error.from_file_handle_id
            ex = migration_error.__cause__
            status = MigrationStatus.ERRORED.value
            completed_file_handles.add(from_file_handle_id)

        _update_migration_database(conn, cursor, key, to_file_handle_id, status, ex)
        pending_keys.discard(key)
        pending_file_handles.discard(from_file_handle_id)

        if not continue_on_error and ex:
            raise ex from None


# =============================================================================
# Migration Implementation
# =============================================================================
async def migrate_indexed_files_async(
    db_path: str,
    *,
    create_table_snapshots: bool = True,
    continue_on_error: bool = False,
    force: bool = False,
    synapse_client: Optional["Synapse"] = None,
) -> MigrationResult:
    """Migrate files that have been indexed.

    This is the second step in migrating files to a new storage location.
    Files must first be indexed using `index_files_for_migration_async`.

    **Interactive confirmation:** When called from an interactive shell and
    `force=False` (the default), this function will print the number of items
    queued for migration and prompt the user to confirm before proceeding
    (``"N items for migration to <location>. Proceed? (y/n)?``). If standard
    output is not connected to an interactive terminal (e.g. a script or CI
    environment), migration is aborted unless ``force=True`` is set.

    Arguments:
        db_path: Path to SQLite database created by index_files_for_migration_async.
        create_table_snapshots: Whether to create table snapshots before migrating. Defaults to True.
        continue_on_error: Whether to continue on individual migration errors. Defaults to False.
        force: Skip the interactive confirmation prompt and proceed with migration
            automatically. Set to ``True`` when running non-interactively (scripts,
            CI, automated pipelines). Defaults to False.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.

    Returns:
        MigrationResult object, or None if migration was aborted (user declined
        the confirmation prompt, or the session is non-interactive and force=False).
    """
    test_import_sqlite3()
    client = Synapse.get_client(synapse_client=synapse_client)

    # Retrieve settings
    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        _ensure_schema(cursor)
        existing_settings = _retrieve_index_settings(cursor)
        if existing_settings is None:
            raise ValueError(
                f"Unable to retrieve existing index settings from '{db_path}'. "
                "Either this path does not represent a previously created migration index "
                "or the file is corrupt."
            )
        dest_storage_location_id = existing_settings.dest_storage_location_id

        # Confirm migration
        confirmed = _confirm_migration(
            cursor, dest_storage_location_id, force, synapse_client=client
        )
        if not confirmed:
            client.logger.info("Migration aborted.")
            return None

        # Execute migration
        await _execute_migration_async(
            conn=conn,
            cursor=cursor,
            dest_storage_location_id=dest_storage_location_id,
            create_table_snapshots=create_table_snapshots,
            continue_on_error=continue_on_error,
            synapse_client=client,
        )
        return MigrationResult(db_path=db_path, synapse_client=client)


async def _execute_migration_async(
    conn: sqlite3.Connection,
    cursor: sqlite3.Cursor,
    dest_storage_location_id: str,
    create_table_snapshots: bool,
    continue_on_error: bool,
    *,
    synapse_client: Optional[Synapse] = None,
) -> None:
    """Execute the actual file migration.

    Arguments:
        conn: The connection to the SQLite database.
        cursor: The cursor object from the connection to the SQLite database.
        dest_storage_location_id: Destination storage location ID.
        create_table_snapshots: Whether to create table snapshots.
        continue_on_error: Whether to continue on errors.
        max_concurrent: Maximum concurrent operations.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created instance from the Synapse class constructor.
    """
    pending_file_handles: Set[str] = set()
    completed_file_handles: Set[str] = set()
    pending_keys: Set[MigrationKey] = set()

    semaphore = asyncio.Semaphore(MAX_CONCURRENT_FILE_COPIES)
    active_tasks: Set[asyncio.Task] = set()

    # Initialize last key to an empty key so the first iteration can proceed.
    key = MigrationKey(id="", type=None, row_id=-1, col_id=-1, version=-1)
    while True:
        # Query next batch
        batch = _query_migration_batch(
            cursor,
            key,
            pending_file_handles,
            completed_file_handles,
            min(BATCH_SIZE, MAX_CONCURRENT_FILE_COPIES - len(active_tasks)),
        )
        row_count = 0
        for item in batch:
            row_count += 1
            last_key = key
            key = MigrationKey(
                id=item["id"],
                type=MigrationType(item["type"]),
                version=item["version"],
                row_id=item["row_id"],
                col_id=item["col_id"],
            )
            from_file_handle_id = item["from_file_handle_id"]
            if key in pending_keys or from_file_handle_id in pending_file_handles:
                # if this record is already being migrated or it shares a file handle
                # with a record that is being migrated then skip this.
                # if it the record shares a file handle it will be picked up later
                # when its file handle is completed.
                continue

            pending_keys.add(key)

            # Check for existing copy
            to_file_handle_id = _check_file_handle_exists(cursor, from_file_handle_id)

            if not to_file_handle_id:
                pending_file_handles.add(from_file_handle_id)

            # Create table snapshot if needed using the async API
            if (
                key.type == MigrationType.TABLE_ATTACHED_FILE.value
                and create_table_snapshots
                and last_key.id != key.id
            ):
                await Table(id=key.id).snapshot_async(synapse_client=synapse_client)

            # Create migration task
            task = asyncio.create_task(
                _migrate_item_async(
                    key=key,
                    from_file_handle_id=from_file_handle_id,
                    to_file_handle_id=to_file_handle_id,
                    file_size=item["file_size"] or 0,
                    dest_storage_location_id=dest_storage_location_id,
                    semaphore=semaphore,
                    synapse_client=synapse_client,
                )
            )
            active_tasks.add(task)

        if row_count == 0 and not pending_file_handles:
            # we've run out of migratable sqlite rows, we have nothing else
            # to submit, so we break out and wait for all remaining
            # tasks to conclude.
            break

        # Wait for tasks if at capacity or end of batch
        if len(active_tasks) >= MAX_CONCURRENT_FILE_COPIES or len(batch) < BATCH_SIZE:
            await track_migration_results_async(
                conn,
                cursor,
                active_tasks,
                pending_file_handles,
                completed_file_handles,
                pending_keys,
                asyncio.FIRST_COMPLETED,
                continue_on_error,
            )

    # Wait for any remaining tasks
    if active_tasks:
        await track_migration_results_async(
            conn,
            cursor,
            active_tasks,
            pending_file_handles,
            completed_file_handles,
            pending_keys,
            asyncio.ALL_COMPLETED,
            continue_on_error,
        )
