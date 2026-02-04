"""
Async migration service for migrating files between storage locations.

This module provides native async implementations of the migration functionality,
replacing the threading-based approach in synapseutils.migrate_functions.
"""

import asyncio
import collections.abc
import json
import logging
import os
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

from synapseclient.api.entity_services import get_children
from synapseclient.api.file_services import get_file_handle_for_download_async
from synapseclient.api.table_services import create_table_snapshot, get_columns
from synapseclient.core import utils
from synapseclient.core.constants import concrete_types
from synapseclient.core.upload.multipart_upload import (
    MAX_NUMBER_OF_PARTS,
    multipart_copy,
)
from synapseclient.models.table_components import (
    AppendableRowSetRequest,
    PartialRow,
    PartialRowSet,
    TableUpdateTransaction,
)

from .migration_types import (
    IndexingError,
    MigrationError,
    MigrationKey,
    MigrationResult,
    MigrationSettings,
    MigrationStatus,
    MigrationType,
)

if TYPE_CHECKING:
    from synapseclient import Synapse

# Default part size for multipart copy (100 MB)
DEFAULT_PART_SIZE = 100 * utils.MB

# Batch size for database operations
BATCH_SIZE = 500

logger = logging.getLogger(__name__)


# =============================================================================
# Temp Directory Helpers
# =============================================================================


def _get_default_db_path(entity_id: str) -> str:
    """Generate a default temp database path for migration tracking.

    Arguments:
        entity_id: The Synapse entity ID being migrated.

    Returns:
        Path to a SQLite database file in a temp directory.
    """
    temp_dir = tempfile.mkdtemp(prefix="synapse_migration_")
    return os.path.join(temp_dir, f"migration_{entity_id}.db")


# =============================================================================
# Column Name Helpers (replaces legacy synapseclient.table functions)
# =============================================================================


def _escape_column_name(column: Union[str, collections.abc.Mapping]) -> str:
    """Escape a column name for use in a Synapse table query statement.

    Arguments:
        column: A string column name or a dictionary with a 'name' key.

    Returns:
        Escaped column name wrapped in double quotes.
    """
    col_name = (
        column["name"] if isinstance(column, collections.abc.Mapping) else str(column)
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


# =============================================================================
# Database Helper Functions (Synchronous - wrapped with asyncio.to_thread)
# =============================================================================


def _ensure_schema(cursor) -> None:
    """Ensure the SQLite database has the required schema."""
    # Settings table - stores JSON configuration
    cursor.execute(
        "CREATE TABLE IF NOT EXISTS migration_settings (settings TEXT NOT NULL)"
    )

    # Main migrations table
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

    # Indexes for common queries
    cursor.execute("CREATE INDEX IF NOT EXISTS ix_status ON migrations(status)")
    cursor.execute(
        "CREATE INDEX IF NOT EXISTS ix_file_handle_ids "
        "ON migrations(from_file_handle_id, to_file_handle_id)"
    )


def _initialize_database(
    db_path: str,
    root_id: str,
    dest_storage_location_id: str,
    source_storage_location_ids: List[str],
    file_version_strategy: str,
    include_table_files: bool,
) -> None:
    """Initialize the migration database with schema and settings.

    Arguments:
        db_path: Path to the SQLite database file.
        root_id: The root entity ID being migrated.
        dest_storage_location_id: Destination storage location ID.
        source_storage_location_ids: List of source storage location IDs to filter.
        file_version_strategy: Strategy for handling file versions.
        include_table_files: Whether to include table-attached files.
    """
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        _ensure_schema(cursor)

        # Check if settings already exist
        existing = cursor.execute("SELECT settings FROM migration_settings").fetchone()

        settings = MigrationSettings(
            root_id=root_id,
            dest_storage_location_id=dest_storage_location_id,
            source_storage_location_ids=source_storage_location_ids,
            file_version_strategy=file_version_strategy,
            include_table_files=include_table_files,
        )

        if existing:
            # Verify settings match
            existing_settings = json.loads(existing[0])
            if existing_settings.get("root_id") != root_id:
                raise ValueError(
                    f"Root entity ID mismatch: database has {existing_settings.get('root_id')}, "
                    f"but {root_id} was provided"
                )
            if (
                existing_settings.get("dest_storage_location_id")
                != dest_storage_location_id
            ):
                raise ValueError(
                    f"Destination storage location mismatch: database has "
                    f"{existing_settings.get('dest_storage_location_id')}, "
                    f"but {dest_storage_location_id} was provided"
                )
        else:
            # Insert new settings
            settings_json = json.dumps(
                {
                    "root_id": settings.root_id,
                    "dest_storage_location_id": settings.dest_storage_location_id,
                    "source_storage_location_ids": settings.source_storage_location_ids,
                    "file_version_strategy": settings.file_version_strategy,
                    "include_table_files": settings.include_table_files,
                }
            )
            cursor.execute(
                "INSERT INTO migration_settings (settings) VALUES (?)",
                (settings_json,),
            )

        conn.commit()


def _retrieve_index_settings(db_path: str) -> Optional[Dict[str, Any]]:
    """Retrieve index settings from the database.

    Arguments:
        db_path: Path to the SQLite database file.

    Returns:
        Dictionary of settings or None if not found.
    """
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        _ensure_schema(cursor)

        row = cursor.execute("SELECT settings FROM migration_settings").fetchone()
        if row:
            return json.loads(row[0])
        return None


def _check_indexed(db_path: str, entity_id: str) -> bool:
    """Check if an entity has already been indexed.

    Arguments:
        db_path: Path to the SQLite database file.
        entity_id: The entity ID to check.

    Returns:
        True if the entity is already indexed, False otherwise.
    """
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        row = cursor.execute(
            "SELECT 1 FROM migrations WHERE id = ? LIMIT 1",
            (entity_id,),
        ).fetchone()
        return row is not None


def _mark_container_indexed(
    db_path: str,
    entity_id: str,
    parent_id: Optional[str],
    migration_type: MigrationType,
) -> None:
    """Mark a container (Project or Folder) as indexed.

    Arguments:
        db_path: Path to the SQLite database file.
        entity_id: The entity ID.
        parent_id: The parent entity ID.
        migration_type: The type of container.
    """
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO migrations (id, type, parent_id, status)
            VALUES (?, ?, ?, ?)
            """,
            (
                entity_id,
                migration_type.value,
                parent_id,
                MigrationStatus.INDEXED.value,
            ),
        )
        conn.commit()


def _insert_file_migration(
    db_path: str,
    entity_id: str,
    version: Optional[int],
    parent_id: Optional[str],
    from_storage_location_id: int,
    from_file_handle_id: str,
    file_size: int,
    status: MigrationStatus,
) -> None:
    """Insert a file migration entry.

    Arguments:
        db_path: Path to the SQLite database file.
        entity_id: The file entity ID.
        version: The file version (None for new version).
        parent_id: The parent entity ID.
        from_storage_location_id: Source storage location ID.
        from_file_handle_id: Source file handle ID.
        file_size: File size in bytes.
        status: Migration status.
    """
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO migrations (
                id, type, version, parent_id,
                from_storage_location_id, from_file_handle_id,
                file_size, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entity_id,
                MigrationType.FILE.value,
                version,
                parent_id,
                from_storage_location_id,
                from_file_handle_id,
                file_size,
                status.value,
            ),
        )
        conn.commit()


def _insert_table_file_migration(
    db_path: str,
    entity_id: str,
    row_id: int,
    col_id: int,
    row_version: int,
    parent_id: Optional[str],
    from_storage_location_id: int,
    from_file_handle_id: str,
    file_size: int,
    status: MigrationStatus,
) -> None:
    """Insert a table-attached file migration entry.

    Arguments:
        db_path: Path to the SQLite database file.
        entity_id: The table entity ID.
        row_id: The table row ID.
        col_id: The table column ID.
        row_version: The row version.
        parent_id: The parent entity ID.
        from_storage_location_id: Source storage location ID.
        from_file_handle_id: Source file handle ID.
        file_size: File size in bytes.
        status: Migration status.
    """
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO migrations (
                id, type, row_id, col_id, version, parent_id,
                from_storage_location_id, from_file_handle_id,
                file_size, status
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                entity_id,
                MigrationType.TABLE_ATTACHED_FILE.value,
                row_id,
                col_id,
                row_version,
                parent_id,
                from_storage_location_id,
                from_file_handle_id,
                file_size,
                status.value,
            ),
        )
        conn.commit()


def _record_indexing_error(
    db_path: str,
    entity_id: str,
    parent_id: Optional[str],
    exception: Exception,
) -> None:
    """Record an indexing error in the database.

    Arguments:
        db_path: Path to the SQLite database file.
        entity_id: The entity ID that failed.
        parent_id: The parent entity ID.
        exception: The exception that occurred.
    """
    import sqlite3

    tb_str = "".join(
        traceback.format_exception(type(exception), exception, exception.__traceback__)
    )

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        cursor.execute(
            """
            INSERT OR IGNORE INTO migrations (
                id, type, parent_id, status, exception
            ) VALUES (?, ?, ?, ?, ?)
            """,
            (
                entity_id,
                MigrationType.FILE.value,  # Default type for errors
                parent_id,
                MigrationStatus.ERRORED.value,
                tb_str,
            ),
        )
        conn.commit()


def _check_file_handle_exists(db_path: str, from_file_handle_id: str) -> Optional[str]:
    """Check if a file handle has already been copied.

    Arguments:
        db_path: Path to the SQLite database file.
        from_file_handle_id: The source file handle ID.

    Returns:
        The destination file handle ID if found, None otherwise.
    """
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        row = cursor.execute(
            """
            SELECT to_file_handle_id FROM migrations
            WHERE from_file_handle_id = ? AND to_file_handle_id IS NOT NULL
            """,
            (from_file_handle_id,),
        ).fetchone()
        return row[0] if row else None


def _query_migration_batch(
    db_path: str,
    last_id: str,
    last_version: int,
    last_row_id: int,
    last_col_id: int,
    pending_file_handles: Set[str],
    completed_file_handles: Set[str],
    limit: int,
) -> List[Dict[str, Any]]:
    """Query the next batch of items to migrate.

    This matches the original synapseutils query logic:
    - Forward progress through entities ordered by id, type, row_id, col_id, version
    - Backtracking to pick up files with completed file handles that were skipped

    Arguments:
        db_path: Path to the SQLite database file.
        last_id: Last processed entity ID.
        last_version: Last processed version.
        last_row_id: Last processed row ID.
        last_col_id: Last processed column ID.
        pending_file_handles: Set of file handles currently being processed.
        completed_file_handles: Set of file handles already completed.
        limit: Maximum number of items to return.

    Returns:
        List of migration entries as dictionaries.
    """
    import sqlite3

    if limit <= 0:
        return []

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        file_type = MigrationType.FILE.value
        table_type = MigrationType.TABLE_ATTACHED_FILE.value
        indexed_status = MigrationStatus.INDEXED.value

        # Build the IN clauses for file handles
        # We use string formatting for the IN clause since sqlite3 doesn't support array parameters
        pending_in = (
            "('" + "','".join(pending_file_handles) + "')"
            if pending_file_handles
            else "('')"
        )
        completed_in = (
            "('" + "','".join(completed_file_handles) + "')"
            if completed_file_handles
            else "('')"
        )

        # Match the original synapseutils query structure exactly
        # This handles:
        # 1. Forward progress: entities after the current position
        # 2. Backtracking: entities before current position that share completed file handles
        query = f"""
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
                        AND from_file_handle_id NOT IN {pending_in}
                    ) OR
                    (
                        id <= :id
                        AND from_file_handle_id IN {completed_in}
                    )
                )
            ORDER BY
                id,
                type,
                row_id,
                col_id,
                version
            LIMIT :limit
        """

        params = {
            "indexed_status": indexed_status,
            "id": last_id,
            "file_type": file_type,
            "table_type": table_type,
            "version": last_version,
            "row_id": last_row_id,
            "col_id": last_col_id,
            "limit": limit,
        }

        results = cursor.execute(query, params)

        batch = []
        for row in results:
            batch.append(
                {
                    "id": row[0],
                    "type": MigrationType(row[1]),
                    "version": row[2],
                    "row_id": row[3],
                    "col_id": row[4],
                    "from_file_handle_id": row[5],
                    "file_size": row[6],
                }
            )
        return batch


def _update_migration_success(
    db_path: str,
    key: MigrationKey,
    to_file_handle_id: str,
) -> None:
    """Update a migration entry as successful.

    Arguments:
        db_path: Path to the SQLite database file.
        key: The migration key.
        to_file_handle_id: The destination file handle ID.
    """
    import sqlite3

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        update_sql = """
            UPDATE migrations SET status = ?, to_file_handle_id = ?
            WHERE id = ? AND type = ?
        """
        params = [
            MigrationStatus.MIGRATED.value,
            to_file_handle_id,
            key.id,
            key.type.value,
        ]

        if key.version is not None:
            update_sql += " AND version = ?"
            params.append(key.version)
        else:
            update_sql += " AND version IS NULL"

        if key.row_id is not None:
            update_sql += " AND row_id = ?"
            params.append(key.row_id)

        if key.col_id is not None:
            update_sql += " AND col_id = ?"
            params.append(key.col_id)

        cursor.execute(update_sql, tuple(params))
        conn.commit()


def _update_migration_error(
    db_path: str,
    key: MigrationKey,
    exception: Exception,
) -> None:
    """Update a migration entry with an error.

    Arguments:
        db_path: Path to the SQLite database file.
        key: The migration key.
        exception: The exception that occurred.
    """
    import sqlite3

    tb_str = "".join(
        traceback.format_exception(type(exception), exception, exception.__traceback__)
    )

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()

        update_sql = """
            UPDATE migrations SET status = ?, exception = ?
            WHERE id = ? AND type = ?
        """
        params = [MigrationStatus.ERRORED.value, tb_str, key.id, key.type.value]

        if key.version is not None:
            update_sql += " AND version = ?"
            params.append(key.version)
        else:
            update_sql += " AND version IS NULL"

        if key.row_id is not None:
            update_sql += " AND row_id = ?"
            params.append(key.row_id)

        if key.col_id is not None:
            update_sql += " AND col_id = ?"
            params.append(key.col_id)

        cursor.execute(update_sql, tuple(params))
        conn.commit()


def _confirm_migration(
    db_path: str, dest_storage_location_id: str, force: bool
) -> bool:
    """Confirm migration with user if in interactive mode.

    Arguments:
        db_path: Path to the SQLite database file.
        dest_storage_location_id: Destination storage location ID.
        force: Whether to skip confirmation.

    Returns:
        True if migration should proceed, False otherwise.
    """
    import sqlite3

    if force:
        return True

    with sqlite3.connect(db_path) as conn:
        cursor = conn.cursor()
        count = cursor.execute(
            "SELECT count(*) FROM migrations WHERE status = ?",
            (MigrationStatus.INDEXED.value,),
        ).fetchone()[0]

        if count == 0:
            logger.info("No items for migration.")
            return False

        if sys.stdout.isatty():
            user_input = input(
                f"{count} items for migration to {dest_storage_location_id}. Proceed? (y/n)? "
            )
            return user_input.strip().lower() == "y"
        else:
            logger.info(
                "%s items for migration. "
                "force option not used, and console input not available to confirm migration, aborting. "
                "Use the force option or run from an interactive shell to proceed with migration.",
                count,
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


# =============================================================================
# Storage Location Validation
# =============================================================================


async def _verify_storage_location_ownership_async(
    storage_location_id: str,
    *,
    synapse_client: "Synapse",
) -> None:
    """Verify the user owns the destination storage location.

    Arguments:
        storage_location_id: The storage location ID to verify.
        synapse_client: The Synapse client.

    Raises:
        ValueError: If the user does not own the storage location.
    """
    try:
        await synapse_client.rest_get_async(f"/storageLocation/{storage_location_id}")
    except Exception as ex:
        raise ValueError(
            f"Unable to verify ownership of storage location {storage_location_id}. "
            f"You must be the creator of the destination storage location. Error: {ex}"
        ) from ex


def _include_file_in_migration(
    file_handle: Dict[str, Any],
    source_storage_location_ids: List[str],
    dest_storage_location_id: str,
) -> Optional[MigrationStatus]:
    """Determine if a file should be included in migration.

    Only S3 file handles can be migrated. External URLs and other file handle types
    are skipped.

    Arguments:
        file_handle: The file handle metadata.
        source_storage_location_ids: List of source storage locations to filter.
        dest_storage_location_id: Destination storage location ID.

    Returns:
        MigrationStatus if file should be included, None otherwise.
    """
    # Only S3 file handles can be migrated
    if file_handle.get("concreteType") != concrete_types.S3_FILE_HANDLE:
        return None

    from_storage_location_id = str(file_handle.get("storageLocationId", 1))

    # Check if file matches the migration criteria:
    # - If source_storage_location_ids is specified, from_storage_location must be in it
    #   OR already at the destination
    # - If not specified, include all files not already at destination
    if source_storage_location_ids:
        if (
            from_storage_location_id not in source_storage_location_ids
            and from_storage_location_id != dest_storage_location_id
        ):
            return None

    # Already at destination - mark as already migrated
    if from_storage_location_id == dest_storage_location_id:
        return MigrationStatus.ALREADY_MIGRATED

    return MigrationStatus.INDEXED


# =============================================================================
# Public API Functions
# =============================================================================


async def index_files_for_migration_async(
    entity_id: str,
    dest_storage_location_id: str,
    db_path: Optional[str] = None,
    *,
    source_storage_location_ids: Optional[List[str]] = None,
    file_version_strategy: str = "new",
    include_table_files: bool = False,
    continue_on_error: bool = False,
    synapse_client: Optional["Synapse"] = None,
) -> MigrationResult:
    """Index files for migration to a new storage location.

    This is the first step in migrating files to a new storage location.
    After indexing, use `migrate_indexed_files_async` to perform the actual migration.

    Arguments:
        entity_id: The Synapse entity ID to migrate (Project, Folder, File, or Table).
        dest_storage_location_id: The destination storage location ID.
        db_path: Path to create SQLite database. If None, uses temp directory.
        source_storage_location_ids: Optional list of source storage locations to filter.
        file_version_strategy: Strategy for file versions: "new", "all", "latest", "skip".
        include_table_files: Whether to include files attached to tables.
        continue_on_error: Whether to continue on individual errors.
        synapse_client: Optional Synapse client instance.

    Returns:
        MigrationResult object for inspecting the index.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Validate parameters
    valid_strategies = {"new", "all", "latest", "skip"}
    if file_version_strategy not in valid_strategies:
        raise ValueError(
            f"Invalid file_version_strategy: {file_version_strategy}, "
            f"must be one of {valid_strategies}"
        )

    if file_version_strategy == "skip" and not include_table_files:
        raise ValueError(
            "Skipping both file entities and table attached files, nothing to migrate"
        )

    # Convert to strings
    dest_storage_location_id = str(dest_storage_location_id)
    source_storage_location_ids = [str(s) for s in (source_storage_location_ids or [])]

    # Verify ownership
    await _verify_storage_location_ownership_async(
        storage_location_id=dest_storage_location_id,
        synapse_client=client,
    )

    # Create database path if not provided
    if db_path is None:
        db_path = _get_default_db_path(entity_id)

    # Initialize database
    await asyncio.to_thread(
        _initialize_database,
        db_path,
        entity_id,
        dest_storage_location_id,
        source_storage_location_ids,
        file_version_strategy,
        include_table_files,
    )

    # Get entity and start indexing
    entity = await client.get_async(entity_id, downloadFile=False)

    try:
        await _index_entity_async(
            entity=entity,
            parent_id=None,
            db_path=db_path,
            dest_storage_location_id=dest_storage_location_id,
            source_storage_location_ids=source_storage_location_ids,
            file_version_strategy=file_version_strategy,
            include_table_files=include_table_files,
            continue_on_error=continue_on_error,
            synapse_client=client,
        )
    except IndexingError as ex:
        logger.exception(
            "Aborted due to failure to index entity %s of type %s. "
            "Use continue_on_error=True to skip individual failures.",
            ex.entity_id,
            ex.concrete_type,
        )
        raise ex

    return MigrationResult(db_path=db_path, synapse_client=client)


async def migrate_indexed_files_async(
    db_path: str,
    *,
    create_table_snapshots: bool = True,
    continue_on_error: bool = False,
    force: bool = False,
    max_concurrent_copies: Optional[int] = None,
    synapse_client: Optional["Synapse"] = None,
) -> Optional[MigrationResult]:
    """Migrate files that have been indexed.

    This is the second step in migrating files to a new storage location.
    Files must first be indexed using `index_files_for_migration_async`.

    Arguments:
        db_path: Path to SQLite database created by index_files_for_migration_async.
        create_table_snapshots: Whether to create table snapshots before migrating.
        continue_on_error: Whether to continue on individual migration errors.
        force: Whether to skip interactive confirmation.
        max_concurrent_copies: Maximum concurrent file copy operations.
        synapse_client: Optional Synapse client instance.

    Returns:
        MigrationResult object or None if migration was aborted.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Retrieve settings
    settings = await asyncio.to_thread(_retrieve_index_settings, db_path)
    if settings is None:
        raise ValueError(
            f"Unable to retrieve existing index settings from '{db_path}'. "
            "Either this path does not represent a previously created migration index "
            "or the file is corrupt."
        )

    dest_storage_location_id = settings["dest_storage_location_id"]

    # Confirm migration
    confirmed = await asyncio.to_thread(
        _confirm_migration, db_path, dest_storage_location_id, force
    )
    if not confirmed:
        logger.info("Migration aborted.")
        return None

    # Determine concurrency
    max_concurrent = max_concurrent_copies or max(client.max_threads // 2, 1)

    # Execute migration
    await _execute_migration_async(
        db_path=db_path,
        dest_storage_location_id=dest_storage_location_id,
        create_table_snapshots=create_table_snapshots,
        continue_on_error=continue_on_error,
        max_concurrent=max_concurrent,
        synapse_client=client,
    )

    return MigrationResult(db_path=db_path, synapse_client=client)


# =============================================================================
# Indexing Implementation
# =============================================================================


async def _index_entity_async(
    entity: Any,
    parent_id: Optional[str],
    db_path: str,
    dest_storage_location_id: str,
    source_storage_location_ids: List[str],
    file_version_strategy: str,
    include_table_files: bool,
    continue_on_error: bool,
    *,
    synapse_client: "Synapse",
) -> None:
    """Recursively index an entity and its children.

    Arguments:
        entity: The Synapse entity object.
        parent_id: The parent entity ID.
        db_path: Path to the SQLite database.
        dest_storage_location_id: Destination storage location ID.
        source_storage_location_ids: List of source storage locations to filter.
        file_version_strategy: Strategy for file versions.
        include_table_files: Whether to include table-attached files.
        continue_on_error: Whether to continue on errors.
        synapse_client: The Synapse client.
    """
    entity_id = utils.id_of(entity)
    concrete_type = utils.concrete_type_of(entity)

    # Check if already indexed
    is_indexed = await asyncio.to_thread(_check_indexed, db_path, entity_id)
    if is_indexed:
        return

    try:
        if concrete_type == concrete_types.FILE_ENTITY:
            if file_version_strategy != "skip":
                await _index_file_entity_async(
                    entity_id=entity_id,
                    parent_id=parent_id,
                    db_path=db_path,
                    dest_storage_location_id=dest_storage_location_id,
                    source_storage_location_ids=source_storage_location_ids,
                    file_version_strategy=file_version_strategy,
                    synapse_client=synapse_client,
                )

        elif concrete_type == concrete_types.TABLE_ENTITY:
            if include_table_files:
                await _index_table_entity_async(
                    entity_id=entity_id,
                    parent_id=parent_id,
                    db_path=db_path,
                    dest_storage_location_id=dest_storage_location_id,
                    source_storage_location_ids=source_storage_location_ids,
                    synapse_client=synapse_client,
                )

        elif concrete_type in (
            concrete_types.FOLDER_ENTITY,
            concrete_types.PROJECT_ENTITY,
        ):
            await _index_container_async(
                entity_id=entity_id,
                parent_id=parent_id,
                db_path=db_path,
                concrete_type=concrete_type,
                dest_storage_location_id=dest_storage_location_id,
                source_storage_location_ids=source_storage_location_ids,
                file_version_strategy=file_version_strategy,
                include_table_files=include_table_files,
                continue_on_error=continue_on_error,
                synapse_client=synapse_client,
            )

    except IndexingError:
        raise
    except Exception as ex:
        if continue_on_error:
            logger.warning("Error indexing entity %s: %s", entity_id, ex)
            await asyncio.to_thread(
                _record_indexing_error, db_path, entity_id, parent_id, ex
            )
        else:
            raise IndexingError(entity_id, concrete_type) from ex


async def _index_file_entity_async(
    entity_id: str,
    parent_id: Optional[str],
    db_path: str,
    dest_storage_location_id: str,
    source_storage_location_ids: List[str],
    file_version_strategy: str,
    *,
    synapse_client: "Synapse",
) -> None:
    """Index a file entity for migration.

    Arguments:
        entity_id: The file entity ID.
        parent_id: The parent entity ID.
        db_path: Path to the SQLite database.
        dest_storage_location_id: Destination storage location ID.
        source_storage_location_ids: List of source storage locations to filter.
        file_version_strategy: Strategy for file versions.
        synapse_client: The Synapse client.
    """
    logger.info("Indexing file entity %s", entity_id)

    entity_versions: List[Tuple[Any, Optional[int]]] = []

    if file_version_strategy == "new":
        entity = await synapse_client.get_async(entity_id, downloadFile=False)
        entity_versions.append((entity, None))

    elif file_version_strategy == "all":
        # Get all versions
        async for version in _get_version_numbers_async(entity_id, synapse_client):
            entity = await synapse_client.get_async(
                entity_id, version=version, downloadFile=False
            )
            entity_versions.append((entity, version))

    elif file_version_strategy == "latest":
        entity = await synapse_client.get_async(entity_id, downloadFile=False)
        entity_versions.append((entity, entity.versionNumber))

    for entity, version in entity_versions:
        file_handle = entity._file_handle
        status = _include_file_in_migration(
            file_handle, source_storage_location_ids, dest_storage_location_id
        )
        if status:
            await asyncio.to_thread(
                _insert_file_migration,
                db_path,
                entity_id,
                version,
                parent_id,
                file_handle["storageLocationId"],
                entity.dataFileHandleId,
                file_handle["contentSize"],
                status,
            )


async def _get_version_numbers_async(
    entity_id: str,
    synapse_client: "Synapse",
) -> AsyncGenerator[int, None]:
    """Get all version numbers for an entity.

    Arguments:
        entity_id: The entity ID.
        synapse_client: The Synapse client.

    Yields:
        Version numbers.
    """
    offset = 0
    limit = 100

    while True:
        response = await synapse_client.rest_get_async(
            f"/entity/{entity_id}/version?offset={offset}&limit={limit}"
        )
        results = response.get("results", [])

        for version_info in results:
            yield version_info["versionNumber"]

        if len(results) < limit:
            break
        offset += limit


async def _index_table_entity_async(
    entity_id: str,
    parent_id: Optional[str],
    db_path: str,
    dest_storage_location_id: str,
    source_storage_location_ids: List[str],
    *,
    synapse_client: "Synapse",
) -> None:
    """Index a table entity's file attachments for migration.

    Arguments:
        entity_id: The table entity ID.
        parent_id: The parent entity ID.
        db_path: Path to the SQLite database.
        dest_storage_location_id: Destination storage location ID.
        source_storage_location_ids: List of source storage locations to filter.
        synapse_client: The Synapse client.
    """
    logger.info("Indexing table entity %s", entity_id)

    # Get file handle columns using the async API
    columns = await get_columns(table_id=entity_id, synapse_client=synapse_client)
    file_handle_columns = [c for c in columns if c.column_type == "FILEHANDLEID"]

    if not file_handle_columns:
        return

    # Query table for file handles using local helper
    file_column_select = _join_column_names(file_handle_columns)

    # tableQuery is still a synchronous method on the Synapse client
    results = await asyncio.to_thread(
        synapse_client.tableQuery,
        f"SELECT {file_column_select} FROM {entity_id}",
    )

    for row in results:
        row_id, row_version = row[:2]
        file_handle_ids = row[2:]

        for i, file_handle_id in enumerate(file_handle_ids):
            if not file_handle_id:
                continue

            col_id = file_handle_columns[i].id

            # Get file handle metadata using the async API
            fh_response = await get_file_handle_for_download_async(
                file_handle_id=str(file_handle_id),
                synapse_id=entity_id,
                entity_type="TableEntity",
                synapse_client=synapse_client,
            )
            file_handle = fh_response["fileHandle"]

            status = _include_file_in_migration(
                file_handle, source_storage_location_ids, dest_storage_location_id
            )
            if status:
                await asyncio.to_thread(
                    _insert_table_file_migration,
                    db_path,
                    entity_id,
                    row_id,
                    int(col_id),
                    row_version,
                    parent_id,
                    file_handle["storageLocationId"],
                    file_handle_id,
                    file_handle["contentSize"],
                    status,
                )


async def _index_container_async(
    entity_id: str,
    parent_id: Optional[str],
    db_path: str,
    concrete_type: str,
    dest_storage_location_id: str,
    source_storage_location_ids: List[str],
    file_version_strategy: str,
    include_table_files: bool,
    continue_on_error: bool,
    *,
    synapse_client: "Synapse",
) -> None:
    """Index a container (Project or Folder) and its children.

    Arguments:
        entity_id: The container entity ID.
        parent_id: The parent entity ID.
        db_path: Path to the SQLite database.
        concrete_type: The concrete type of the container.
        dest_storage_location_id: Destination storage location ID.
        source_storage_location_ids: List of source storage locations to filter.
        file_version_strategy: Strategy for file versions.
        include_table_files: Whether to include table-attached files.
        continue_on_error: Whether to continue on errors.
        synapse_client: The Synapse client.
    """
    logger.info("Indexing container %s", entity_id)

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

    # Use bounded concurrency for indexing children
    semaphore = asyncio.Semaphore(10)

    async def index_child(child: Dict[str, Any]) -> None:
        async with semaphore:
            child_entity = await synapse_client.get_async(
                child["id"], downloadFile=False
            )
            await _index_entity_async(
                entity=child_entity,
                parent_id=entity_id,
                db_path=db_path,
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
        MigrationType.PROJECT
        if concrete_type == concrete_types.PROJECT_ENTITY
        else MigrationType.FOLDER
    )
    await asyncio.to_thread(
        _mark_container_indexed, db_path, entity_id, parent_id, migration_type
    )


# =============================================================================
# Migration Execution
# =============================================================================


async def _execute_migration_async(
    db_path: str,
    dest_storage_location_id: str,
    create_table_snapshots: bool,
    continue_on_error: bool,
    max_concurrent: int,
    *,
    synapse_client: "Synapse",
) -> None:
    """Execute the actual file migration.

    Arguments:
        db_path: Path to the SQLite database.
        dest_storage_location_id: Destination storage location ID.
        create_table_snapshots: Whether to create table snapshots.
        continue_on_error: Whether to continue on errors.
        max_concurrent: Maximum concurrent operations.
        synapse_client: The Synapse client.
    """
    pending_file_handles: Set[str] = set()
    completed_file_handles: Set[str] = set()
    pending_keys: Set[MigrationKey] = set()
    table_snapshots_created: Set[str] = set()

    semaphore = asyncio.Semaphore(max_concurrent)
    active_tasks: Set[asyncio.Task] = set()

    last_id = ""
    last_version = -1
    last_row_id = -1
    last_col_id = -1

    while True:
        # Query next batch
        batch = await asyncio.to_thread(
            _query_migration_batch,
            db_path,
            last_id,
            last_version,
            last_row_id,
            last_col_id,
            pending_file_handles,
            completed_file_handles,
            min(BATCH_SIZE, max_concurrent - len(active_tasks)),
        )

        if not batch and not active_tasks:
            break

        # Process batch items
        for item in batch:
            key = MigrationKey(
                id=item["id"],
                type=item["type"],
                version=item["version"],
                row_id=item["row_id"],
                col_id=item["col_id"],
            )

            if key in pending_keys:
                continue

            pending_keys.add(key)
            from_file_handle_id = item["from_file_handle_id"]

            # Check for existing copy
            to_file_handle_id = await asyncio.to_thread(
                _check_file_handle_exists, db_path, from_file_handle_id
            )

            if not to_file_handle_id:
                pending_file_handles.add(from_file_handle_id)

            # Create table snapshot if needed using the async API
            if (
                item["type"] == MigrationType.TABLE_ATTACHED_FILE
                and create_table_snapshots
                and item["id"] not in table_snapshots_created
            ):
                await create_table_snapshot(
                    table_id=item["id"],
                    synapse_client=synapse_client,
                )
                table_snapshots_created.add(item["id"])

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

            # Update tracking for next batch
            last_id = item["id"]
            last_version = item["version"] if item["version"] is not None else -1
            last_row_id = item["row_id"] if item["row_id"] is not None else -1
            last_col_id = item["col_id"] if item["col_id"] is not None else -1

        # Wait for tasks if at capacity or end of batch
        if active_tasks and (
            len(active_tasks) >= max_concurrent or len(batch) < BATCH_SIZE
        ):
            done, active_tasks = await asyncio.wait(
                active_tasks,
                return_when=asyncio.FIRST_COMPLETED,
            )

            for completed_task in done:
                try:
                    result = completed_task.result()
                    key = result["key"]
                    from_fh_id = result["from_file_handle_id"]
                    to_fh_id = result["to_file_handle_id"]

                    # Update database
                    await asyncio.to_thread(
                        _update_migration_success, db_path, key, to_fh_id
                    )

                    completed_file_handles.add(from_fh_id)
                    pending_file_handles.discard(from_fh_id)
                    pending_keys.discard(key)

                except Exception as ex:
                    if hasattr(ex, "key"):
                        key = ex.key
                        await asyncio.to_thread(
                            _update_migration_error, db_path, key, ex.__cause__ or ex
                        )
                        pending_keys.discard(key)

                    if not continue_on_error:
                        # Cancel remaining tasks
                        for task in active_tasks:
                            task.cancel()
                        raise

    # Wait for any remaining tasks
    if active_tasks:
        done, _ = await asyncio.wait(active_tasks)
        for completed_task in done:
            try:
                result = completed_task.result()
                await asyncio.to_thread(
                    _update_migration_success,
                    db_path,
                    result["key"],
                    result["to_file_handle_id"],
                )
            except Exception as ex:
                if hasattr(ex, "key"):
                    await asyncio.to_thread(
                        _update_migration_error, db_path, ex.key, ex.__cause__ or ex
                    )
                if not continue_on_error:
                    raise


async def _migrate_item_async(
    key: MigrationKey,
    from_file_handle_id: str,
    to_file_handle_id: Optional[str],
    file_size: int,
    dest_storage_location_id: str,
    semaphore: asyncio.Semaphore,
    *,
    synapse_client: "Synapse",
) -> Dict[str, Any]:
    """Migrate a single item.

    Arguments:
        key: The migration key.
        from_file_handle_id: Source file handle ID.
        to_file_handle_id: Destination file handle ID (if already copied).
        file_size: File size in bytes.
        dest_storage_location_id: Destination storage location ID.
        semaphore: Concurrency semaphore.
        synapse_client: The Synapse client.

    Returns:
        Dictionary with key, from_file_handle_id, to_file_handle_id.
    """
    async with semaphore:
        try:
            # Copy file handle if needed
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

                # Use thread for multipart_copy (it uses threading internally)
                to_file_handle_id = await asyncio.to_thread(
                    multipart_copy,
                    synapse_client,
                    source_association,
                    dest_storage_location_id,
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
                    await _update_file_version_async(
                        entity_id=key.id,
                        version=key.version,
                        from_file_handle_id=from_file_handle_id,
                        to_file_handle_id=to_file_handle_id,
                        synapse_client=synapse_client,
                    )
            elif key.type == MigrationType.TABLE_ATTACHED_FILE:
                await _update_table_file_async(
                    entity_id=key.id,
                    row_id=key.row_id,
                    col_id=key.col_id,
                    to_file_handle_id=to_file_handle_id,
                    synapse_client=synapse_client,
                )

            return {
                "key": key,
                "from_file_handle_id": from_file_handle_id,
                "to_file_handle_id": to_file_handle_id,
            }

        except Exception as ex:
            error = MigrationError(key, from_file_handle_id, to_file_handle_id)
            error.__cause__ = ex
            raise error


async def _create_new_file_version_async(
    entity_id: str,
    to_file_handle_id: str,
    *,
    synapse_client: "Synapse",
) -> None:
    """Create a new version of a file entity with the new file handle.

    Arguments:
        entity_id: The file entity ID.
        to_file_handle_id: The new file handle ID.
        synapse_client: The Synapse client.
    """
    entity = await synapse_client.get_async(entity_id, downloadFile=False)
    entity.dataFileHandleId = to_file_handle_id
    await synapse_client.store_async(entity)


async def _update_file_version_async(
    entity_id: str,
    version: int,
    from_file_handle_id: str,
    to_file_handle_id: str,
    *,
    synapse_client: "Synapse",
) -> None:
    """Update an existing file version's file handle.

    Arguments:
        entity_id: The file entity ID.
        version: The version number.
        from_file_handle_id: The original file handle ID.
        to_file_handle_id: The new file handle ID.
        synapse_client: The Synapse client.
    """
    await synapse_client.rest_put_async(
        f"/entity/{entity_id}/version/{version}/filehandle",
        body=json.dumps(
            {
                "oldFileHandleId": from_file_handle_id,
                "newFileHandleId": to_file_handle_id,
            }
        ),
    )


async def _update_table_file_async(
    entity_id: str,
    row_id: int,
    col_id: int,
    to_file_handle_id: str,
    *,
    synapse_client: "Synapse",
) -> None:
    """Update a table cell with a new file handle.

    Arguments:
        entity_id: The table entity ID.
        row_id: The row ID.
        col_id: The column ID.
        to_file_handle_id: The new file handle ID.
        synapse_client: The Synapse client.
    """
    # Create the partial row update using new OOP models
    partial_row = PartialRow(
        row_id=str(row_id),
        values=[{"key": str(col_id), "value": to_file_handle_id}],
    )
    partial_row_set = PartialRowSet(
        table_id=entity_id,
        rows=[partial_row],
    )
    appendable_request = AppendableRowSetRequest(
        entity_id=entity_id,
        to_append=partial_row_set,
    )

    # Execute the update using TableUpdateTransaction
    transaction = TableUpdateTransaction(
        entity_id=entity_id,
        changes=[appendable_request],
    )
    await transaction.send_job_and_wait_async(synapse_client=synapse_client)
