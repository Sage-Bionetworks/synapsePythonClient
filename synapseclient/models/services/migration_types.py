"""
Data classes and enums for the async migration service.

These types are used to track the state of file migrations between storage locations.
"""

import asyncio
import csv
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

from synapseclient.core.constants import concrete_types

if TYPE_CHECKING:
    from synapseclient import Synapse


class MigrationStatus(Enum):
    """Status of a migration entry in the tracking database."""

    INDEXED = 1
    """The file has been indexed and is ready to be migrated."""

    MIGRATED = 2
    """The file has been successfully migrated to the new storage location."""

    ALREADY_MIGRATED = 3
    """The file was already at the destination storage location."""

    ERRORED = 4
    """An error occurred during indexing or migration."""


class MigrationType(Enum):
    """Type of entity being tracked in the migration database."""

    PROJECT = 1
    """A project container (used for tracking indexed containers)."""

    FOLDER = 2
    """A folder container (used for tracking indexed containers)."""

    FILE = 3
    """A file entity."""

    TABLE_ATTACHED_FILE = 4
    """A file attached to a table column."""

    @classmethod
    def from_concrete_type(cls, concrete_type: str) -> "MigrationType":
        """Convert a Synapse concrete type string to a MigrationType.

        Arguments:
            concrete_type: The concrete type string from Synapse API.

        Returns:
            The corresponding MigrationType enum value.

        Raises:
            ValueError: If the concrete type is not recognized.
        """
        if concrete_type == concrete_types.PROJECT_ENTITY:
            return cls.PROJECT
        elif concrete_type == concrete_types.FOLDER_ENTITY:
            return cls.FOLDER
        elif concrete_type == concrete_types.FILE_ENTITY:
            return cls.FILE
        elif concrete_type == concrete_types.TABLE_ENTITY:
            return cls.TABLE_ATTACHED_FILE

        raise ValueError(f"Unhandled concrete type: {concrete_type}")


@dataclass
class MigrationKey:
    """Unique identifier for a migration entry in the tracking database.

    Attributes:
        id: The Synapse entity ID.
        type: The type of entity being migrated.
        version: The file version number (None for new versions or containers).
        row_id: The table row ID (for table attached files).
        col_id: The table column ID (for table attached files).
    """

    id: str
    type: MigrationType
    version: Optional[int] = None
    row_id: Optional[int] = None
    col_id: Optional[int] = None

    def __hash__(self) -> int:
        return hash((self.id, self.type, self.version, self.row_id, self.col_id))

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, MigrationKey):
            return False
        return (
            self.id == other.id
            and self.type == other.type
            and self.version == other.version
            and self.row_id == other.row_id
            and self.col_id == other.col_id
        )


@dataclass
class MigrationEntry:
    """A single migration entry with full details.

    Attributes:
        key: The unique identifier for this migration entry.
        parent_id: The parent entity ID.
        from_storage_location_id: The original storage location ID.
        from_file_handle_id: The original file handle ID.
        to_file_handle_id: The new file handle ID after migration.
        file_size: The file size in bytes.
        status: The current migration status.
        exception: Stack trace if an error occurred.
    """

    key: MigrationKey
    parent_id: Optional[str] = None
    from_storage_location_id: Optional[int] = None
    from_file_handle_id: Optional[str] = None
    to_file_handle_id: Optional[str] = None
    file_size: Optional[int] = None
    status: MigrationStatus = MigrationStatus.INDEXED
    exception: Optional[str] = None


@dataclass
class MigrationSettings:
    """Settings for a migration index stored in the database.

    Attributes:
        root_id: The root entity ID being migrated.
        dest_storage_location_id: The destination storage location ID.
        source_storage_location_ids: List of source storage location IDs to filter.
        file_version_strategy: Strategy for handling file versions.
        include_table_files: Whether to include files attached to tables.
    """

    root_id: str
    dest_storage_location_id: str
    source_storage_location_ids: List[str] = field(default_factory=list)
    file_version_strategy: str = "new"
    include_table_files: bool = False


@dataclass
class MigrationResult:
    """Result of a migration operation - proxy to the SQLite tracking database.

    This class provides methods to query the migration database for status counts,
    individual migration entries, and CSV export.

    Attributes:
        db_path: Path to the SQLite database file.
        synapse_client: Optional Synapse client for column name lookups.
    """

    db_path: str
    synapse_client: Optional["Synapse"] = None

    @property
    def counts_by_status(self) -> Dict[str, int]:
        """Get counts by migration status (synchronous).

        Returns:
            Dictionary mapping status names to counts.
        """
        return self.get_counts_by_status()

    def get_counts_by_status(self) -> Dict[str, int]:
        """Get counts by migration status (synchronous).

        Returns:
            Dictionary mapping status names to counts.
        """
        import sqlite3

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            # Only count FILE and TABLE_ATTACHED_FILE entries
            result = cursor.execute(
                "SELECT status, count(*) FROM migrations "
                "WHERE type IN (?, ?) GROUP BY status",
                (MigrationType.FILE.value, MigrationType.TABLE_ATTACHED_FILE.value),
            )

            counts = {status.name: 0 for status in MigrationStatus}
            for row in result:
                status_value = row[0]
                count = row[1]
                counts[MigrationStatus(status_value).name] = count

            return counts

    async def get_counts_by_status_async(self) -> Dict[str, int]:
        """Get counts by migration status (asynchronous).

        Returns:
            Dictionary mapping status names to counts.
        """
        return await asyncio.to_thread(self.get_counts_by_status)

    def get_migrations(self) -> Iterator[Dict[str, Any]]:
        """Iterate over all migration entries (synchronous).

        Yields:
            Dictionary for each migration entry with keys:
            id, type, version, row_id, col_name, from_storage_location_id,
            from_file_handle_id, to_file_handle_id, file_size, status, exception.
        """
        import sqlite3

        with sqlite3.connect(self.db_path) as conn:
            cursor = conn.cursor()

            batch_size = 500
            rowid = -1
            column_names_cache: Dict[int, str] = {}

            while True:
                results = cursor.execute(
                    """
                    SELECT
                        rowid,
                        id,
                        type,
                        version,
                        row_id,
                        col_id,
                        from_storage_location_id,
                        from_file_handle_id,
                        to_file_handle_id,
                        file_size,
                        status,
                        exception
                    FROM migrations
                    WHERE
                        rowid > ?
                        AND type IN (?, ?)
                    ORDER BY rowid
                    LIMIT ?
                    """,
                    (
                        rowid,
                        MigrationType.FILE.value,
                        MigrationType.TABLE_ATTACHED_FILE.value,
                        batch_size,
                    ),
                )

                rows = results.fetchall()
                if not rows:
                    break

                for row in rows:
                    rowid = row[0]
                    col_id = row[5]

                    # Resolve column name if needed
                    col_name = None
                    if col_id is not None and self.synapse_client:
                        if col_id not in column_names_cache:
                            try:
                                col_info = self.synapse_client.restGET(
                                    f"/column/{col_id}"
                                )
                                column_names_cache[col_id] = col_info.get("name", "")
                            except Exception:
                                column_names_cache[col_id] = ""
                        col_name = column_names_cache[col_id]

                    yield {
                        "id": row[1],
                        "type": (
                            "file" if row[2] == MigrationType.FILE.value else "table"
                        ),
                        "version": row[3],
                        "row_id": row[4],
                        "col_name": col_name,
                        "from_storage_location_id": row[6],
                        "from_file_handle_id": row[7],
                        "to_file_handle_id": row[8],
                        "file_size": row[9],
                        "status": MigrationStatus(row[10]).name,
                        "exception": row[11],
                    }

    async def get_migrations_async(self) -> List[Dict[str, Any]]:
        """Get all migration entries (asynchronous).

        Returns:
            List of dictionaries for each migration entry.
        """
        # Convert to list since generators can't be returned from to_thread
        return await asyncio.to_thread(lambda: list(self.get_migrations()))

    def as_csv(self, path: str) -> None:
        """Export migration results to a CSV file (synchronous).

        Arguments:
            path: Path to write the CSV file.
        """
        fieldnames = [
            "id",
            "type",
            "version",
            "row_id",
            "col_name",
            "from_storage_location_id",
            "from_file_handle_id",
            "to_file_handle_id",
            "file_size",
            "status",
            "exception",
        ]

        with open(path, "w", newline="") as csvfile:
            writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
            writer.writeheader()
            for migration in self.get_migrations():
                writer.writerow(migration)

    async def as_csv_async(self, path: str) -> None:
        """Export migration results to a CSV file (asynchronous).

        Arguments:
            path: Path to write the CSV file.
        """
        await asyncio.to_thread(self.as_csv, path)


class MigrationError(Exception):
    """Error during a migration operation.

    Attributes:
        key: The migration key that failed.
        from_file_handle_id: The source file handle ID.
        to_file_handle_id: The destination file handle ID (if partially complete).
    """

    def __init__(
        self,
        key: MigrationKey,
        from_file_handle_id: str,
        to_file_handle_id: Optional[str] = None,
    ):
        self.key = key
        self.from_file_handle_id = from_file_handle_id
        self.to_file_handle_id = to_file_handle_id
        super().__init__(f"Migration failed for {key.id}")


class IndexingError(Exception):
    """Error during an indexing operation.

    Attributes:
        entity_id: The entity ID that failed to index.
        concrete_type: The concrete type of the entity.
    """

    def __init__(self, entity_id: str, concrete_type: str):
        self.entity_id = entity_id
        self.concrete_type = concrete_type
        super().__init__(f"Indexing failed for {entity_id} ({concrete_type})")
