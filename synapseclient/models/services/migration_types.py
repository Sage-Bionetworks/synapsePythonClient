"""
Data classes and enums for the async migration service.

These types are used to track the state of file migrations between storage locations.
"""

import asyncio
import csv
from dataclasses import dataclass, fields
from enum import Enum
from typing import TYPE_CHECKING, Any, Dict, Iterator, List, Optional

from synapseclient.core.constants import concrete_types

if TYPE_CHECKING:
    from synapseclient import Synapse


class MigrationStatus(Enum):
    """Internal enum used by the SQLite database to track the state of entities during indexing and migration."""

    INDEXED = 1
    """The file has been indexed and is ready to be migrated."""

    MIGRATED = 2
    """The file has been successfully migrated to the new storage location."""

    ALREADY_MIGRATED = 3
    """The file was already at the destination storage location and no migration is needed."""

    ERRORED = 4
    """An error occurred during indexing or migration for this entity."""


class MigrationType(Enum):
    """Type of entity being tracked in the migration database.
    Container types (projects and folders) are only used during the indexing phase.
    we record the containers we've indexed so we don't reindex them on a subsequent
    run using the same db file (or reindex them after an indexing dry run)"""

    PROJECT = 1
    """A project entity."""

    FOLDER = 2
    """A folder entity."""

    FILE = 3
    """A file entity."""

    TABLE_ATTACHED_FILE = 4
    """A file handle that is attached to a table column."""

    @classmethod
    def from_concrete_type(cls, concrete_type: str) -> "MigrationType":
        """Convert a Synapse concrete type string to a MigrationType.

        Arguments:
            concrete_type: The concrete type of the entity.

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


@dataclass(frozen=True)
class MigrationKey:
    """Unique identifier for a entry in the migrations database.

    Attributes:
        id: The Synapse entity ID.
        type: The migration type of entity being migrated.
        version: The file version number (None for new versions or containers). #TODO double check if versions are NONE for containers
        row_id: The table row ID (for table attached files).
        col_id: The table column ID (for table attached files).
    """

    id: str
    type: MigrationType
    version: Optional[int] = None
    row_id: Optional[int] = None
    col_id: Optional[int] = None


@dataclass
class MigrationSettings:
    """Settings for a migration index stored in the database.

    Attributes:
        root_id: The root entity ID being migrated.
        dest_storage_location_id: The destination storage location ID.
        source_storage_location_ids: List of of storage location ids that will be migrated.
        file_version_strategy: Strategy for handling file versions.
        include_table_files: Whether to include files attached to tables.
    """

    root_id: str
    dest_storage_location_id: str
    source_storage_location_ids: List[str] = None
    file_version_strategy: str = "new"
    include_table_files: bool = False

    def to_dict(self) -> Dict[str, Any]:
        """Return a dict suitable for JSON serialization in the database."""
        return {
            "root_id": self.root_id,
            "dest_storage_location_id": self.dest_storage_location_id,
            "source_storage_location_ids": self.source_storage_location_ids,
            "file_version_strategy": self.file_version_strategy,
            "include_table_files": 1 if self.include_table_files else 0,
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "MigrationSettings":
        """Build MigrationSettings from a dict (e.g. from JSON in the database)."""
        include = d.get("include_table_files", False)
        if isinstance(include, int):
            include = bool(include)
        return cls(
            root_id=d["root_id"],
            dest_storage_location_id=d["dest_storage_location_id"],
            source_storage_location_ids=d.get("source_storage_location_ids") or [],
            file_version_strategy=d.get("file_version_strategy", "new"),
            include_table_files=include,
        )

    def verify_migration_settings(
        self, existing_settings: "MigrationSettings", db_path: str
    ) -> None:
        """Raise ValueError if the migration settings do not match the existing settings"""
        # compare all fields
        for field in fields(self):
            if getattr(self, field.name) != getattr(existing_settings, field.name):
                # we can't resume indexing with an existing index file using a different setting.
                raise ValueError(
                    "Index parameter does not match the setting recorded in the existing index file. "
                    "To change the index settings start over by deleting the file or using a different path. "
                    f"Expected {field.name} {getattr(existing_settings, field.name)}, found {getattr(self, field.name)} in index file {db_path}"
                )


class IndexingError(Exception):
    """Error during an indexing operation.

    Attributes:
        entity_id: The entity ID that failed to index.
        concrete_type: The concrete type of the entity.
    """

    def __init__(self, entity_id: str, concrete_type: str):
        self.entity_id = entity_id
        self.concrete_type = concrete_type


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
        cause: Optional[Exception] = None,
    ):
        self.key = key
        self.from_file_handle_id = from_file_handle_id
        self.to_file_handle_id = to_file_handle_id
        message = f"Migration failed for {key.id}"
        if cause is not None:
            message += f": {cause}"
        super().__init__(message)
