"""
The manifest, plan and outcome types for bulk curation tasks.

A task manifest is a CSV with one row per curation task. `CurationTaskManifest` reads
and checks it locally, before Synapse is contacted. `TaskPlan` says what a run would
do for one row, and `TaskOutcome` what it did.
"""

import csv
import os
import re
from collections.abc import Iterable
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import NamedTuple

from synapseclient.models import AuthorizationMode, TaskState

_SYNAPSE_ID = re.compile(r"syn\d+")
_AUTHORIZATION_MODES = tuple(mode.value for mode in AuthorizationMode)

_REQUIRED_COLUMNS = (
    "name",
    "project_id",
    "folder_id",
    "curation_task_name",
    "schema_uri",
    "upsert_keys",
    "assignee_principal_id",
    "authorization_mode",
    "instructions",
    "path",
)
_OPTIONAL_COLUMNS = ("record_set_name", "leave_out_columns", "archive")
_LIST_COLUMNS = ("upsert_keys", "leave_out_columns", "archive")
_ALL_COLUMNS = frozenset(_REQUIRED_COLUMNS + _OPTIONAL_COLUMNS)


class CsvRecord(NamedTuple):
    """A CSV record and the physical line where it starts."""

    line: int
    cells: list[str]


def read_csv_records(path: str | Path) -> tuple[list[str], list[CsvRecord]]:
    """Read a UTF-8 CSV, tolerating a byte-order mark, with every cell as read.

    Arguments:
        path: The CSV file.

    Returns:
        The header (empty for an empty file) and every later record, blank ones
        included, each with the physical line where it starts. A quoted value that
        spans lines does not shift the line of the records after it.
    """
    with open(path, newline="", encoding="utf-8-sig") as file:
        reader = csv.reader(file)
        records = []
        start = 1
        for cells in reader:
            records.append(CsvRecord(start, cells))
            start = reader.line_num + 1
    if not records:
        return [], []
    return records[0].cells, records[1:]


def _split_list(cell: str) -> list[str]:
    """Split a list cell on `;`, stripping each item and dropping empty ones."""
    return [item.strip() for item in cell.split(";") if item.strip()]


def _is_blank(value: object) -> bool:
    """Whether a row value counts as missing: None, whitespace, or a list of those."""
    if value is None:
        return True
    if isinstance(value, str):
        return not value.strip()
    if isinstance(value, (list, tuple)):
        return all(_is_blank(item) for item in value)
    return False


class CurationManifestError(ValueError):
    """A task manifest, or a selection from one, was rejected.

    Attributes:
        problems: Every problem found, each naming its row.
    """

    def __init__(self, problems: list[str]) -> None:
        self.problems = list(problems)
        super().__init__(
            "The task manifest was rejected:\n"
            + "\n".join(f"  - {problem}" for problem in self.problems)
        )


@dataclass(kw_only=True)
class CurationTaskRow:
    """One manifest row: everything needed to create and fill one curation task.

    Field names are the manifest's column names. A `str` given for `path` becomes a
    `Path`, and one of the mode names given for `authorization_mode` becomes an
    `AuthorizationMode`.

    Attributes:
        name: Short name used to select the task; unique in the manifest.
        project_id: The Synapse ID of the task's project.
        folder_id: The Synapse ID of the folder that holds the task's table.
        curation_task_name: The task's name in Curator; unique within its project.
        record_set_name: The table's name. Blank means `curation_task_name`.
        schema_uri: A registered schema with its version, `ORG-NAME-VERSION`.
        upsert_keys: The key columns; at least one.
        assignee_principal_id: The ID of the team the task is assigned to.
        authorization_mode: Who may open the task's grid session.
        instructions: Instructions contributors see.
        path: The input CSV. Absolute when read from a manifest file.
        leave_out_columns: Input columns not to load.
        archive: Curation task names in the same project to archive.
        line: The manifest line the row starts on, or None for a row built in code.

    Example: Build a row in code
        &nbsp;
        A manifest can be built in code instead of read from a CSV.
        ```python
        import synapseclient
        from synapseclient.extensions.curator import (
            CurationTaskManifest,
            CurationTaskRow,
        )
        from synapseclient.models import AuthorizationMode, TaskState

        syn = synapseclient.Synapse()
        syn.login()

        row = CurationTaskRow(
            name="lab_a",
            project_id="syn111",
            folder_id="syn112",
            curation_task_name="Lab A biospecimens",
            schema_uri="org.namhub-biospecimen-1.2.0",
            upsert_keys=["specimenID"],
            assignee_principal_id="3412345",
            authorization_mode=AuthorizationMode.SOURCE_BENEFACTOR,
            instructions="Fill one row per specimen.",
            path="/data/lab_a.csv",
        )
        manifest = CurationTaskManifest(rows=[row])
        manifest.check()
        ```
    """

    name: str
    """Short name used to select the task; unique in the manifest."""

    project_id: str
    """The Synapse ID of the task's project."""

    folder_id: str
    """The Synapse ID of the folder that holds the task's table."""

    curation_task_name: str
    """The task's name in Curator; unique within its project."""

    record_set_name: str = ""
    """The table's name. Blank means `curation_task_name`."""

    schema_uri: str
    """A registered schema with its version, `ORG-NAME-VERSION`."""

    upsert_keys: list[str]
    """The key columns; at least one."""

    assignee_principal_id: str
    """The ID of the team the task is assigned to."""

    authorization_mode: AuthorizationMode
    """Who may open the task's grid session."""

    instructions: str
    """Instructions contributors see."""

    path: Path
    """The input CSV. Absolute when read from a manifest file."""

    leave_out_columns: list[str] = field(default_factory=list)
    """Input columns not to load."""

    archive: list[str] = field(default_factory=list)
    """Curation task names in the same project to archive."""

    line: int | None = None
    """The manifest line the row starts on, or None for a row built in code."""

    def __post_init__(self) -> None:
        if _is_blank(self.record_set_name) and not _is_blank(self.curation_task_name):
            self.record_set_name = self.curation_task_name
        if isinstance(self.path, str):
            self.path = Path(self.path) if self.path.strip() else None
        if (
            isinstance(self.authorization_mode, str)
            and not isinstance(self.authorization_mode, AuthorizationMode)
            and self.authorization_mode in _AUTHORIZATION_MODES
        ):
            self.authorization_mode = AuthorizationMode(self.authorization_mode)


@dataclass
class CurationTaskManifest:
    """The rows of a task manifest, read from a CSV or built in code.

    Attributes:
        rows: One row per curation task, in manifest order.
        path: The manifest file, or None for a manifest built in code.

    Example: Read and check a manifest
        &nbsp;
        Read a manifest CSV; it is checked as it is read.
        ```python
        import synapseclient
        from synapseclient.extensions.curator import (
            CurationTaskManifest,
        )

        syn = synapseclient.Synapse()
        syn.login()

        manifest = CurationTaskManifest.from_csv("tasks.csv")
        for row in manifest.rows:
            print(row.name, row.curation_task_name, row.path)
        ```
    """

    rows: list[CurationTaskRow]
    """One row per curation task, in manifest order."""

    path: Path | None = None
    """The manifest file, or None for a manifest built in code."""

    @classmethod
    def from_csv(cls, path: str | Path) -> "CurationTaskManifest":
        """Read a task manifest CSV and check it.

        Cells are stripped of surrounding whitespace. `upsert_keys`,
        `leave_out_columns` and `archive` are split on `;`. A blank
        `record_set_name` becomes `curation_task_name`, and `path` is resolved
        against the manifest's folder. Rows whose cells are all blank are skipped.

        Arguments:
            path: The manifest CSV.

        Returns:
            The manifest, with each row's line set.

        Raises:
            CurationManifestError: Listing every problem in the manifest.

        Example: Read a manifest
            &nbsp;
            Read a manifest and list its tasks.
            ```python
            import synapseclient
            from synapseclient.extensions.curator import (
                CurationTaskManifest,
            )

            syn = synapseclient.Synapse()
            syn.login()

            manifest = CurationTaskManifest.from_csv("tasks.csv")
            print([row.name for row in manifest.rows])
            ```
        """
        manifest_path = Path(os.path.abspath(path))
        header, records = read_csv_records(manifest_path)
        header = [cell.strip() for cell in header]
        problems = []

        if not header:
            problems.append("line 1: the manifest has no header row")
        seen = set()
        for position, column in enumerate(header, start=1):
            if not column:
                problems.append(f"line 1: column {position} has no name")
            elif column in seen:
                problems.append(f"line 1: column '{column}' appears more than once")
            elif column not in _ALL_COLUMNS:
                problems.append(f"line 1: unknown column '{column}'")
            seen.add(column)
        missing = [column for column in _REQUIRED_COLUMNS if column not in seen]
        if header:
            problems.extend(
                f"line 1: missing required column '{column}'" for column in missing
            )

        rows = []
        for record in records:
            cells = [cell.strip() for cell in record.cells]
            if not any(cells):
                continue
            if len(cells) > len(header):
                problems.append(
                    f"line {record.line}: the row has {len(cells)} cells but the "
                    f"header has {len(header)}"
                )
                continue
            values = dict.fromkeys(_ALL_COLUMNS, "")
            for column, cell in zip(header, cells):
                if column in _ALL_COLUMNS:
                    values[column] = cell
            rows.append(
                CurationTaskRow(
                    name=values["name"],
                    project_id=values["project_id"],
                    folder_id=values["folder_id"],
                    curation_task_name=values["curation_task_name"],
                    record_set_name=values["record_set_name"],
                    schema_uri=values["schema_uri"],
                    upsert_keys=_split_list(values["upsert_keys"]),
                    assignee_principal_id=values["assignee_principal_id"],
                    authorization_mode=values["authorization_mode"],
                    instructions=values["instructions"],
                    path=cls._resolve_input_path(values["path"], manifest_path.parent),
                    leave_out_columns=_split_list(values["leave_out_columns"]),
                    archive=_split_list(values["archive"]),
                    line=record.line,
                )
            )

        manifest = cls(rows=rows, path=manifest_path)
        problems.extend(manifest._find_problems(columns_missing=missing))
        if problems:
            raise CurationManifestError(problems)
        return manifest

    @staticmethod
    def _resolve_input_path(cell: str, folder: Path) -> Path | None:
        """Expand `~` and environment variables, then resolve against `folder`."""
        if not cell:
            return None
        expanded = Path(os.path.expandvars(os.path.expanduser(cell)))
        return Path(os.path.abspath(folder / expanded))

    def check(self) -> None:
        """Check the manifest's rows without contacting Synapse.

        A row is named by its manifest line, or by its position in `rows` when it
        was built in code.

        Raises:
            CurationManifestError: Listing every problem: a blank required value, a
                malformed Synapse ID or authorization mode, a missing input file, a
                duplicate `name`, two rows naming the same task in one project or
                the same table in one folder, or a task that a row archives in the
                same project.

        Example: Check a manifest built in code
            &nbsp;
            Report every problem in a manifest assembled from rows.
            ```python
            import synapseclient
            from synapseclient.extensions.curator import (
                CurationManifestError,
                CurationTaskManifest,
            )

            syn = synapseclient.Synapse()
            syn.login()

            manifest = CurationTaskManifest(rows=my_rows)
            try:
                manifest.check()
            except CurationManifestError as error:
                for problem in error.problems:
                    print(problem)
            ```
        """
        problems = self._find_problems()
        if problems:
            raise CurationManifestError(problems)

    def select(self, names: list[str] | None) -> list[CurationTaskRow]:
        """Pick rows by `name`.

        Arguments:
            names: The names to pick, or None for every row.

        Returns:
            The rows named, once each, in manifest order.

        Raises:
            CurationManifestError: Listing every name no row defines.

        Example: Pick two tasks
            &nbsp;
            Select the rows for two tasks from a manifest.
            ```python
            import synapseclient
            from synapseclient.extensions.curator import (
                CurationTaskManifest,
            )

            syn = synapseclient.Synapse()
            syn.login()

            manifest = CurationTaskManifest.from_csv("tasks.csv")
            rows = manifest.select(["lab_a", "lab_b"])
            ```
        """
        if names is None:
            return list(self.rows)
        defined = {row.name for row in self.rows}
        undefined = list(dict.fromkeys(name for name in names if name not in defined))
        if undefined:
            raise CurationManifestError(
                [f"no task named '{name}' in the manifest" for name in undefined]
            )
        wanted = set(names)
        return [row for row in self.rows if row.name in wanted]

    def _where(self, index: int) -> str:
        """Name a row in a message: its line, or its position, and its `name`."""
        row = self.rows[index]
        where = f"line {row.line}" if row.line is not None else f"row {index + 1}"
        if isinstance(row.name, str) and row.name.strip():
            where += f" ({row.name})"
        return where

    def _find_problems(self, columns_missing: Iterable[str] = ()) -> list[str]:
        """List every problem in the rows, in row order.

        Arguments:
            columns_missing: Required columns the manifest file lacks, whose blank
                values are already reported once as a missing column.
        """
        skip_blank = set(columns_missing)
        problems: list[list[str]] = [[] for _ in self.rows]
        first_name: dict[str, int] = {}
        first_task: dict[tuple[str, str], int] = {}
        first_table: dict[tuple[str, str], int] = {}
        archived: dict[tuple[str, str], int] = {}

        for index, row in enumerate(self.rows):
            for task_name in _archive_targets(row):
                archived.setdefault((row.project_id, task_name), index)

        for index, row in enumerate(self.rows):
            where = self._where(index)
            found = problems[index]
            for column in _REQUIRED_COLUMNS:
                if column not in skip_blank and _is_blank(getattr(row, column)):
                    found.append(f"{where}: required value '{column}' is blank")
            for column in ("project_id", "folder_id"):
                value = getattr(row, column)
                if not _is_blank(value) and not (
                    isinstance(value, str) and _SYNAPSE_ID.fullmatch(value)
                ):
                    found.append(
                        f"{where}: {column} '{value}' is not a Synapse ID (synNNN)"
                    )
            mode = row.authorization_mode
            if not _is_blank(mode) and mode not in _AUTHORIZATION_MODES:
                found.append(
                    f"{where}: authorization_mode '{mode}' is not "
                    + " or ".join(_AUTHORIZATION_MODES)
                )
            if not _is_blank(row.path) and not Path(row.path).is_file():
                found.append(f"{where}: input file {row.path} does not exist")

            if not _is_blank(row.name):
                other = first_name.setdefault(row.name, index)
                if other != index:
                    found.append(
                        f"{where}: name '{row.name}' is also used by "
                        f"{self._where(other)}"
                    )
            if not _is_blank(row.project_id) and not _is_blank(row.curation_task_name):
                task = (row.project_id, row.curation_task_name)
                other = first_task.setdefault(task, index)
                if other != index:
                    found.append(
                        f"{where}: curation task '{row.curation_task_name}' in "
                        f"{row.project_id} is also defined by {self._where(other)}"
                    )
                archiver = archived.get(task)
                if archiver == index:
                    found.append(
                        f"{where}: archive lists the row's own curation task "
                        f"'{row.curation_task_name}'"
                    )
                elif archiver is not None:
                    found.append(
                        f"{where}: curation task '{row.curation_task_name}' in "
                        f"{row.project_id} is archived by {self._where(archiver)}"
                    )
            if not _is_blank(row.folder_id) and not _is_blank(row.record_set_name):
                table = (row.folder_id, row.record_set_name)
                other = first_table.setdefault(table, index)
                if other != index:
                    found.append(
                        f"{where}: table '{row.record_set_name}' in "
                        f"{row.folder_id} is also defined by {self._where(other)}"
                    )
        return [problem for found in problems for problem in found]


def _archive_targets(row: CurationTaskRow) -> list[str]:
    """The non-blank names in a row's `archive` list."""
    return [name for name in row.archive or [] if not _is_blank(name)]


class TaskAction(str, Enum):
    """What a run would do with a manifest row's curation task."""

    CREATE = "CREATE"
    """No task of that name exists in the project; one would be created."""

    EXISTS = "EXISTS"
    """A task of that name exists and would be filled."""

    BLOCKED = "BLOCKED"
    """A blocking problem was found; nothing would be written to the task."""


class ArchiveResult(str, Enum):
    """What happens to an old task that a manifest row names in `archive`."""

    ARCHIVE = "ARCHIVE"
    """The task is active and will be archived once the row's task is in place."""

    ALREADY_ARCHIVED = "ALREADY_ARCHIVED"
    """The task is already archived; nothing is done to it."""

    NOT_FOUND = "NOT_FOUND"
    """No task of that name exists in the project."""

    ARCHIVED = "ARCHIVED"
    """The task was archived in this run."""

    FAILED = "FAILED"
    """Archiving the task failed."""


class ProblemKind(str, Enum):
    """Where a problem was found."""

    INPUT = "INPUT"
    """In the row's input file."""

    SYNAPSE = "SYNAPSE"
    """In Synapse's current state, or in a call to Synapse."""

    VALIDATION = "VALIDATION"
    """In Synapse's validation of a row against the task's schema."""


class TaskResult(str, Enum):
    """What applying a manifest row did."""

    CREATED = "CREATED"
    """The task was created, and filled when its file had rows."""

    UPDATED = "UPDATED"
    """Rows were written to a task that existed."""

    UNCHANGED = "UNCHANGED"
    """The task existed and nothing was written to it."""

    BLOCKED = "BLOCKED"
    """The plan was blocked; nothing was written."""

    FAILED = "FAILED"
    """An error stopped the task, or the table read back did not hold what was
    written."""


@dataclass(kw_only=True)
class CurationProblem:
    """Something wrong with one task, found in its input file or in Synapse.

    Attributes:
        task: The manifest row's `name`.
        kind: Where the problem was found.
        blocking: Whether the problem stops the task. False only for validation
            problems.
        message: What is wrong, naming the task and, where they apply, the file,
            line, key and column. For validation, Synapse's messages joined.
        file: The input file, or None.
        line: The input file line the row starts on, or None.
        key: The row's key, key column to value, or None.
        column: The column, or None.
    """

    task: str
    """The manifest row's `name`."""

    kind: ProblemKind
    """Where the problem was found."""

    blocking: bool = True
    """Whether the problem stops the task. False only for validation problems."""

    message: str
    """What is wrong, naming the task and, where they apply, the file, line, key and
    column. For validation, Synapse's messages joined."""

    file: str | None = None
    """The input file, or None."""

    line: int | None = None
    """The input file line the row starts on, or None."""

    key: dict[str, str] | None = None
    """The row's key, key column to value, or None."""

    column: str | None = None
    """The column, or None."""


@dataclass(kw_only=True)
class RowDifference:
    """A file row whose key is in the table and whose non-blank cells differ from it.

    Attributes:
        key: The row's key, key column to value.
        line: The input file line the row starts on.
        columns: Each differing column, mapped to its file value and its Synapse
            value.
    """

    key: dict[str, str]
    """The row's key, key column to value."""

    line: int
    """The input file line the row starts on."""

    columns: dict[str, tuple[str, str]]
    """Each differing column, mapped to its file value and its Synapse value."""


@dataclass(kw_only=True)
class RowChanges:
    """How a task's input file compares with its table, row by row.

    Attributes:
        to_add: File rows whose key is not in the table.
        unchanged: File rows that match the table.
        differing: File rows that differ from the table; left alone unless
            overwriting.
        absent_from_file: Keys in the table that the file lacks; left in place.
    """

    to_add: int = 0
    """File rows whose key is not in the table."""

    unchanged: int = 0
    """File rows that match the table."""

    differing: list[RowDifference] = field(default_factory=list)
    """File rows that differ from the table; left alone unless overwriting."""

    absent_from_file: list[dict[str, str]] = field(default_factory=list)
    """Keys in the table that the file lacks; left in place."""


@dataclass(kw_only=True)
class ArchiveEntry:
    """An old task that a manifest row names in `archive`, and what happens to it.

    Attributes:
        curation_task_name: The old task's name.
        task_id: The old task's ID, or None when it was not found.
        result: What is planned for the old task, or what was done to it.
        error: The error that stopped archiving, or None.
    """

    curation_task_name: str
    """The old task's name."""

    task_id: int | None = None
    """The old task's ID, or None when it was not found."""

    result: ArchiveResult
    """What is planned for the old task, or what was done to it."""

    error: str | None = None
    """The error that stopped archiving, or None."""


@dataclass(kw_only=True)
class TaskPlan:
    """What a run would do for one manifest row, worked out without changing Synapse.

    Attributes:
        row: The manifest row.
        action: Create the task, fill the one that exists, or nothing because a
            problem blocks it.
        task_id: The existing task's ID, or None.
        record_set_id: The existing task's table, or None.
        state: The existing task's state, or None.
        attached_session_id: The existing task's attached grid session, when that
            session still exists, or None.
        setting_differences: Each setting of an existing task that differs from the
            manifest, mapped to the manifest's value and Synapse's. Key columns are
            joined with `;`.
        key_columns: The columns rows are matched on: the manifest's `upsert_keys`
            for a task to create, the table's own for a task that exists.
        table_columns: The table's columns: for a task to create, the key columns,
            then the file's other columns in file order, then the schema properties
            the file lacks; for a task that exists, the table's header.
        missing_columns: Schema properties the file lacks.
        skipped_columns: Schema properties in the file that an existing table's
            header lacks; they are not loaded.
        input_empty: Whether the input file has no data rows.
        rows: How the file's rows compare with the table.
        archives: The old tasks the row names in `archive`.
        problems: What is wrong with the task; each one blocks it.
        overwrite: Whether the run overwrites differing rows.
    """

    row: CurationTaskRow
    """The manifest row."""

    action: TaskAction
    """Create the task, fill the one that exists, or nothing because a problem
    blocks it."""

    task_id: int | None = None
    """The existing task's ID, or None."""

    record_set_id: str | None = None
    """The existing task's table, or None."""

    state: TaskState | None = None
    """The existing task's state, or None."""

    attached_session_id: str | None = None
    """The existing task's attached grid session, when that session still exists, or
    None."""

    setting_differences: dict[str, tuple[str, str]] = field(default_factory=dict)
    """Each setting of an existing task that differs from the manifest, mapped to the
    manifest's value and Synapse's. Key columns are joined with `;`."""

    key_columns: list[str] = field(default_factory=list)
    """The columns rows are matched on: the manifest's `upsert_keys` for a task to
    create, the table's own for a task that exists."""

    table_columns: list[str] = field(default_factory=list)
    """The table's columns: for a task to create, the key columns, then the file's
    other columns in file order, then the schema properties the file lacks; for a
    task that exists, the table's header."""

    missing_columns: list[str] = field(default_factory=list)
    """Schema properties the file lacks."""

    skipped_columns: list[str] = field(default_factory=list)
    """Schema properties in the file that an existing table's header lacks; they are
    not loaded."""

    input_empty: bool = False
    """Whether the input file has no data rows."""

    rows: RowChanges = field(default_factory=RowChanges)
    """How the file's rows compare with the table."""

    archives: list[ArchiveEntry] = field(default_factory=list)
    """The old tasks the row names in `archive`."""

    problems: list[CurationProblem] = field(default_factory=list)
    """What is wrong with the task; each one blocks it."""

    overwrite: bool = False
    """Whether the run overwrites differing rows."""

    @property
    def blocked(self) -> bool:
        """Whether any problem blocks the task."""
        return any(problem.blocking for problem in self.problems)

    @property
    def rows_to_write(self) -> int:
        """The rows a run would write: new rows, plus differing rows when
        overwriting."""
        return self.rows.to_add + (len(self.rows.differing) if self.overwrite else 0)


@dataclass(kw_only=True)
class TaskOutcome:
    """What applying one manifest row did.

    Attributes:
        plan: The plan that was carried out.
        result: What happened to the task.
        task_id: The created or existing task's ID, or None.
        record_set_id: The task's table, or None.
        rows_written: The rows written to the table.
        validation_problems: Synapse's validation problems in the rows, as
            non-blocking problems.
        errors: Synapse errors, import or synchronize errors, and read-back
            failures: a written row missing, or an overwritten cell not holding the
            value written.
    """

    plan: TaskPlan
    """The plan that was carried out."""

    result: TaskResult
    """What happened to the task."""

    task_id: int | None = None
    """The created or existing task's ID, or None."""

    record_set_id: str | None = None
    """The task's table, or None."""

    rows_written: int = 0
    """The rows written to the table."""

    validation_problems: list[CurationProblem] = field(default_factory=list)
    """Synapse's validation problems in the rows, as non-blocking problems."""

    errors: list[str] = field(default_factory=list)
    """Synapse errors, import or synchronize errors, and read-back failures: a written
    row missing, or an overwritten cell not holding the value written."""
