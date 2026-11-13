"""
Create and fill many record-based curation tasks from one manifest.

`plan_curation_tasks_async` works out what each manifest row would do in Synapse and
changes nothing; `apply_curation_tasks_async` carries each plan out.
"""

import asyncio
import csv
import json
import os
import tempfile
import time
from dataclasses import dataclass, field
from decimal import Decimal, InvalidOperation
from pathlib import Path
from typing import Any, Hashable, NamedTuple, Optional

from synapseclient import Synapse
from synapseclient.core.async_utils import wrap_async_to_sync
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.extensions.curator.curation_task_types import (
    ArchiveEntry,
    ArchiveResult,
    CsvRecord,
    CurationProblem,
    CurationTaskManifest,
    CurationTaskRow,
    ProblemKind,
    RowChanges,
    RowDifference,
    TaskAction,
    TaskOutcome,
    TaskPlan,
    TaskResult,
    read_csv_records,
)
from synapseclient.extensions.curator.record_based_metadata_task import (
    create_record_based_metadata_task_async,
)
from synapseclient.extensions.curator.utils import project_id_from_entity_id_async
from synapseclient.models import (
    ColumnType,
    CurationTask,
    Folder,
    Grid,
    GridExecutionDetails,
    JSONSchema,
    RecordBasedMetadataTaskProperties,
    RecordSet,
    SyncType,
    TaskState,
    Team,
)
from synapseclient.models.curation import GridQuery, GridRow, QueryRequest, SelectAll
from synapseclient.operations import find_entity_id_async

# Input files


class _InputRow(NamedTuple):
    """An input file row: the line it starts on, and its cells by column.

    Left-out columns are dropped, and a blank cell is held as `""`.
    """

    line: int
    cells: dict[str, str]


@dataclass
class _InputFile:
    """An input file as planning reads it.

    Attributes:
        path: The file.
        columns: The header, left-out columns dropped.
        rows: The data rows, blank ones skipped.
        problems: What blocks the task in the file.
        empty: Whether the file was read and has no data rows.
    """

    path: Path
    """The file."""

    columns: list[str] = field(default_factory=list)
    """The header, left-out columns dropped."""

    rows: list[_InputRow] = field(default_factory=list)
    """The data rows, blank ones skipped."""

    problems: list[CurationProblem] = field(default_factory=list)
    """What blocks the task in the file."""

    empty: bool = False
    """Whether the file was read and has no data rows."""


def _is_blank_cell(cell: str) -> bool:
    """Whether an input cell is blank: empty or only whitespace."""
    return not cell.strip()


def _describe_key(key: dict[str, str]) -> str:
    """A key for a message: `specimenID 'S1', individualID 'I1'`."""
    return ", ".join(f"{column} '{value}'" for column, value in key.items())


def _load_input_file(
    row: CurationTaskRow,
    key_columns: list[str],
    schema_properties: list[str] | None,
) -> _InputFile:
    """Read a row's input file and find what in it blocks the task.

    Left-out columns are dropped. A column outside the schema blocks the task (FR-016),
    as do a key column the file lacks and blank or duplicate key values (FR-017). Key
    columns outside the schema are left to the schema check. A file with no header is
    empty and has nothing to judge.

    Arguments:
        row: The manifest row.
        key_columns: The columns rows are matched on.
        schema_properties: The schema's property names, or None when the schema was
            not read and columns are not judged against it.

    Returns:
        The file's columns, rows and problems.
    """
    loaded = _InputFile(path=row.path)
    file = str(row.path)

    def problem(message: str, **where: Any) -> None:
        line = where.get("line")
        at = f" line {line}" if line is not None else ""
        loaded.problems.append(
            CurationProblem(
                task=row.name,
                kind=ProblemKind.INPUT,
                message=f"{row.name}: input file {file}{at}: {message}",
                file=file,
                **where,
            )
        )

    try:
        header, records = read_csv_records(row.path)
    except (OSError, UnicodeDecodeError, csv.Error) as error:
        loaded.problems.append(
            CurationProblem(
                task=row.name,
                kind=ProblemKind.INPUT,
                message=f"{row.name}: input file {file} could not be read: {error}",
                file=file,
            )
        )
        return loaded
    if not header:
        loaded.empty = True
        return loaded

    leave_out = set(row.leave_out_columns)
    seen = set()
    for position, column in enumerate(header, start=1):
        if _is_blank_cell(column):
            problem(f"column {position} has no name", line=1)
        elif column in seen:
            problem(f"column '{column}' appears more than once", line=1)
        seen.add(column)
    loaded.columns = [column for column in header if column not in leave_out]

    if schema_properties is not None:
        properties = set(schema_properties)
        for column in dict.fromkeys(loaded.columns):
            if (
                not _is_blank_cell(column)
                and column not in properties
                and column not in key_columns
            ):
                problem(
                    f"column '{column}' is not a property of schema {row.schema_uri}; "
                    "list it in leave_out_columns to leave it out",
                    line=1,
                    column=column,
                )

    present_keys = [column for column in key_columns if column in loaded.columns]
    for column in key_columns:
        if column not in loaded.columns:
            problem(f"key column '{column}' is not in the file", column=column)

    first_line_of_key: dict[tuple[str, ...], int] = {}
    for record in records:
        if all(_is_blank_cell(cell) for cell in record.cells):
            continue
        if len(record.cells) > len(header):
            problem(
                f"the row has {len(record.cells)} cells but the header has "
                f"{len(header)}",
                line=record.line,
            )
            continue
        cells = {}
        for position, column in enumerate(header):
            if column in leave_out:
                continue
            cell = record.cells[position] if position < len(record.cells) else ""
            cells[column] = "" if _is_blank_cell(cell) else cell
        loaded.rows.append(_InputRow(record.line, cells))

        key = {column: cells[column] for column in present_keys}
        blank = [column for column in present_keys if not cells[column]]
        for column in blank:
            problem(
                f"key column '{column}' is blank",
                line=record.line,
                key=key,
                column=column,
            )
        if blank or not present_keys:
            continue
        first = first_line_of_key.setdefault(tuple(key.values()), record.line)
        if first != record.line:
            problem(
                f"key {_describe_key(key)} is also on line {first}",
                line=record.line,
                key=key,
                column=present_keys[0] if len(present_keys) == 1 else None,
            )

    loaded.empty = not loaded.rows
    return loaded


# Synapse lookups


class _NotFoundError(Exception):
    """What a lookup was asked for does not exist in Synapse."""


def _is_not_found(error: Exception) -> bool:
    """Whether a Synapse error is a 404."""
    return (
        isinstance(error, SynapseHTTPError)
        and error.response is not None
        and error.response.status_code == 404
    )


class _Schema(NamedTuple):
    """A registered schema's property names, in schema order, and its body."""

    properties: list[str]
    body: dict[str, Any]


class _RunLookups:
    """The Synapse reads one planning run makes, each cached for the run.

    Every method reads only. A method raises `_NotFoundError` when what it is asked for
    does not exist, and lets any other error through.
    """

    def __init__(self, synapse_client: Synapse) -> None:
        self.synapse_client = synapse_client
        self._project_tasks: dict[str, list[CurationTask]] = {}
        self._schemas: dict[str, _Schema] = {}
        self._folder_can_edit: dict[str, bool] = {}
        self._teams: dict[str, Team] = {}
        self._folder_projects: dict[str, str] = {}

    async def project_tasks(self, project_id: str) -> list[CurationTask]:
        """Every curation task in a project."""
        if project_id not in self._project_tasks:
            self._project_tasks[project_id] = [
                task
                async for task in CurationTask.list_async(
                    project_id=project_id, synapse_client=self.synapse_client
                )
            ]
        return self._project_tasks[project_id]

    async def schema(self, schema_uri: str) -> _Schema:
        """The schema `ORG-NAME-VERSION` names, at that version.

        A malformed URI, an unknown organization or schema name, and an unregistered
        version all count as not found.
        """
        if schema_uri not in self._schemas:
            try:
                schema = JSONSchema.from_uri(schema_uri)
                parts = schema_uri.split("-")
                version = parts[2] if len(parts) == 3 else None
                await schema.get_async(synapse_client=self.synapse_client)
                body = await schema.get_body_async(
                    version=version, synapse_client=self.synapse_client
                )
            except ValueError as error:
                raise _NotFoundError(schema_uri) from error
            except SynapseHTTPError as error:
                if _is_not_found(error):
                    raise _NotFoundError(schema_uri) from error
                raise
            properties = body.get("properties")
            self._schemas[schema_uri] = _Schema(
                properties=list(properties) if isinstance(properties, dict) else [],
                body=body,
            )
        return self._schemas[schema_uri]

    async def can_edit_folder(self, folder_id: str) -> bool:
        """Whether the user can edit a folder."""
        if folder_id not in self._folder_can_edit:
            try:
                permissions = await Folder(id=folder_id).get_permissions_async(
                    synapse_client=self.synapse_client
                )
            except SynapseHTTPError as error:
                if _is_not_found(error):
                    raise _NotFoundError(folder_id) from error
                raise
            self._folder_can_edit[folder_id] = bool(permissions.can_edit)
        return self._folder_can_edit[folder_id]

    async def team(self, team_id: str) -> Team:
        """A team by ID."""
        if team_id not in self._teams:
            try:
                self._teams[team_id] = await Team(id=team_id).get_async(
                    synapse_client=self.synapse_client
                )
            except SynapseHTTPError as error:
                if _is_not_found(error):
                    raise _NotFoundError(team_id) from error
                raise
        return self._teams[team_id]

    async def folder_project(self, folder_id: str) -> str:
        """The ID of the project that holds a folder."""
        if folder_id not in self._folder_projects:
            self._folder_projects[folder_id] = await project_id_from_entity_id_async(
                entity_id=folder_id, synapse_client=self.synapse_client
            )
        return self._folder_projects[folder_id]

    async def entity_in_folder(self, folder_id: str, name: str) -> str | None:
        """The ID of the entity named `name` in a folder, or None."""
        return await find_entity_id_async(
            name=name, parent=folder_id, synapse_client=self.synapse_client
        )


# Planning


def _synapse_problem(
    row: CurationTaskRow, message: str, column: str | None = None
) -> CurationProblem:
    """A blocking problem found in Synapse, its message naming the task."""
    return CurationProblem(
        task=row.name,
        kind=ProblemKind.SYNAPSE,
        message=f"{row.name}: {message}",
        column=column,
    )


async def _check_schema(
    row: CurationTaskRow,
    lookups: _RunLookups,
    problems: list[CurationProblem],
    key_columns: list[str],
) -> list[str] | None:
    """Read the row's schema and check the key columns are properties of it.

    Returns:
        The schema's property names, or None when it could not be read.
    """
    try:
        schema = await lookups.schema(row.schema_uri)
    except _NotFoundError:
        problems.append(
            _synapse_problem(
                row, f"schema {row.schema_uri} is not registered in Synapse"
            )
        )
        return None
    except Exception as error:
        problems.append(
            _synapse_problem(
                row, f"Synapse error while reading schema {row.schema_uri}: {error}"
            )
        )
        return None
    for column in key_columns:
        if column not in schema.properties:
            problems.append(
                _synapse_problem(
                    row,
                    f"key column '{column}' is not a property of schema "
                    f"{row.schema_uri}",
                    column=column,
                )
            )
    return schema.properties


async def _check_folder(
    row: CurationTaskRow, lookups: _RunLookups, problems: list[CurationProblem]
) -> None:
    """Check the folder exists, the user can edit it, it is in the row's project, and
    nothing in it already has the table's name.

    The project and the table's name are checked only once the folder is known to
    exist.
    """
    folder_id = row.folder_id
    try:
        can_edit = await lookups.can_edit_folder(folder_id)
    except _NotFoundError:
        problems.append(_synapse_problem(row, f"folder {folder_id} does not exist"))
        return
    except Exception as error:
        problems.append(
            _synapse_problem(
                row, f"Synapse error while checking folder {folder_id}: {error}"
            )
        )
        return
    if not can_edit:
        problems.append(_synapse_problem(row, f"you cannot edit folder {folder_id}"))

    try:
        project_id = await lookups.folder_project(folder_id)
    except Exception as error:
        problems.append(
            _synapse_problem(
                row,
                f"Synapse error while finding the project of folder {folder_id}: "
                f"{error}",
            )
        )
    else:
        if project_id != row.project_id:
            problems.append(
                _synapse_problem(
                    row,
                    f"folder {folder_id} is in project {project_id}, not "
                    f"{row.project_id}",
                )
            )

    try:
        entity_id = await lookups.entity_in_folder(folder_id, row.record_set_name)
    except Exception as error:
        problems.append(
            _synapse_problem(
                row,
                f"Synapse error while looking for '{row.record_set_name}' in folder "
                f"{folder_id}: {error}",
            )
        )
        return
    if entity_id is not None:
        problems.append(
            _synapse_problem(
                row,
                f"folder {folder_id} already holds an entity named "
                f"'{row.record_set_name}' ({entity_id}), but project "
                f"{row.project_id} has no curation task named "
                f"'{row.curation_task_name}'; rename or remove that entity, or give "
                "the row another record_set_name",
            )
        )


async def _check_team(
    row: CurationTaskRow, lookups: _RunLookups, problems: list[CurationProblem]
) -> None:
    """Check the assignee team exists."""
    team_id = row.assignee_principal_id
    try:
        await lookups.team(team_id)
    except _NotFoundError:
        problems.append(_synapse_problem(row, f"team {team_id} does not exist"))
    except Exception as error:
        problems.append(
            _synapse_problem(
                row, f"Synapse error while reading team {team_id}: {error}"
            )
        )


def _new_table_columns(
    key_columns: list[str], file_columns: list[str], schema_properties: list[str] | None
) -> list[str]:
    """A new table's columns in FR-008's order.

    The key columns, then the file's other schema properties in file order, then the
    schema properties the file lacks in schema order. Without a schema, the file's
    other named columns stand in for its properties.
    """
    columns = list(key_columns)
    for column in file_columns:
        if (
            not _is_blank_cell(column)
            and column not in columns
            and (schema_properties is None or column in schema_properties)
        ):
            columns.append(column)
    for column in schema_properties or []:
        if column not in columns:
            columns.append(column)
    return columns


async def _plan_new_task(
    row: CurationTaskRow, lookups: _RunLookups, overwrite: bool
) -> tuple[TaskPlan, _InputFile, list["_RowWrite"]]:
    """Plan a row whose task does not exist: check Synapse, then read the file.

    Returns:
        The plan, the input file as read, and its rows, every one to write.
    """
    problems: list[CurationProblem] = []
    schema_properties = await _check_schema(row, lookups, problems, row.upsert_keys)
    await _check_folder(row, lookups, problems)
    await _check_team(row, lookups, problems)

    key_columns = list(row.upsert_keys)
    loaded = _load_input_file(row, key_columns, schema_properties)
    problems.extend(loaded.problems)

    plan = TaskPlan(
        row=row,
        action=TaskAction.CREATE,
        key_columns=key_columns,
        table_columns=_new_table_columns(
            key_columns, loaded.columns, schema_properties
        ),
        missing_columns=[
            column for column in schema_properties or [] if column not in loaded.columns
        ],
        input_empty=loaded.empty,
        rows=RowChanges(to_add=len(loaded.rows)),
        problems=problems,
        overwrite=overwrite,
    )
    writes = [_RowWrite(line, cells, {}) for line, cells in loaded.rows]
    return plan, loaded, writes


class _Table(NamedTuple):
    """An existing task's table, read from its RecordSet's CSV.

    Attributes:
        header: The table's columns.
        records: Its rows, each with the CSV line it starts on.
        upsert_keys: The RecordSet's key columns.
        parent_id: The folder that holds the RecordSet.
    """

    header: list[str]
    records: list[CsvRecord]
    upsert_keys: list[str]
    parent_id: str | None


class _RowWrite(NamedTuple):
    """A row to write to a table.

    Attributes:
        line: The input file line the row comes from.
        cells: The row as written, column to text.
        overwritten: Each cell whose Synapse value the row replaces, column to the
            value written; empty for a row to add.
    """

    line: int
    cells: dict[str, str]
    overwritten: dict[str, str]


class _RowMatch(NamedTuple):
    """How a file's rows match a table's: the counts and differences a plan shows,
    the file rows whose keys the table lacks, and each differing row merged onto
    its Synapse row."""

    changes: RowChanges
    new_rows: list[_RowWrite]
    merged_rows: list[_RowWrite]


def _property_types(schema_body: dict[str, Any]) -> dict[str, str]:
    """Each schema property's JSON type, for properties that name one type."""
    properties = schema_body.get("properties")
    if not isinstance(properties, dict):
        return {}
    return {
        name: schema["type"]
        for name, schema in properties.items()
        if isinstance(schema, dict) and isinstance(schema.get("type"), str)
    }


def _comparable(cell: str, kind: str | None) -> Hashable:
    """A cell's value for comparison, typed by its property's JSON type (R11).

    `integer` and `number` cells become numbers and `boolean` cells `true` or
    `false` in any case become booleans, both ignoring surrounding whitespace. An
    `array` cell becomes a tuple of its items: a JSON list's items, or else the
    cell split on commas as Synapse's import splits it, each string item stripped
    of surrounding whitespace. A cell that does not convert, and every cell of
    another type, stays as its text.
    """
    if kind in ("integer", "number"):
        try:
            number = Decimal(cell.strip())
        except InvalidOperation:
            return cell
        return number if number.is_finite() else cell
    if kind == "boolean":
        word = cell.strip().lower()
        return word == "true" if word in ("true", "false") else cell
    if kind == "array":
        try:
            items = json.loads(cell)
        except ValueError:
            items = None
        if not isinstance(items, list):
            items = cell.split(",")
        return tuple(
            item.strip() if isinstance(item, str) else json.dumps(item, sort_keys=True)
            for item in items
        )
    return cell


def _is_blank_synapse_cell(cell: str, kind: str | None) -> bool:
    """Whether a RecordSet CSV cell is blank: empty, the text `null`, or, for an
    `array` property, a JSON list holding only empty strings (R11)."""
    if cell in ("", "null"):
        return True
    if kind != "array":
        return False
    try:
        items = json.loads(cell)
    except ValueError:
        return False
    return isinstance(items, list) and all(item == "" for item in items)


def _cells_equal(file_cell: str, synapse_cell: str, kind: str | None) -> bool:
    """Whether a non-blank file cell holds the same value as a Synapse cell.

    The same text is equal. A blank Synapse cell equals no non-blank file cell.
    Otherwise both are typed by `_comparable` and compared.
    """
    if file_cell == synapse_cell:
        return True
    if _is_blank_synapse_cell(synapse_cell, kind):
        return False
    return _comparable(file_cell, kind) == _comparable(synapse_cell, kind)


def _row_key(
    cells: dict[str, str], key_columns: list[str], types: dict[str, str]
) -> tuple[Hashable, ...]:
    """A row's key values, typed for matching."""
    return tuple(
        _comparable(cells.get(column, ""), types.get(column)) for column in key_columns
    )


def _record_cells(header: list[str], record: CsvRecord) -> dict[str, str]:
    """A table record's cells by column; a missing cell is `""`."""
    return {
        column: record.cells[position] if position < len(record.cells) else ""
        for position, column in enumerate(header)
    }


def _match_rows(
    rows: list[_InputRow],
    table: _Table,
    key_columns: list[str],
    types: dict[str, str],
) -> _RowMatch:
    """Match a file's rows to a table's on `key_columns` (FR-012 to FR-015).

    Keys and cells compare by `_cells_equal`'s typing, so `01` in a file matches `1`
    in an integer key column. A file row whose key the table lacks is to add. One
    whose key the table has differs when any non-blank cell in a column of the
    table differs from the table's; its merged row is the table's row overlaid with
    the file's non-blank cells, keys kept as the table writes them. A table key
    repeated in the table is matched to its first row. File rows with a blank key
    are left out; they block the task. Table keys no file row matches are absent
    from the file.
    """
    table_rows = []
    first_row: dict[tuple[Hashable, ...], dict[str, str]] = {}
    for record in table.records:
        if not any(record.cells):
            continue
        cells = _record_cells(table.header, record)
        key = _row_key(cells, key_columns, types)
        table_rows.append((key, cells))
        first_row.setdefault(key, cells)

    match = _RowMatch(RowChanges(), [], [])
    matched = set()
    for line, cells in rows:
        if any(_is_blank_cell(cells.get(column, "")) for column in key_columns):
            continue
        key = _row_key(cells, key_columns, types)
        synapse = first_row.get(key)
        if synapse is None:
            match.changes.to_add += 1
            match.new_rows.append(_RowWrite(line, cells, {}))
            continue
        matched.add(key)
        given = {
            column: cells[column]
            for column in table.header
            if column not in key_columns and not _is_blank_cell(cells.get(column, ""))
        }
        differing = {
            column: (value, synapse[column])
            for column, value in given.items()
            if not _cells_equal(value, synapse[column], types.get(column))
        }
        if not differing:
            match.changes.unchanged += 1
            continue
        match.changes.differing.append(
            RowDifference(
                key={column: cells[column] for column in key_columns},
                line=line,
                columns=differing,
            )
        )
        match.merged_rows.append(
            _RowWrite(
                line,
                {**synapse, **given},
                {column: values[0] for column, values in differing.items()},
            )
        )
    match.changes.absent_from_file.extend(
        {column: cells[column] for column in key_columns}
        for key, cells in table_rows
        if key not in matched
    )
    return match


def _setting_differences(
    row: CurationTaskRow,
    task: CurationTask,
    properties: RecordBasedMetadataTaskProperties,
    upsert_keys: list[str],
) -> dict[str, tuple[str, str]]:
    """The settings of an existing task that differ from its manifest row, each
    mapped to the manifest's value and Synapse's.

    A setting Synapse does not hold reads as `""`. Key columns are joined with `;`
    and differ only as sets, not by order.
    """
    mode = properties.suggested_authorization_mode
    settings = {
        "assignee_principal_id": (
            row.assignee_principal_id,
            (
                ""
                if task.assignee_principal_id is None
                else str(task.assignee_principal_id)
            ),
        ),
        "instructions": (row.instructions, task.instructions or ""),
        "authorization_mode": (
            getattr(row.authorization_mode, "value", row.authorization_mode),
            getattr(mode, "value", mode) or "",
        ),
    }
    differences = {
        setting: values
        for setting, values in settings.items()
        if values[0] != values[1]
    }
    if set(row.upsert_keys) != set(upsert_keys):
        differences["upsert_keys"] = (";".join(row.upsert_keys), ";".join(upsert_keys))
    return differences


async def _attached_session(
    row: CurationTaskRow,
    task: CurationTask,
    lookups: _RunLookups,
    problems: list[CurationProblem],
) -> tuple[Any, str | None]:
    """Read an existing task's state and the grid session its status names.

    The session is confirmed with one read of it, the only call made on it (R19): a
    404 means it is gone. Any other error, on either read, is a problem.

    Returns:
        The task's state, or None when it could not be read, and the attached
        session's ID, or None.
    """
    described = f"curation task '{row.curation_task_name}' ({task.task_id})"
    try:
        status = await task.get_status_async(synapse_client=lookups.synapse_client)
    except Exception as error:
        problems.append(
            _synapse_problem(
                row, f"Synapse error while reading the state of {described}: {error}"
            )
        )
        return None, None
    details = status.execution_details
    session_id = (
        details.active_session_id if isinstance(details, GridExecutionDetails) else None
    )
    if not session_id:
        return status.state, None
    try:
        await Grid(session_id=session_id).get_async(
            synapse_client=lookups.synapse_client
        )
    except Exception as error:
        if not _is_not_found(error):
            problems.append(
                _synapse_problem(
                    row,
                    f"Synapse error while reading grid session {session_id} "
                    f"attached to {described}: {error}",
                )
            )
        return status.state, None
    return status.state, session_id


async def _plan_existing_task(
    row: CurationTaskRow,
    task: CurationTask,
    lookups: _RunLookups,
    overwrite: bool,
) -> tuple[TaskPlan, _InputFile | None, list[_RowWrite]]:
    """Plan a row whose task exists, against the task's table as it stands.

    The task must be record-based and not canceled, and its table bound to the
    row's schema in the row's folder (FR-011). Rows are matched on the table's own
    keys and loaded under its own header (R25). A task marked complete, or with a
    live attached grid session, blocks only when rows would be written (FR-018).
    Settings that differ from the row are listed, never changed.

    Returns:
        The plan, the input file as read or None when it was not, and the rows a
        run would write: the rows to add and, when overwriting, the merged
        differing rows.
    """
    plan = TaskPlan(
        row=row,
        action=TaskAction.EXISTS,
        task_id=task.task_id,
        key_columns=list(row.upsert_keys),
        overwrite=overwrite,
    )
    problems = plan.problems
    described = f"curation task '{row.curation_task_name}' ({task.task_id})"
    properties = task.task_properties
    if not isinstance(properties, RecordBasedMetadataTaskProperties):
        problems.append(
            _synapse_problem(row, f"{described} is not a record-based metadata task")
        )
        return plan, None, []
    plan.record_set_id = properties.record_set_id

    plan.state, plan.attached_session_id = await _attached_session(
        row, task, lookups, problems
    )
    if plan.state == TaskState.CANCELED:
        problems.append(
            _synapse_problem(
                row, f"{described} is archived (CANCELED) and is not reopened"
            )
        )

    table_id = plan.record_set_id
    client = lookups.synapse_client
    try:
        table = await _read_table(table_id, client)
        bound_schema = await _bound_schema_uri(table_id, client)
    except Exception as error:
        problems.append(
            _synapse_problem(
                row, f"Synapse error while reading table {table_id}: {error}"
            )
        )
        return plan, None, []
    plan.table_columns = list(table.header)
    if table.upsert_keys:
        plan.key_columns = list(table.upsert_keys)
    else:
        problems.append(_synapse_problem(row, f"table {table_id} has no upsert keys"))
    plan.setting_differences = _setting_differences(
        row, task, properties, plan.key_columns
    )
    if bound_schema != row.schema_uri:
        bound = f"schema {bound_schema}" if bound_schema else "no schema"
        problems.append(
            _synapse_problem(
                row,
                f"table {table_id} is bound to {bound}, not {row.schema_uri}; it is "
                "not rebound",
            )
        )
    if table.parent_id != row.folder_id:
        problems.append(
            _synapse_problem(
                row,
                f"table {table_id} is in folder {table.parent_id}, not "
                f"{row.folder_id}; it is not moved",
            )
        )

    schema_properties = await _check_schema(row, lookups, problems, plan.key_columns)
    types = {}
    if schema_properties is not None:
        types = _property_types((await lookups.schema(row.schema_uri)).body)
    loaded = _load_input_file(row, plan.key_columns, schema_properties)
    problems.extend(loaded.problems)
    plan.input_empty = loaded.empty
    plan.missing_columns = [
        column for column in schema_properties or [] if column not in loaded.columns
    ]
    plan.skipped_columns = [
        column
        for column in dict.fromkeys(loaded.columns)
        if not _is_blank_cell(column)
        and column not in table.header
        and (schema_properties is None or column in schema_properties)
    ]

    match = _match_rows(loaded.rows, table, plan.key_columns, types)
    plan.rows = match.changes
    if plan.rows_to_write:
        rows = f"{_count(plan.rows_to_write, 'row')} would be written to it"
        if plan.state == TaskState.COMPLETED:
            problems.append(
                _synapse_problem(row, f"{described} is marked complete, and {rows}")
            )
        if plan.attached_session_id:
            problems.append(
                _synapse_problem(
                    row,
                    f"{described} has grid session {plan.attached_session_id} "
                    f"attached, and {rows}",
                )
            )
    writes = match.new_rows + (match.merged_rows if overwrite else [])
    return plan, loaded, writes


async def _plan_archives(
    row: CurationTaskRow, tasks: list[CurationTask], synapse_client: Synapse
) -> list[ArchiveEntry]:
    """An entry for each old task the row names in `archive`, once per name.

    Each named task's state is read from Synapse at the time of the call, so a task
    archived earlier in the same run reads as archived.

    Arguments:
        row: The manifest row.
        tasks: Every curation task in the row's project.
        synapse_client: The client.

    Returns:
        `ARCHIVE` for a task that is not canceled, `ALREADY_ARCHIVED` for one that
        is, `NOT_FOUND` for a name the project lacks, and `FAILED` with the error
        when the task's state could not be read.
    """
    entries = []
    for name in dict.fromkeys(row.archive):
        old_task = next((task for task in tasks if task.data_type == name), None)
        if old_task is None:
            entries.append(
                ArchiveEntry(curation_task_name=name, result=ArchiveResult.NOT_FOUND)
            )
            continue
        entry = ArchiveEntry(
            curation_task_name=name,
            task_id=old_task.task_id,
            result=ArchiveResult.ARCHIVE,
        )
        try:
            status = await old_task.get_status_async(synapse_client=synapse_client)
        except Exception as error:
            entry.result = ArchiveResult.FAILED
            entry.error = f"Synapse error while reading its state: {error}"
        else:
            if status.state == TaskState.CANCELED:
                entry.result = ArchiveResult.ALREADY_ARCHIVED
        entries.append(entry)
    return entries


async def _plan_task(
    row: CurationTaskRow, lookups: _RunLookups, overwrite: bool
) -> tuple[TaskPlan, _InputFile | None, list[_RowWrite]]:
    """Plan one manifest row.

    Any failed check or Synapse error becomes a blocking problem on the plan; none is
    raised.

    Returns:
        The plan, the input file as read or None when it was not read, and the rows
        a run would write.
    """
    try:
        tasks = await lookups.project_tasks(row.project_id)
    except Exception as error:
        plan = TaskPlan(
            row=row,
            action=TaskAction.BLOCKED,
            key_columns=list(row.upsert_keys),
            problems=[
                _synapse_problem(
                    row,
                    "Synapse error while listing the curation tasks in "
                    f"{row.project_id}: {error}",
                )
            ],
            overwrite=overwrite,
        )
        return plan, None, []

    existing = next(
        (task for task in tasks if task.data_type == row.curation_task_name), None
    )
    if existing is not None:
        plan, loaded, writes = await _plan_existing_task(
            row, existing, lookups, overwrite
        )
    else:
        plan, loaded, writes = await _plan_new_task(row, lookups, overwrite)
    plan.archives = await _plan_archives(row, tasks, lookups.synapse_client)
    if plan.blocked:
        plan.action = TaskAction.BLOCKED
    return plan, loaded, writes


def _describe_plan(plan: TaskPlan) -> str:
    """A one-line log message for a plan, naming its task."""
    row = plan.row
    if plan.action is TaskAction.BLOCKED:
        count = len(plan.problems)
        return f"{row.name}: blocked by {count} problem{'' if count == 1 else 's'}"
    if plan.action is TaskAction.EXISTS:
        return (
            f"{row.name}: curation task '{row.curation_task_name}' exists "
            f"({plan.task_id})"
        )
    count = plan.rows.to_add
    return (
        f"{row.name}: curation task '{row.curation_task_name}' would be created with "
        f"{count} row{'' if count == 1 else 's'}"
    )


def plan_curation_tasks(
    manifest: CurationTaskManifest,
    *,
    tasks: Optional[list[str]] = None,
    overwrite: bool = False,
    synapse_client: Optional[Synapse] = None,
) -> list[TaskPlan]:
    """Work out what applying a task manifest would do, changing nothing in Synapse.

    Rows are planned one at a time, in manifest order. A task with no match in its
    project is checked for creation: its schema is registered and holds its key
    columns, its folder exists, can be edited, is in its project and holds nothing
    with the table's name, its team exists, and its input file's columns and keys
    are sound. A task that exists is compared with its table, downloaded without a
    grid session: rows are matched on the table's own key columns and counted as
    to add, unchanged, differing, or in Synapse but absent from the file, and
    settings that differ from the manifest are listed. It is blocked when it is
    archived, is not record-based, or its table is bound to another schema or lies
    in another folder, and, when rows would be written to it, when it is marked
    complete or has a grid session attached. Whatever fails, a Synapse error
    included, becomes a blocking problem on that task's plan and leaves the other
    tasks to be planned. Each old task a row names in `archive` is listed as to be
    archived, already archived, or not found.

    Arguments:
        manifest: The task manifest.
        tasks: The `name`s of the rows to plan, or None for every row.
        overwrite: Whether differing rows count as rows to write.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        One plan per selected row, in manifest order.

    Raises:
        CurationManifestError: When `tasks` names a row the manifest lacks.

    Example: Plan a manifest
        &nbsp;
        Read a manifest and print what applying it would do.
        ```python
        import synapseclient
        from synapseclient.extensions.curator import (
            CurationTaskManifest,
            plan_curation_tasks,
        )

        syn = synapseclient.Synapse()
        syn.login()

        manifest = CurationTaskManifest.from_csv("tasks.csv")
        for plan in plan_curation_tasks(manifest, tasks=["lab_a"]):
            print(plan.row.name, plan.action, plan.rows.to_add)
            for problem in plan.problems:
                print(problem.message)
        ```
    """
    return wrap_async_to_sync(
        coroutine=plan_curation_tasks_async(
            manifest,
            tasks=tasks,
            overwrite=overwrite,
            synapse_client=synapse_client,
        )
    )


async def plan_curation_tasks_async(
    manifest: CurationTaskManifest,
    *,
    tasks: Optional[list[str]] = None,
    overwrite: bool = False,
    synapse_client: Optional[Synapse] = None,
) -> list[TaskPlan]:
    """Work out what applying a task manifest would do, changing nothing in Synapse.

    Rows are planned one at a time, in manifest order. A task with no match in its
    project is checked for creation: its schema is registered and holds its key
    columns, its folder exists, can be edited, is in its project and holds nothing
    with the table's name, its team exists, and its input file's columns and keys
    are sound. A task that exists is compared with its table, downloaded without a
    grid session: rows are matched on the table's own key columns and counted as
    to add, unchanged, differing, or in Synapse but absent from the file, and
    settings that differ from the manifest are listed. It is blocked when it is
    archived, is not record-based, or its table is bound to another schema or lies
    in another folder, and, when rows would be written to it, when it is marked
    complete or has a grid session attached. Whatever fails, a Synapse error
    included, becomes a blocking problem on that task's plan and leaves the other
    tasks to be planned. Each old task a row names in `archive` is listed as to be
    archived, already archived, or not found.

    Arguments:
        manifest: The task manifest.
        tasks: The `name`s of the rows to plan, or None for every row.
        overwrite: Whether differing rows count as rows to write.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        One plan per selected row, in manifest order.

    Raises:
        CurationManifestError: When `tasks` names a row the manifest lacks.

    Example: Plan a manifest
        &nbsp;
        Read a manifest and print what applying it would do.
        ```python
        import asyncio

        import synapseclient
        from synapseclient.extensions.curator import (
            CurationTaskManifest,
            plan_curation_tasks_async,
        )

        syn = synapseclient.Synapse()
        syn.login()

        manifest = CurationTaskManifest.from_csv("tasks.csv")
        plans = asyncio.run(plan_curation_tasks_async(manifest, tasks=["lab_a"]))
        for plan in plans:
            print(plan.row.name, plan.action, plan.rows.to_add)
            for problem in plan.problems:
                print(problem.message)
        ```
    """
    rows = manifest.select(tasks)
    client = Synapse.get_client(synapse_client=synapse_client)
    lookups = _RunLookups(client)
    plans = []
    for number, row in enumerate(rows, start=1):
        client.logger.info(f"Planning task {number} of {len(rows)}: {row.name}")
        plan, _, _ = await _plan_task(row, lookups, overwrite)
        client.logger.info(_describe_plan(plan))
        plans.append(plan)
    return plans


# Filling a table

_VALIDATION_PAGE_SIZE = 500
"""Rows read per validation query."""

_VALIDATION_POLL_SECONDS = 2
"""Seconds between reads of a session's rows while any lacks a validation result."""

_VALIDATION_WAIT_SECONDS = 120
"""Seconds to keep reading before reporting rows Synapse has not validated."""

_COLUMN_TYPES = {
    "string": ColumnType.STRING,
    "integer": ColumnType.INTEGER,
    "number": ColumnType.DOUBLE,
    "boolean": ColumnType.BOOLEAN,
}


class _FillError(Exception):
    """A fill step finished, but not as the write needs; the message says how."""


class _FillResult(NamedTuple):
    """What filling a table through a grid session found.

    Attributes:
        session_id: The grid session used, deleted by the time this is returned.
        rows_imported: The rows the import counted.
        rows: The session's rows, with their validation results, as the last read
            gave them.
        unvalidated: How many of `rows` still lacked a validation result when the
            wait ran out.
        sync_errors: The synchronize's error messages; empty when it had none.
    """

    session_id: str
    rows_imported: int
    rows: list[GridRow]
    unvalidated: int
    sync_errors: list[str]


def _column_types(
    schema_body: dict[str, Any], columns: list[str]
) -> dict[str, ColumnType]:
    """The import type of each column whose schema property has one plain type.

    `string`, `integer`, `number` and `boolean` map to their column types, and an
    `array` of strings to `STRING_LIST`. A column whose property has no `type`,
    several types, a `$ref`, or array items of another shape is left out.
    """
    properties = schema_body.get("properties")
    if not isinstance(properties, dict):
        return {}
    types = {}
    for column in columns:
        schema = properties.get(column)
        if not isinstance(schema, dict) or "$ref" in schema:
            continue
        kind = schema.get("type")
        if kind == "array":
            items = schema.get("items")
            if (
                isinstance(items, dict)
                and "$ref" not in items
                and items.get("type") == "string"
            ):
                types[column] = ColumnType.STRING_LIST
        elif isinstance(kind, str) and kind in _COLUMN_TYPES:
            types[column] = _COLUMN_TYPES[kind]
    return types


async def _read_session_rows(
    grid: Grid, synapse_client: Synapse
) -> tuple[list[GridRow], int]:
    """Read every row of a connected session with its validation results.

    Rows are read a page at a time until a short page. Synapse validates rows a
    moment after an import, so the read repeats while any row lacks a result, until
    `_VALIDATION_WAIT_SECONDS` have passed.

    Returns:
        The rows of the last read, and how many of them lack a result.
    """
    deadline = time.monotonic() + _VALIDATION_WAIT_SECONDS
    while True:
        rows: list[GridRow] = []
        while True:
            result = await grid.validate_rows_async(
                query_request=QueryRequest(
                    query=GridQuery(
                        column_selection=[SelectAll()],
                        limit=_VALIDATION_PAGE_SIZE,
                        offset=len(rows),
                        include_validation_messages=True,
                    )
                ),
                synapse_client=synapse_client,
            )
            page = (result.rows if result is not None else None) or []
            rows.extend(page)
            if len(page) < _VALIDATION_PAGE_SIZE:
                break
        unvalidated = sum(1 for row in rows if row.validation_results is None)
        if not unvalidated or time.monotonic() >= deadline:
            return rows, unvalidated
        await asyncio.sleep(_VALIDATION_POLL_SECONDS)


async def _fill_table(
    record_set_id: str,
    columns: list[str],
    rows: list[dict[str, str]],
    schema_body: dict[str, Any],
    synapse_client: Synapse,
) -> _FillResult:
    """Write rows to a table through a grid session of its own (research R12).

    The rows go to a temporary CSV under `columns`, every cell quoted and a missing
    cell written as `""`. A new session on the RecordSet imports it, typed from the
    schema; is read until every row has a validation result; and is synchronized
    with `PULL_PUSH`. The session is never attached to a task. The session and the
    CSV are removed whatever happens; a session that cannot be deleted is logged.

    Arguments:
        record_set_id: The table's RecordSet.
        columns: The table's header.
        rows: The rows to write, each a mapping of column to text.
        schema_body: The bound schema's body, which types the import.
        synapse_client: The client.

    Returns:
        The import count, the session's rows and the synchronize's errors.

    Raises:
        _FillError: When the import counts other than `len(rows)` rows; the session
            is then not synchronized.
    """
    descriptor, csv_path = tempfile.mkstemp(prefix="curation_task_", suffix=".csv")
    grid = Grid(record_set_id=record_set_id)
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="") as file:
            writer = csv.writer(file, quoting=csv.QUOTE_ALL)
            writer.writerow(columns)
            writer.writerows(
                [row.get(column, "") for column in columns] for row in rows
            )

        await grid.create_async(synapse_client=synapse_client)
        await grid.import_csv_async(
            path=csv_path,
            column_types=_column_types(schema_body, columns),
            synapse_client=synapse_client,
        )
        imported = grid.import_total_count
        if imported != len(rows):
            raise _FillError(
                f"grid session {grid.session_id} imported "
                f"{_count(imported or 0, 'row')}, not the {len(rows)} written; it "
                f"was not synchronized, so table {record_set_id} is unchanged"
            )
        async with grid.connect_async(synapse_client=synapse_client) as session:
            session_rows, unvalidated = await _read_session_rows(
                session, synapse_client
            )
        await grid.synchronize_async(
            sync_type=SyncType.PULL_PUSH, synapse_client=synapse_client
        )
        return _FillResult(
            session_id=grid.session_id,
            rows_imported=imported,
            rows=session_rows,
            unvalidated=unvalidated,
            sync_errors=list(grid.sync_error_messages or []),
        )
    finally:
        Path(csv_path).unlink(missing_ok=True)
        if grid.session_id:
            try:
                await grid.delete_async(synapse_client=synapse_client)
            except Exception as error:
                synapse_client.logger.warning(
                    f"Could not delete grid session {grid.session_id} on table "
                    f"{record_set_id}: {error}"
                )


async def _read_table(record_set_id: str, synapse_client: Synapse) -> _Table:
    """Read a table without a grid session (research R10): download its RecordSet to
    a temporary folder, removed before returning, and read the CSV.

    Returns:
        The table's header, records, upsert keys and folder.
    """
    with tempfile.TemporaryDirectory(prefix="curation_task_") as folder:
        record_set = await RecordSet(id=record_set_id, path=folder).get_async(
            synapse_client=synapse_client
        )
        header, records = read_csv_records(record_set.path)
    return _Table(
        header=header,
        records=records,
        upsert_keys=list(record_set.upsert_keys or []),
        parent_id=record_set.parent_id,
    )


async def _bound_schema_uri(record_set_id: str, synapse_client: Synapse) -> str | None:
    """The `ORG-NAME-VERSION` of the schema bound to a RecordSet, or None when none
    is (a 404)."""
    try:
        binding = await RecordSet(id=record_set_id).get_schema_async(
            synapse_client=synapse_client
        )
    except SynapseHTTPError as error:
        if _is_not_found(error):
            return None
        raise
    return binding.json_schema_version_info.id


# Applying


def _cell_text(value: Any) -> str:
    """A grid row's value as text: None as empty, a string as it is, anything else
    as JSON."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value)


def _validation_problems(
    outcome: TaskOutcome,
    written: list[_RowWrite],
    fill: _FillResult,
    types: dict[str, str],
) -> list[CurationProblem]:
    """The session's invalid rows as non-blocking validation problems.

    A row is named by its key values, and by its input file and line when a written
    row has that key. Its message is Synapse's one-line message followed by each of
    its detailed messages. Rows Synapse had not validated in time add one problem
    saying how many.
    """
    plan = outcome.plan
    row = plan.row
    file = str(row.path)
    lines = {
        _row_key(written_row.cells, plan.key_columns, types): written_row.line
        for written_row in written
    }
    problems = []
    for grid_row in fill.rows:
        results = grid_row.validation_results
        if results is None or results.is_valid is not False:
            continue
        data = grid_row.data or {}
        key = {column: _cell_text(data.get(column)) for column in plan.key_columns}
        messages = "; ".join(
            message
            for message in [
                results.validation_error_message,
                *(results.all_validation_messages or []),
            ]
            if message
        )
        line = lines.get(_row_key(key, plan.key_columns, types))
        if line is None:
            where = f"table row with key {_describe_key(key)}"
        else:
            where = f"input file {file} line {line}: key {_describe_key(key)}"
        problems.append(
            CurationProblem(
                task=row.name,
                kind=ProblemKind.VALIDATION,
                blocking=False,
                message=f"{row.name}: {where}: {messages}",
                file=None if line is None else file,
                line=line,
                key=key,
            )
        )
    if fill.unvalidated:
        problems.append(
            CurationProblem(
                task=row.name,
                kind=ProblemKind.VALIDATION,
                blocking=False,
                message=(
                    f"{row.name}: Synapse had not validated {fill.unvalidated} of "
                    f"the {_count(len(fill.rows), 'row')} in table "
                    f"{outcome.record_set_id} after {_VALIDATION_WAIT_SECONDS} "
                    "seconds; their validation problems are not reported"
                ),
            )
        )
    return problems


def _check_read_back(
    outcome: TaskOutcome,
    written: list[_RowWrite],
    table: _Table,
    types: dict[str, str],
) -> list[str]:
    """Check the table read back after a write holds every row written, and every
    overwritten cell the value written (FR-019).

    Keys and cells compare by `_cells_equal`'s typing.

    Returns:
        One error per key column the table's header lacks, or else one per written
        key the table lacks and one per overwritten cell holding another value.
    """
    plan = outcome.plan
    table_id = outcome.record_set_id
    absent = [column for column in plan.key_columns if column not in table.header]
    if absent:
        return [
            f"table {table_id} read back has no key column '{column}'"
            for column in absent
        ]
    present: dict[tuple[Hashable, ...], dict[str, str]] = {}
    for record in table.records:
        cells = _record_cells(table.header, record)
        present.setdefault(_row_key(cells, plan.key_columns, types), cells)
    errors = []
    for row in written:
        key = {column: row.cells.get(column, "") for column in plan.key_columns}
        where = (
            f"key {_describe_key(key)} from input file {plan.row.path} line {row.line}"
        )
        stored = present.get(_row_key(row.cells, plan.key_columns, types))
        if stored is None:
            errors.append(f"{where} is not in table {table_id} after writing")
            continue
        for column, value in row.overwritten.items():
            held = stored.get(column, "")
            if not _cells_equal(value, held, types.get(column)):
                errors.append(
                    f"{where}: column '{column}' holds '{held}' in table {table_id} "
                    f"after writing, not '{value}'"
                )
    return errors


async def _write_rows(
    outcome: TaskOutcome,
    written: list[_RowWrite],
    lookups: _RunLookups,
) -> None:
    """Fill the outcome's table with rows, then read it back and confirm them.

    Validation problems, errors, `rows_written` and a `FAILED` result are recorded
    on the outcome; nothing is raised.
    """
    plan = outcome.plan
    record_set_id = outcome.record_set_id
    client = lookups.synapse_client
    types: dict[str, str] = {}
    try:
        schema = await lookups.schema(plan.row.schema_uri)
        types = _property_types(schema.body)
        fill = await _fill_table(
            record_set_id,
            plan.table_columns,
            [row.cells for row in written],
            schema.body,
            client,
        )
    except _FillError as error:
        outcome.errors.append(str(error))
    except Exception as error:
        outcome.errors.append(
            f"Synapse error while filling table {record_set_id}: {error}"
        )
    else:
        outcome.validation_problems = _validation_problems(
            outcome, written, fill, types
        )
        outcome.errors.extend(
            f"synchronizing grid session {fill.session_id} into table "
            f"{record_set_id} failed: {message}"
            for message in fill.sync_errors
        )
        if not fill.sync_errors:
            outcome.rows_written = len(written)
            try:
                table = await _read_table(record_set_id, client)
            except Exception as error:
                outcome.errors.append(
                    f"Synapse error while reading table {record_set_id} back: {error}"
                )
            else:
                outcome.errors.extend(_check_read_back(outcome, written, table, types))
    if outcome.errors:
        outcome.result = TaskResult.FAILED


async def _apply_new_task(
    plan: TaskPlan,
    loaded: _InputFile,
    writes: list[_RowWrite],
    lookups: _RunLookups,
) -> TaskOutcome:
    """Create a planned task with research R13's arguments, remove the template CSV
    the creator leaves, then fill the task when the file has rows."""
    row = plan.row
    outcome = TaskOutcome(plan=plan, result=TaskResult.CREATED)
    try:
        record_set, task = await create_record_based_metadata_task_async(
            folder_id=row.folder_id,
            record_set_name=row.record_set_name,
            record_set_description=row.curation_task_name,
            curation_task_name=row.curation_task_name,
            upsert_keys=list(row.upsert_keys),
            instructions=row.instructions,
            schema_uri=row.schema_uri,
            assignee_principal_id=row.assignee_principal_id,
            authorization_mode=row.authorization_mode,
            column_order=[
                column
                for column in plan.table_columns
                if column in loaded.columns and column not in plan.key_columns
            ],
            create_grid=False,
            synapse_client=lookups.synapse_client,
        )
    except Exception as error:
        outcome.result = TaskResult.FAILED
        outcome.errors.append(
            f"Synapse error while creating the curation task: {error}"
        )
        return outcome
    outcome.task_id = task.task_id
    outcome.record_set_id = record_set.id
    if record_set.path:
        try:
            Path(record_set.path).unlink(missing_ok=True)
        except OSError as error:
            lookups.synapse_client.logger.warning(
                f"{row.name}: could not remove {record_set.path}: {error}"
            )
    if writes:
        await _write_rows(outcome, writes, lookups)
    return outcome


async def _apply_existing_task(
    plan: TaskPlan, writes: list[_RowWrite], lookups: _RunLookups
) -> TaskOutcome:
    """Write a plan's rows to a task that exists: the rows to add and, when
    overwriting, the merged differing rows, then confirm them on the read-back.

    With nothing to write, no session is opened and the task is unchanged, so a
    table is never replaced or emptied (FR-010, SC-002).
    """
    outcome = TaskOutcome(
        plan=plan,
        result=TaskResult.UNCHANGED,
        task_id=plan.task_id,
        record_set_id=plan.record_set_id,
    )
    if writes:
        outcome.result = TaskResult.UPDATED
        await _write_rows(outcome, writes, lookups)
    return outcome


async def _apply_task(
    row: CurationTaskRow, lookups: _RunLookups, overwrite: bool
) -> TaskOutcome:
    """Plan one manifest row and carry the plan out.

    A blocked plan writes nothing. Any error while carrying a plan out fails that
    task and is recorded on its outcome; none is raised.
    """
    plan, loaded, writes = await _plan_task(row, lookups, overwrite)
    if plan.blocked:
        return TaskOutcome(
            plan=plan,
            result=TaskResult.BLOCKED,
            task_id=plan.task_id,
            record_set_id=plan.record_set_id,
        )
    try:
        if plan.action is TaskAction.CREATE:
            return await _apply_new_task(plan, loaded, writes, lookups)
        return await _apply_existing_task(plan, writes, lookups)
    except Exception as error:
        return TaskOutcome(
            plan=plan,
            result=TaskResult.FAILED,
            task_id=plan.task_id,
            record_set_id=plan.record_set_id,
            errors=[f"unexpected error: {error}"],
        )


_ARCHIVING_RESULTS = (TaskResult.CREATED, TaskResult.UPDATED, TaskResult.UNCHANGED)


async def _archive_old_tasks(outcome: TaskOutcome, synapse_client: Synapse) -> None:
    """Archive the old tasks a row names, once its own task is in place (FR-026).

    Only entries planned as `ARCHIVE` are archived, by setting the old task's state
    to `CANCELED`, and only when the row's result is created, updated or unchanged;
    otherwise they stay `ARCHIVE`. Each entry becomes `ARCHIVED`, or `FAILED` with
    the error; the row's result is never changed. The old task's table is not
    touched.
    """
    if outcome.result not in _ARCHIVING_RESULTS:
        return
    name = outcome.plan.row.name
    for entry in outcome.plan.archives:
        if entry.result is not ArchiveResult.ARCHIVE:
            continue
        old_task = f"curation task '{entry.curation_task_name}' ({entry.task_id})"
        try:
            await CurationTask(task_id=entry.task_id).set_task_state_async(
                TaskState.CANCELED, synapse_client=synapse_client
            )
        except Exception as error:
            entry.result = ArchiveResult.FAILED
            entry.error = str(error)
            synapse_client.logger.warning(
                f"{name}: archiving {old_task} failed: {error}"
            )
        else:
            entry.result = ArchiveResult.ARCHIVED
            synapse_client.logger.info(f"{name}: archived {old_task}")


def _describe_outcome(outcome: TaskOutcome) -> str:
    """A one-line log message for an outcome, naming its task."""
    plan = outcome.plan
    row = plan.row
    task = f"curation task '{row.curation_task_name}' ({outcome.task_id})"
    if outcome.result is TaskResult.BLOCKED:
        return _describe_plan(plan)
    if outcome.result is TaskResult.FAILED:
        return f"{row.name}: failed with {_count(len(outcome.errors), 'error')}"
    if outcome.result is TaskResult.UNCHANGED:
        return f"{row.name}: nothing written to {task}"
    rows = _count(outcome.rows_written, "row")
    message = (
        f"{row.name}: created {task} with {rows}"
        if outcome.result is TaskResult.CREATED
        else f"{row.name}: wrote {rows} to {task}"
    )
    if outcome.validation_problems:
        message += f"; {_count(len(outcome.validation_problems), 'validation problem')}"
    return message


def apply_curation_tasks(
    manifest: CurationTaskManifest,
    *,
    tasks: Optional[list[str]] = None,
    overwrite: bool = False,
    synapse_client: Optional[Synapse] = None,
) -> list[TaskOutcome]:
    """Create and fill the curation tasks a manifest describes.

    Rows are planned and carried out one at a time, in manifest order, each planned
    just before it is applied. A blocked task is left alone. A task with no match
    in its project is created and filled with its file's rows; a task that exists
    gets the rows its table lacks and, with `overwrite`, each differing row as its
    Synapse row overlaid with the file's non-blank cells, so a blank cell never
    clears a value. Rows are written through a temporary grid session of the
    run's own: they are imported, Synapse's validation results are read, the
    session is synchronized into the task's table and deleted, and the table is
    read back to confirm every row written and every cell overwritten. A task with
    nothing to write opens no session. Validation problems are reported and never
    stop a task. Once a row's task is in place, the
    old tasks it names in `archive` are archived; their tables are not touched. An
    error in one task fails that task and leaves the others to run. Nothing is
    deleted.

    Arguments:
        manifest: The task manifest.
        tasks: The `name`s of the rows to apply, or None for every row.
        overwrite: Whether differing rows are overwritten.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        One outcome per selected row, in manifest order, each holding the plan it
        carried out.

    Raises:
        CurationManifestError: When `tasks` names a row the manifest lacks.

    Example: Apply a manifest
        &nbsp;
        Read a manifest, apply it, and print each task's result and problems.
        ```python
        import synapseclient
        from synapseclient.extensions.curator import (
            CurationTaskManifest,
            apply_curation_tasks,
        )

        syn = synapseclient.Synapse()
        syn.login()

        manifest = CurationTaskManifest.from_csv("tasks.csv")
        for outcome in apply_curation_tasks(manifest, tasks=["lab_a"]):
            print(outcome.plan.row.name, outcome.result, outcome.rows_written)
            for problem in outcome.validation_problems:
                print(problem.message)
            for error in outcome.errors:
                print(error)
        ```
    """
    return wrap_async_to_sync(
        coroutine=apply_curation_tasks_async(
            manifest,
            tasks=tasks,
            overwrite=overwrite,
            synapse_client=synapse_client,
        )
    )


async def apply_curation_tasks_async(
    manifest: CurationTaskManifest,
    *,
    tasks: Optional[list[str]] = None,
    overwrite: bool = False,
    synapse_client: Optional[Synapse] = None,
) -> list[TaskOutcome]:
    """Create and fill the curation tasks a manifest describes.

    Rows are planned and carried out one at a time, in manifest order, each planned
    just before it is applied. A blocked task is left alone. A task with no match
    in its project is created and filled with its file's rows; a task that exists
    gets the rows its table lacks and, with `overwrite`, each differing row as its
    Synapse row overlaid with the file's non-blank cells, so a blank cell never
    clears a value. Rows are written through a temporary grid session of the
    run's own: they are imported, Synapse's validation results are read, the
    session is synchronized into the task's table and deleted, and the table is
    read back to confirm every row written and every cell overwritten. A task with
    nothing to write opens no session. Validation problems are reported and never
    stop a task. Once a row's task is in place, the
    old tasks it names in `archive` are archived; their tables are not touched. An
    error in one task fails that task and leaves the others to run. Nothing is
    deleted.

    Arguments:
        manifest: The task manifest.
        tasks: The `name`s of the rows to apply, or None for every row.
        overwrite: Whether differing rows are overwritten.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        One outcome per selected row, in manifest order, each holding the plan it
        carried out.

    Raises:
        CurationManifestError: When `tasks` names a row the manifest lacks.

    Example: Apply a manifest
        &nbsp;
        Read a manifest, apply it, and print each task's result and problems.
        ```python
        import asyncio

        import synapseclient
        from synapseclient.extensions.curator import (
            CurationTaskManifest,
            apply_curation_tasks_async,
        )

        syn = synapseclient.Synapse()
        syn.login()

        manifest = CurationTaskManifest.from_csv("tasks.csv")
        outcomes = asyncio.run(apply_curation_tasks_async(manifest, tasks=["lab_a"]))
        for outcome in outcomes:
            print(outcome.plan.row.name, outcome.result, outcome.rows_written)
            for problem in outcome.validation_problems:
                print(problem.message)
            for error in outcome.errors:
                print(error)
        ```
    """
    rows = manifest.select(tasks)
    client = Synapse.get_client(synapse_client=synapse_client)
    lookups = _RunLookups(client)
    outcomes = []
    for number, row in enumerate(rows, start=1):
        client.logger.info(f"Applying task {number} of {len(rows)}: {row.name}")
        outcome = await _apply_task(row, lookups, overwrite)
        client.logger.info(_describe_outcome(outcome))
        await _archive_old_tasks(outcome, client)
        outcomes.append(outcome)
    return outcomes


# Summary and report

_SUMMARY_COLUMNS = (
    "command",
    "overwrite",
    "name",
    "curation_task_name",
    "project_id",
    "task_id",
    "record_set_id",
    "action",
    "result",
    "rows_to_add",
    "rows_unchanged",
    "rows_differing",
    "rows_absent_from_file",
    "rows_written",
    "validation_problems",
    "archives",
    "setting_differences",
    "key_columns",
    "missing_columns",
    "skipped_columns",
    "input_empty",
)
_DIFFERENCES_COLUMNS = (
    "name",
    "key",
    "line",
    "column",
    "file_value",
    "synapse_value",
    "overwrite",
)
_PROBLEMS_COLUMNS = (
    "name",
    "kind",
    "blocking",
    "file",
    "line",
    "key",
    "column",
    "message",
)


def _split_results(
    results: list[TaskPlan] | list[TaskOutcome],
) -> list[tuple[TaskPlan, TaskOutcome | None]]:
    """Each result's plan, with its outcome when the result is one."""
    return [
        (result.plan, result) if isinstance(result, TaskOutcome) else (result, None)
        for result in results
    ]


def _count(number: int, noun: str, plural: str | None = None) -> str:
    """`1 row`, `2 rows`."""
    return f"{number} {noun if number == 1 else plural or noun + 's'}"


def _csv_bool(value: bool) -> str:
    """A boolean as the report writes it."""
    return "true" if value else "false"


def _csv_key(key: dict[str, str] | None) -> str:
    """A key as the report writes it: `column=value` pairs joined with `;`."""
    return ";".join(f"{column}={value}" for column, value in (key or {}).items())


def _csv_text(value: object) -> str:
    """A cell for the report: None as empty, anything else as text."""
    return "" if value is None else str(value)


def _archive_text(entry: ArchiveEntry) -> str:
    """An old task and its archive result: `Lab A v1: archived`."""
    return f"{entry.curation_task_name}: {entry.result.value.lower()}"


def _overwrite_shown(plan: TaskPlan, outcome: TaskOutcome | None) -> bool:
    """Whether a task's differing rows show as overwritten, or to be: the run
    overwrites, the task is not blocked, and an apply wrote its rows."""
    return (
        plan.overwrite
        and not _is_blocked(plan, outcome)
        and (outcome is None or outcome.rows_written > 0)
    )


def _notes(plan: TaskPlan, outcome: TaskOutcome | None) -> list[str]:
    """What the summary's NOTES column says about a task.

    A blocked or failed task names how many problems or errors are listed under the
    table. Every task names its empty input file, missing and skipped columns,
    differing rows and what happens to them, rows absent from the file, and
    settings that differ.
    """
    notes = []
    blocking = [problem for problem in plan.problems if problem.blocking]
    if blocking:
        notes.append(_count(len(blocking), "blocking problem"))
    if outcome is not None and outcome.errors:
        notes.append(_count(len(outcome.errors), "error"))
    if plan.input_empty:
        notes.append("input file is empty")
    if plan.missing_columns:
        notes.append("not in file: " + ", ".join(plan.missing_columns))
    if plan.skipped_columns:
        notes.append("not loaded: " + ", ".join(plan.skipped_columns))
    differing = len(plan.rows.differing)
    if differing:
        if not _overwrite_shown(plan, outcome):
            fate = "left alone"
        elif outcome is None:
            fate = "to be overwritten"
        else:
            fate = "overwritten"
        notes.append(f"{_count(differing, 'differing row')} {fate}")
    absent = len(plan.rows.absent_from_file)
    if absent:
        notes.append(f"{_count(absent, 'row')} absent from file")
    if plan.setting_differences:
        notes.append("settings differ: " + ", ".join(plan.setting_differences.keys()))
    return notes


def _is_blocked(plan: TaskPlan, outcome: TaskOutcome | None) -> bool:
    """Whether a task's plan or outcome is blocked."""
    return plan.blocked or (
        outcome is not None and outcome.result is TaskResult.BLOCKED
    )


def _shown_archives(plan: TaskPlan, outcome: TaskOutcome | None) -> list[ArchiveEntry]:
    """The archive entries the summary shows: none for a blocked or failed task,
    whose old tasks are left active."""
    if _is_blocked(plan, outcome) or (
        outcome is not None and outcome.result is TaskResult.FAILED
    ):
        return []
    return plan.archives


def _table_row(plan: TaskPlan, outcome: TaskOutcome | None) -> list[str]:
    """A task's row in the summary table.

    A blocked task's count cells are `-`. The archive cell is `-` when no archive
    entry is shown.
    """
    blocked = _is_blocked(plan, outcome)
    status = (outcome.result if outcome is not None else plan.action).value.lower()
    counts = [
        str(plan.rows.to_add),
        str(plan.rows.unchanged),
        str(len(plan.rows.differing)),
        str(len(plan.rows.absent_from_file)),
    ]
    if outcome is not None:
        counts.append(str(len(outcome.validation_problems)))
    if blocked:
        counts = ["-"] * len(counts)
    archives = ", ".join(
        _archive_text(entry) for entry in _shown_archives(plan, outcome)
    )
    return [
        plan.row.name,
        status,
        *counts,
        archives or "-",
        "; ".join(_notes(plan, outcome)),
    ]


def _format_table(rows: list[list[str]]) -> list[str]:
    """Rows as lines of left-aligned columns two spaces apart."""
    widths = [max(len(row[index]) for row in rows) for index in range(len(rows[0]))]
    return [
        "  ".join(cell.ljust(width) for cell, width in zip(row, widths)).rstrip()
        for row in rows
    ]


def _count_line(pairs: list[tuple[TaskPlan, TaskOutcome | None]], applied: bool) -> str:
    """The summary's last line: tasks by action or result, then, for `apply`,
    validation problems, then the archive entries the table shows, by result."""
    statuses = (
        [outcome.result for _, outcome in pairs]
        if applied
        else [plan.action for plan, _ in pairs]
    )
    enum = TaskResult if applied else TaskAction
    by_status = [
        f"{statuses.count(status)} {status.value.lower()}"
        for status in enum
        if status in statuses
    ]
    line = _count(len(pairs), "task")
    if by_status:
        line += ": " + ", ".join(by_status)
    if applied:
        validation = sum(len(outcome.validation_problems) for _, outcome in pairs)
        line += f"; {_count(validation, 'validation problem')}"
    archive_results = [
        entry.result
        for plan, outcome in pairs
        for entry in _shown_archives(plan, outcome)
    ]
    if archive_results:
        line += "; archives: " + ", ".join(
            f"{archive_results.count(result)} {result.value.lower()}"
            for result in ArchiveResult
            if result in archive_results
        )
    return line


def format_curation_summary(
    command: str, results: list[TaskPlan] | list[TaskOutcome]
) -> str:
    """The summary `synapse curate` logs at the end of a run.

    A table with one line per task, then every blocking problem, every setting that
    differs on an existing task, every error and failed archive, and a count line.
    `plan` shows each task's action and `apply` its result and validation problem
    count. Problem messages already name their task and are printed as they are.

    Arguments:
        command: `plan` or `apply`.
        results: What `plan_curation_tasks` or `apply_curation_tasks` returned.

    Returns:
        The summary, lines joined with newlines.
    """
    applied = command == "apply"
    pairs = _split_results(results)
    header = ["TASK", "RESULT" if applied else "ACTION", "ADD", "UNCHANGED"]
    header += ["DIFFERS", "ABSENT"] + (["VALIDATION"] if applied else [])
    header += ["ARCHIVE", "NOTES"]
    lines = _format_table(
        [header] + [_table_row(plan, outcome) for plan, outcome in pairs]
    )

    problems = [
        problem.message
        for plan, _ in pairs
        for problem in plan.problems
        if problem.blocking
    ]
    settings = [
        f"{plan.row.name}: {setting} is '{manifest_value}' in the manifest and "
        f"'{synapse_value}' in Synapse; it is not changed"
        for plan, _ in pairs
        for setting, (manifest_value, synapse_value) in (
            plan.setting_differences.items()
        )
    ]
    errors = [
        f"{plan.row.name}: {error}"
        for plan, outcome in pairs
        if outcome is not None
        for error in outcome.errors
    ] + [
        f"{plan.row.name}: archiving '{entry.curation_task_name}' failed: "
        f"{entry.error}"
        for plan, _ in pairs
        for entry in plan.archives
        if entry.result is ArchiveResult.FAILED
    ]
    for heading, section in (
        ("Blocking problems:", problems),
        ("Settings that differ from the manifest:", settings),
        ("Errors:", errors),
    ):
        if section:
            lines += ["", heading] + [f"  {line}" for line in section]
    lines += ["", _count_line(pairs, applied)]
    return "\n".join(lines)


def _summary_record(
    command: str, plan: TaskPlan, outcome: TaskOutcome | None
) -> list[str]:
    """A task's row in `summary.csv`. The `apply`-only cells are empty for `plan`."""
    row = plan.row
    applied = outcome is not None
    return [
        command,
        _csv_bool(plan.overwrite),
        row.name,
        row.curation_task_name,
        row.project_id,
        _csv_text(outcome.task_id if applied else plan.task_id),
        _csv_text(outcome.record_set_id if applied else plan.record_set_id),
        plan.action.value.lower(),
        outcome.result.value.lower() if applied else "",
        str(plan.rows.to_add),
        str(plan.rows.unchanged),
        str(len(plan.rows.differing)),
        str(len(plan.rows.absent_from_file)),
        str(outcome.rows_written) if applied else "",
        str(len(outcome.validation_problems)) if applied else "",
        ";".join(_archive_text(entry) for entry in plan.archives),
        ";".join(plan.setting_differences),
        ";".join(plan.key_columns),
        ";".join(plan.missing_columns),
        ";".join(plan.skipped_columns),
        _csv_bool(plan.input_empty),
    ]


def _difference_records(plan: TaskPlan, outcome: TaskOutcome | None) -> list[list[str]]:
    """A task's rows in `differences.csv`: one per differing cell, `overwrite`
    true only when `_overwrite_shown`."""
    overwrite = _csv_bool(_overwrite_shown(plan, outcome))
    return [
        [
            plan.row.name,
            _csv_key(difference.key),
            str(difference.line),
            column,
            file_value,
            synapse_value,
            overwrite,
        ]
        for difference in plan.rows.differing
        for column, (file_value, synapse_value) in difference.columns.items()
    ]


def _problem_records(plan: TaskPlan, outcome: TaskOutcome | None) -> list[list[str]]:
    """A task's rows in `problems.csv`.

    The plan's problems, then an outcome's validation problems, errors and failed
    archives. An error or failed archive is a blocking Synapse problem whose message
    names the task.
    """
    problems = list(plan.problems)
    if outcome is not None:
        problems += outcome.validation_problems
        problems += [
            CurationProblem(
                task=plan.row.name,
                kind=ProblemKind.SYNAPSE,
                message=f"{plan.row.name}: {error}",
            )
            for error in outcome.errors
        ]
    problems += [
        CurationProblem(
            task=plan.row.name,
            kind=ProblemKind.SYNAPSE,
            message=(
                f"{plan.row.name}: archiving '{entry.curation_task_name}' failed: "
                f"{entry.error}"
            ),
        )
        for entry in plan.archives
        if entry.result is ArchiveResult.FAILED
    ]
    return [
        [
            problem.task,
            problem.kind.value.lower(),
            _csv_bool(problem.blocking),
            _csv_text(problem.file),
            _csv_text(problem.line),
            _csv_key(problem.key),
            _csv_text(problem.column),
            problem.message,
        ]
        for problem in problems
    ]


def _write_csv(path: Path, header: tuple[str, ...], records: list[list[str]]) -> None:
    """Replace a UTF-8 CSV with a header and records."""
    with open(path, "w", encoding="utf-8", newline="") as file:
        writer = csv.writer(file)
        writer.writerow(header)
        writer.writerows(records)


def write_curation_report(
    report_dir: str | Path,
    command: str,
    results: list[TaskPlan] | list[TaskOutcome],
) -> None:
    """Write a run's report: `summary.csv`, `differences.csv` and `problems.csv`.

    The folder is created if needed. The three files are replaced if present, and
    nothing else in the folder is touched.

    Arguments:
        report_dir: The folder to write into.
        command: `plan` or `apply`.
        results: What `plan_curation_tasks` or `apply_curation_tasks` returned.
    """
    folder = Path(report_dir)
    folder.mkdir(parents=True, exist_ok=True)
    pairs = _split_results(results)
    _write_csv(
        folder / "summary.csv",
        _SUMMARY_COLUMNS,
        [_summary_record(command, plan, outcome) for plan, outcome in pairs],
    )
    _write_csv(
        folder / "differences.csv",
        _DIFFERENCES_COLUMNS,
        [
            record
            for plan, outcome in pairs
            for record in _difference_records(plan, outcome)
        ],
    )
    _write_csv(
        folder / "problems.csv",
        _PROBLEMS_COLUMNS,
        [
            record
            for plan, outcome in pairs
            for record in _problem_records(plan, outcome)
        ],
    )


def curation_run_failed(results: list[TaskPlan] | list[TaskOutcome]) -> bool:
    """Whether a run exits non-zero: any task blocked or failed, or any archive
    failed. Validation problems alone do not count.

    Arguments:
        results: What `plan_curation_tasks` or `apply_curation_tasks` returned.

    Returns:
        True when the run failed.
    """
    for plan, outcome in _split_results(results):
        if plan.blocked or any(
            entry.result is ArchiveResult.FAILED for entry in plan.archives
        ):
            return True
        if outcome is not None and outcome.result in (
            TaskResult.BLOCKED,
            TaskResult.FAILED,
        ):
            return True
    return False
