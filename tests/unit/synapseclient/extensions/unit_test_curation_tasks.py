"""Unit tests for bulk curation tasks: the manifest, planning, applying and reports."""

import csv
import inspect
import io
import tempfile
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.core.models.permission import Permissions
from synapseclient.extensions.curator import curation_tasks
from synapseclient.extensions.curator.curation_task_types import (
    ArchiveEntry,
    ArchiveResult,
    CsvRecord,
    CurationManifestError,
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
from synapseclient.extensions.curator.curation_tasks import (
    apply_curation_tasks_async,
    curation_run_failed,
    format_curation_summary,
    plan_curation_tasks_async,
    write_curation_report,
)
from synapseclient.models import (
    AuthorizationMode,
    ColumnType,
    CurationTask,
    CurationTaskStatus,
    FileBasedMetadataTaskProperties,
    Folder,
    Grid,
    GridExecutionDetails,
    JSONSchema,
    RecordBasedMetadataTaskProperties,
    RecordSet,
    TaskState,
    Team,
)
from synapseclient.models.curation import (
    GridQueryResult,
    GridQueryValidationResult,
    GridRow,
    SelectAll,
    SyncType,
)

HEADER = (
    "name,project_id,folder_id,curation_task_name,record_set_name,schema_uri,"
    "upsert_keys,assignee_principal_id,authorization_mode,instructions,path,"
    "leave_out_columns,archive"
)


def _row(**overrides: str) -> str:
    """A manifest line for HEADER, with any cells replaced."""
    cells = {
        "name": "lab_a",
        "project_id": "syn111",
        "folder_id": "syn112",
        "curation_task_name": "Lab A biospecimens",
        "record_set_name": "",
        "schema_uri": "org.namhub-biospecimen-1.2.0",
        "upsert_keys": "specimenID",
        "assignee_principal_id": "3412345",
        "authorization_mode": "SOURCE_BENEFACTOR",
        "instructions": "Fill one row per specimen.",
        "path": "lab_a.csv",
        "leave_out_columns": "",
        "archive": "",
    }
    cells.update(overrides)
    return ",".join(cells[column] for column in HEADER.split(","))


def _write(folder: Path, *lines: str, inputs: tuple[str, ...] = ("lab_a.csv",)) -> Path:
    """Write a manifest and empty input files under `folder`."""
    for name in inputs:
        (folder / name).write_text("specimenID\n", encoding="utf-8")
    manifest = folder / "tasks.csv"
    manifest.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return manifest


def _problems(manifest: Path) -> list[str]:
    """The problems from_csv raises for a manifest."""
    with pytest.raises(CurationManifestError) as error:
        CurationTaskManifest.from_csv(manifest)
    return error.value.problems


class TestCurationTaskManifest:
    def test_valid_manifest_parses(self, tmp_path: Path) -> None:
        # GIVEN a manifest with a byte-order mark, padded cells and list cells
        (tmp_path / "inputs").mkdir()
        (tmp_path / "inputs" / "lab_b.csv").write_text("x\n", encoding="utf-8")
        manifest_path = _write(
            tmp_path,
            "﻿" + HEADER,
            _row(
                name=" lab_a ",
                upsert_keys="specimenID; individualID ;",
                leave_out_columns="notes;;extra",
                archive="Lab A v1",
            ),
            _row(
                name="lab_b",
                project_id="syn221",
                folder_id="syn222",
                curation_task_name="Lab B biospecimens",
                record_set_name="Lab B table",
                authorization_mode="SESSION_OWNER",
                path="inputs/lab_b.csv",
            ),
        )

        # WHEN I read it
        manifest = CurationTaskManifest.from_csv(manifest_path)

        # THEN every row is built with stripped, split and resolved values
        assert manifest.path == manifest_path
        lab_a, lab_b = manifest.rows
        assert lab_a.name == "lab_a"
        assert lab_a.line == 2
        assert lab_a.upsert_keys == ["specimenID", "individualID"]
        assert lab_a.leave_out_columns == ["notes", "extra"]
        assert lab_a.archive == ["Lab A v1"]
        assert lab_a.record_set_name == "Lab A biospecimens"
        assert lab_a.authorization_mode is AuthorizationMode.SOURCE_BENEFACTOR
        assert lab_a.path == tmp_path / "lab_a.csv"
        assert lab_b.line == 3
        assert lab_b.record_set_name == "Lab B table"
        assert lab_b.authorization_mode is AuthorizationMode.SESSION_OWNER
        assert lab_b.path == tmp_path / "inputs" / "lab_b.csv"
        assert lab_b.path.is_absolute()
        assert lab_b.leave_out_columns == []
        assert lab_b.archive == []

    def test_optional_columns_may_be_absent(self, tmp_path: Path) -> None:
        # GIVEN a manifest with only the required columns, in another order
        columns = [
            "path",
            "name",
            "project_id",
            "folder_id",
            "curation_task_name",
            "schema_uri",
            "upsert_keys",
            "assignee_principal_id",
            "authorization_mode",
            "instructions",
        ]
        values = [
            "lab_a.csv",
            "lab_a",
            "syn111",
            "syn112",
            "Task",
            "org-s-1.0.0",
            "id",
            "1",
            "SESSION_OWNER",
            "Do it.",
        ]
        manifest_path = _write(tmp_path, ",".join(columns), ",".join(values))

        # WHEN I read it
        manifest = CurationTaskManifest.from_csv(manifest_path)

        # THEN the optional values take their defaults
        (row,) = manifest.rows
        assert row.record_set_name == "Task"
        assert row.leave_out_columns == []
        assert row.archive == []

    def test_quoted_commas_and_line_breaks_survive(self, tmp_path: Path) -> None:
        # GIVEN a manifest whose instructions hold a comma and a line break
        manifest_path = _write(
            tmp_path,
            HEADER,
            _row(instructions='"One row, per specimen.\nNo blanks."'),
            _row(name="lab_b", folder_id="syn113", curation_task_name="B"),
        )

        # WHEN I read it
        manifest = CurationTaskManifest.from_csv(manifest_path)

        # THEN the value is kept whole and the next row starts on its physical line
        lab_a, lab_b = manifest.rows
        assert lab_a.instructions == "One row, per specimen.\nNo blanks."
        assert lab_a.line == 2
        assert lab_b.line == 4

    def test_csv_reader_keeps_cells_and_physical_lines(self, tmp_path: Path) -> None:
        # GIVEN a CSV with a byte-order mark, padded cells, a blank line and a
        # quoted value spanning lines
        path = tmp_path / "input.csv"
        path.write_bytes('﻿a,b\r\n" x ",""\r\n\r\n"1\r\n2",y \r\n'.encode("utf-8"))

        # WHEN I read it
        header, rows = read_csv_records(path)

        # THEN cells are as read, and each record has the line where it starts
        assert header == ["a", "b"]
        assert [(row.line, row.cells) for row in rows] == [
            (2, [" x ", ""]),
            (3, []),
            (4, ["1\r\n2", "y "]),
        ]

    def test_blank_rows_are_skipped(self, tmp_path: Path) -> None:
        # GIVEN a manifest with an empty line and a row of empty cells
        manifest_path = _write(tmp_path, HEADER, "", " , ,", _row())

        # WHEN I read it
        manifest = CurationTaskManifest.from_csv(manifest_path)

        # THEN only the task row is kept, at its own line
        assert [row.line for row in manifest.rows] == [4]

    def test_missing_and_unknown_columns(self, tmp_path: Path) -> None:
        # GIVEN a manifest without schema_uri, with an unknown, a duplicate and an
        # unnamed column
        header = HEADER.replace("schema_uri,", "") + ",color,name,"
        row = _row().replace(",org.namhub-biospecimen-1.2.0", "") + ",red,lab_a,"
        manifest_path = _write(tmp_path, header, row)

        # WHEN I read it
        problems = _problems(manifest_path)

        # THEN each column problem is reported on line 1, and the missing column's
        # blank values are not reported again
        assert problems == [
            "line 1: unknown column 'color'",
            "line 1: column 'name' appears more than once",
            "line 1: column 15 has no name",
            "line 1: missing required column 'schema_uri'",
        ]

    @pytest.mark.parametrize(
        "column",
        [
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
        ],
    )
    def test_blank_required_value(self, tmp_path: Path, column: str) -> None:
        # GIVEN a manifest with one required value blank
        manifest_path = _write(tmp_path, HEADER, _row(**{column: "  "}))

        # WHEN I read it
        problems = _problems(manifest_path)

        # THEN the blank value is reported with its line
        where = "line 2" if column == "name" else "line 2 (lab_a)"
        assert f"{where}: required value '{column}' is blank" in problems

    def test_upsert_keys_of_only_separators_is_blank(self, tmp_path: Path) -> None:
        # GIVEN a manifest whose upsert_keys holds only separators
        manifest_path = _write(tmp_path, HEADER, _row(upsert_keys=" ; ;"))

        # WHEN I read it
        problems = _problems(manifest_path)

        # THEN upsert_keys is reported blank
        assert problems == ["line 2 (lab_a): required value 'upsert_keys' is blank"]

    def test_malformed_synapse_ids_and_authorization_mode(self, tmp_path: Path) -> None:
        # GIVEN a manifest with malformed IDs and an unknown authorization mode
        manifest_path = _write(
            tmp_path,
            HEADER,
            _row(
                project_id="111",
                folder_id="syn112.3",
                authorization_mode="source_benefactor",
            ),
        )

        # WHEN I read it
        problems = _problems(manifest_path)

        # THEN each is reported with its line
        assert problems == [
            "line 2 (lab_a): project_id '111' is not a Synapse ID (synNNN)",
            "line 2 (lab_a): folder_id 'syn112.3' is not a Synapse ID (synNNN)",
            "line 2 (lab_a): authorization_mode 'source_benefactor' is not "
            "SESSION_OWNER or SOURCE_BENEFACTOR",
        ]

    def test_missing_input_file(self, tmp_path: Path) -> None:
        # GIVEN a manifest whose input file does not exist
        manifest_path = _write(tmp_path, HEADER, _row(path="gone.csv"))

        # WHEN I read it
        problems = _problems(manifest_path)

        # THEN the resolved path is reported
        assert problems == [
            f"line 2 (lab_a): input file {tmp_path / 'gone.csv'} does not exist"
        ]

    def test_extra_cells(self, tmp_path: Path) -> None:
        # GIVEN a row with more cells than the header
        manifest_path = _write(tmp_path, HEADER, _row() + ",surplus")

        # WHEN I read it
        problems = _problems(manifest_path)

        # THEN the row is reported
        assert problems == ["line 2: the row has 14 cells but the header has 13"]

    def test_duplicate_name(self, tmp_path: Path) -> None:
        # GIVEN two rows with the same name
        manifest_path = _write(
            tmp_path,
            HEADER,
            _row(),
            _row(folder_id="syn113", curation_task_name="Other"),
        )

        # WHEN I read it
        problems = _problems(manifest_path)

        # THEN the second row is reported against the first
        assert problems == [
            "line 3 (lab_a): name 'lab_a' is also used by line 2 (lab_a)"
        ]

    def test_duplicate_task_in_project(self, tmp_path: Path) -> None:
        # GIVEN two rows naming the same task in one project
        manifest_path = _write(
            tmp_path, HEADER, _row(), _row(name="lab_b", folder_id="syn113")
        )

        # WHEN I read it
        problems = _problems(manifest_path)

        # THEN the second row is reported against the first
        assert problems == [
            "line 3 (lab_b): curation task 'Lab A biospecimens' in syn111 is also "
            "defined by line 2 (lab_a)"
        ]

    def test_same_task_name_in_other_project_is_allowed(self, tmp_path: Path) -> None:
        # GIVEN two rows naming the same task in different projects
        manifest_path = _write(
            tmp_path,
            HEADER,
            _row(),
            _row(name="lab_b", project_id="syn221", folder_id="syn222"),
        )

        # WHEN I read it
        manifest = CurationTaskManifest.from_csv(manifest_path)

        # THEN both rows are kept
        assert len(manifest.rows) == 2

    def test_duplicate_table_in_folder(self, tmp_path: Path) -> None:
        # GIVEN two rows whose tables share a name in one folder, one by default
        manifest_path = _write(
            tmp_path,
            HEADER,
            _row(),
            _row(
                name="lab_b",
                curation_task_name="Lab B",
                record_set_name="Lab A biospecimens",
            ),
        )

        # WHEN I read it
        problems = _problems(manifest_path)

        # THEN the second row is reported against the first
        assert problems == [
            "line 3 (lab_b): table 'Lab A biospecimens' in syn112 is also defined by "
            "line 2 (lab_a)"
        ]

    def test_task_archived_in_same_project(self, tmp_path: Path) -> None:
        # GIVEN a row archiving another row's task in the same project, a row
        # archiving its own task, and an archive of a task in another project
        manifest_path = _write(
            tmp_path,
            HEADER,
            _row(archive="Lab B; Lab C"),
            _row(name="lab_b", folder_id="syn113", curation_task_name="Lab B"),
            _row(
                name="lab_c",
                project_id="syn221",
                folder_id="syn222",
                curation_task_name="Lab C",
                archive="Lab C",
            ),
        )

        # WHEN I read it
        problems = _problems(manifest_path)

        # THEN both conflicts in a shared project are reported
        assert problems == [
            "line 3 (lab_b): curation task 'Lab B' in syn111 is archived by "
            "line 2 (lab_a)",
            "line 4 (lab_c): archive lists the row's own curation task 'Lab C'",
        ]

    def test_every_problem_is_reported_together(self, tmp_path: Path) -> None:
        # GIVEN a manifest with problems on several rows
        manifest_path = _write(
            tmp_path,
            HEADER + ",color",
            _row(project_id="project") + ",",
            _row(path="gone.csv", authorization_mode="NOBODY") + ",",
        )

        # WHEN I read it
        with pytest.raises(CurationManifestError) as error:
            CurationTaskManifest.from_csv(manifest_path)

        # THEN all of them are listed, and the message holds each one
        assert error.value.problems == [
            "line 1: unknown column 'color'",
            "line 2 (lab_a): project_id 'project' is not a Synapse ID (synNNN)",
            "line 3 (lab_a): authorization_mode 'NOBODY' is not SESSION_OWNER or "
            "SOURCE_BENEFACTOR",
            f"line 3 (lab_a): input file {tmp_path / 'gone.csv'} does not exist",
            "line 3 (lab_a): name 'lab_a' is also used by line 2 (lab_a)",
            "line 3 (lab_a): table 'Lab A biospecimens' in syn112 is also defined "
            "by line 2 (lab_a)",
        ]
        assert isinstance(error.value, ValueError)
        for problem in error.value.problems:
            assert problem in str(error.value)

    def test_select(self, tmp_path: Path) -> None:
        # GIVEN a manifest with three rows
        manifest_path = _write(
            tmp_path,
            HEADER,
            _row(),
            _row(name="lab_b", folder_id="syn113", curation_task_name="B"),
            _row(name="lab_c", folder_id="syn114", curation_task_name="C"),
        )
        manifest = CurationTaskManifest.from_csv(manifest_path)

        # WHEN I select rows by name
        # THEN they come back once each, in manifest order; None selects every row
        assert [r.name for r in manifest.select(["lab_c", "lab_a", "lab_c"])] == [
            "lab_a",
            "lab_c",
        ]
        assert manifest.select(None) == manifest.rows

    def test_select_rejects_undefined_names(self, tmp_path: Path) -> None:
        # GIVEN a manifest with one row
        manifest = CurationTaskManifest.from_csv(_write(tmp_path, HEADER, _row()))

        # WHEN I select names it does not define
        with pytest.raises(CurationManifestError) as error:
            manifest.select(["lab_a", "lab_x", "lab_y"])

        # THEN each undefined name is reported
        assert error.value.problems == [
            "no task named 'lab_x' in the manifest",
            "no task named 'lab_y' in the manifest",
        ]

    def test_manifest_built_in_code_passes_check(self, tmp_path: Path) -> None:
        # GIVEN a manifest built in code, with string path and mode
        input_path = tmp_path / "lab_a.csv"
        input_path.write_text("specimenID\n", encoding="utf-8")
        row = CurationTaskRow(
            name="lab_a",
            project_id="syn111",
            folder_id="syn112",
            curation_task_name="Lab A biospecimens",
            schema_uri="org.namhub-biospecimen-1.2.0",
            upsert_keys=["specimenID"],
            assignee_principal_id="3412345",
            authorization_mode="SESSION_OWNER",
            instructions="Fill one row per specimen.",
            path=str(input_path),
        )
        manifest = CurationTaskManifest(rows=[row])

        # WHEN I check it
        manifest.check()

        # THEN it passes, with defaults and coerced types filled in
        assert row.record_set_name == "Lab A biospecimens"
        assert row.authorization_mode is AuthorizationMode.SESSION_OWNER
        assert row.path == input_path
        assert row.line is None
        assert manifest.path is None

    def test_manifest_built_in_code_names_rows_by_position(
        self, tmp_path: Path
    ) -> None:
        # GIVEN a manifest built in code whose second row has problems
        input_path = tmp_path / "lab_a.csv"
        input_path.write_text("specimenID\n", encoding="utf-8")
        fields = dict(
            project_id="syn111",
            folder_id="syn112",
            schema_uri="org.namhub-biospecimen-1.2.0",
            upsert_keys=["specimenID"],
            assignee_principal_id="3412345",
            authorization_mode=AuthorizationMode.SESSION_OWNER,
            instructions="Fill one row per specimen.",
            path=input_path,
        )
        manifest = CurationTaskManifest(
            rows=[
                CurationTaskRow(name="lab_a", curation_task_name="A", **fields),
                CurationTaskRow(name="", curation_task_name="B", **fields),
            ]
        )

        # WHEN I check it
        with pytest.raises(CurationManifestError) as error:
            manifest.check()

        # THEN the row is named by its position
        assert error.value.problems == ["row 2: required value 'name' is blank"]


SCHEMA_URI = "org.namhub-biospecimen-1.2.0"
OTHER_SCHEMA_URI = "org.namhub-assay-2.0.0"
SCHEMA_BODY = {
    "properties": {
        "specimenID": {"type": "string"},
        "individualID": {"type": "string"},
        "tissue": {"type": "string"},
        "age": {"type": "integer"},
    }
}
INPUT_CSV = (
    "specimenID,tissue,internal_notes,age\n" "S1,brain,keep out,4\n" "S2,liver,,5\n"
)


def _not_found() -> SynapseHTTPError:
    """A Synapse 404."""
    return SynapseHTTPError(
        "404 Client Error: The resource could not be found.",
        response=MagicMock(status_code=404),
    )


def _task_row(folder: Path, csv_text: str = INPUT_CSV, **overrides) -> CurationTaskRow:
    """A manifest row for lab_a whose input file holds `csv_text`."""
    name = overrides.get("name", "lab_a")
    path = folder / f"{name}.csv"
    path.write_text(csv_text, encoding="utf-8")
    fields = dict(
        name=name,
        project_id="syn111",
        folder_id="syn112",
        curation_task_name="Lab A biospecimens",
        schema_uri=SCHEMA_URI,
        upsert_keys=["specimenID"],
        assignee_principal_id="3412345",
        authorization_mode=AuthorizationMode.SOURCE_BENEFACTOR,
        instructions="Fill one row per specimen.",
        path=path,
        leave_out_columns=["internal_notes"],
    )
    fields.update(overrides)
    return CurationTaskRow(**fields)


def _lab_b(folder: Path, **overrides) -> CurationTaskRow:
    """A second row, sharing nothing with lab_a's row but the schema body."""
    fields = dict(
        name="lab_b",
        project_id="syn221",
        folder_id="syn222",
        curation_task_name="Lab B biospecimens",
        schema_uri=OTHER_SCHEMA_URI,
        assignee_principal_id="3412346",
    )
    fields.update(overrides)
    return _task_row(folder, **fields)


class TestPlanNewTasks:
    @pytest.fixture(autouse=True)
    def mock_synapse(self, mocker, syn: Synapse) -> None:
        """Mock every Synapse call planning makes, and every call that writes."""
        self.syn = syn
        self.tasks_by_project: dict[str, list[CurationTask]] = {}
        self.missing_schemas: set[str] = set()
        self.missing_folders: set[str] = set()
        self.read_only_folders: set[str] = set()
        self.missing_teams: set[str] = set()
        self.folder_projects = {"syn112": "syn111", "syn113": "syn111"}
        self.folder_entities: dict[tuple[str, str], str] = {}

        async def list_tasks(project_id: str, **kwargs) -> AsyncIterator[CurationTask]:
            for task in self.tasks_by_project.get(project_id, []):
                yield task

        async def get_schema(schema: JSONSchema, **kwargs) -> JSONSchema:
            if f"{schema.organization_name}-{schema.name}" in {
                uri.rsplit("-", 1)[0] for uri in self.missing_schemas
            }:
                raise ValueError(
                    f"Organization: '{schema.organization_name}' does not contain a "
                    f"schema with name: '{schema.name}'"
                )
            return schema

        async def get_body(schema: JSONSchema, version=None, **kwargs) -> dict:
            return SCHEMA_BODY

        async def get_permissions(folder: Folder, **kwargs) -> Permissions:
            if folder.id in self.missing_folders:
                raise _not_found()
            return Permissions(can_edit=folder.id not in self.read_only_folders)

        async def get_team(team: Team, **kwargs) -> Team:
            if str(team.id) in self.missing_teams:
                raise _not_found()
            return team

        async def folder_project(entity_id: str, synapse_client: Synapse) -> str:
            return self.folder_projects.get(entity_id, "syn221")

        async def find_entity(name: str, parent: str, **kwargs) -> str | None:
            return self.folder_entities.get((parent, name))

        self.list_tasks = mocker.patch.object(
            CurationTask, "list_async", side_effect=list_tasks
        )
        self.get_schema = mocker.patch.object(
            JSONSchema, "get_async", autospec=True, side_effect=get_schema
        )
        self.get_body = mocker.patch.object(
            JSONSchema, "get_body_async", autospec=True, side_effect=get_body
        )
        self.get_permissions = mocker.patch.object(
            Folder, "get_permissions_async", autospec=True, side_effect=get_permissions
        )
        self.get_team = mocker.patch.object(
            Team, "get_async", autospec=True, side_effect=get_team
        )
        self.folder_project = mocker.patch.object(
            curation_tasks,
            "project_id_from_entity_id_async",
            side_effect=folder_project,
        )
        self.find_entity = mocker.patch.object(
            curation_tasks, "find_entity_id_async", side_effect=find_entity
        )
        self.writes = [
            mocker.patch.object(model, method)
            for model, method in (
                (CurationTask, "store_async"),
                (CurationTask, "set_task_state_async"),
                (CurationTask, "delete_async"),
                (RecordSet, "store_async"),
                (RecordSet, "delete_async"),
                (Grid, "create_async"),
                (Grid, "import_csv_async"),
                (Grid, "synchronize_async"),
                (Grid, "delete_async"),
            )
        ]

    def _assert_nothing_written(self) -> None:
        for write in self.writes:
            write.assert_not_called()

    async def _plan(self, *rows: CurationTaskRow, **kwargs):
        return await plan_curation_tasks_async(
            CurationTaskManifest(rows=list(rows)), synapse_client=self.syn, **kwargs
        )

    async def test_task_to_create(self, tmp_path: Path) -> None:
        # GIVEN a row for a task that does not exist, whose file leaves out one
        # column and lacks one schema property
        row = _task_row(tmp_path)

        # WHEN I plan it
        (plan,) = await self._plan(row)

        # THEN it is to be created with every file row, keys first, then the file's
        # columns in order, then the schema properties the file lacks
        assert plan.row is row
        assert plan.action is TaskAction.CREATE
        assert not plan.blocked
        assert plan.problems == []
        assert plan.rows == RowChanges(to_add=2)
        assert plan.rows_to_write == 2
        assert plan.key_columns == ["specimenID"]
        assert plan.table_columns == ["specimenID", "tissue", "age", "individualID"]
        assert plan.missing_columns == ["individualID"]
        assert plan.skipped_columns == []
        assert plan.input_empty is False
        assert plan.task_id is None
        assert plan.record_set_id is None
        assert plan.state is None
        assert plan.attached_session_id is None
        assert plan.setting_differences == {}
        assert plan.archives == []
        assert plan.overwrite is False

        # AND each check was made against the row's own values
        self.list_tasks.assert_called_once_with(
            project_id="syn111", synapse_client=self.syn
        )
        assert self.get_body.call_args.kwargs["version"] == "1.2.0"
        assert self.get_permissions.call_args.args[0].id == "syn112"
        assert self.get_team.call_args.args[0].id == "3412345"
        self.folder_project.assert_called_once_with(
            entity_id="syn112", synapse_client=self.syn
        )
        self.find_entity.assert_called_once_with(
            name="Lab A biospecimens", parent="syn112", synapse_client=self.syn
        )
        self._assert_nothing_written()

    async def test_key_columns_lead_the_table(self, tmp_path: Path) -> None:
        # GIVEN two key columns, the second one last in the file
        row = _task_row(
            tmp_path,
            "tissue,specimenID,individualID\nbrain,S1,I1\n",
            upsert_keys=["specimenID", "individualID"],
        )

        # WHEN I plan it
        (plan,) = await self._plan(row)

        # THEN the keys come first, in the manifest's order
        assert plan.action is TaskAction.CREATE
        assert plan.table_columns == ["specimenID", "individualID", "tissue", "age"]
        assert plan.missing_columns == ["age"]

    async def test_overwrite_is_carried(self, tmp_path: Path) -> None:
        # GIVEN a row for a task to create
        row = _task_row(tmp_path)

        # WHEN I plan it with overwrite
        (plan,) = await self._plan(row, overwrite=True)

        # THEN the plan carries the setting, and only new rows are to be written
        assert plan.overwrite is True
        assert plan.rows_to_write == 2

    async def test_column_outside_schema_blocks(self, tmp_path: Path) -> None:
        # GIVEN an input file with a column that is neither a schema property nor
        # left out
        row = _task_row(tmp_path, "specimenID,color,internal_notes\nS1,red,x\n")

        # WHEN I plan it
        (plan,) = await self._plan(row)

        # THEN the task is blocked and the column named
        assert plan.action is TaskAction.BLOCKED
        assert plan.blocked
        assert plan.problems == [
            CurationProblem(
                task="lab_a",
                kind=ProblemKind.INPUT,
                blocking=True,
                message=(
                    f"lab_a: input file {row.path} line 1: column 'color' is not a "
                    f"property of schema {SCHEMA_URI}; list it in leave_out_columns "
                    "to leave it out"
                ),
                file=str(row.path),
                line=1,
                column="color",
            )
        ]
        assert plan.table_columns == ["specimenID", "individualID", "tissue", "age"]

    async def test_duplicate_and_blank_keys_block(self, tmp_path: Path) -> None:
        # GIVEN a file with a quoted line break, a duplicate key, an empty key and
        # a key of only whitespace
        row = _task_row(
            tmp_path,
            'specimenID,tissue\nS1,"brain\nstem"\nS2,liver\nS1,heart\n,lung\n  ,skin\n',
        )

        # WHEN I plan it
        (plan,) = await self._plan(row)

        # THEN each is a blocking problem at the physical line its row starts on
        file = str(row.path)
        assert plan.action is TaskAction.BLOCKED
        assert [
            (problem.kind, problem.line, problem.key, problem.column, problem.message)
            for problem in plan.problems
        ] == [
            (
                ProblemKind.INPUT,
                5,
                {"specimenID": "S1"},
                "specimenID",
                f"lab_a: input file {file} line 5: key specimenID 'S1' is also on "
                "line 2",
            ),
            (
                ProblemKind.INPUT,
                6,
                {"specimenID": ""},
                "specimenID",
                f"lab_a: input file {file} line 6: key column 'specimenID' is blank",
            ),
            (
                ProblemKind.INPUT,
                7,
                {"specimenID": ""},
                "specimenID",
                f"lab_a: input file {file} line 7: key column 'specimenID' is blank",
            ),
        ]
        assert all(problem.file == file for problem in plan.problems)
        assert plan.rows.to_add == 5

    async def test_duplicate_compound_key_names_every_key_column(
        self, tmp_path: Path
    ) -> None:
        # GIVEN two key columns and a row repeating another's pair of values
        row = _task_row(
            tmp_path,
            "specimenID,individualID\nS1,I1\nS1,I2\nS1,I1\n",
            upsert_keys=["specimenID", "individualID"],
        )

        # WHEN I plan it
        (plan,) = await self._plan(row)

        # THEN only the repeated pair blocks, with both values named
        (problem,) = plan.problems
        assert problem.message == (
            f"lab_a: input file {row.path} line 4: key specimenID 'S1', "
            "individualID 'I1' is also on line 2"
        )
        assert problem.key == {"specimenID": "S1", "individualID": "I1"}
        assert problem.column is None

    async def test_key_column_absent_from_file_blocks(self, tmp_path: Path) -> None:
        # GIVEN a file without a key column, and one that leaves the other key out
        row = _task_row(
            tmp_path,
            "tissue,individualID\nbrain,I1\n",
            upsert_keys=["specimenID", "individualID"],
            leave_out_columns=["individualID"],
        )

        # WHEN I plan it
        (plan,) = await self._plan(row)

        # THEN each missing key column blocks once, not once per row
        assert [problem.message for problem in plan.problems] == [
            f"lab_a: input file {row.path}: key column 'specimenID' is not in the "
            "file",
            f"lab_a: input file {row.path}: key column 'individualID' is not in the "
            "file",
        ]
        assert [problem.column for problem in plan.problems] == [
            "specimenID",
            "individualID",
        ]
        assert plan.table_columns == ["specimenID", "individualID", "tissue", "age"]
        assert plan.missing_columns == ["specimenID", "individualID", "age"]

    async def test_key_column_outside_schema_blocks(self, tmp_path: Path) -> None:
        # GIVEN a key column that the schema does not define
        row = _task_row(
            tmp_path, "sampleID,tissue\nX1,brain\n", upsert_keys=["sampleID"]
        )

        # WHEN I plan it
        (plan,) = await self._plan(row)

        # THEN the key is reported once, as the schema's problem
        (problem,) = plan.problems
        assert problem.kind is ProblemKind.SYNAPSE
        assert problem.column == "sampleID"
        assert problem.message == (
            f"lab_a: key column 'sampleID' is not a property of schema {SCHEMA_URI}"
        )

    async def test_header_problems_block(self, tmp_path: Path) -> None:
        # GIVEN a file with an unnamed column, a repeated column and a long row
        row = _task_row(
            tmp_path,
            "specimenID,,tissue,tissue\nS1,x,brain,liver\nS2,y,a,b,surplus\n",
        )

        # WHEN I plan it
        (plan,) = await self._plan(row)

        # THEN each blocks, with its line
        file = row.path
        assert [problem.message for problem in plan.problems] == [
            f"lab_a: input file {file} line 1: column 2 has no name",
            f"lab_a: input file {file} line 1: column 'tissue' appears more than "
            "once",
            f"lab_a: input file {file} line 3: the row has 5 cells but the header "
            "has 4",
        ]

    @pytest.mark.parametrize(
        "text", ["", "specimenID,tissue\n", "specimenID,tissue\n,\n\n"]
    )
    async def test_empty_input_file(self, tmp_path: Path, text: str) -> None:
        # GIVEN an input file with no data rows
        row = _task_row(tmp_path, text)

        # WHEN I plan it
        (plan,) = await self._plan(row)

        # THEN the task is still to be created, with the file noted as empty
        assert plan.action is TaskAction.CREATE
        assert plan.problems == []
        assert plan.input_empty is True
        assert plan.rows == RowChanges(to_add=0)

    async def test_unreadable_input_file_blocks(self, tmp_path: Path) -> None:
        # GIVEN an input file that is not UTF-8
        row = _task_row(tmp_path)
        row.path.write_bytes(b"specimenID\n\xff\xfe\n")

        # WHEN I plan it
        (plan,) = await self._plan(row)

        # THEN the task is blocked, and the file is not called empty
        (problem,) = plan.problems
        assert problem.kind is ProblemKind.INPUT
        assert problem.message.startswith(
            f"lab_a: input file {row.path} could not be read: "
        )
        assert plan.input_empty is False

    @pytest.mark.parametrize(
        "case, message",
        [
            (
                "missing schema",
                f"lab_a: schema {SCHEMA_URI} is not registered in Synapse",
            ),
            ("missing folder", "lab_a: folder syn112 does not exist"),
            ("read-only folder", "lab_a: you cannot edit folder syn112"),
            ("missing team", "lab_a: team 3412345 does not exist"),
            (
                "folder in another project",
                "lab_a: folder syn112 is in project syn999, not syn111",
            ),
            (
                "table name taken",
                "lab_a: folder syn112 already holds an entity named 'Lab A "
                "biospecimens' (syn500), but project syn111 has no curation task "
                "named 'Lab A biospecimens'; rename or remove that entity, or give "
                "the row another record_set_name",
            ),
        ],
    )
    async def test_synapse_state_blocks_only_its_task(
        self, tmp_path: Path, case: str, message: str
    ) -> None:
        # GIVEN two rows, and Synapse state that is wrong for the first only
        if case == "missing schema":
            self.missing_schemas.add(SCHEMA_URI)
        elif case == "missing folder":
            self.missing_folders.add("syn112")
        elif case == "read-only folder":
            self.read_only_folders.add("syn112")
        elif case == "missing team":
            self.missing_teams.add("3412345")
        elif case == "folder in another project":
            self.folder_projects["syn112"] = "syn999"
        elif case == "table name taken":
            self.folder_entities[("syn112", "Lab A biospecimens")] = "syn500"
        lab_a = _task_row(tmp_path)
        lab_b = _lab_b(tmp_path)

        # WHEN I plan both
        plan_a, plan_b = await self._plan(lab_a, lab_b)

        # THEN only the first is blocked, with the reason
        assert plan_a.action is TaskAction.BLOCKED
        assert [(p.kind, p.blocking, p.message) for p in plan_a.problems] == [
            (ProblemKind.SYNAPSE, True, message)
        ]
        assert plan_b.action is TaskAction.CREATE
        assert plan_b.problems == []
        self._assert_nothing_written()

    async def test_missing_schema_version_blocks(self, tmp_path: Path) -> None:
        # GIVEN a schema whose named version is not registered
        self.get_body.side_effect = _not_found()
        row = _task_row(tmp_path)

        # WHEN I plan it
        (plan,) = await self._plan(row)

        # THEN the task is blocked as for a missing schema, and its file's columns
        # are not judged against a schema that was not read
        assert [p.message for p in plan.problems] == [
            f"lab_a: schema {SCHEMA_URI} is not registered in Synapse"
        ]
        assert plan.table_columns == ["specimenID", "tissue", "age"]
        assert plan.missing_columns == []

    async def test_missing_folder_skips_the_folder_checks(self, tmp_path: Path) -> None:
        # GIVEN a folder that does not exist
        self.missing_folders.add("syn112")

        # WHEN I plan a task in it
        (plan,) = await self._plan(_task_row(tmp_path))

        # THEN the folder's project and contents are not looked up
        assert [p.message for p in plan.problems] == [
            "lab_a: folder syn112 does not exist"
        ]
        self.folder_project.assert_not_called()
        self.find_entity.assert_not_called()

    @pytest.mark.parametrize(
        "lookup, message",
        [
            (
                "list_tasks",
                "lab_a: Synapse error while listing the curation tasks in syn111: "
                "503 Server Error",
            ),
            (
                "get_body",
                f"lab_a: Synapse error while reading schema {SCHEMA_URI}: "
                "503 Server Error",
            ),
            (
                "get_permissions",
                "lab_a: Synapse error while checking folder syn112: 503 Server Error",
            ),
            (
                "get_team",
                "lab_a: Synapse error while reading team 3412345: 503 Server Error",
            ),
            (
                "folder_project",
                "lab_a: Synapse error while finding the project of folder syn112: "
                "503 Server Error",
            ),
            (
                "find_entity",
                "lab_a: Synapse error while looking for 'Lab A biospecimens' in "
                "folder syn112: 503 Server Error",
            ),
        ],
    )
    async def test_synapse_error_becomes_a_problem(
        self, tmp_path: Path, lookup: str, message: str
    ) -> None:
        # GIVEN a Synapse call that fails for every task
        getattr(self, lookup).side_effect = SynapseHTTPError(
            "503 Server Error", response=MagicMock(status_code=503)
        )

        # WHEN I plan two tasks
        plans = await self._plan(_task_row(tmp_path), _lab_b(tmp_path))

        # THEN each is blocked by the error, and none is raised
        assert [plan.action for plan in plans] == [TaskAction.BLOCKED] * 2
        problem = plans[0].problems[0]
        assert problem.kind is ProblemKind.SYNAPSE
        assert problem.blocking
        assert problem.message == message
        assert plans[1].problems[0].message.startswith("lab_b: Synapse error while ")
        self._assert_nothing_written()

    async def test_existing_task_is_not_planned_for_creation(
        self, tmp_path: Path
    ) -> None:
        # GIVEN a project holding a task of the row's name and one of another name
        self.tasks_by_project["syn111"] = [
            CurationTask(task_id=6, data_type="Lab A v1", project_id="syn111"),
            CurationTask(
                task_id=7, data_type="Lab A biospecimens", project_id="syn111"
            ),
        ]

        # WHEN I plan the row
        (plan,) = await self._plan(_task_row(tmp_path))

        # THEN it names the existing task and checks nothing for a new one
        assert plan.action is not TaskAction.CREATE
        assert plan.task_id == 7
        self.find_entity.assert_not_called()
        self._assert_nothing_written()

    async def test_lookups_are_made_once_per_run(self, tmp_path: Path) -> None:
        # GIVEN two rows sharing a project, folder, schema and team
        lab_a = _task_row(tmp_path)
        lab_b = _task_row(
            tmp_path, name="lab_b", curation_task_name="Lab B biospecimens"
        )

        # WHEN I plan them, twice
        await self._plan(lab_a, lab_b)
        await self._plan(lab_a, lab_b)

        # THEN each shared lookup is made once per run, and the table name is
        # looked up per task
        assert self.list_tasks.call_count == 2
        assert self.get_schema.call_count == 2
        assert self.get_body.call_count == 2
        assert self.get_permissions.call_count == 2
        assert self.get_team.call_count == 2
        assert self.folder_project.call_count == 2
        assert self.find_entity.call_count == 4

    async def test_tasks_limits_the_run(self, tmp_path: Path) -> None:
        # GIVEN a manifest with two rows
        lab_a = _task_row(tmp_path)
        lab_b = _lab_b(tmp_path)

        # WHEN I plan one of them
        plans = await self._plan(lab_a, lab_b, tasks=["lab_b"])

        # THEN only that one is planned
        assert [plan.row.name for plan in plans] == ["lab_b"]
        self.list_tasks.assert_called_once_with(
            project_id="syn221", synapse_client=self.syn
        )

    async def test_undefined_task_name_raises(self, tmp_path: Path) -> None:
        # GIVEN a manifest with one row
        row = _task_row(tmp_path)

        # WHEN I plan a name it does not define
        with pytest.raises(CurationManifestError) as error:
            await self._plan(row, tasks=["lab_x"])

        # THEN the selection is rejected before Synapse is contacted
        assert error.value.problems == ["no task named 'lab_x' in the manifest"]
        self.list_tasks.assert_not_called()

    async def test_progress_is_logged(self, tmp_path: Path, mocker) -> None:
        # GIVEN two rows, the second blocked
        self.missing_teams.add("3412346")
        info = mocker.patch.object(self.syn.logger, "info")

        # WHEN I plan them
        await self._plan(_task_row(tmp_path), _lab_b(tmp_path))

        # THEN each task's start and result are logged by name
        assert [call.args[0] for call in info.call_args_list] == [
            "Planning task 1 of 2: lab_a",
            "lab_a: curation task 'Lab A biospecimens' would be created with 2 rows",
            "Planning task 2 of 2: lab_b",
            "lab_b: blocked by 1 problem",
        ]


def _summary_plans(folder: Path, overwrite: bool = False) -> list[TaskPlan]:
    """Three plans: one to create, one existing with a setting difference and a
    differing row, one blocked."""
    created = TaskPlan(
        row=_task_row(folder, archive=["Lab A v1"]),
        action=TaskAction.CREATE,
        key_columns=["specimenID"],
        missing_columns=["individualID"],
        rows=RowChanges(to_add=2),
        archives=[
            ArchiveEntry(
                curation_task_name="Lab A v1", task_id=7, result=ArchiveResult.ARCHIVE
            )
        ],
        overwrite=overwrite,
    )
    existing = TaskPlan(
        row=_lab_b(folder),
        action=TaskAction.EXISTS,
        task_id=12,
        record_set_id="syn225",
        setting_differences={"instructions": ("Fill one row per specimen.", "Old")},
        key_columns=["specimenID"],
        skipped_columns=["age"],
        rows=RowChanges(
            to_add=1,
            unchanged=3,
            differing=[
                RowDifference(
                    key={"specimenID": "S2"},
                    line=3,
                    columns={"tissue": ("liver", "lung")},
                )
            ],
            absent_from_file=[{"specimenID": "S9"}],
        ),
        overwrite=overwrite,
    )
    blocked = TaskPlan(
        row=_task_row(folder, name="lab_c", curation_task_name="Lab C"),
        action=TaskAction.BLOCKED,
        key_columns=["specimenID"],
        rows=RowChanges(to_add=2),
        problems=[
            CurationProblem(
                task="lab_c",
                kind=ProblemKind.INPUT,
                message=(
                    "lab_c: input file lab_c.csv line 1: column 'site' is not a "
                    f"property of schema {SCHEMA_URI}"
                ),
                file="lab_c.csv",
                line=1,
                column="site",
            )
        ],
        overwrite=overwrite,
    )
    return [created, existing, blocked]


def _summary_outcomes(folder: Path) -> list[TaskOutcome]:
    """Three outcomes: one created with validation problems and an archived old task,
    one failed with an error, one blocked."""
    created, existing, blocked = _summary_plans(folder)
    created.archives[0].result = ArchiveResult.ARCHIVED
    validation = [
        CurationProblem(
            task="lab_a",
            kind=ProblemKind.VALIDATION,
            blocking=False,
            message=f"lab_a: input file lab_a.csv line {line}: #/age: expected integer",
            file="lab_a.csv",
            line=line,
            key={"specimenID": f"S{line - 1}"},
            column="age",
        )
        for line in (2, 3)
    ]
    return [
        TaskOutcome(
            plan=created,
            result=TaskResult.CREATED,
            task_id=21,
            record_set_id="syn230",
            rows_written=2,
            validation_problems=validation,
        ),
        TaskOutcome(
            plan=existing,
            result=TaskResult.FAILED,
            task_id=12,
            record_set_id="syn225",
            errors=["synchronize reported: row 4 rejected"],
        ),
        TaskOutcome(plan=blocked, result=TaskResult.BLOCKED),
    ]


def _read_report(folder: Path, name: str) -> list[list[str]]:
    """A report file's rows, header first."""
    with open(folder / name, encoding="utf-8", newline="") as file:
        return list(csv.reader(file))


class TestSummaryAndReport:
    def test_plan_summary(self, tmp_path: Path) -> None:
        # GIVEN a plan of three tasks: one to create, one existing, one blocked
        plans = _summary_plans(tmp_path)

        # WHEN I format its summary
        summary = format_curation_summary("plan", plans)

        # THEN the table shows each action, `-` for the blocked task's cells, the
        # notes, then the problem, the setting that differs and the count line
        assert summary == (
            "TASK   ACTION   ADD  UNCHANGED  DIFFERS  ABSENT  ARCHIVE            NOTES\n"
            "lab_a  create   2    0          0        0       Lab A v1: archive  "
            "not in file: individualID\n"
            "lab_b  exists   1    3          1        1       -                  "
            "not loaded: age; 1 differing row left alone; 1 row absent from file; "
            "settings differ: instructions\n"
            "lab_c  blocked  -    -          -        -       -                  "
            "1 blocking problem\n"
            "\n"
            "Blocking problems:\n"
            "  lab_c: input file lab_c.csv line 1: column 'site' is not a property "
            f"of schema {SCHEMA_URI}\n"
            "\n"
            "Settings that differ from the manifest:\n"
            "  lab_b: instructions is 'Fill one row per specimen.' in the manifest "
            "and 'Old' in Synapse; it is not changed\n"
            "\n"
            "3 tasks: 1 create, 1 exists, 1 blocked; archives: 1 archive"
        )

    def test_plan_summary_with_overwrite(self, tmp_path: Path) -> None:
        # GIVEN the plan made with overwrite
        plans = _summary_plans(tmp_path, overwrite=True)

        # WHEN I format its summary
        summary = format_curation_summary("plan", plans)

        # THEN the differing row is shown as to be overwritten
        assert "1 differing row to be overwritten" in summary
        assert "left alone" not in summary

    @pytest.mark.parametrize(
        "case, notes, overwritten",
        [
            (
                "blocked plan",
                "1 blocking problem; not loaded: age; 1 differing row left alone; "
                "1 row absent from file; settings differ: instructions",
                False,
            ),
            (
                "blocked apply",
                "1 blocking problem; not loaded: age; 1 differing row left alone; "
                "1 row absent from file; settings differ: instructions",
                False,
            ),
            (
                "failed apply, not synchronized",
                "1 error; not loaded: age; 1 differing row left alone; 1 row absent "
                "from file; settings differ: instructions",
                False,
            ),
            (
                "failed apply, rows written",
                "1 error; not loaded: age; 1 differing row overwritten; 1 row absent "
                "from file; settings differ: instructions",
                True,
            ),
        ],
    )
    def test_overwrite_shown_only_when_rows_are_written(
        self, tmp_path: Path, case: str, notes: str, overwritten: bool
    ) -> None:
        # GIVEN an existing task's plan made with overwrite, with a differing row,
        # that is blocked, or whose apply failed before or after its write
        _, plan, _ = _summary_plans(tmp_path, overwrite=True)
        outcome = None
        if case.startswith("blocked"):
            plan.action = TaskAction.BLOCKED
            plan.problems.append(
                CurationProblem(
                    task="lab_b",
                    kind=ProblemKind.SYNAPSE,
                    message="lab_b: curation task 'Lab B biospecimens' (12) has grid "
                    "session sess-1 attached, and 2 rows would be written to it",
                )
            )
            if case == "blocked apply":
                outcome = TaskOutcome(plan=plan, result=TaskResult.BLOCKED)
        else:
            outcome = TaskOutcome(
                plan=plan,
                result=TaskResult.FAILED,
                rows_written=2 if overwritten else 0,
                errors=["Synapse error while filling table syn225: 503 x"],
            )
        command = "plan" if outcome is None else "apply"
        results = [plan] if outcome is None else [outcome]

        # WHEN I format its summary and write its report
        summary = format_curation_summary(command, results)
        write_curation_report(tmp_path / "report", command, results)

        # THEN the differing row is shown as overwritten only when rows were
        # written, in the NOTES column and in differences.csv
        assert "; ".join(curation_tasks._notes(plan, outcome)) == notes
        assert notes in summary
        differences = _read_report(tmp_path / "report", "differences.csv")
        assert [record[-1] for record in differences[1:]] == [
            "true" if overwritten else "false"
        ]
        summary_csv = _read_report(tmp_path / "report", "summary.csv")
        assert summary_csv[1][1] == "true"

    def test_apply_summary(self, tmp_path: Path) -> None:
        # GIVEN outcomes: created with validation problems and an archived old task,
        # failed with an error and an old task left active, and blocked
        outcomes = _summary_outcomes(tmp_path)
        outcomes[1].plan.archives = [
            ArchiveEntry(curation_task_name="Lab B v1", result=ArchiveResult.ARCHIVE)
        ]

        # WHEN I format their summary
        summary = format_curation_summary("apply", outcomes)

        # THEN the table shows each result and validation count, the failed task's
        # archive cell is `-`, and the error is listed by task
        lines = summary.split("\n")
        assert lines[0].split() == [
            "TASK",
            "RESULT",
            "ADD",
            "UNCHANGED",
            "DIFFERS",
            "ABSENT",
            "VALIDATION",
            "ARCHIVE",
            "NOTES",
        ]
        assert lines[1].split()[:9] == [
            "lab_a",
            "created",
            "2",
            "0",
            "0",
            "0",
            "2",
            "Lab",
            "A",
        ]
        assert "Lab A v1: archived" in lines[1]
        assert lines[2].split()[:8] == ["lab_b", "failed", "1", "3", "1", "1", "0", "-"]
        assert "1 error" in lines[2]
        assert lines[3].split()[:8] == ["lab_c", "blocked"] + ["-"] * 6
        assert "Errors:\n  lab_b: synchronize reported: row 4 rejected" in summary
        assert "#/age" not in summary
        assert lines[-1] == (
            "3 tasks: 1 created, 1 blocked, 1 failed; 2 validation problems; "
            "archives: 1 archived"
        )

    def test_notes_are_not_problems(self, tmp_path: Path) -> None:
        # GIVEN a plan whose only findings are notes: an empty file, an old task not
        # found, missing columns and rows absent from the file
        plan = TaskPlan(
            row=_task_row(tmp_path),
            action=TaskAction.EXISTS,
            missing_columns=["age"],
            input_empty=True,
            rows=RowChanges(absent_from_file=[{"specimenID": "S1"}]),
            archives=[
                ArchiveEntry(
                    curation_task_name="Lab A v0", result=ArchiveResult.NOT_FOUND
                )
            ],
        )

        # WHEN I format its summary and write its report
        summary = format_curation_summary("plan", [plan])
        write_curation_report(tmp_path / "report", "plan", [plan])

        # THEN the notes are in the table and summary.csv, and nothing is a problem
        assert "input file is empty" in summary
        assert "Lab A v0: not_found" in summary
        assert "Blocking problems" not in summary
        assert _read_report(tmp_path / "report", "problems.csv") == [
            list(curation_tasks._PROBLEMS_COLUMNS)
        ]
        assert not curation_run_failed([plan])

    def test_empty_run(self) -> None:
        # GIVEN no results
        # WHEN I format the summary
        summary = format_curation_summary("apply", [])

        # THEN the table holds its header and the count line says so
        assert summary.split("\n")[-1] == "0 tasks; 0 validation problems"

    @pytest.mark.parametrize(
        "command, overwrite, expected_summary",
        [
            (
                "plan",
                False,
                [
                    ["plan", "false", "lab_a", "Lab A biospecimens", "syn111", "",
                     "", "create", "", "2", "0", "0", "0", "", "",
                     "Lab A v1: archive", "", "specimenID", "individualID", "",
                     "false"],
                    ["plan", "false", "lab_b", "Lab B biospecimens", "syn221", "12",
                     "syn225", "exists", "", "1", "3", "1", "1", "", "", "",
                     "instructions", "specimenID", "", "age", "false"],
                    ["plan", "false", "lab_c", "Lab C", "syn111", "", "",
                     "blocked", "", "2", "0", "0", "0", "", "", "", "",
                     "specimenID", "", "", "false"],
                ],
            ),
            (
                "plan",
                True,
                [
                    ["plan", "true", "lab_a", "Lab A biospecimens", "syn111", "",
                     "", "create", "", "2", "0", "0", "0", "", "",
                     "Lab A v1: archive", "", "specimenID", "individualID", "",
                     "false"],
                    ["plan", "true", "lab_b", "Lab B biospecimens", "syn221", "12",
                     "syn225", "exists", "", "1", "3", "1", "1", "", "", "",
                     "instructions", "specimenID", "", "age", "false"],
                    ["plan", "true", "lab_c", "Lab C", "syn111", "", "",
                     "blocked", "", "2", "0", "0", "0", "", "", "", "",
                     "specimenID", "", "", "false"],
                ],
            ),
        ],
    )  # fmt: skip
    def test_plan_report(
        self,
        tmp_path: Path,
        command: str,
        overwrite: bool,
        expected_summary: list[list[str]],
    ) -> None:
        # GIVEN the three plans
        plans = _summary_plans(tmp_path, overwrite=overwrite)

        # WHEN I write their report
        write_curation_report(tmp_path / "report", command, plans)

        # THEN each file has the contract's columns, in order, and one row per task,
        # differing cell or problem
        report = tmp_path / "report"
        assert (
            _read_report(report, "summary.csv")
            == [list(curation_tasks._SUMMARY_COLUMNS)] + expected_summary
        )
        assert _read_report(report, "differences.csv") == [
            [
                "name",
                "key",
                "line",
                "column",
                "file_value",
                "synapse_value",
                "overwrite",
            ],
            [
                "lab_b",
                "specimenID=S2",
                "3",
                "tissue",
                "liver",
                "lung",
                "true" if overwrite else "false",
            ],
        ]
        assert _read_report(report, "problems.csv") == [
            ["name", "kind", "blocking", "file", "line", "key", "column", "message"],
            [
                "lab_c",
                "input",
                "true",
                "lab_c.csv",
                "1",
                "",
                "site",
                "lab_c: input file lab_c.csv line 1: column 'site' is not a property "
                f"of schema {SCHEMA_URI}",
            ],
        ]

    def test_summary_columns_follow_the_contract(self) -> None:
        # GIVEN the contract's summary.csv columns
        # THEN the writer's columns are exactly these, in order
        assert curation_tasks._SUMMARY_COLUMNS == (
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

    def test_apply_report(self, tmp_path: Path) -> None:
        # GIVEN the three outcomes
        outcomes = _summary_outcomes(tmp_path)

        # WHEN I write their report
        write_curation_report(tmp_path, "apply", outcomes)

        # THEN summary.csv fills the apply-only cells from each outcome
        summary = _read_report(tmp_path, "summary.csv")
        columns = summary[0]
        records = [dict(zip(columns, record)) for record in summary[1:]]
        assert [
            (
                record["task_id"],
                record["record_set_id"],
                record["result"],
                record["rows_written"],
                record["validation_problems"],
                record["archives"],
            )
            for record in records
        ] == [
            ("21", "syn230", "created", "2", "2", "Lab A v1: archived"),
            ("12", "syn225", "failed", "0", "0", ""),
            ("", "", "blocked", "0", "0", ""),
        ]
        # AND problems.csv holds the blocking problem, the validation problems and
        # the error, each naming its task
        problems = _read_report(tmp_path, "problems.csv")[1:]
        assert [row[:7] for row in problems] == [
            ["lab_a", "validation", "false", "lab_a.csv", "2", "specimenID=S1", "age"],
            ["lab_a", "validation", "false", "lab_a.csv", "3", "specimenID=S2", "age"],
            ["lab_b", "synapse", "true", "", "", "", ""],
            ["lab_c", "input", "true", "lab_c.csv", "1", "", "site"],
        ]
        assert problems[2][7] == "lab_b: synchronize reported: row 4 rejected"

    def test_report_replaces_its_files_only(self, tmp_path: Path) -> None:
        # GIVEN a report folder holding old report files and another file
        report = tmp_path / "report"
        report.mkdir()
        for name in ("summary.csv", "differences.csv", "problems.csv"):
            (report / name).write_text("old\n", encoding="utf-8")
        (report / "notes.txt").write_text("mine\n", encoding="utf-8")

        # WHEN I write a report into it
        write_curation_report(report, "plan", _summary_plans(tmp_path))

        # THEN the three files are replaced and the other is untouched
        assert sorted(path.name for path in report.iterdir()) == [
            "differences.csv",
            "notes.txt",
            "problems.csv",
            "summary.csv",
        ]
        for name in ("summary.csv", "differences.csv", "problems.csv"):
            assert "old" not in (report / name).read_text(encoding="utf-8")
        assert (report / "notes.txt").read_text(encoding="utf-8") == "mine\n"

    def test_report_creates_nested_folder(self, tmp_path: Path) -> None:
        # GIVEN a folder that does not exist
        report = tmp_path / "a" / "b"

        # WHEN I write a report into it
        write_curation_report(str(report), "plan", [])

        # THEN it is created with the three files, each holding its header
        assert sorted(path.name for path in report.iterdir()) == [
            "differences.csv",
            "problems.csv",
            "summary.csv",
        ]
        assert _read_report(report, "differences.csv") == [
            list(curation_tasks._DIFFERENCES_COLUMNS)
        ]

    def test_no_credential_can_reach_the_output(self) -> None:
        # GIVEN the three helpers
        # WHEN I read their parameters
        parameters = {
            helper.__name__: list(inspect.signature(helper).parameters)
            for helper in (
                format_curation_summary,
                write_curation_report,
                curation_run_failed,
            )
        }

        # THEN each takes only the command, the results and the folder: no client,
        # so nothing they print or write can hold a credential
        assert parameters == {
            "format_curation_summary": ["command", "results"],
            "write_curation_report": ["report_dir", "command", "results"],
            "curation_run_failed": ["results"],
        }

    @pytest.mark.parametrize(
        "changes, failed",
        [
            ({}, False),
            ({"validation": True}, False),
            ({"blocked": True}, True),
            ({"result": TaskResult.FAILED}, True),
            ({"archive": ArchiveResult.FAILED}, True),
            ({"archive": ArchiveResult.NOT_FOUND}, False),
        ],
    )
    def test_run_failed(self, tmp_path: Path, changes: dict, failed: bool) -> None:
        # GIVEN a created task changed by `changes`
        outcome = _summary_outcomes(tmp_path)[0]
        if not changes.get("validation"):
            outcome.validation_problems = []
        if changes.get("blocked"):
            outcome.plan.problems = _summary_plans(tmp_path)[2].problems
            outcome.result = TaskResult.BLOCKED
        if "result" in changes:
            outcome.result = changes["result"]
        if "archive" in changes:
            outcome.plan.archives[0].result = changes["archive"]

        # WHEN I ask whether the run failed, for the outcome and for its plan
        # THEN only a blocked or failed task, or a failed archive, fails it
        assert curation_run_failed([outcome]) is failed
        assert curation_run_failed([outcome.plan]) is (
            changes.get("blocked", False)
            or changes.get("archive") is ArchiveResult.FAILED
        )


def _grid_row(
    key: str, valid: bool | None = True, message: str = "", details=None
) -> GridRow:
    """A session row keyed `specimenID`: valid, invalid with Synapse's messages, or
    not yet validated when `valid` is None."""
    results = None
    if valid is not None:
        results = GridQueryValidationResult(
            is_valid=valid,
            validation_error_message=None if valid else message,
            all_validation_messages=None if valid else details,
        )
    return GridRow(
        row_id=f"7.{key}", data={"specimenID": key}, validation_results=results
    )


def _server_error(text: str = "503 Server Error") -> SynapseHTTPError:
    """A Synapse 5xx."""
    return SynapseHTTPError(text, response=MagicMock(status_code=503))


class _ApplyMocks:
    """Mocks every Synapse call applying makes, recording each in `self.calls`.

    A step named in `self.fail_at` raises its exception once. Planning finds every
    folder editable, every team and schema present and no entity in the way.
    """

    @pytest.fixture(autouse=True)
    def mock_synapse(self, mocker, syn: Synapse, tmp_path: Path) -> None:
        self.syn = syn
        self.calls: list[str] = []
        self.fail_at: dict[str, Exception] = {}
        self.schema_body = SCHEMA_BODY
        self.tasks_by_project: dict[str, list[CurationTask]] = {}
        self.read_only_folders: set[str] = set()
        self.states: dict[int, TaskState] = {}
        self.status_errors: set[int] = set()
        self.archive_errors: set[int] = set()
        self.archived: list[tuple[int, TaskState]] = []
        self.created: list[dict] = []
        self.imported: list[dict] = []
        self.import_count: int | None = None
        self.validation_reads: list[list[GridRow]] = [[]]
        self.read_index = 0
        self.queries = []
        self.sync_types = []
        self.sync_errors: list[str] = []
        self.deleted_sessions: list[str] = []
        self.table_text: str | None = None
        self.read_ids: list[str] = []
        self.temp = tmp_path / "temp"
        self.temp.mkdir()
        mocker.patch.object(tempfile, "tempdir", str(self.temp))
        mocker.patch.object(curation_tasks, "_VALIDATION_POLL_SECONDS", 0)

        def step(name: str) -> None:
            self.calls.append(name)
            error = self.fail_at.pop(name, None)
            if error is not None:
                raise error

        async def list_tasks(project_id: str, **kwargs) -> AsyncIterator[CurationTask]:
            for task in self.tasks_by_project.get(project_id, []):
                yield task

        async def get_schema(schema: JSONSchema, **kwargs) -> JSONSchema:
            return schema

        async def get_body(schema: JSONSchema, version=None, **kwargs) -> dict:
            return self.schema_body

        async def get_permissions(folder: Folder, **kwargs) -> Permissions:
            return Permissions(can_edit=folder.id not in self.read_only_folders)

        async def get_team(team: Team, **kwargs) -> Team:
            return team

        async def folder_project(entity_id: str, synapse_client: Synapse) -> str:
            return {"syn112": "syn111", "syn113": "syn111"}.get(entity_id, "syn221")

        async def find_entity(name: str, parent: str, **kwargs) -> None:
            return None

        async def create_task(**kwargs):
            step("create_task")
            self.created.append(kwargs)
            number = len(self.created)
            return (
                RecordSet(id=f"syn90{number}"),
                CurationTask(
                    task_id=900 + number, data_type=kwargs["curation_task_name"]
                ),
            )

        async def create_grid(grid: Grid, *args, **kwargs) -> Grid:
            step("grid.create")
            grid.session_id = f"session-{len(self.created)}"
            return grid

        async def import_csv(grid: Grid, path: str, **kwargs) -> Grid:
            step("import")
            _, records = read_csv_records(path)
            self.imported.append(
                dict(
                    path=path,
                    text=Path(path).read_bytes().decode("utf-8"),
                    column_types=kwargs.get("column_types"),
                )
            )
            count = self.import_count
            grid.import_total_count = len(records) if count is None else count
            return grid

        @asynccontextmanager
        async def connect(grid: Grid, **kwargs):
            step("connect")
            yield grid

        async def validate_rows(grid: Grid, *, query_request, **kwargs):
            step("validate")
            query = query_request.query
            self.queries.append(query)
            rows = self.validation_reads[
                min(self.read_index, len(self.validation_reads) - 1)
            ]
            page = rows[query.offset : query.offset + query.limit]
            if len(page) < query.limit:
                self.read_index += 1
            return GridQueryResult(rows=page or None)

        async def synchronize(grid: Grid, **kwargs) -> Grid:
            step("synchronize")
            self.sync_types.append(kwargs.get("sync_type"))
            grid.sync_error_messages = list(self.sync_errors)
            return grid

        async def delete_grid(grid: Grid, **kwargs) -> None:
            step("grid.delete")
            self.deleted_sessions.append(grid.session_id)

        async def get_record_set(record_set: RecordSet, **kwargs) -> RecordSet:
            step("record_set.get")
            self.read_ids.append(record_set.id)
            text = self.table_text
            if text is None:
                text = self.imported[-1]["text"]
            file = Path(record_set.path) / "table.csv"
            file.write_text(text, encoding="utf-8", newline="")
            record_set.path = str(file)
            return record_set

        async def get_status(task: CurationTask, **kwargs) -> CurationTaskStatus:
            if task.task_id in self.status_errors:
                raise _server_error()
            return CurationTaskStatus(
                task_id=task.task_id,
                state=self.states.get(task.task_id, TaskState.NOT_STARTED),
            )

        async def set_state(task: CurationTask, state, **kwargs) -> None:
            step("archive")
            if task.task_id in self.archive_errors:
                raise _server_error()
            self.archived.append((task.task_id, state))
            self.states[task.task_id] = TaskState(state)

        mocker.patch.object(CurationTask, "list_async", side_effect=list_tasks)
        mocker.patch.object(
            JSONSchema, "get_async", autospec=True, side_effect=get_schema
        )
        mocker.patch.object(
            JSONSchema, "get_body_async", autospec=True, side_effect=get_body
        )
        mocker.patch.object(
            Folder, "get_permissions_async", autospec=True, side_effect=get_permissions
        )
        mocker.patch.object(Team, "get_async", autospec=True, side_effect=get_team)
        mocker.patch.object(
            curation_tasks,
            "project_id_from_entity_id_async",
            side_effect=folder_project,
        )
        mocker.patch.object(
            curation_tasks, "find_entity_id_async", side_effect=find_entity
        )
        self.create_task = mocker.patch.object(
            curation_tasks,
            "create_record_based_metadata_task_async",
            side_effect=create_task,
        )
        mocker.patch.object(
            Grid, "create_async", autospec=True, side_effect=create_grid
        )
        mocker.patch.object(
            Grid, "import_csv_async", autospec=True, side_effect=import_csv
        )
        mocker.patch.object(Grid, "connect_async", new=connect)
        mocker.patch.object(
            Grid, "validate_rows_async", autospec=True, side_effect=validate_rows
        )
        mocker.patch.object(
            Grid, "synchronize_async", autospec=True, side_effect=synchronize
        )
        mocker.patch.object(
            Grid, "delete_async", autospec=True, side_effect=delete_grid
        )
        mocker.patch.object(
            RecordSet, "get_async", autospec=True, side_effect=get_record_set
        )
        mocker.patch.object(
            CurationTask, "get_status_async", autospec=True, side_effect=get_status
        )
        self.set_state = mocker.patch.object(
            CurationTask, "set_task_state_async", autospec=True, side_effect=set_state
        )
        self.forbidden = [
            mocker.patch.object(model, method)
            for model, method in (
                (CurationTask, "delete_async"),
                (CurationTask, "store_async"),
                (RecordSet, "delete_async"),
                (RecordSet, "store_async"),
            )
        ]

    def _assert_nothing_deleted_and_no_file_left(self) -> None:
        """No task or RecordSet was deleted or stored outside the creator (FR-027),
        and no temporary file or folder is left (SC-007)."""
        for call in self.forbidden:
            call.assert_not_called()
        assert list(self.temp.iterdir()) == []

    async def _apply(self, *rows: CurationTaskRow, **kwargs) -> list[TaskOutcome]:
        return await apply_curation_tasks_async(
            CurationTaskManifest(rows=list(rows)), synapse_client=self.syn, **kwargs
        )


GRID_FLOW = [
    "create_task",
    "grid.create",
    "import",
    "connect",
    "validate",
    "synchronize",
    "grid.delete",
    "record_set.get",
]


class TestApplyNewTasks(_ApplyMocks):
    async def test_task_is_created_and_filled(self, tmp_path: Path) -> None:
        # GIVEN a row for a new task whose file leaves out one column, lacks one
        # schema property, and holds a quoted comma and line break
        row = _task_row(
            tmp_path,
            'specimenID,tissue,internal_notes,age\nS1,"brain, left\nstem",x,4\n'
            "S2,liver,,\n",
        )

        # AND a creator that leaves its header-only template on disk, as the real
        # one does
        create_task = self.create_task.side_effect
        template = self.temp / "template.csv"

        async def create_with_template(**kwargs):
            record_set, task = await create_task(**kwargs)
            template.write_text("specimenID\n", encoding="utf-8")
            record_set.path = str(template)
            return record_set, task

        self.create_task.side_effect = create_with_template

        # WHEN I apply it
        (outcome,) = await self._apply(row)

        # THEN the creator is given R13's arguments
        self.create_task.assert_awaited_once_with(
            folder_id="syn112",
            record_set_name="Lab A biospecimens",
            record_set_description="Lab A biospecimens",
            curation_task_name="Lab A biospecimens",
            upsert_keys=["specimenID"],
            instructions="Fill one row per specimen.",
            schema_uri=SCHEMA_URI,
            assignee_principal_id="3412345",
            authorization_mode=AuthorizationMode.SOURCE_BENEFACTOR,
            column_order=["tissue", "age"],
            create_grid=False,
            synapse_client=self.syn,
        )

        # AND the grid flow runs in order, through one session that is deleted
        assert self.calls == GRID_FLOW
        assert self.deleted_sessions == ["session-1"]
        assert self.sync_types == [SyncType.PULL_PUSH]
        assert self.read_ids == ["syn901"]

        # AND the upload holds the table's header and the file's rows, every cell
        # quoted, a blank as "", values exactly as read
        (upload,) = self.imported
        assert upload["text"] == (
            '"specimenID","tissue","age","individualID"\r\n'
            '"S1","brain, left\nstem","4",""\r\n'
            '"S2","liver","",""\r\n'
        )

        # AND the task is created with its rows written and confirmed
        assert outcome.plan.row is row
        assert outcome.result is TaskResult.CREATED
        assert outcome.task_id == 901
        assert outcome.record_set_id == "syn901"
        assert outcome.rows_written == 2
        assert outcome.validation_problems == []
        assert outcome.errors == []
        assert not template.exists()
        self._assert_nothing_deleted_and_no_file_left()

    async def test_import_is_typed_from_the_schema(self, tmp_path: Path) -> None:
        # GIVEN a schema whose properties have every shape R29 names, and others
        self.schema_body = {
            "properties": {
                "specimenID": {"type": "string"},
                "tissue": {"type": "string", "enum": ["brain", "liver"]},
                "age": {"type": "integer"},
                "weight": {"type": "number"},
                "isTumor": {"type": "boolean"},
                "tags": {"type": "array", "items": {"type": "string"}},
                "counts": {"type": "array", "items": {"type": "integer"}},
                "refs": {"type": "array", "items": {"$ref": "#/$defs/tag"}},
                "either": {"type": ["string", "null"]},
                "linked": {"$ref": "#/$defs/link"},
                "untyped": {"description": "anything"},
            }
        }
        row = _task_row(tmp_path, "specimenID,tissue,age\nS1,brain,4\n")

        # WHEN I apply it
        await self._apply(row)

        # THEN each column with a single plain type is typed, and the rest are not
        assert self.imported[0]["column_types"] == {
            "specimenID": ColumnType.STRING,
            "tissue": ColumnType.STRING,
            "age": ColumnType.INTEGER,
            "weight": ColumnType.DOUBLE,
            "isTumor": ColumnType.BOOLEAN,
            "tags": ColumnType.STRING_LIST,
        }

    async def test_validation_problems_are_read_until_every_row_has_a_result(
        self, mocker, tmp_path: Path
    ) -> None:
        # GIVEN a session whose rows are validated only on the second read, one
        # invalid row from the file and one invalid row the file lacks, read two
        # rows a page
        mocker.patch.object(curation_tasks, "_VALIDATION_PAGE_SIZE", 2)
        row = _task_row(tmp_path)
        self.validation_reads = [
            [_grid_row("S1", None), _grid_row("S2", None), _grid_row("S9", True)],
            [
                _grid_row("S1"),
                _grid_row(
                    "S2",
                    False,
                    "2 schema violations found",
                    ["#/age: expected type: Integer, found: String", "#/tissue: x"],
                ),
                _grid_row("S9", False, "string [x] does not match pattern ^[0-9]+$"),
            ],
        ]

        # WHEN I apply it
        (outcome,) = await self._apply(row)

        # THEN the rows are read in pages until a short page, and read again
        # until every row has a result, with Synapse's messages asked for
        assert [(q.offset, q.limit) for q in self.queries] == [(0, 2), (2, 2)] * 2
        assert all(q.include_validation_messages for q in self.queries)
        assert all(q.filters is None for q in self.queries)
        assert all(q.column_selection == [SelectAll()] for q in self.queries)

        # AND each invalid row is a non-blocking validation problem, named by its
        # file and line when the file has its key
        assert outcome.validation_problems == [
            CurationProblem(
                task="lab_a",
                kind=ProblemKind.VALIDATION,
                blocking=False,
                message=(
                    f"lab_a: input file {row.path} line 3: key specimenID 'S2': "
                    "2 schema violations found; #/age: expected type: Integer, "
                    "found: String; #/tissue: x"
                ),
                file=str(row.path),
                line=3,
                key={"specimenID": "S2"},
            ),
            CurationProblem(
                task="lab_a",
                kind=ProblemKind.VALIDATION,
                blocking=False,
                message=(
                    "lab_a: table row with key specimenID 'S9': string [x] does not "
                    "match pattern ^[0-9]+$"
                ),
                key={"specimenID": "S9"},
            ),
        ]

        # AND validation problems do not fail the task
        assert outcome.result is TaskResult.CREATED
        assert outcome.errors == []

    async def test_rows_not_validated_in_time_are_reported(
        self, mocker, tmp_path: Path
    ) -> None:
        # GIVEN a session where one row never gets a result, and no time to wait
        mocker.patch.object(curation_tasks, "_VALIDATION_WAIT_SECONDS", 0)
        self.validation_reads = [[_grid_row("S1", None), _grid_row("S2")]]

        # WHEN I apply a row
        (outcome,) = await self._apply(_task_row(tmp_path))

        # THEN one more validation problem says how many rows were not validated,
        # and the write goes on
        assert [
            (p.kind, p.blocking, p.message) for p in outcome.validation_problems
        ] == [
            (
                ProblemKind.VALIDATION,
                False,
                "lab_a: Synapse had not validated 1 of the 2 rows in table syn901 "
                "after 0 seconds; their validation problems are not reported",
            )
        ]
        assert self.calls == GRID_FLOW
        assert outcome.result is TaskResult.CREATED

    @pytest.mark.parametrize(
        "case, flow, rows_written, error",
        [
            (
                "import count",
                ["create_task", "grid.create", "import", "grid.delete"],
                0,
                "grid session session-1 imported 1 row, not the 2 written; it was not "
                "synchronized, so table syn901 is unchanged",
            ),
            (
                "sync errors",
                GRID_FLOW[:-1],
                0,
                "synchronizing grid session session-1 into table syn901 failed: "
                "Row 2: bad value",
            ),
            (
                "key missing",
                GRID_FLOW,
                2,
                "key specimenID 'S2' from input file {path} line 3 is not in table "
                "syn901 after writing",
            ),
        ],
    )
    async def test_unconfirmed_write_fails_the_task(
        self, tmp_path: Path, case: str, flow: list, rows_written: int, error: str
    ) -> None:
        # GIVEN a write Synapse does not confirm
        if case == "import count":
            self.import_count = 1
        elif case == "sync errors":
            self.sync_errors = ["Row 2: bad value"]
        else:
            self.table_text = '"specimenID","tissue","age","individualID"\n"S1",,,\n'
        row = _task_row(tmp_path)

        # WHEN I apply it
        (outcome,) = await self._apply(row)

        # THEN the task, which now exists, has failed with the reason
        assert outcome.result is TaskResult.FAILED
        assert outcome.task_id == 901
        assert outcome.record_set_id == "syn901"
        assert outcome.rows_written == rows_written
        assert outcome.errors == [error.format(path=row.path)]
        assert self.calls == flow
        self._assert_nothing_deleted_and_no_file_left()

    @pytest.mark.parametrize(
        "step, error",
        [
            ("grid.create", "Synapse error while filling table syn901: 503 x"),
            ("import", "Synapse error while filling table syn901: 503 x"),
            ("connect", "Synapse error while filling table syn901: 503 x"),
            ("validate", "Synapse error while filling table syn901: 503 x"),
            ("synchronize", "Synapse error while filling table syn901: 503 x"),
            ("record_set.get", "Synapse error while reading table syn901 back: 503 x"),
        ],
    )
    async def test_failed_step_cleans_up_and_others_finish(
        self, tmp_path: Path, step: str, error: str
    ) -> None:
        # GIVEN a step that raises for the first of two tasks
        self.fail_at[step] = _server_error("503 x")

        # WHEN I apply both
        outcome_a, outcome_b = await self._apply(_task_row(tmp_path), _lab_b(tmp_path))

        # THEN the first fails with the error, and the second is created and filled
        assert outcome_a.result is TaskResult.FAILED
        assert outcome_a.errors == [error]
        assert outcome_a.task_id == 901
        assert outcome_b.result is TaskResult.CREATED
        assert outcome_b.errors == []

        # AND every session opened was deleted, and no temporary file is left
        opened = ["session-1"] if step != "grid.create" else []
        assert self.deleted_sessions == opened + ["session-2"]
        self._assert_nothing_deleted_and_no_file_left()

    @pytest.mark.parametrize("import_fails", [False, True])
    async def test_failed_session_delete_is_logged_and_masks_nothing(
        self, mocker, tmp_path: Path, import_fails: bool
    ) -> None:
        # GIVEN a session that cannot be deleted, and perhaps an import that fails
        self.fail_at["grid.delete"] = _server_error("500 delete")
        if import_fails:
            self.fail_at["import"] = _server_error("503 import")
        warning = mocker.patch.object(self.syn.logger, "warning")

        # WHEN I apply a row
        (outcome,) = await self._apply(_task_row(tmp_path))

        # THEN the failed delete is logged, and the task's result is its own
        warning.assert_any_call(
            "Could not delete grid session session-1 on table syn901: 500 delete"
        )
        if import_fails:
            assert outcome.result is TaskResult.FAILED
            assert outcome.errors == [
                "Synapse error while filling table syn901: 503 import"
            ]
        else:
            assert outcome.result is TaskResult.CREATED
            assert outcome.errors == []
        assert list(self.temp.iterdir()) == []

    async def test_failed_creation_leaves_others_to_finish(
        self, tmp_path: Path
    ) -> None:
        # GIVEN a creator that raises for the first of two tasks
        self.fail_at["create_task"] = ValueError("upsert_keys were not found")

        # WHEN I apply both
        outcome_a, outcome_b = await self._apply(_task_row(tmp_path), _lab_b(tmp_path))

        # THEN the first fails with no task and no session, and the second finishes
        assert outcome_a.result is TaskResult.FAILED
        assert outcome_a.task_id is None
        assert outcome_a.errors == [
            "Synapse error while creating the curation task: upsert_keys were not "
            "found"
        ]
        assert outcome_b.result is TaskResult.CREATED
        assert self.calls == ["create_task"] + GRID_FLOW
        self._assert_nothing_deleted_and_no_file_left()

    async def test_empty_input_creates_the_task_and_opens_no_session(
        self, tmp_path: Path
    ) -> None:
        # GIVEN a row whose file has no data rows
        row = _task_row(tmp_path, "specimenID,tissue\n")

        # WHEN I apply it
        (outcome,) = await self._apply(row)

        # THEN the task is created and nothing else is called
        assert outcome.result is TaskResult.CREATED
        assert outcome.task_id == 901
        assert outcome.rows_written == 0
        assert self.calls == ["create_task"]
        self._assert_nothing_deleted_and_no_file_left()

    async def test_blocked_task_is_not_created(self, tmp_path: Path) -> None:
        # GIVEN a row whose folder cannot be edited
        self.read_only_folders.add("syn112")

        # WHEN I apply it
        (outcome,) = await self._apply(_task_row(tmp_path))

        # THEN it is blocked, with nothing created or written
        assert outcome.result is TaskResult.BLOCKED
        assert outcome.plan.blocked
        assert outcome.task_id is None
        assert outcome.rows_written == 0
        assert self.calls == []

    async def test_tasks_limits_the_run(self, tmp_path: Path) -> None:
        # GIVEN a manifest with two rows
        lab_a = _task_row(tmp_path)
        lab_b = _lab_b(tmp_path)

        # WHEN I apply one of them
        outcomes = await self._apply(lab_a, lab_b, tasks=["lab_b"])

        # THEN only that one is created
        assert [outcome.plan.row.name for outcome in outcomes] == ["lab_b"]
        assert [kwargs["curation_task_name"] for kwargs in self.created] == [
            "Lab B biospecimens"
        ]

        # AND a name the manifest lacks is rejected before Synapse is contacted
        with pytest.raises(CurationManifestError):
            await self._apply(lab_a, tasks=["lab_x"])
        assert len(self.created) == 1

    async def test_progress_is_logged(self, mocker, tmp_path: Path) -> None:
        # GIVEN two rows, the second blocked
        self.read_only_folders.add("syn222")
        info = mocker.patch.object(self.syn.logger, "info")

        # WHEN I apply them
        await self._apply(_task_row(tmp_path), _lab_b(tmp_path))

        # THEN each task's start and result are logged by name
        assert [call.args[0] for call in info.call_args_list] == [
            "Applying task 1 of 2: lab_a",
            "lab_a: created curation task 'Lab A biospecimens' (901) with 2 rows",
            "Applying task 2 of 2: lab_b",
            "lab_b: blocked by 1 problem",
        ]


OLD_TASKS = [
    CurationTask(task_id=6, data_type="Lab A v1", project_id="syn111"),
    CurationTask(task_id=5, data_type="Lab A v0", project_id="syn111"),
]


class TestArchiving(_ApplyMocks):
    @pytest.fixture(autouse=True)
    def old_tasks(self) -> None:
        self.tasks_by_project["syn111"] = list(OLD_TASKS)
        self.states[5] = TaskState.CANCELED

    async def test_plan_lists_each_old_task(self, tmp_path: Path) -> None:
        # GIVEN a row naming a live old task, an archived one, one the project
        # lacks, and the live one again
        row = _task_row(
            tmp_path, archive=["Lab A v1", "Lab A v0", "Lab A v9", "Lab A v1"]
        )

        # WHEN I plan it
        (plan,) = await plan_curation_tasks_async(
            CurationTaskManifest(rows=[row]), synapse_client=self.syn
        )

        # THEN each name is listed once with what would happen to it, and none
        # blocks the row
        assert plan.archives == [
            ArchiveEntry(
                curation_task_name="Lab A v1", task_id=6, result=ArchiveResult.ARCHIVE
            ),
            ArchiveEntry(
                curation_task_name="Lab A v0",
                task_id=5,
                result=ArchiveResult.ALREADY_ARCHIVED,
            ),
            ArchiveEntry(curation_task_name="Lab A v9", result=ArchiveResult.NOT_FOUND),
        ]
        assert plan.action is TaskAction.CREATE
        assert plan.problems == []
        self.set_state.assert_not_called()

    async def test_unreadable_state_is_a_failed_archive(self, tmp_path: Path) -> None:
        # GIVEN an old task whose state cannot be read
        self.status_errors.add(6)

        # WHEN I apply a row naming it
        (outcome,) = await self._apply(_task_row(tmp_path, archive=["Lab A v1"]))

        # THEN its archive fails with the error, and the row's task is created
        assert outcome.plan.archives == [
            ArchiveEntry(
                curation_task_name="Lab A v1",
                task_id=6,
                result=ArchiveResult.FAILED,
                error="Synapse error while reading its state: 503 Server Error",
            )
        ]
        assert outcome.result is TaskResult.CREATED
        self.set_state.assert_not_called()
        assert curation_run_failed([outcome])

    @pytest.mark.parametrize(
        "result, archived",
        [
            (TaskResult.CREATED, True),
            (TaskResult.UPDATED, True),
            (TaskResult.UNCHANGED, True),
            (TaskResult.BLOCKED, False),
            (TaskResult.FAILED, False),
        ],
    )
    async def test_archives_only_after_the_row_succeeds(
        self, tmp_path: Path, result: TaskResult, archived: bool
    ) -> None:
        # GIVEN an outcome with a live, an archived and a missing old task
        entries = [
            ArchiveEntry(
                curation_task_name="Lab A v1", task_id=6, result=ArchiveResult.ARCHIVE
            ),
            ArchiveEntry(
                curation_task_name="Lab A v0",
                task_id=5,
                result=ArchiveResult.ALREADY_ARCHIVED,
            ),
            ArchiveEntry(curation_task_name="Lab A v9", result=ArchiveResult.NOT_FOUND),
        ]
        plan = TaskPlan(
            row=_task_row(tmp_path), action=TaskAction.CREATE, archives=entries
        )
        outcome = TaskOutcome(plan=plan, result=result)

        # WHEN its old tasks are archived
        await curation_tasks._archive_old_tasks(outcome, self.syn)

        # THEN only the live one is archived, and only when the row succeeded
        if archived:
            assert self.archived == [(6, TaskState.CANCELED)]
            assert entries[0].result is ArchiveResult.ARCHIVED
        else:
            self.set_state.assert_not_called()
            assert entries[0].result is ArchiveResult.ARCHIVE
        assert [entry.result for entry in entries[1:]] == [
            ArchiveResult.ALREADY_ARCHIVED,
            ArchiveResult.NOT_FOUND,
        ]
        assert outcome.result is result

    @pytest.mark.parametrize("case", ["created", "blocked", "failed"])
    async def test_apply_archives_after_the_row_is_in_place(
        self, tmp_path: Path, case: str
    ) -> None:
        # GIVEN a row naming a live old task, created, blocked or failing
        if case == "blocked":
            self.read_only_folders.add("syn112")
        elif case == "failed":
            self.fail_at["synchronize"] = _server_error()
        row = _task_row(tmp_path, archive=["Lab A v1"])

        # WHEN I apply it
        (outcome,) = await self._apply(row)

        # THEN the old task is archived after the row's task is filled and
        # confirmed, and is left active otherwise
        (entry,) = outcome.plan.archives
        if case == "created":
            assert self.calls == GRID_FLOW + ["archive"]
            assert self.archived == [(6, TaskState.CANCELED)]
            assert entry.result is ArchiveResult.ARCHIVED
        else:
            self.set_state.assert_not_called()
            assert entry.result is ArchiveResult.ARCHIVE
        assert entry.error is None

        # AND nothing reads, stores or deletes the old task's table
        assert set(self.read_ids) <= {"syn901"}
        self._assert_nothing_deleted_and_no_file_left()

    async def test_failed_archive_keeps_the_row_result(self, tmp_path: Path) -> None:
        # GIVEN a live old task that cannot be archived
        self.archive_errors.add(6)

        # WHEN I apply a row naming it
        (outcome,) = await self._apply(_task_row(tmp_path, archive=["Lab A v1"]))

        # THEN the archive fails with its error, the row keeps its result, and
        # the run fails
        assert outcome.plan.archives == [
            ArchiveEntry(
                curation_task_name="Lab A v1",
                task_id=6,
                result=ArchiveResult.FAILED,
                error="503 Server Error",
            )
        ]
        assert outcome.result is TaskResult.CREATED
        assert outcome.errors == []
        assert curation_run_failed([outcome])

    async def test_old_task_named_by_two_rows_is_archived_once(
        self, tmp_path: Path
    ) -> None:
        # GIVEN two rows in one project that both name the same live old task
        lab_a = _task_row(tmp_path, archive=["Lab A v1"])
        lab_c = _task_row(
            tmp_path,
            name="lab_c",
            folder_id="syn113",
            curation_task_name="Lab C biospecimens",
            archive=["Lab A v1"],
        )

        # WHEN I apply both
        outcome_a, outcome_c = await self._apply(lab_a, lab_c)

        # THEN the first archives it, and the second finds it already archived
        assert self.archived == [(6, TaskState.CANCELED)]
        assert outcome_a.plan.archives[0].result is ArchiveResult.ARCHIVED
        assert outcome_c.plan.archives[0].result is ArchiveResult.ALREADY_ARCHIVED
        assert outcome_c.result is TaskResult.CREATED


TABLE_ID = "syn500"
TABLE_HEADER = ["specimenID", "tissue", "age", "individualID"]
WRITE_STEPS = {
    "create_task",
    "grid.create",
    "import",
    "connect",
    "validate",
    "synchronize",
    "grid.delete",
    "archive",
}


def _table_csv(header: list[str], rows: list[list[str | None]]) -> str:
    """A RecordSet CSV as Synapse writes it: every value quoted except a null, which
    is empty, and `\\n` line ends."""
    lines = [
        ",".join(
            "" if cell is None else '"' + cell.replace('"', '""') + '"'
            for cell in cells
        )
        for cells in [header, *rows]
    ]
    return "\n".join(lines) + "\n"


def _binding(schema_uri: str) -> MagicMock:
    """What `get_schema_async` returns for a table bound to `schema_uri`."""
    binding = MagicMock()
    binding.json_schema_version_info.id = schema_uri
    return binding


class _ExistingTaskMocks(_ApplyMocks):
    """Adds lab_a's task as one that exists, with table syn500 in folder syn112
    bound to the row's schema, holding S1 and S2 as INPUT_CSV gives them.

    `self.table_rows` is the table; a synchronize upserts the imported rows into it
    by their key text, then applies `self.read_back_changes`, keyed by
    (specimenID, column).
    """

    @pytest.fixture(autouse=True)
    def existing_task(self, mock_synapse, mocker) -> None:
        self.task = CurationTask(
            task_id=7,
            data_type="Lab A biospecimens",
            project_id="syn111",
            instructions="Fill one row per specimen.",
            assignee_principal_id="3412345",
            task_properties=RecordBasedMetadataTaskProperties(
                record_set_id=TABLE_ID,
                suggested_authorization_mode=AuthorizationMode.SOURCE_BENEFACTOR,
            ),
        )
        self.tasks_by_project["syn111"] = [self.task]
        self.table_header = list(TABLE_HEADER)
        self.table_rows: list[list[str | None]] = [
            ["S1", "brain", "4", None],
            ["S2", "liver", "5", None],
        ]
        self.table_keys = ["specimenID"]
        self.table_folder = "syn112"
        self.bound_schema: str | None = SCHEMA_URI
        self.sessions: dict[int, str] = {}
        self.live_sessions: set[str] = set()
        self.session_errors: dict[str, Exception] = {}
        self.session_reads: list[str] = []
        self.read_back_changes: dict[tuple[str, str], str] = {}

        def step(name: str) -> None:
            self.calls.append(name)
            error = self.fail_at.pop(name, None)
            if error is not None:
                raise error

        async def get_record_set(record_set: RecordSet, **kwargs) -> RecordSet:
            step("record_set.get")
            self.read_ids.append(record_set.id)
            file = Path(record_set.path) / "table.csv"
            file.write_text(
                _table_csv(self.table_header, self.table_rows),
                encoding="utf-8",
                newline="",
            )
            record_set.path = str(file)
            record_set.upsert_keys = list(self.table_keys)
            record_set.parent_id = self.table_folder
            return record_set

        async def get_bound_schema(record_set: RecordSet, **kwargs) -> MagicMock:
            step("record_set.get_schema")
            if self.bound_schema is None:
                raise _not_found()
            return _binding(self.bound_schema)

        async def get_status(task: CurationTask, **kwargs) -> CurationTaskStatus:
            if task.task_id in self.status_errors:
                raise _server_error()
            session = self.sessions.get(task.task_id)
            return CurationTaskStatus(
                task_id=task.task_id,
                state=self.states.get(task.task_id, TaskState.NOT_STARTED),
                execution_details=(
                    GridExecutionDetails(active_session_id=session) if session else None
                ),
            )

        async def get_session(grid: Grid, **kwargs) -> Grid:
            self.session_reads.append(grid.session_id)
            if grid.session_id in self.session_errors:
                raise self.session_errors[grid.session_id]
            if grid.session_id not in self.live_sessions:
                raise _not_found()
            return grid

        async def synchronize(grid: Grid, **kwargs) -> Grid:
            step("synchronize")
            self.sync_types.append(kwargs.get("sync_type"))
            grid.sync_error_messages = list(self.sync_errors)
            if not self.sync_errors:
                self._upsert(self.imported[-1]["text"])
            return grid

        mocker.patch.object(RecordSet, "get_async", new=get_record_set)
        self.get_bound_schema = mocker.patch.object(
            RecordSet, "get_schema_async", autospec=True, side_effect=get_bound_schema
        )
        mocker.patch.object(CurationTask, "get_status_async", new=get_status)
        self.get_session = mocker.patch.object(
            Grid, "get_async", autospec=True, side_effect=get_session
        )
        mocker.patch.object(Grid, "synchronize_async", new=synchronize)

    def _upsert(self, text: str) -> None:
        """Upsert an imported CSV's rows into the table by key text."""
        header, *rows = list(csv.reader(io.StringIO(text)))
        position = self.table_header.index("specimenID")
        for cells in rows:
            imported = dict(zip(header, cells))
            new = [imported.get(column, "") for column in self.table_header]
            for index, existing in enumerate(self.table_rows):
                if existing[position] == new[position]:
                    self.table_rows[index] = new
                    break
            else:
                self.table_rows.append(new)
        for (key, column), value in self.read_back_changes.items():
            for cells in self.table_rows:
                if cells[position] == key:
                    cells[self.table_header.index(column)] = value

    def _assert_nothing_written(self) -> None:
        """No call that writes to Synapse was made, and no file is left."""
        assert not WRITE_STEPS & set(self.calls)
        self.create_task.assert_not_called()
        self.set_state.assert_not_called()
        self._assert_nothing_deleted_and_no_file_left()

    async def _plan(self, *rows: CurationTaskRow, **kwargs) -> list[TaskPlan]:
        return await plan_curation_tasks_async(
            CurationTaskManifest(rows=list(rows)), synapse_client=self.syn, **kwargs
        )


class TestPlanExistingTasks(_ExistingTaskMocks):
    async def test_existing_task_is_planned_against_its_table(
        self, tmp_path: Path
    ) -> None:
        # GIVEN a row whose task exists and whose file matches its table
        row = _task_row(tmp_path)

        # WHEN I plan it
        (plan,) = await self._plan(row)

        # THEN the task exists, with its table, state and own columns, and every
        # file row unchanged
        assert plan.action is TaskAction.EXISTS
        assert plan.problems == []
        assert plan.task_id == 7
        assert plan.record_set_id == TABLE_ID
        assert plan.state is TaskState.NOT_STARTED
        assert plan.attached_session_id is None
        assert plan.setting_differences == {}
        assert plan.key_columns == ["specimenID"]
        assert plan.table_columns == TABLE_HEADER
        assert plan.missing_columns == ["individualID"]
        assert plan.skipped_columns == []
        assert plan.rows == RowChanges(unchanged=2)
        assert plan.rows_to_write == 0

        # AND the table is read without a session, and its download removed
        assert self.read_ids == [TABLE_ID]
        assert self.get_bound_schema.call_args.args[0].id == TABLE_ID
        self.get_session.assert_not_called()
        self._assert_nothing_written()

    async def test_differing_settings_are_listed_and_not_changed(
        self, tmp_path: Path
    ) -> None:
        # GIVEN a task whose instructions, assignee and authorization mode differ
        # from the row's
        self.task.instructions = "Old instructions"
        self.task.assignee_principal_id = "999"
        self.task.task_properties.suggested_authorization_mode = None

        # WHEN I plan the row
        (plan,) = await self._plan(_task_row(tmp_path))

        # THEN each difference is listed, manifest value first, and none blocks
        assert plan.setting_differences == {
            "assignee_principal_id": ("3412345", "999"),
            "instructions": ("Fill one row per specimen.", "Old instructions"),
            "authorization_mode": ("SOURCE_BENEFACTOR", ""),
        }
        assert plan.action is TaskAction.EXISTS
        self._assert_nothing_written()

    async def test_the_table_keeps_its_own_keys(self, tmp_path: Path) -> None:
        # GIVEN a table keyed on two columns, and a manifest naming one
        self.table_keys = ["specimenID", "tissue"]
        self.table_rows = [["S1", "brain", "4", None], ["S1", "liver", "5", None]]
        row = _task_row(tmp_path, "specimenID,tissue,age\nS1,brain,4\nS1,liver,5\n")

        # WHEN I plan it
        (plan,) = await self._plan(row)

        # THEN rows are matched on the table's keys, so the file's rows sharing a
        # specimenID are neither duplicates nor new, and the keys are listed as a
        # setting that differs without blocking
        assert plan.key_columns == ["specimenID", "tissue"]
        assert plan.rows == RowChanges(unchanged=2)
        assert plan.setting_differences == {
            "upsert_keys": ("specimenID", "specimenID;tissue")
        }
        assert plan.action is TaskAction.EXISTS

    async def test_a_file_lacking_a_table_key_blocks(self, tmp_path: Path) -> None:
        # GIVEN a table keyed on a column the file lacks
        self.table_keys = ["specimenID", "individualID"]

        # WHEN I plan the row
        (plan,) = await self._plan(_task_row(tmp_path))

        # THEN the task is blocked for the missing key column
        assert plan.action is TaskAction.BLOCKED
        assert [problem.message for problem in plan.problems] == [
            f"lab_a: input file {plan.row.path}: key column 'individualID' is not in "
            "the file"
        ]

    async def test_a_column_the_table_lacks_is_skipped(self, tmp_path: Path) -> None:
        # GIVEN a table without the tissue column, and a file that has it
        self.table_header = ["specimenID", "age", "individualID"]
        self.table_rows = [["S1", "4", None], ["S2", "5", None]]

        # WHEN I plan the row
        (plan,) = await self._plan(_task_row(tmp_path))

        # THEN the column is noted as skipped, is not compared, and blocks nothing
        assert plan.skipped_columns == ["tissue"]
        assert plan.table_columns == ["specimenID", "age", "individualID"]
        assert plan.rows == RowChanges(unchanged=2)
        assert plan.action is TaskAction.EXISTS

    @pytest.mark.parametrize(
        "case, message",
        [
            (
                "other schema",
                f"lab_a: table syn500 is bound to schema {OTHER_SCHEMA_URI}, not "
                f"{SCHEMA_URI}; it is not rebound",
            ),
            (
                "no schema",
                f"lab_a: table syn500 is bound to no schema, not {SCHEMA_URI}; it is "
                "not rebound",
            ),
            (
                "other folder",
                "lab_a: table syn500 is in folder syn999, not syn112; it is not moved",
            ),
            (
                "canceled",
                "lab_a: curation task 'Lab A biospecimens' (7) is archived "
                "(CANCELED) and is not reopened",
            ),
            (
                "not record-based",
                "lab_a: curation task 'Lab A biospecimens' (7) is not a "
                "record-based metadata task",
            ),
            (
                "state unreadable",
                "lab_a: Synapse error while reading the state of curation task "
                "'Lab A biospecimens' (7): 503 Server Error",
            ),
            (
                "table unreadable",
                "lab_a: Synapse error while reading table syn500: 503 x",
            ),
        ],
    )
    async def test_task_is_blocked(
        self, tmp_path: Path, case: str, message: str
    ) -> None:
        # GIVEN an existing task that cannot be written to as the row describes
        if case == "other schema":
            self.bound_schema = OTHER_SCHEMA_URI
        elif case == "no schema":
            self.bound_schema = None
        elif case == "other folder":
            self.table_folder = "syn999"
        elif case == "canceled":
            self.states[7] = TaskState.CANCELED
        elif case == "not record-based":
            self.task.task_properties = FileBasedMetadataTaskProperties()
        elif case == "state unreadable":
            self.status_errors.add(7)
        else:
            self.fail_at["record_set.get"] = _server_error("503 x")

        # WHEN I plan a row whose file has nothing new
        (plan,) = await self._plan(_task_row(tmp_path))

        # THEN only that problem blocks it, and nothing is written
        assert plan.action is TaskAction.BLOCKED
        assert plan.task_id == 7
        assert [(p.kind, p.blocking, p.message) for p in plan.problems] == [
            (ProblemKind.SYNAPSE, True, message)
        ]
        self._assert_nothing_written()

    @pytest.mark.parametrize(
        "condition, message",
        [
            (
                "completed",
                "lab_a: curation task 'Lab A biospecimens' (7) is marked complete, "
                "and 1 row would be written to it",
            ),
            (
                "session",
                "lab_a: curation task 'Lab A biospecimens' (7) has grid session "
                "sess-1 attached, and 1 row would be written to it",
            ),
        ],
    )
    @pytest.mark.parametrize(
        "file_row, overwrite, blocked",
        [
            ("S1,brain,4", False, False),
            ("S3,lung,6", False, True),
            ("S1,lung,4", False, False),
            ("S1,lung,4", True, True),
        ],
    )
    async def test_complete_or_attached_task_blocks_only_a_write(
        self,
        tmp_path: Path,
        condition: str,
        message: str,
        file_row: str,
        overwrite: bool,
        blocked: bool,
    ) -> None:
        # GIVEN a task marked complete, or with a live attached session, and a file
        # row that is unchanged, new, or differing
        if condition == "completed":
            self.states[7] = TaskState.COMPLETED
        else:
            self.sessions[7] = "sess-1"
            self.live_sessions.add("sess-1")
        row = _task_row(tmp_path, f"specimenID,tissue,age\n{file_row}\n")

        # WHEN I plan it, with or without overwrite
        (plan,) = await self._plan(row, overwrite=overwrite)

        # THEN it is blocked only when a row would be written
        assert plan.blocked is blocked
        assert [problem.message for problem in plan.problems] == (
            [message] if blocked else []
        )
        assert plan.action is (TaskAction.BLOCKED if blocked else TaskAction.EXISTS)

        # AND the session, when there is one, is read once and nothing else is
        # done with it
        if condition == "session":
            assert plan.attached_session_id == "sess-1"
            assert self.session_reads == ["sess-1"]
        else:
            assert plan.state is TaskState.COMPLETED
            self.get_session.assert_not_called()
        self._assert_nothing_written()

    @pytest.mark.parametrize(
        "error, attached, message",
        [
            (None, None, None),
            (
                _server_error("503 x"),
                None,
                "lab_a: Synapse error while reading grid session sess-1 attached to "
                "curation task 'Lab A biospecimens' (7): 503 x",
            ),
        ],
    )
    async def test_session_that_cannot_be_read(
        self,
        tmp_path: Path,
        error: Exception | None,
        attached: str | None,
        message: str | None,
    ) -> None:
        # GIVEN a status naming a session that is gone (a 404) or cannot be read,
        # and a file with a new row
        self.sessions[7] = "sess-1"
        if error is not None:
            self.session_errors["sess-1"] = error
        row = _task_row(tmp_path, "specimenID,tissue,age\nS3,lung,6\n")

        # WHEN I plan it
        (plan,) = await self._plan(row)

        # THEN a gone session counts as none, and any other error blocks with
        # Synapse's message
        assert plan.attached_session_id == attached
        assert [problem.message for problem in plan.problems] == (
            [message] if message else []
        )
        assert self.session_reads == ["sess-1"]
        self._assert_nothing_written()


def _input_rows(*rows: tuple[int, dict[str, str]]) -> list:
    """Input file rows from (line, cells) pairs."""
    return [curation_tasks._InputRow(line, cells) for line, cells in rows]


def _table(header: list[str], *rows: list[str], keys=("specimenID",)):
    """A table as read from its RecordSet CSV."""
    return curation_tasks._Table(
        header=header,
        records=[CsvRecord(line, cells) for line, cells in enumerate(rows, start=2)],
        upsert_keys=list(keys),
        parent_id="syn112",
    )


class TestRowComparison:
    @pytest.mark.parametrize(
        "kind, file_cell, synapse_cell, equal",
        [
            ("number", "1.0", "1", True),
            ("number", "1e3", "1000", True),
            ("integer", "01", "1", True),
            ("integer", " 5 ", "5", True),
            ("integer", "4", "5", False),
            ("integer", "four", "four", True),
            ("integer", "four", "4", False),
            ("boolean", "TRUE", "true", True),
            ("boolean", "False", "false", True),
            ("boolean", "yes", "true", False),
            ("array", '["a", "b"]', '["a","b"]', True),
            ("array", '["b","a"]', '["a","b"]', False),
            ("array", "a,b", '["a","b"]', True),
            ("array", "a, b", '["a","b"]', True),
            ("array", "a", '["a"]', True),
            ("array", "a,b", '["a","c"]', False),
            ("array", '[" a ","b"]', '["a","b"]', True),
            ("string", "x", "", False),
            ("string", "x", "null", False),
            ("string", "null", "null", True),
            ("array", "a", '[""]', False),
            ("string", " x", "x", False),
            ("string", "007", "7", False),
            (None, "brain", "brain", True),
        ],
    )
    def test_cells_compare_by_property_type(
        self, kind: str | None, file_cell: str, synapse_cell: str, equal: bool
    ) -> None:
        # GIVEN a file cell and a Synapse cell of a property's type
        # WHEN they are compared
        # THEN they are equal only when their typed values are
        assert curation_tasks._cells_equal(file_cell, synapse_cell, kind) is equal

    @pytest.mark.parametrize(
        "kind, cell, blank",
        [
            ("string", "", True),
            ("string", "null", True),
            ("integer", "", True),
            ("array", '[""]', True),
            ("array", '["",""]', True),
            ("array", '["a"]', False),
            ("string", '[""]', False),
            ("string", "   ", False),
            ("boolean", "false", False),
        ],
    )
    def test_synapse_blank(self, kind: str, cell: str, blank: bool) -> None:
        # GIVEN a cell as the RecordSet CSV gives it
        # WHEN it is judged
        # THEN empty, null and an array of empty strings are blank (R11)
        assert curation_tasks._is_blank_synapse_cell(cell, kind) is blank

    def test_rows_are_matched_on_key_columns(self) -> None:
        # GIVEN a table keyed on an integer column and a file that writes one key
        # with a leading zero, changes one row, blanks cells of another, adds one,
        # and lacks one
        header = ["sampleNumber", "tissue", "age", "tags"]
        table = _table(
            header,
            ["1", "brain", "4", '["a","b"]'],
            ["2", "liver", "", '[""]'],
            ["3", "lung", "6", '["c"]'],
            ["4", "skin", "7", '["d"]'],
            keys=("sampleNumber",),
        )
        rows = _input_rows(
            (2, {"sampleNumber": "01", "tissue": "brain", "age": "4.0", "tags": ""}),
            (3, {"sampleNumber": "2", "tissue": "", "age": "", "tags": ""}),
            (4, {"sampleNumber": "3", "tissue": "heart", "age": "8", "tags": '["c"]'}),
            (5, {"sampleNumber": "9", "tissue": "bone", "age": "", "tags": ""}),
        )
        types = {"sampleNumber": "integer", "age": "integer", "tags": "array"}

        # WHEN they are matched
        match = curation_tasks._match_rows(rows, table, ["sampleNumber"], types)

        # THEN the leading-zero key matches, blank cells never differ, the changed
        # row lists both values of each differing column, the new key is to add,
        # and the table's other key is absent from the file
        assert match.changes == RowChanges(
            to_add=1,
            unchanged=2,
            differing=[
                RowDifference(
                    key={"sampleNumber": "3"},
                    line=4,
                    columns={"tissue": ("heart", "lung"), "age": ("8", "6")},
                )
            ],
            absent_from_file=[{"sampleNumber": "4"}],
        )

        # AND the row to add is the file's, and the merged row is the table's
        # row overlaid with the file's non-blank cells
        assert [(w.line, w.cells, w.overwritten) for w in match.new_rows] == [
            (5, rows[3].cells, {})
        ]
        assert [(w.line, w.cells, w.overwritten) for w in match.merged_rows] == [
            (
                4,
                {"sampleNumber": "3", "tissue": "heart", "age": "8", "tags": '["c"]'},
                {"tissue": "heart", "age": "8"},
            )
        ]

    def test_compound_keys_and_text_keys(self) -> None:
        # GIVEN a table keyed on two text columns
        header = ["specimenID", "tissue", "age"]
        table = _table(
            header,
            ["S1", "brain", "4"],
            ["S1", "liver", "5"],
            keys=("specimenID", "tissue"),
        )
        rows = _input_rows(
            (2, {"specimenID": "S1", "tissue": "liver", "age": "5"}),
            (3, {"specimenID": "s1", "tissue": "brain", "age": "4"}),
        )

        # WHEN they are matched
        match = curation_tasks._match_rows(rows, table, ["specimenID", "tissue"], {})

        # THEN both key columns decide the match, and text keys match exactly
        assert match.changes == RowChanges(
            to_add=1,
            unchanged=1,
            absent_from_file=[{"specimenID": "S1", "tissue": "brain"}],
        )


class TestApplyExistingTasks(_ExistingTaskMocks):
    async def test_unchanged_rerun_opens_no_session(self, tmp_path: Path) -> None:
        # GIVEN a row whose file matches its existing task's table
        row = _task_row(tmp_path)

        # WHEN I apply it
        (outcome,) = await self._apply(row)

        # THEN it is unchanged: no task created, no session opened, nothing written
        assert outcome.result is TaskResult.UNCHANGED
        assert outcome.task_id == 7
        assert outcome.record_set_id == TABLE_ID
        assert outcome.rows_written == 0
        assert outcome.errors == []
        assert self.calls == ["record_set.get", "record_set.get_schema"]
        self._assert_nothing_written()

    async def test_empty_input_writes_nothing(self, tmp_path: Path) -> None:
        # GIVEN an existing task and an input file with no rows
        row = _task_row(tmp_path, "specimenID,tissue\n")

        # WHEN I apply it
        (outcome,) = await self._apply(row)

        # THEN the table is left as it is, every row noted as absent from the file
        assert outcome.result is TaskResult.UNCHANGED
        assert outcome.plan.input_empty
        assert outcome.plan.rows.absent_from_file == [
            {"specimenID": "S1"},
            {"specimenID": "S2"},
        ]
        assert self.table_rows == [
            ["S1", "brain", "4", None],
            ["S2", "liver", "5", None],
        ]
        self._assert_nothing_written()

    async def test_new_rows_are_written_and_differing_rows_left_alone(
        self, tmp_path: Path
    ) -> None:
        # GIVEN a file with a new row and a row that differs from the table
        row = _task_row(
            tmp_path,
            "specimenID,tissue,internal_notes,age\nS1,lung,x,4\nS3,heart,y,6\n",
        )

        # WHEN I apply it without overwrite
        (outcome,) = await self._apply(row)

        # THEN only the new row is written, under the table's own header, through
        # one session of the feature's own
        assert outcome.result is TaskResult.UPDATED
        assert outcome.rows_written == 1
        assert outcome.errors == []
        assert self.calls == ["record_set.get", "record_set.get_schema"] + GRID_FLOW[1:]
        (upload,) = self.imported
        assert upload["text"] == (
            '"specimenID","tissue","age","individualID"\r\n"S3","heart","6",""\r\n'
        )
        assert outcome.plan.rows.differing == [
            RowDifference(
                key={"specimenID": "S1"}, line=2, columns={"tissue": ("lung", "brain")}
            )
        ]

        # AND the differing row in Synapse is unchanged
        assert self.table_rows[0] == ["S1", "brain", "4", None]
        assert self.read_ids == [TABLE_ID, TABLE_ID]
        self.create_task.assert_not_called()
        self._assert_nothing_deleted_and_no_file_left()

    async def test_overwrite_writes_the_merged_row(self, tmp_path: Path) -> None:
        # GIVEN a table row with a value the file leaves blank, and a file cell
        # that differs from it
        self.table_rows[0] = ["S1", "brain", "4", "I1"]
        row = _task_row(
            tmp_path, "specimenID,tissue,age,individualID\nS1,lung,  ,\nS2,liver,5,\n"
        )

        # WHEN I apply it with overwrite
        (outcome,) = await self._apply(row, overwrite=True)

        # THEN the row written is the Synapse row overlaid with the file's
        # non-blank cells, so blank file cells keep their Synapse values
        (upload,) = self.imported
        assert upload["text"] == (
            '"specimenID","tissue","age","individualID"\r\n' '"S1","lung","4","I1"\r\n'
        )
        assert outcome.result is TaskResult.UPDATED
        assert outcome.rows_written == 1
        assert outcome.errors == []
        assert self.table_rows[0] == ["S1", "lung", "4", "I1"]

    @pytest.mark.parametrize(
        "column, file_value, stored, error",
        [
            ("tissue", "lung", "lung", None),
            ("age", "040", "40", None),
            (
                "tissue",
                "lung",
                "brain",
                "key specimenID 'S1' from input file {path} line 2: column 'tissue' "
                "holds 'brain' in table syn500 after writing, not 'lung'",
            ),
        ],
    )
    async def test_overwritten_cells_are_read_back(
        self,
        tmp_path: Path,
        column: str,
        file_value: str,
        stored: str,
        error: str | None,
    ) -> None:
        # GIVEN a differing cell to overwrite, which Synapse stores as `stored`
        cells = {"specimenID": "S1", "tissue": "brain", "age": "4"}
        cells[column] = file_value
        row = _task_row(
            tmp_path, "specimenID,tissue,age\n" + ",".join(cells.values()) + "\n"
        )
        self.read_back_changes[("S1", column)] = stored

        # WHEN I apply it with overwrite
        (outcome,) = await self._apply(row, overwrite=True)

        # THEN the read-back compares the cell by its type, and a cell not holding
        # the value written fails the task
        if error is None:
            assert outcome.result is TaskResult.UPDATED
            assert outcome.errors == []
        else:
            assert outcome.result is TaskResult.FAILED
            assert outcome.errors == [error.format(path=row.path)]
        assert outcome.rows_written == 1

    async def test_blocked_or_failed_task_leaves_the_others_to_finish(
        self, tmp_path: Path
    ) -> None:
        # GIVEN a completed task with a new row, a second existing task whose
        # session cannot be created, and a third task to create
        self.states[7] = TaskState.COMPLETED
        self.tasks_by_project["syn111"].append(
            CurationTask(
                task_id=8,
                data_type="Lab C biospecimens",
                project_id="syn111",
                instructions="Fill one row per specimen.",
                assignee_principal_id="3412345",
                task_properties=RecordBasedMetadataTaskProperties(
                    record_set_id=TABLE_ID,
                    suggested_authorization_mode=AuthorizationMode.SOURCE_BENEFACTOR,
                ),
            )
        )
        new_row = "specimenID,tissue,age\nS3,lung,6\n"
        lab_a = _task_row(tmp_path, new_row)
        lab_c = _task_row(
            tmp_path, new_row, name="lab_c", curation_task_name="Lab C biospecimens"
        )
        self.fail_at["grid.create"] = _server_error("503 x")

        # WHEN I apply all three
        outcome_a, outcome_c, outcome_b = await self._apply(
            lab_a, lab_c, _lab_b(tmp_path)
        )

        # THEN the completed task is blocked with nothing written, the failed one
        # carries its error, and the new one is created and filled
        assert outcome_a.result is TaskResult.BLOCKED
        assert outcome_a.rows_written == 0
        assert outcome_c.result is TaskResult.FAILED
        assert outcome_c.errors == ["Synapse error while filling table syn500: 503 x"]
        assert outcome_b.result is TaskResult.CREATED
        assert len(self.imported) == 1
        assert self.deleted_sessions == ["session-1"]
        self._assert_nothing_deleted_and_no_file_left()
