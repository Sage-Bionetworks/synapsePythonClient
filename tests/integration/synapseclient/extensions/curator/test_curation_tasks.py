"""Integration tests for planning and applying a curation task manifest.

The tests in `TestCurationTasks` run in order and build on each other, as the
scenarios of `specs/001-bulk-curation-tasks/quickstart.md` do: two tasks are created,
rerun, given new and differing rows, overwritten, replaced by a third task that
archives the first, and blocked by an attached grid session. A full run creates three
curation tasks and their RecordSets, one team, one schema organization with one
schema, and one attached grid session.
"""

import csv
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import AsyncGenerator, Callable

import pytest

from synapseclient import Synapse
from synapseclient.extensions.curator import (
    ArchiveResult,
    CurationTaskManifest,
    ProblemKind,
    TaskAction,
    TaskOutcome,
    TaskPlan,
    TaskResult,
    apply_curation_tasks_async,
    get_or_create_curator_grid_async,
    plan_curation_tasks_async,
)
from synapseclient.models import (
    AuthorizationMode,
    CurationTask,
    Folder,
    Grid,
    JSONSchema,
    Organization,
    Project,
    RecordSet,
    TaskState,
    Team,
)

SCHEMA_BODY = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "type": "object",
    "properties": {
        "specimenID": {"type": "string"},
        "participantID": {"type": "string", "pattern": "^P[0-9]+$"},
        "sampleCode": {"type": "string"},
        "count": {"type": "integer"},
        "tags": {"type": "array", "items": {"type": "string"}},
        "notes": {"type": "string"},
    },
    "required": ["specimenID", "participantID"],
}
"""A string key, a required string with a pattern, a string holding leading zeros,
an integer, an array of strings and a free-text string."""

# Line 1 is the header; A2's quoted value spans lines 3 and 4, so A3 starts on line 5.
# A3's participantID fails the pattern: a blank required string would pass `required`.
LAB_A_CSV = (
    "specimenID,notes,sampleCode,count,tags,internal_notes,participantID\n"
    'A1,plain,00123,5,"[""a"",""b""]",checked twice,P1\n'
    'A2,"has, comma\nand a line break",007,10,"[""c""]",,P2\n'
    "A3,x,0042,3,,ignore me,unknown\n"
)
LAB_A_COLUMNS = ["specimenID", "notes", "sampleCode", "count", "tags", "participantID"]

# lab_b leaves out the schema's `notes`.
LAB_B_CSV = (
    "specimenID,sampleCode,participantID,count,tags\n"
    'B1,0001,P10,1,"[""x""]"\n'
    'B2,0002,P11,2,"[""y"",""z""]"\n'
)
LAB_B_COLUMNS = ["specimenID", "sampleCode", "participantID", "count", "tags", "notes"]

# A1's count changes from 5 to 7 and its notes are blank; A4 is new.
LAB_A_EDITED_CSV = (
    "specimenID,notes,sampleCode,count,tags,internal_notes,participantID\n"
    'A1,,00123,7,"[""a"",""b""]",checked twice,P1\n'
    'A2,"has, comma\nand a line break",007,10,"[""c""]",,P2\n'
    "A3,x,0042,3,,ignore me,unknown\n"
    'A4,new,0099,8,"[""d""]",,P4\n'
)

LAB_A2_CSV = (
    "specimenID,participantID,sampleCode,count,tags,notes\n"
    'C1,P20,010,4,"[""m""]",first\n'
    'C2,P21,011,5,"[""n""]",second\n'
)
LAB_A2_NEW_ROW = 'C3,P22,012,6,"[""o""]",third\n'

MANIFEST_COLUMNS = [
    "name",
    "project_id",
    "folder_id",
    "curation_task_name",
    "record_set_name",
    "schema_uri",
    "upsert_keys",
    "assignee_principal_id",
    "authorization_mode",
    "instructions",
    "path",
    "leave_out_columns",
    "archive",
]


def _create_test_name() -> str:
    """A random name of letters, with no dash to break an `ORG-NAME-VERSION` URI."""
    random_string = "".join(i for i in str(uuid.uuid4()) if i.isalpha())
    return f"SYNPY.TEST.{random_string}"


def _write_text(path: Path, text: str) -> None:
    """Write a CSV's text exactly as given, with `\\n` line ends."""
    with open(path, "w", encoding="utf-8", newline="") as file:
        file.write(text)


@dataclass
class _Walk:
    """What the scenarios share: the input folder, the manifest rows, and what each
    scenario created for the next to use and for teardown to clean up."""

    folder: Path
    """The folder that holds the input CSVs and manifests."""

    rows: dict[str, dict[str, str]]
    """Each manifest row's cells, by its `name`."""

    schedule_for_cleanup: Callable[..., None]
    """The session's cleanup scheduler."""

    task_ids: dict[str, int] = field(default_factory=dict)
    """Each created task's ID, by manifest row `name`."""

    record_set_ids: dict[str, str] = field(default_factory=dict)
    """Each created task's RecordSet ID, by manifest row `name`."""

    versions: dict[str, int] = field(default_factory=dict)
    """Each RecordSet's version number when last read, by manifest row `name`."""

    grids: list[Grid] = field(default_factory=list)
    """Grid sessions attached during the scenarios."""

    def manifest(self, file_name: str, names: list[str]) -> CurationTaskManifest:
        """Write a manifest of the named rows into the input folder and read it."""
        path = self.folder / file_name
        with open(path, "w", encoding="utf-8", newline="") as file:
            writer = csv.DictWriter(file, fieldnames=MANIFEST_COLUMNS)
            writer.writeheader()
            for name in names:
                writer.writerow(self.rows[name])
        return CurationTaskManifest.from_csv(path)

    def record(self, outcome: TaskOutcome) -> None:
        """Keep a created task's IDs and schedule its task and RecordSet for
        cleanup."""
        name = outcome.plan.row.name
        if outcome.record_set_id is not None:
            self.record_set_ids[name] = outcome.record_set_id
            self.schedule_for_cleanup(outcome.record_set_id)
        if outcome.task_id is not None:
            self.task_ids[name] = outcome.task_id
            self.schedule_for_cleanup(CurationTask(task_id=outcome.task_id))


async def _download_table(
    record_set_id: str, syn: Synapse
) -> tuple[int, list[str], dict[str, dict[str, str]]]:
    """Download a RecordSet and read it with stdlib `csv`.

    Returns:
        Its version number, its header, and its rows as column-to-text mappings,
        keyed by `specimenID`.
    """
    with tempfile.TemporaryDirectory() as folder:
        record_set = await RecordSet(id=record_set_id, path=folder).get_async(
            synapse_client=syn
        )
        with open(record_set.path, encoding="utf-8", newline="") as file:
            reader = csv.reader(file)
            header = next(reader)
            rows = [dict(zip(header, cells)) for cells in reader]
    return (
        record_set.version_number,
        header,
        {row["specimenID"]: row for row in rows},
    )


async def _version_number(record_set_id: str, syn: Synapse) -> int:
    """A RecordSet's current version number, read without downloading it."""
    record_set = await RecordSet(id=record_set_id, download_file=False).get_async(
        synapse_client=syn
    )
    return record_set.version_number


def _by_name(results: list[TaskPlan] | list[TaskOutcome]) -> dict:
    """Plans or outcomes by their manifest row's `name`."""
    return {
        (result.plan if isinstance(result, TaskOutcome) else result).row.name: result
        for result in results
    }


class TestCurationTasks:
    """Quickstart scenarios 1, 2, 5, 6, 7, 9, 10 and 15, run in order."""

    @pytest.fixture(scope="class")
    async def schema_uri(self, syn: Synapse) -> AsyncGenerator[str, None]:
        """A registered schema's `ORG-NAME-VERSION` URI.

        The schema and its organization are deleted at class teardown, after the
        `walk` fixture has unbound the schema from every RecordSet: Synapse refuses
        to delete a schema bound to an entity, and a trashed entity cannot be
        unbound.
        """
        organization = Organization(name=_create_test_name())
        await organization.store_async(synapse_client=syn)
        json_schema = JSONSchema(
            name="curation.tasks", organization_name=organization.name
        )
        await json_schema.store_async(
            schema_body=SCHEMA_BODY, version="1.0.0", synapse_client=syn
        )

        yield f"{organization.name}-curation.tasks-1.0.0"

        await json_schema.delete_async(synapse_client=syn)
        await organization.delete_async(synapse_client=syn)

    @pytest.fixture(scope="class")
    async def folders(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
    ) -> tuple[Folder, Folder]:
        """Two folders in the worker's project, one for each lab."""
        created = []
        for _ in range(2):
            folder = await Folder(
                name=str(uuid.uuid4()), parent_id=project_model.id
            ).store_async(synapse_client=syn)
            schedule_for_cleanup(folder.id)
            created.append(folder)
        return created[0], created[1]

    @pytest.fixture(scope="class")
    async def team(
        self, syn: Synapse, schedule_for_cleanup: Callable[..., None]
    ) -> Team:
        """The team the tasks are assigned to."""
        team = await Team(
            name=f"curation_tasks_team_{uuid.uuid4()}",
            description="Assignee of the curation task integration tests",
        ).create_async(synapse_client=syn)
        schedule_for_cleanup(team)
        return team

    @pytest.fixture(scope="class")
    def input_folder(self, tmp_path_factory: pytest.TempPathFactory) -> Path:
        """A folder holding `lab_a.csv` and `lab_b.csv`."""
        folder = tmp_path_factory.mktemp("curation_tasks")
        _write_text(folder / "lab_a.csv", LAB_A_CSV)
        _write_text(folder / "lab_b.csv", LAB_B_CSV)
        return folder

    @pytest.fixture(scope="class")
    async def walk(
        self,
        syn: Synapse,
        project_model: Project,
        schedule_for_cleanup: Callable[..., None],
        schema_uri: str,
        folders: tuple[Folder, Folder],
        team: Team,
        input_folder: Path,
    ) -> AsyncGenerator[_Walk, None]:
        """The manifest rows and the state the scenarios pass along.

        At class teardown, every attached grid session is deleted and the schema is
        unbound from every RecordSet the scenarios created, before the `schema_uri`
        fixture deletes the schema. The tasks, RecordSets, folders and team are
        deleted at session teardown by `schedule_for_cleanup`.
        """
        suffix = uuid.uuid4().hex[:8]
        folder_a, folder_b = folders

        def row(name: str, folder: Folder, mode: AuthorizationMode) -> dict[str, str]:
            return {
                "name": name,
                "project_id": project_model.id,
                "folder_id": folder.id,
                "curation_task_name": f"{name} biospecimens {suffix}",
                "record_set_name": "",
                "schema_uri": schema_uri,
                "upsert_keys": "specimenID",
                "assignee_principal_id": str(team.id),
                "authorization_mode": mode.value,
                "instructions": f"Fill one row per {name} specimen.",
                "path": f"{name}.csv",
                "leave_out_columns": "",
                "archive": "",
            }

        rows = {
            "lab_a": row("lab_a", folder_a, AuthorizationMode.SOURCE_BENEFACTOR),
            "lab_b": row("lab_b", folder_b, AuthorizationMode.SESSION_OWNER),
            "lab_a2": row("lab_a2", folder_a, AuthorizationMode.SOURCE_BENEFACTOR),
        }
        rows["lab_a"]["leave_out_columns"] = "internal_notes"
        rows["lab_a2"]["archive"] = rows["lab_a"]["curation_task_name"]

        walk = _Walk(
            folder=input_folder, rows=rows, schedule_for_cleanup=schedule_for_cleanup
        )
        yield walk

        for grid in walk.grids:
            try:
                await grid.delete_async(synapse_client=syn)
            except Exception as error:
                print(f"Error deleting grid session {grid.session_id}: {error}")
        for record_set_id in walk.record_set_ids.values():
            try:
                await RecordSet(id=record_set_id).unbind_schema_async(
                    synapse_client=syn
                )
            except Exception as error:
                print(f"Error unbinding the schema from {record_set_id}: {error}")

    async def test_plan_and_apply_new_tasks(
        self, syn: Synapse, project_model: Project, team: Team, walk: _Walk
    ) -> None:
        # GIVEN a manifest of two tasks that do not exist
        manifest = walk.manifest("manifest.csv", ["lab_a", "lab_b"])

        # WHEN I plan it (scenario 1)
        plans = _by_name(await plan_curation_tasks_async(manifest, synapse_client=syn))

        # THEN both tasks would be created, with 3 and 2 rows to add
        assert plans["lab_a"].action == TaskAction.CREATE
        assert plans["lab_b"].action == TaskAction.CREATE
        assert plans["lab_a"].rows.to_add == 3
        assert plans["lab_b"].rows.to_add == 2
        assert plans["lab_a"].problems == []
        assert plans["lab_b"].problems == []

        # AND nothing was created in Synapse
        task_names = {walk.rows[name]["curation_task_name"] for name in plans}
        existing = [
            task
            async for task in CurationTask.list_async(
                project_id=project_model.id, synapse_client=syn
            )
            if task.data_type in task_names
        ]
        assert existing == []

        # WHEN I apply it (scenario 2)
        outcomes = _by_name(
            await apply_curation_tasks_async(manifest, synapse_client=syn)
        )
        for outcome in outcomes.values():
            walk.record(outcome)

        # THEN both tasks are created and filled without errors
        for name, rows_written in (("lab_a", 3), ("lab_b", 2)):
            outcome = outcomes[name]
            assert outcome.result == TaskResult.CREATED, outcome.errors
            assert outcome.errors == []
            assert outcome.rows_written == rows_written

        # AND one validation problem is reported on lab_a, naming its file and the
        # line its row starts on, and none on lab_b
        assert outcomes["lab_b"].validation_problems == []
        [problem] = outcomes["lab_a"].validation_problems
        assert problem.kind == ProblemKind.VALIDATION
        assert problem.blocking is False
        assert Path(problem.file).name == "lab_a.csv"
        assert problem.line == 5
        assert problem.key == {"specimenID": "A3"}

        # AND each task in Curator is assigned to the team, with its instructions
        # and authorization mode
        for name, mode in (
            ("lab_a", AuthorizationMode.SOURCE_BENEFACTOR),
            ("lab_b", AuthorizationMode.SESSION_OWNER),
        ):
            task = await CurationTask(task_id=walk.task_ids[name]).get_async(
                synapse_client=syn
            )
            assert task.data_type == walk.rows[name]["curation_task_name"]
            assert task.assignee_principal_id == str(team.id)
            assert task.instructions == walk.rows[name]["instructions"]
            assert task.task_properties.suggested_authorization_mode == mode
            assert task.task_properties.record_set_id == walk.record_set_ids[name]

        # AND each table's columns are the key, then the file's order, then the
        # schema fields the file lacks, without the left-out column
        version_a, header_a, table_a = await _download_table(
            walk.record_set_ids["lab_a"], syn
        )
        version_b, header_b, table_b = await _download_table(
            walk.record_set_ids["lab_b"], syn
        )
        walk.versions.update(lab_a=version_a, lab_b=version_b)
        assert header_a == LAB_A_COLUMNS
        assert header_b == LAB_B_COLUMNS
        assert "internal_notes" not in header_a

        # AND every row is in its table, with the comma and line-break value and the
        # leading-zero values read exactly as in the file
        assert set(table_a) == {"A1", "A2", "A3"}
        assert set(table_b) == {"B1", "B2"}
        assert table_a["A2"]["notes"] == "has, comma\nand a line break"
        assert table_a["A1"]["sampleCode"] == "00123"
        assert table_a["A2"]["sampleCode"] == "007"
        assert table_b["B1"]["sampleCode"] == "0001"

    async def test_unchanged_rerun(self, syn: Synapse, walk: _Walk) -> None:
        # GIVEN the two tasks the manifest created, and its unchanged input files
        manifest = walk.manifest("manifest.csv", ["lab_a", "lab_b"])

        # WHEN I plan it again (scenario 5)
        plans = _by_name(await plan_curation_tasks_async(manifest, synapse_client=syn))

        # THEN every task exists with nothing to add and no row differing, so the
        # number and list cells Synapse wrote compare equal to the file's
        for name in ("lab_a", "lab_b"):
            plan = plans[name]
            assert plan.action == TaskAction.EXISTS, plan.problems
            assert plan.rows.to_add == 0
            assert plan.rows.differing == []

        # WHEN I apply it again
        outcomes = _by_name(
            await apply_curation_tasks_async(manifest, synapse_client=syn)
        )

        # THEN every task is unchanged
        for name in ("lab_a", "lab_b"):
            assert outcomes[name].result == TaskResult.UNCHANGED, outcomes[name].errors
            assert outcomes[name].plan.rows.differing == []

        # AND neither RecordSet's version number changed
        for name in ("lab_a", "lab_b"):
            assert (
                await _version_number(walk.record_set_ids[name], syn)
                == walk.versions[name]
            )

    async def test_new_and_differing_rows(self, syn: Synapse, walk: _Walk) -> None:
        # GIVEN lab_a's file with a new row A4, and A1's count changed and its notes
        # blank
        _write_text(walk.folder / "lab_a.csv", LAB_A_EDITED_CSV)
        manifest = walk.manifest("manifest.csv", ["lab_a", "lab_b"])

        # WHEN I plan lab_a (scenario 6)
        [plan] = await plan_curation_tasks_async(
            manifest, tasks=["lab_a"], synapse_client=syn
        )

        # THEN one row is to be added and one differs, with both values of the
        # changed cell and nothing for the blank one
        assert plan.action == TaskAction.EXISTS, plan.problems
        assert plan.rows.to_add == 1
        [difference] = plan.rows.differing
        assert difference.key == {"specimenID": "A1"}
        assert difference.columns == {"count": ("7", "5")}

        # WHEN I apply lab_a
        [outcome] = await apply_curation_tasks_async(
            manifest, tasks=["lab_a"], synapse_client=syn
        )

        # THEN the new row is written
        assert outcome.result == TaskResult.UPDATED, outcome.errors
        assert outcome.rows_written == 1

        # AND the differing row is left as it is in Synapse
        _, _, table = await _download_table(walk.record_set_ids["lab_a"], syn)
        assert set(table) == {"A1", "A2", "A3", "A4"}
        assert table["A1"]["count"] == "5"
        assert table["A1"]["notes"] == "plain"

    async def test_overwrite_differing_rows(self, syn: Synapse, walk: _Walk) -> None:
        # GIVEN lab_a's file, whose A1 still differs from Synapse
        manifest = walk.manifest("manifest.csv", ["lab_a", "lab_b"])

        # WHEN I plan lab_a with overwrite (scenario 7)
        [plan] = await plan_curation_tasks_async(
            manifest, tasks=["lab_a"], overwrite=True, synapse_client=syn
        )

        # THEN the differing row is to be overwritten
        assert plan.action == TaskAction.EXISTS, plan.problems
        assert plan.overwrite is True
        assert len(plan.rows.differing) == 1
        assert plan.rows_to_write == 1

        # WHEN I apply lab_a with overwrite
        [outcome] = await apply_curation_tasks_async(
            manifest, tasks=["lab_a"], overwrite=True, synapse_client=syn
        )
        assert outcome.result == TaskResult.UPDATED, outcome.errors

        # THEN the differing row takes the file's non-blank value, and the cell blank
        # in the file keeps its Synapse value
        _, _, table = await _download_table(walk.record_set_ids["lab_a"], syn)
        assert table["A1"]["count"] == "7"
        assert table["A1"]["notes"] == "plain"

    async def test_archive_replaced_task(self, syn: Synapse, walk: _Walk) -> None:
        # GIVEN a manifest row lab_a2 for a new task in lab_a's project that names
        # lab_a's task in `archive`; lab_a's own row is left out, since a manifest
        # may not archive a task one of its rows names
        _write_text(walk.folder / "lab_a2.csv", LAB_A2_CSV)
        manifest = walk.manifest("manifest_archive.csv", ["lab_b", "lab_a2"])
        old_name = walk.rows["lab_a"]["curation_task_name"]

        # WHEN I plan lab_a2 (scenario 9)
        [plan] = await plan_curation_tasks_async(
            manifest, tasks=["lab_a2"], synapse_client=syn
        )

        # THEN it would be created, and lab_a's task archived
        assert plan.action == TaskAction.CREATE, plan.problems
        [entry] = plan.archives
        assert entry.curation_task_name == old_name
        assert entry.result == ArchiveResult.ARCHIVE

        # WHEN I apply lab_a2 (scenario 10)
        [outcome] = await apply_curation_tasks_async(
            manifest, tasks=["lab_a2"], synapse_client=syn
        )
        walk.record(outcome)

        # THEN it is created and filled, and lab_a's task is archived
        assert outcome.result == TaskResult.CREATED, outcome.errors
        assert outcome.rows_written == 2
        [entry] = outcome.plan.archives
        assert entry.result == ArchiveResult.ARCHIVED
        status = await CurationTask(task_id=walk.task_ids["lab_a"]).get_status_async(
            synapse_client=syn
        )
        assert status.state == TaskState.CANCELED

        # AND the old task's table still downloads with its rows
        _, _, table = await _download_table(walk.record_set_ids["lab_a"], syn)
        assert set(table) == {"A1", "A2", "A3", "A4"}

        # WHEN I plan lab_a2 again
        [plan] = await plan_curation_tasks_async(
            manifest, tasks=["lab_a2"], synapse_client=syn
        )

        # THEN lab_a's task shows as already archived
        [entry] = plan.archives
        assert entry.result == ArchiveResult.ALREADY_ARCHIVED

    async def test_attached_session_blocks_writes(
        self, syn: Synapse, walk: _Walk
    ) -> None:
        # GIVEN a grid session attached to lab_a2's task
        grid = await get_or_create_curator_grid_async(
            task_id=walk.task_ids["lab_a2"], synapse_client=syn
        )
        walk.grids.append(grid)
        manifest = walk.manifest("manifest_archive.csv", ["lab_b", "lab_a2"])

        # WHEN I plan with nothing to write (scenario 15)
        plans = _by_name(await plan_curation_tasks_async(manifest, synapse_client=syn))

        # THEN lab_a2 is not blocked
        assert plans["lab_a2"].action == TaskAction.EXISTS, plans["lab_a2"].problems
        assert plans["lab_a2"].attached_session_id == grid.session_id

        # WHEN I add a row to lab_a2's file and apply
        version = await _version_number(walk.record_set_ids["lab_a2"], syn)
        _write_text(walk.folder / "lab_a2.csv", LAB_A2_CSV + LAB_A2_NEW_ROW)
        outcomes = _by_name(
            await apply_curation_tasks_async(manifest, synapse_client=syn)
        )

        # THEN lab_a2 is blocked, with the session ID named
        outcome = outcomes["lab_a2"]
        assert outcome.result == TaskResult.BLOCKED
        assert any(
            grid.session_id in problem.message for problem in outcome.plan.problems
        )

        # AND nothing is written to its table
        assert await _version_number(walk.record_set_ids["lab_a2"], syn) == version

        # AND lab_b proceeds
        assert outcomes["lab_b"].result == TaskResult.UNCHANGED, outcomes[
            "lab_b"
        ].errors
