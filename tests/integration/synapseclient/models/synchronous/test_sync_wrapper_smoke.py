"""Smoke tests for synchronous (non-async) wrapper methods.

These tests verify that the @async_to_sync decorator-generated sync methods
work correctly against the real Synapse API. They cover representative
operations across the most commonly used model classes.

The full async integration test suite tests all business logic. These tests
only verify that the sync-to-async wrapping works end-to-end.
"""

import sys
import uuid
from typing import Callable

import pytest

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import (
    Column,
    ColumnType,
    File,
    Folder,
    Project,
    Table,
    WikiPage,
)

pytestmark = pytest.mark.skipif(
    sys.version_info >= (3, 14),
    reason=(
        "Sync wrappers raise RuntimeError on Python 3.14+ when an event loop "
        "is active (which pytest-asyncio creates). Use async methods directly."
    ),
)


class TestSyncWrapperSmoke:
    """Smoke tests for sync wrapper methods across core model classes."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse, schedule_for_cleanup: Callable[..., None]) -> None:
        self.syn = syn
        self.schedule_for_cleanup = schedule_for_cleanup

    def test_project_store_get_delete(self) -> None:
        """Verify Project store/get/delete sync wrappers work."""
        # GIVEN a project
        project = Project(
            name=f"sync_smoke_project_{uuid.uuid4()}",
            description="Sync wrapper smoke test",
        )

        # WHEN I store the project using the sync method
        project = project.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project.id)

        # THEN the project should be stored
        assert project.id is not None
        assert project.etag is not None

        # WHEN I get the project using the sync method
        retrieved = Project(id=project.id).get(synapse_client=self.syn)

        # THEN the project should be retrieved
        assert retrieved.id == project.id
        assert retrieved.name == project.name

        # WHEN I delete the project using the sync method
        project.delete(synapse_client=self.syn)

        # THEN the project should be deleted
        with pytest.raises(SynapseHTTPError, match="404"):
            Project(id=project.id).get(synapse_client=self.syn)

    def test_file_store_and_get(self, project_model: Project) -> None:
        """Verify File store/get sync wrappers work."""
        # GIVEN a file
        filename = utils.make_bogus_uuid_file()
        self.schedule_for_cleanup(filename)
        file = File(
            path=filename,
            name=f"sync_smoke_file_{uuid.uuid4()}.txt",
            description="Sync wrapper smoke test",
            parent_id=project_model.id,
        )

        # WHEN I store the file using the sync method
        file = file.store(synapse_client=self.syn)
        self.schedule_for_cleanup(file.id)

        # THEN the file should be stored
        assert file.id is not None
        assert file.version_number == 1
        assert file.data_file_handle_id is not None

        # WHEN I get the file using the sync method
        retrieved = File(id=file.id, download_file=False).get(synapse_client=self.syn)

        # THEN the file should be retrieved
        assert retrieved.id == file.id
        assert retrieved.name == file.name

    def test_folder_store_and_get(self, project_model: Project) -> None:
        """Verify Folder store/get sync wrappers work."""
        # GIVEN a folder
        folder = Folder(
            name=f"sync_smoke_folder_{uuid.uuid4()}",
            parent_id=project_model.id,
        )

        # WHEN I store the folder using the sync method
        folder = folder.store(synapse_client=self.syn)
        self.schedule_for_cleanup(folder.id)

        # THEN the folder should be stored
        assert folder.id is not None

        # WHEN I get the folder using the sync method
        retrieved = Folder(id=folder.id).get(synapse_client=self.syn)

        # THEN the folder should be retrieved
        assert retrieved.id == folder.id
        assert retrieved.name == folder.name

    def test_table_store_and_query(self, project_model: Project) -> None:
        """Verify Table store and query sync wrappers work."""
        # GIVEN a table with columns
        table = Table(
            name=f"sync_smoke_table_{uuid.uuid4()}",
            parent_id=project_model.id,
            columns=[
                Column(name="name", column_type=ColumnType.STRING, maximum_size=50),
                Column(name="value", column_type=ColumnType.INTEGER),
            ],
        )

        # WHEN I store the table using the sync method
        table = table.store(synapse_client=self.syn)
        self.schedule_for_cleanup(table.id)

        # THEN the table should be stored
        assert table.id is not None
        assert len(table.columns) == 2

    def test_wiki_store_and_get(self, project_model: Project) -> None:
        """Verify WikiPage store/get sync wrappers work."""
        # GIVEN a wiki page
        wiki = WikiPage(
            owner_id=project_model.id,
            title=f"sync_smoke_wiki_{uuid.uuid4()}",
            markdown="# Smoke Test\nThis is a sync wrapper test.",
        )

        # WHEN I store the wiki using the sync method
        wiki = wiki.store(synapse_client=self.syn)
        self.schedule_for_cleanup(wiki)

        # THEN the wiki should be stored
        assert wiki.id is not None
        assert wiki.etag is not None

        # WHEN I get the wiki using the sync method
        retrieved = WikiPage(owner_id=project_model.id, id=wiki.id).get(
            synapse_client=self.syn
        )

        # THEN the wiki should be retrieved
        assert retrieved.id == wiki.id
        assert retrieved.title == wiki.title

    def test_project_with_annotations(self) -> None:
        """Verify annotation handling through sync wrappers."""
        # GIVEN a project with annotations
        annotations = {
            "my_string": ["hello"],
            "my_int": [42],
            "my_float": [3.14],
        }
        project = Project(
            name=f"sync_smoke_annotations_{uuid.uuid4()}",
            annotations=annotations,
        )

        # WHEN I store the project using the sync method
        project = project.store(synapse_client=self.syn)
        self.schedule_for_cleanup(project.id)

        # THEN the annotations should be stored
        assert project.annotations["my_string"] == ["hello"]
        assert project.annotations["my_int"] == [42]
        assert project.annotations["my_float"] == [3.14]

        # WHEN I get the project using the sync method
        retrieved = Project(id=project.id).get(synapse_client=self.syn)

        # THEN the annotations should be retrieved
        assert retrieved.annotations == project.annotations
