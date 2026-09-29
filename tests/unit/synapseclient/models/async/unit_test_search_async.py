"""Unit tests for synapseclient.models.services.search.get_id."""

from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseNotFoundError
from synapseclient.models import File, Folder, Project
from synapseclient.models.services.search import get_id

ENTITY_ID = "syn123"
PARENT_ID = "syn456"
ENTITY_NAME = "my_test_entity"


class TestGetId:
    """Unit tests for the get_id service function."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_returns_id_when_set_without_searching(self) -> None:
        # GIVEN a folder with an ID already set
        folder = Folder(id=ENTITY_ID, name=ENTITY_NAME, parent_id=PARENT_ID)

        with patch(
            "synapseclient.operations.find_entity_id_async",
            new_callable=AsyncMock,
        ) as mocked_find:
            # WHEN I resolve its ID
            result = await get_id(entity=folder, synapse_client=self.syn)

            # THEN the ID field is returned without searching Synapse
            assert result == ENTITY_ID
            mocked_find.assert_not_called()

    async def test_searches_by_name_and_parent(self) -> None:
        # GIVEN a file with a name and parent but no ID
        file = File(name=ENTITY_NAME, parent_id=PARENT_ID)

        with patch(
            "synapseclient.operations.find_entity_id_async",
            new_callable=AsyncMock,
            return_value=ENTITY_ID,
        ) as mocked_find:
            # WHEN I resolve its ID
            result = await get_id(entity=file, synapse_client=self.syn)

            # THEN the ID is found by name/parent and set on the entity
            assert result == ENTITY_ID
            assert file.id == ENTITY_ID
            mocked_find.assert_called_once_with(
                name=ENTITY_NAME,
                parent=PARENT_ID,
                synapse_client=self.syn,
            )

    async def test_searches_project_by_name_without_parent(self) -> None:
        # GIVEN a project with only a name
        project = Project(name=ENTITY_NAME)

        with patch(
            "synapseclient.operations.find_entity_id_async",
            new_callable=AsyncMock,
            return_value=ENTITY_ID,
        ) as mocked_find:
            # WHEN I resolve its ID
            result = await get_id(entity=project, synapse_client=self.syn)

            # THEN the ID is found by name alone
            assert result == ENTITY_ID
            assert project.id == ENTITY_ID
            mocked_find.assert_called_once_with(
                name=ENTITY_NAME,
                parent=None,
                synapse_client=self.syn,
            )

    async def test_raises_when_name_set_but_parent_missing(self) -> None:
        # GIVEN a folder with a name but no ID or parent
        folder = Folder(name=ENTITY_NAME)

        # WHEN I resolve its ID
        # THEN a ValueError is raised
        with pytest.raises(ValueError, match="Entity ID or Name/Parent is required"):
            await get_id(entity=folder, synapse_client=self.syn)

    async def test_raises_when_nothing_set(self) -> None:
        # GIVEN a folder with no identifying fields
        folder = Folder()

        # WHEN I resolve its ID
        # THEN a ValueError is raised
        with pytest.raises(ValueError, match="Entity ID or Name/Parent is required"):
            await get_id(entity=folder, synapse_client=self.syn)

    async def test_returns_none_when_unsearchable_and_failure_strategy_none(
        self,
    ) -> None:
        # GIVEN a folder with no identifying fields
        folder = Folder()

        # WHEN I resolve its ID with failure_strategy=None
        result = await get_id(
            entity=folder, failure_strategy=None, synapse_client=self.syn
        )

        # THEN None is returned instead of raising
        assert result is None

    async def test_raises_when_entity_not_found(self) -> None:
        # GIVEN a folder whose name/parent do not exist in Synapse
        folder = Folder(name=ENTITY_NAME, parent_id=PARENT_ID)

        with patch(
            "synapseclient.operations.find_entity_id_async",
            new_callable=AsyncMock,
            return_value=None,
        ):
            # WHEN I resolve its ID
            # THEN a SynapseNotFoundError is raised
            with pytest.raises(SynapseNotFoundError, match="Folder .* not found"):
                await get_id(entity=folder, synapse_client=self.syn)

    async def test_returns_none_when_not_found_and_failure_strategy_none(self) -> None:
        # GIVEN a folder whose name/parent do not exist in Synapse
        folder = Folder(name=ENTITY_NAME, parent_id=PARENT_ID)

        with patch(
            "synapseclient.operations.find_entity_id_async",
            new_callable=AsyncMock,
            return_value=None,
        ):
            # WHEN I resolve its ID with failure_strategy=None
            result = await get_id(
                entity=folder, failure_strategy=None, synapse_client=self.syn
            )

            # THEN None is returned instead of raising
            assert result is None
            assert folder.id is None
