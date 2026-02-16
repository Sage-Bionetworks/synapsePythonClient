from typing import TYPE_CHECKING
from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.models import DockerRepository

if TYPE_CHECKING:
    from synapseclient import Synapse


TEST_ID = "syn1234"
TEST_NAME = "syn1234"
TEST_DESCRIPTION = "test description"
TEST_ETAG = "test-etag"
TEST_CREATED_ON = "2026-02-16T18:33:27.371"
TEST_MODIFIED_ON = "2026-02-16T18:33:27.371Z"
TEST_CREATED_BY = "5678"
TEST_MODIFIED_BY = "5678"
TEST_PARENT_ID = "syn111111"
TEST_REPOSITORY_NAME = "username/test"
TEST_IS_MANAGED = False
TEST_ANNOTATIONS = None
TEST_CONCRETE_TYPE = "org.sagebionetworks.repo.model.docker.DockerRepository"


class TestDockerRepository:
    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def get_example_docker_output(self) -> dict[str, any]:
        """Returns the entity dict that would be inside a bundle response."""
        return {
            "id": TEST_ID,
            "name": TEST_NAME,
            "description": TEST_DESCRIPTION,
            "etag": TEST_ETAG,
            "createdOn": TEST_CREATED_ON,
            "modifiedOn": TEST_MODIFIED_ON,
            "createdBy": TEST_CREATED_BY,
            "modifiedBy": TEST_MODIFIED_BY,
            "parentId": TEST_PARENT_ID,
            "concreteType": TEST_CONCRETE_TYPE,
            "repositoryName": TEST_REPOSITORY_NAME,
            "isManaged": TEST_IS_MANAGED,
        }

    def get_example_bundle_response(self) -> dict[str, any]:
        """Returns the full bundle response from the API."""
        return {
            "entity": {
                "id": TEST_ID,
                "name": TEST_NAME,
                "description": TEST_DESCRIPTION,
                "etag": TEST_ETAG,
                "createdOn": TEST_CREATED_ON,
                "modifiedOn": TEST_MODIFIED_ON,
                "createdBy": TEST_CREATED_BY,
                "modifiedBy": TEST_MODIFIED_BY,
                "parentId": TEST_PARENT_ID,
                "concreteType": TEST_CONCRETE_TYPE,
                "repositoryName": TEST_REPOSITORY_NAME,
                "isManaged": TEST_IS_MANAGED,
            },
            "entityType": "dockerrepo",
            "annotations": {
                "id": TEST_ID,
                "etag": TEST_ETAG,
                "annotations": TEST_ANNOTATIONS,
            },
            "fileHandles": [],
            "restrictionInformation": {
                "objectId": int(TEST_ID.replace("syn", "")),
                "restrictionLevel": "OPEN",
                "hasUnmetAccessRequirement": False,
            },
        }

    async def test_fill_from_dict(self) -> None:
        docker_output = DockerRepository().fill_from_dict(
            self.get_example_docker_output()
        )

        assert docker_output.id == TEST_ID
        assert docker_output.name == TEST_NAME
        assert docker_output.description == TEST_DESCRIPTION
        assert docker_output.etag == TEST_ETAG
        assert docker_output.created_on == TEST_CREATED_ON
        assert docker_output.modified_on == TEST_MODIFIED_ON
        assert docker_output.created_by == TEST_CREATED_BY
        assert docker_output.modified_by == TEST_MODIFIED_BY
        assert docker_output.parent_id == TEST_PARENT_ID
        assert docker_output.repository_name == TEST_REPOSITORY_NAME
        assert docker_output.is_managed == TEST_IS_MANAGED
        assert docker_output.annotations == TEST_ANNOTATIONS
        assert docker_output.concrete_type == TEST_CONCRETE_TYPE

    async def test_to_synapse_request(self):
        docker = DockerRepository(
            id=TEST_ID,
            name=TEST_NAME,
            description=TEST_DESCRIPTION,
            etag=TEST_ETAG,
            created_on=TEST_CREATED_ON,
            modified_on=TEST_MODIFIED_ON,
            created_by=TEST_CREATED_BY,
            modified_by=TEST_MODIFIED_BY,
            parent_id=TEST_PARENT_ID,
            repository_name=TEST_REPOSITORY_NAME,
            is_managed=TEST_IS_MANAGED,
            annotations=TEST_ANNOTATIONS,
        )

        request_dict = docker.to_synapse_request()
        assert request_dict["id"] == TEST_ID
        assert request_dict["name"] == TEST_NAME
        assert request_dict["description"] == TEST_DESCRIPTION
        assert request_dict["etag"] == TEST_ETAG
        assert request_dict["createdOn"] == TEST_CREATED_ON
        assert request_dict["modifiedOn"] == TEST_MODIFIED_ON
        assert request_dict["createdBy"] == TEST_CREATED_BY
        assert request_dict["modifiedBy"] == TEST_MODIFIED_BY
        assert request_dict["parentId"] == TEST_PARENT_ID
        assert request_dict["repositoryName"] == TEST_REPOSITORY_NAME

    async def test_get_docker_by_id(self) -> None:
        """Test getting a Docker repository by ID."""
        docker = DockerRepository(id=TEST_ID)

        # Mock get_from_entity_factory to simulate filling the entity with data.
        # The implementation (see entity_factory.py:_cast_into_class_type):
        #   1. Fetches entity bundle from Synapse API (get_entity_id_bundle2)
        #   2. Calls _cast_into_class_type which calls entity.fill_from_dict(set_annotations=False)
        #   3. Then separately sets entity.annotations from the bundle
        test_annotation = {"anno": "value"}

        async def mock_get_from_entity_factory(
            entity_to_update, synapse_id_or_path, synapse_client
        ):
            from synapseclient.models import Annotations

            entity_to_update.fill_from_dict(
                self.get_example_docker_output(), set_annotations=False
            )
            entity_to_update.annotations = Annotations.from_dict(test_annotation)

        with patch(
            "synapseclient.models.docker.get_from_entity_factory",
            new_callable=AsyncMock,
            side_effect=mock_get_from_entity_factory,
        ) as mocked_get_from_factory:
            result = await docker.get_async(synapse_client=self.syn)

            # Verify get_from_entity_factory was called with correct parameters
            mocked_get_from_factory.assert_called_once_with(
                entity_to_update=docker,
                synapse_id_or_path=TEST_ID,
                synapse_client=self.syn,
            )

            # Verify the entity was populated correctly
            assert result.id == TEST_ID
            assert result.name == TEST_NAME
            assert result.description == TEST_DESCRIPTION
            assert result.etag == TEST_ETAG
            assert result.created_on == TEST_CREATED_ON
            assert result.modified_on == TEST_MODIFIED_ON
            assert result.created_by == TEST_CREATED_BY
            assert result.modified_by == TEST_MODIFIED_BY
            assert result.parent_id == TEST_PARENT_ID
            assert result.repository_name == TEST_REPOSITORY_NAME
            assert result.is_managed == TEST_IS_MANAGED
            assert result.annotations == test_annotation

    async def test_get_docker_by_repository_name(self) -> None:
        """Test getting a managed Docker repository by repository name."""
        docker = DockerRepository(repository_name=TEST_REPOSITORY_NAME)

        # Mock the repository name lookup
        async def mock_get_entity_id_by_repository_name(
            repository_name, synapse_client
        ):
            return TEST_ID

        # Mock get_from_entity_factory to simulate filling the entity with data
        async def mock_get_from_entity_factory(
            entity_to_update, synapse_id_or_path, synapse_client
        ):
            from synapseclient.models import Annotations

            entity_to_update.fill_from_dict(
                self.get_example_docker_output(), set_annotations=False
            )
            # Separately set annotations to match real implementation
            entity_to_update.annotations = Annotations.from_dict(TEST_ANNOTATIONS)

        with patch(
            "synapseclient.models.docker.get_entity_id_by_repository_name",
            new_callable=AsyncMock,
            side_effect=mock_get_entity_id_by_repository_name,
        ) as mocked_get_id, patch(
            "synapseclient.models.docker.get_from_entity_factory",
            new_callable=AsyncMock,
            side_effect=mock_get_from_entity_factory,
        ) as mocked_get_from_factory:
            result = await docker.get_async(synapse_client=self.syn)

            # Verify repository name lookup was called
            mocked_get_id.assert_called_once_with(
                repository_name=TEST_REPOSITORY_NAME,
                synapse_client=self.syn,
            )

            # Verify get_from_entity_factory was called
            mocked_get_from_factory.assert_called_once_with(
                entity_to_update=docker,
                synapse_id_or_path=TEST_ID,
                synapse_client=self.syn,
            )

            # Verify the entity was populated correctly
            assert result.id == TEST_ID
            assert result.repository_name == TEST_REPOSITORY_NAME

    async def test_store_docker(self) -> None:
        """Test storing a Docker repository."""
        docker = DockerRepository(
            parent_id=TEST_PARENT_ID,
            repository_name=TEST_REPOSITORY_NAME,
            name=TEST_NAME,
            description=TEST_DESCRIPTION,
        )

        with patch(
            "synapseclient.models.docker.store_entity",
            new_callable=AsyncMock,
            return_value=self.get_example_docker_output(),
        ) as mocked_store:
            result = await docker.store_async(synapse_client=self.syn)

            # Verify store_entity was called
            mocked_store.assert_called_once()
            call_args = mocked_store.call_args
            assert call_args.kwargs["resource"] == docker
            assert call_args.kwargs["synapse_client"] == self.syn

            # Verify the returned entity has the stored data
            assert result.id == TEST_ID
            assert result.name == TEST_NAME
            assert result.description == TEST_DESCRIPTION
            assert result.repository_name == TEST_REPOSITORY_NAME

    async def test_delete_docker(self) -> None:
        """Test deleting a Docker repository."""
        docker = DockerRepository(id=TEST_ID)

        # Mock the delete function that's imported in docker.py
        with patch(
            "synapseclient.models.docker.delete",
            return_value=None,
        ) as mocked_delete:
            await docker.delete_async(synapse_client=self.syn)

            # Verify delete was called with the entity ID
            mocked_delete.assert_called_once_with(TEST_ID)
