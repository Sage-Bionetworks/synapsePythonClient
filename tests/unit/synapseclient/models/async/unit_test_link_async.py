"""Unit tests for Link."""

from typing import Any, Dict
from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.core.constants.concrete_types import LINK_ENTITY
from synapseclient.models import Folder, Link, Project

LINK_NAME = "my_test_link"
DESCRIPTION = "A test link description"
LINK_ID = "syn123"
PARENT_ID = "syn456"
TARGET_ID = "syn789"
TARGET_VERSION_NUMBER = 3
LINKS_TO_CLASS_NAME = "org.sagebionetworks.repo.model.FileEntity"
ETAG = "some_etag"
CREATED_ON = "2023-01-01T00:00:00Z"
MODIFIED_ON = "2023-01-02T00:00:00Z"
CREATED_BY = "user1"
MODIFIED_BY = "user2"


class TestLink:
    """Unit tests for Link."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def get_example_rest_api_response(self) -> Dict[str, Any]:
        """Return a mock REST API response for a Link entity."""
        return {
            "name": LINK_NAME,
            "description": DESCRIPTION,
            "id": LINK_ID,
            "etag": ETAG,
            "createdOn": CREATED_ON,
            "modifiedOn": MODIFIED_ON,
            "createdBy": CREATED_BY,
            "modifiedBy": MODIFIED_BY,
            "parentId": PARENT_ID,
            "concreteType": LINK_ENTITY,
            "linksTo": {
                "targetId": TARGET_ID,
                "targetVersionNumber": TARGET_VERSION_NUMBER,
            },
            "linksToClassName": LINKS_TO_CLASS_NAME,
            "annotations": {},
        }

    def get_example_rest_api_response_no_version(self) -> Dict[str, Any]:
        """Return a mock REST API response for a Link entity without target version."""
        response = self.get_example_rest_api_response()
        response["linksTo"] = {"targetId": TARGET_ID}
        return response

    # -------------------------------------------------------------------------
    # fill_from_dict tests
    # -------------------------------------------------------------------------
    def test_fill_from_dict(self) -> None:
        # GIVEN a blank Link
        link = Link()

        # WHEN we fill it from a dictionary with all fields
        result = link.fill_from_dict(
            synapse_entity=self.get_example_rest_api_response()
        )

        # THEN the Link should have all fields filled
        assert result.name == LINK_NAME
        assert result.description == DESCRIPTION
        assert result.id == LINK_ID
        assert result.etag == ETAG
        assert result.created_on == CREATED_ON
        assert result.modified_on == MODIFIED_ON
        assert result.created_by == CREATED_BY
        assert result.modified_by == MODIFIED_BY
        assert result.parent_id == PARENT_ID
        assert result.target_id == TARGET_ID
        assert result.target_version_number == TARGET_VERSION_NUMBER
        assert result.links_to_class_name == LINKS_TO_CLASS_NAME
        assert result.annotations == {}

    def test_fill_from_dict_with_no_links_to(self) -> None:
        # GIVEN a blank Link
        link = Link()

        # AND a response without a linksTo field
        response = self.get_example_rest_api_response()
        response.pop("linksTo")

        # WHEN we fill it from that response
        result = link.fill_from_dict(synapse_entity=response)

        # THEN target_id and target_version_number should be None
        assert result.target_id is None
        assert result.target_version_number is None

    def test_fill_from_dict_without_target_version_number(self) -> None:
        # GIVEN a blank Link
        link = Link()

        # WHEN we fill it from a response that has linksTo without targetVersionNumber
        result = link.fill_from_dict(
            synapse_entity=self.get_example_rest_api_response_no_version()
        )

        # THEN target_id should be set but target_version_number should be None
        assert result.target_id == TARGET_ID
        assert result.target_version_number is None

    def test_fill_from_dict_without_annotations(self) -> None:
        # GIVEN a blank Link
        link = Link()

        # WHEN we fill it with set_annotations=False
        result = link.fill_from_dict(
            synapse_entity=self.get_example_rest_api_response(),
            set_annotations=False,
        )

        # THEN annotations should remain the default (empty dict from dataclass)
        assert result.annotations == {}

    # -------------------------------------------------------------------------
    # to_synapse_request tests
    # -------------------------------------------------------------------------
    def test_to_synapse_request(self) -> None:
        # GIVEN a Link with all fields
        link = Link(
            name=LINK_NAME,
            description=DESCRIPTION,
            id=LINK_ID,
            etag=ETAG,
            created_on=CREATED_ON,
            modified_on=MODIFIED_ON,
            created_by=CREATED_BY,
            modified_by=MODIFIED_BY,
            parent_id=PARENT_ID,
            target_id=TARGET_ID,
            target_version_number=TARGET_VERSION_NUMBER,
            links_to_class_name=LINKS_TO_CLASS_NAME,
        )

        # WHEN we convert it to a Synapse request
        request = link.to_synapse_request()

        # THEN the request should contain all expected fields
        assert request["name"] == LINK_NAME
        assert request["description"] == DESCRIPTION
        assert request["id"] == LINK_ID
        assert request["etag"] == ETAG
        assert request["createdOn"] == CREATED_ON
        assert request["modifiedOn"] == MODIFIED_ON
        assert request["createdBy"] == CREATED_BY
        assert request["modifiedBy"] == MODIFIED_BY
        assert request["parentId"] == PARENT_ID
        assert request["concreteType"] == LINK_ENTITY
        assert request["linksTo"]["targetId"] == TARGET_ID
        assert request["linksTo"]["targetVersionNumber"] == TARGET_VERSION_NUMBER
        assert request["linksToClassName"] == LINKS_TO_CLASS_NAME

    def test_to_synapse_request_without_target(self) -> None:
        # GIVEN a Link without a target_id
        link = Link(
            name=LINK_NAME,
            parent_id=PARENT_ID,
        )

        # WHEN we convert it to a Synapse request
        request = link.to_synapse_request()

        # THEN linksTo should not be in the request (None keys are removed)
        assert "linksTo" not in request

    def test_to_synapse_request_without_target_version(self) -> None:
        # GIVEN a Link with target_id but no target_version_number
        link = Link(
            name=LINK_NAME,
            parent_id=PARENT_ID,
            target_id=TARGET_ID,
        )

        # WHEN we convert it to a Synapse request
        request = link.to_synapse_request()

        # THEN linksTo should have targetId but no targetVersionNumber
        assert request["linksTo"]["targetId"] == TARGET_ID
        assert "targetVersionNumber" not in request["linksTo"]

    def test_to_synapse_request_removes_none_keys(self) -> None:
        # GIVEN a Link with only minimal fields
        link = Link(
            name=LINK_NAME,
            target_id=TARGET_ID,
        )

        # WHEN we convert it to a Synapse request
        request = link.to_synapse_request()

        # THEN None values should be removed
        assert "id" not in request
        assert "etag" not in request
        assert "description" not in request
        assert "createdOn" not in request
        assert "modifiedOn" not in request
        assert "createdBy" not in request
        assert "modifiedBy" not in request
        assert "parentId" not in request
        assert "linksToClassName" not in request

        # AND the present fields should be there
        assert request["name"] == LINK_NAME
        assert request["concreteType"] == LINK_ENTITY
        assert request["linksTo"]["targetId"] == TARGET_ID

    # -------------------------------------------------------------------------
    # get_async tests
    # -------------------------------------------------------------------------
    async def test_get_by_id_follow_link_true(self) -> None:
        # GIVEN a Link with an id
        link = Link(id=LINK_ID)

        # WHEN we call get_async with follow_link=True (default)
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=LINK_ID,
            ) as mocked_get_id,
            patch(
                "synapseclient.models.link.get_from_entity_factory",
                new_callable=AsyncMock,
            ) as mocked_get_entity_factory,
            patch(
                "synapseclient.operations.factory_operations.get_async",
                new_callable=AsyncMock,
                return_value="followed_entity",
            ) as mocked_factory_get_async,
        ):
            # Set up get_from_entity_factory to populate the link
            async def fill_link(synapse_id_or_path, entity_to_update, synapse_client):
                entity_to_update.fill_from_dict(self.get_example_rest_api_response())

            mocked_get_entity_factory.side_effect = fill_link

            result = await link.get_async(synapse_client=self.syn)

            # THEN get_id should have been called
            mocked_get_id.assert_called_once_with(entity=link, synapse_client=self.syn)

            # AND get_from_entity_factory should have been called
            mocked_get_entity_factory.assert_called_once_with(
                synapse_id_or_path=LINK_ID,
                entity_to_update=link,
                synapse_client=self.syn,
            )

            # AND factory_get_async should have been called to follow the link
            mocked_factory_get_async.assert_called_once_with(
                synapse_id=TARGET_ID,
                version_number=TARGET_VERSION_NUMBER,
                file_options=None,
                synapse_client=self.syn,
            )

            # AND the result should be the followed entity
            assert result == "followed_entity"

    async def test_get_by_id_follow_link_false(self) -> None:
        # GIVEN a Link with an id
        link = Link(id=LINK_ID)

        # WHEN we call get_async with follow_link=False
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=LINK_ID,
            ) as mocked_get_id,
            patch(
                "synapseclient.models.link.get_from_entity_factory",
                new_callable=AsyncMock,
            ) as mocked_get_entity_factory,
        ):

            async def fill_link(synapse_id_or_path, entity_to_update, synapse_client):
                entity_to_update.fill_from_dict(self.get_example_rest_api_response())

            mocked_get_entity_factory.side_effect = fill_link

            result = await link.get_async(follow_link=False, synapse_client=self.syn)

            # THEN get_id should have been called
            mocked_get_id.assert_called_once_with(entity=link, synapse_client=self.syn)

            # AND get_from_entity_factory should have been called
            mocked_get_entity_factory.assert_called_once_with(
                synapse_id_or_path=LINK_ID,
                entity_to_update=link,
                synapse_client=self.syn,
            )

            # AND the result should be the Link itself
            assert result is link
            assert result.id == LINK_ID
            assert result.name == LINK_NAME
            assert result.target_id == TARGET_ID
            assert result.target_version_number == TARGET_VERSION_NUMBER

    async def test_get_by_name_and_parent_id(self) -> None:
        # GIVEN a Link with a name and parent_id but no id
        link = Link(name=LINK_NAME, parent_id=PARENT_ID)

        # WHEN we call get_async
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=LINK_ID,
            ) as mocked_get_id,
            patch(
                "synapseclient.models.link.get_from_entity_factory",
                new_callable=AsyncMock,
            ) as mocked_get_entity_factory,
        ):

            async def fill_link(synapse_id_or_path, entity_to_update, synapse_client):
                entity_to_update.fill_from_dict(self.get_example_rest_api_response())

            mocked_get_entity_factory.side_effect = fill_link

            result = await link.get_async(follow_link=False, synapse_client=self.syn)

            # THEN get_id should have been called
            mocked_get_id.assert_called_once_with(entity=link, synapse_client=self.syn)

            # AND the result should be the Link
            assert result is link
            assert result.id == LINK_ID

    async def test_get_by_name_and_parent_from_argument(self) -> None:
        # GIVEN a Link with a name but no parent_id
        link = Link(name=LINK_NAME)

        # AND a parent folder passed as argument
        parent = Folder(id=PARENT_ID)

        # WHEN we call get_async with the parent
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=LINK_ID,
            ),
            patch(
                "synapseclient.models.link.get_from_entity_factory",
                new_callable=AsyncMock,
            ) as mocked_get_entity_factory,
        ):

            async def fill_link(synapse_id_or_path, entity_to_update, synapse_client):
                entity_to_update.fill_from_dict(self.get_example_rest_api_response())

            mocked_get_entity_factory.side_effect = fill_link

            result = await link.get_async(
                parent=parent, follow_link=False, synapse_client=self.syn
            )

            # THEN the link's parent_id should be set from the parent argument
            assert result.parent_id == PARENT_ID

    async def test_get_by_name_and_parent_project(self) -> None:
        # GIVEN a Link with a name but no parent_id
        link = Link(name=LINK_NAME)

        # AND a parent project passed as argument
        parent = Project(id=PARENT_ID)

        # WHEN we call get_async with the parent
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=LINK_ID,
            ),
            patch(
                "synapseclient.models.link.get_from_entity_factory",
                new_callable=AsyncMock,
            ) as mocked_get_entity_factory,
        ):

            async def fill_link(synapse_id_or_path, entity_to_update, synapse_client):
                entity_to_update.fill_from_dict(self.get_example_rest_api_response())

            mocked_get_entity_factory.side_effect = fill_link

            result = await link.get_async(
                parent=parent, follow_link=False, synapse_client=self.syn
            )

            # THEN the link's parent_id should be set from the parent argument
            assert result.parent_id == PARENT_ID

    async def test_get_raises_when_no_id_and_no_name(self) -> None:
        # GIVEN a Link with no id and no name
        link = Link()

        # WHEN we call get_async
        # THEN it should raise a ValueError
        with pytest.raises(
            ValueError,
            match="The link must have an id or a "
            "\\(name and \\(`parent_id` or parent with an id\\)\\) set.",
        ):
            await link.get_async(synapse_client=self.syn)

    async def test_get_raises_when_name_but_no_parent(self) -> None:
        # GIVEN a Link with a name but no parent_id and no parent argument
        link = Link(name=LINK_NAME)

        # WHEN we call get_async without a parent
        # THEN it should raise a ValueError
        with pytest.raises(
            ValueError,
            match="The link must have an id or a "
            "\\(name and \\(`parent_id` or parent with an id\\)\\) set.",
        ):
            await link.get_async(synapse_client=self.syn)

    async def test_get_follow_link_with_file_options(self) -> None:
        # GIVEN a Link with an id
        link = Link(id=LINK_ID)

        # AND file options
        mock_file_options = object()

        # WHEN we call get_async with follow_link=True and file_options
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=LINK_ID,
            ),
            patch(
                "synapseclient.models.link.get_from_entity_factory",
                new_callable=AsyncMock,
            ) as mocked_get_entity_factory,
            patch(
                "synapseclient.operations.factory_operations.get_async",
                new_callable=AsyncMock,
                return_value="followed_file_entity",
            ) as mocked_factory_get_async,
        ):

            async def fill_link(synapse_id_or_path, entity_to_update, synapse_client):
                entity_to_update.fill_from_dict(self.get_example_rest_api_response())

            mocked_get_entity_factory.side_effect = fill_link

            result = await link.get_async(
                follow_link=True,
                file_options=mock_file_options,
                synapse_client=self.syn,
            )

            # THEN factory_get_async should be called with the file_options
            mocked_factory_get_async.assert_called_once_with(
                synapse_id=TARGET_ID,
                version_number=TARGET_VERSION_NUMBER,
                file_options=mock_file_options,
                synapse_client=self.syn,
            )

            # AND the result should be the followed entity
            assert result == "followed_file_entity"

    async def test_get_sets_last_persistent_instance(self) -> None:
        # GIVEN a Link with an id
        link = Link(id=LINK_ID)

        # WHEN we call get_async with follow_link=False
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=LINK_ID,
            ),
            patch(
                "synapseclient.models.link.get_from_entity_factory",
                new_callable=AsyncMock,
            ) as mocked_get_entity_factory,
        ):

            async def fill_link(synapse_id_or_path, entity_to_update, synapse_client):
                entity_to_update.fill_from_dict(self.get_example_rest_api_response())

            mocked_get_entity_factory.side_effect = fill_link

            result = await link.get_async(follow_link=False, synapse_client=self.syn)

            # THEN _last_persistent_instance should be set
            assert result._last_persistent_instance is not None
            assert result._last_persistent_instance.id == LINK_ID
            assert result._last_persistent_instance.name == LINK_NAME

    # -------------------------------------------------------------------------
    # store_async tests
    # -------------------------------------------------------------------------
    async def test_store_new_link(self) -> None:
        # GIVEN a new Link with name, parent_id, and target_id
        link = Link(
            name=LINK_NAME,
            parent_id=PARENT_ID,
            target_id=TARGET_ID,
            target_version_number=TARGET_VERSION_NUMBER,
        )

        # WHEN we call store_async
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "synapseclient.models.link.store_entity",
                new_callable=AsyncMock,
                return_value=self.get_example_rest_api_response(),
            ) as mocked_store_entity,
            patch(
                "synapseclient.models.link.store_entity_components",
                new_callable=AsyncMock,
                return_value=False,
            ) as mocked_store_components,
        ):
            result = await link.store_async(synapse_client=self.syn)

            # THEN store_entity should have been called
            mocked_store_entity.assert_called_once()
            call_kwargs = mocked_store_entity.call_args.kwargs
            assert call_kwargs["resource"] is link
            assert call_kwargs["synapse_client"] is self.syn
            request = call_kwargs["entity"]
            assert request["name"] == LINK_NAME
            assert request["parentId"] == PARENT_ID
            assert request["concreteType"] == LINK_ENTITY
            assert request["linksTo"]["targetId"] == TARGET_ID
            assert request["linksTo"]["targetVersionNumber"] == TARGET_VERSION_NUMBER

            # AND store_entity_components should have been called
            mocked_store_components.assert_called_once_with(
                root_resource=link, synapse_client=self.syn
            )

            # AND the link should be filled from the response
            assert result.id == LINK_ID
            assert result.name == LINK_NAME
            assert result.etag == ETAG
            assert result.parent_id == PARENT_ID
            assert result.target_id == TARGET_ID
            assert result.target_version_number == TARGET_VERSION_NUMBER

            # AND _last_persistent_instance should be set
            assert result._last_persistent_instance is not None

    async def test_store_existing_link_with_id(self) -> None:
        # GIVEN a Link with an id (existing entity)
        link = Link(
            id=LINK_ID,
            name=LINK_NAME,
            description="Updated description",
        )

        # WHEN we call store_async
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=LINK_ID,
            ),
            patch(
                "synapseclient.models.link.get_from_entity_factory",
                new_callable=AsyncMock,
            ) as mocked_get_entity_factory,
            patch(
                "synapseclient.models.link.store_entity",
                new_callable=AsyncMock,
                return_value=self.get_example_rest_api_response(),
            ) as mocked_store_entity,
            patch(
                "synapseclient.models.link.store_entity_components",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):
            # Set up get_from_entity_factory to populate the link copy in
            # _find_existing_entity
            async def fill_link(synapse_id_or_path, entity_to_update, synapse_client):
                entity_to_update.fill_from_dict(self.get_example_rest_api_response())

            mocked_get_entity_factory.side_effect = fill_link

            result = await link.store_async(synapse_client=self.syn)

            # THEN store_entity should have been called (entity has changed)
            mocked_store_entity.assert_called_once()

            # AND the result should reflect the stored state
            assert result.id == LINK_ID
            assert result.name == LINK_NAME

    async def test_store_with_parent_argument(self) -> None:
        # GIVEN a Link without parent_id set
        link = Link(
            name=LINK_NAME,
            target_id=TARGET_ID,
        )

        # AND a parent folder
        parent = Folder(id=PARENT_ID)

        # WHEN we call store_async with the parent argument
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "synapseclient.models.link.store_entity",
                new_callable=AsyncMock,
                return_value=self.get_example_rest_api_response(),
            ) as mocked_store_entity,
            patch(
                "synapseclient.models.link.store_entity_components",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):
            result = await link.store_async(parent=parent, synapse_client=self.syn)

            # THEN the parent_id should be set from the parent argument
            assert result.parent_id == PARENT_ID

            # AND store_entity should have been called with parent_id in the request
            call_kwargs = mocked_store_entity.call_args.kwargs
            request = call_kwargs["entity"]
            assert request["parentId"] == PARENT_ID

    async def test_store_with_parent_project(self) -> None:
        # GIVEN a Link without parent_id set
        link = Link(
            name=LINK_NAME,
            target_id=TARGET_ID,
        )

        # AND a parent project
        parent = Project(id=PARENT_ID)

        # WHEN we call store_async with the parent argument
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "synapseclient.models.link.store_entity",
                new_callable=AsyncMock,
                return_value=self.get_example_rest_api_response(),
            ),
            patch(
                "synapseclient.models.link.store_entity_components",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):
            result = await link.store_async(parent=parent, synapse_client=self.syn)

            # THEN the parent_id should be set from the parent argument
            assert result.parent_id == PARENT_ID

    async def test_store_raises_when_no_name_and_no_id(self) -> None:
        # GIVEN a Link with no name and no id
        link = Link(parent_id=PARENT_ID, target_id=TARGET_ID)

        # WHEN we call store_async
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="The link must have a name."):
            await link.store_async(synapse_client=self.syn)

    async def test_store_raises_when_no_parent_id_and_no_id(self) -> None:
        # GIVEN a Link with a name but no parent_id and no id
        link = Link(name=LINK_NAME, target_id=TARGET_ID)

        # WHEN we call store_async
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="The link must have a parent_id."):
            await link.store_async(synapse_client=self.syn)

    async def test_store_raises_when_no_target_id_and_no_id(self) -> None:
        # GIVEN a Link with a name and parent_id but no target_id and no id
        link = Link(name=LINK_NAME, parent_id=PARENT_ID)

        # WHEN we call store_async
        # THEN it should raise a ValueError
        with pytest.raises(ValueError, match="The link must have a target_id."):
            await link.store_async(synapse_client=self.syn)

    async def test_store_skips_validation_when_id_is_set(self) -> None:
        # GIVEN a Link with only an id (no name, no parent_id, no target_id)
        link = Link(id=LINK_ID)

        # WHEN we call store_async, it should NOT raise ValueError
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=LINK_ID,
            ),
            patch(
                "synapseclient.models.link.get_from_entity_factory",
                new_callable=AsyncMock,
            ) as mocked_get_entity_factory,
            patch(
                "synapseclient.models.link.store_entity",
                new_callable=AsyncMock,
                return_value=self.get_example_rest_api_response(),
            ),
            patch(
                "synapseclient.models.link.store_entity_components",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):

            async def fill_link(synapse_id_or_path, entity_to_update, synapse_client):
                entity_to_update.fill_from_dict(self.get_example_rest_api_response())

            mocked_get_entity_factory.side_effect = fill_link

            # THEN no ValueError is raised because id is set
            result = await link.store_async(synapse_client=self.syn)
            assert result.id == LINK_ID

    async def test_store_no_changes_skips_store_entity(self) -> None:
        # GIVEN a Link that was previously retrieved from Synapse
        link = Link(
            id=LINK_ID,
            name=LINK_NAME,
            parent_id=PARENT_ID,
            target_id=TARGET_ID,
        )

        # AND get_async has been called (which sets _last_persistent_instance)
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=LINK_ID,
            ),
            patch(
                "synapseclient.models.link.get_from_entity_factory",
                new_callable=AsyncMock,
            ) as mocked_get_entity_factory,
        ):

            async def fill_link(synapse_id_or_path, entity_to_update, synapse_client):
                entity_to_update.fill_from_dict(self.get_example_rest_api_response())

            mocked_get_entity_factory.side_effect = fill_link
            await link.get_async(follow_link=False, synapse_client=self.syn)

        # WHEN we call store_async without making changes
        with (
            patch(
                "synapseclient.models.link.store_entity",
                new_callable=AsyncMock,
            ) as mocked_store_entity,
            patch(
                "synapseclient.models.link.store_entity_components",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):
            result = await link.store_async(synapse_client=self.syn)

            # THEN store_entity should NOT have been called
            mocked_store_entity.assert_not_called()

            # AND the result should still be the link
            assert result.id == LINK_ID

    async def test_store_with_changes_after_get(self) -> None:
        # GIVEN a Link that was previously retrieved from Synapse
        link = Link(id=LINK_ID)

        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=LINK_ID,
            ),
            patch(
                "synapseclient.models.link.get_from_entity_factory",
                new_callable=AsyncMock,
            ) as mocked_get_entity_factory,
        ):

            async def fill_link(synapse_id_or_path, entity_to_update, synapse_client):
                entity_to_update.fill_from_dict(self.get_example_rest_api_response())

            mocked_get_entity_factory.side_effect = fill_link
            await link.get_async(follow_link=False, synapse_client=self.syn)

        # AND we make a change
        link.description = "New description"

        # WHEN we call store_async
        updated_response = self.get_example_rest_api_response()
        updated_response["description"] = "New description"

        with (
            patch(
                "synapseclient.models.link.store_entity",
                new_callable=AsyncMock,
                return_value=updated_response,
            ) as mocked_store_entity,
            patch(
                "synapseclient.models.link.store_entity_components",
                new_callable=AsyncMock,
                return_value=False,
            ),
        ):
            result = await link.store_async(synapse_client=self.syn)

            # THEN store_entity SHOULD have been called because there are changes
            mocked_store_entity.assert_called_once()

            # AND the result should reflect the updated data
            assert result.id == LINK_ID

    async def test_store_re_reads_when_components_change(self) -> None:
        # GIVEN a new Link
        link = Link(
            name=LINK_NAME,
            parent_id=PARENT_ID,
            target_id=TARGET_ID,
        )

        # WHEN we call store_async and store_entity_components returns True
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "synapseclient.models.link.store_entity",
                new_callable=AsyncMock,
                return_value=self.get_example_rest_api_response(),
            ),
            patch(
                "synapseclient.models.link.store_entity_components",
                new_callable=AsyncMock,
                return_value=True,
            ),
            patch.object(
                link,
                "get_async",
                new_callable=AsyncMock,
            ) as mocked_get_async,
        ):
            result = await link.store_async(synapse_client=self.syn)

            # THEN get_async should have been called for a re-read
            mocked_get_async.assert_called_once_with(
                synapse_client=self.syn,
            )

    async def test_store_does_not_re_read_when_no_component_changes(self) -> None:
        # GIVEN a new Link
        link = Link(
            name=LINK_NAME,
            parent_id=PARENT_ID,
            target_id=TARGET_ID,
        )

        # WHEN we call store_async and store_entity_components returns False
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=None,
            ),
            patch(
                "synapseclient.models.link.store_entity",
                new_callable=AsyncMock,
                return_value=self.get_example_rest_api_response(),
            ),
            patch(
                "synapseclient.models.link.store_entity_components",
                new_callable=AsyncMock,
                return_value=False,
            ),
            patch.object(
                link,
                "get_async",
                new_callable=AsyncMock,
            ) as mocked_get_async,
        ):
            result = await link.store_async(synapse_client=self.syn)

            # THEN get_async should NOT have been called for a re-read
            mocked_get_async.assert_not_called()

    # -------------------------------------------------------------------------
    # _find_existing_entity tests
    # -------------------------------------------------------------------------
    async def test_find_existing_entity_when_entity_exists(self) -> None:
        # GIVEN a Link with a name and parent_id (no _last_persistent_instance)
        link = Link(
            name=LINK_NAME,
            parent_id=PARENT_ID,
            target_id=TARGET_ID,
        )

        # WHEN we call _find_existing_entity and an entity is found
        with (
            patch(
                "synapseclient.models.link.get_id",
                new_callable=AsyncMock,
                return_value=LINK_ID,
            ),
            patch(
                "synapseclient.models.link.get_from_entity_factory",
                new_callable=AsyncMock,
            ) as mocked_get_entity_factory,
        ):

            async def fill_link(synapse_id_or_path, entity_to_update, synapse_client):
                entity_to_update.fill_from_dict(self.get_example_rest_api_response())

            mocked_get_entity_factory.side_effect = fill_link

            result = await link._find_existing_entity(synapse_client=self.syn)

            # THEN the result should be a Link with the existing data
            assert result is not None
            assert result.id == LINK_ID
            assert result.name == LINK_NAME

    async def test_find_existing_entity_when_no_entity_exists(self) -> None:
        # GIVEN a Link with a name and parent_id
        link = Link(
            name=LINK_NAME,
            parent_id=PARENT_ID,
            target_id=TARGET_ID,
        )

        # WHEN we call _find_existing_entity and no entity is found
        with patch(
            "synapseclient.models.link.get_id",
            new_callable=AsyncMock,
            return_value=None,
        ):
            result = await link._find_existing_entity(synapse_client=self.syn)

            # THEN the result should be None
            assert result is None

    async def test_find_existing_entity_skipped_when_last_persistent_instance_set(
        self,
    ) -> None:
        # GIVEN a Link that already has _last_persistent_instance
        link = Link(
            id=LINK_ID,
            name=LINK_NAME,
            parent_id=PARENT_ID,
            target_id=TARGET_ID,
        )
        link._set_last_persistent_instance()

        # WHEN we call _find_existing_entity
        with patch(
            "synapseclient.models.link.get_id",
            new_callable=AsyncMock,
        ) as mocked_get_id:
            result = await link._find_existing_entity(synapse_client=self.syn)

            # THEN get_id should NOT have been called
            mocked_get_id.assert_not_called()

            # AND the result should be None
            assert result is None

    # -------------------------------------------------------------------------
    # has_changed property tests
    # -------------------------------------------------------------------------
    def test_has_changed_when_no_last_persistent_instance(self) -> None:
        # GIVEN a Link with no _last_persistent_instance
        link = Link(name=LINK_NAME)

        # WHEN we check has_changed
        # THEN it should be True
        assert link.has_changed is True

    def test_has_changed_when_no_changes(self) -> None:
        # GIVEN a Link with _last_persistent_instance set
        link = Link(
            id=LINK_ID,
            name=LINK_NAME,
            parent_id=PARENT_ID,
            target_id=TARGET_ID,
        )
        link._set_last_persistent_instance()

        # WHEN we check has_changed without making changes
        # THEN it should be False
        assert link.has_changed is False

    def test_has_changed_after_modification(self) -> None:
        # GIVEN a Link with _last_persistent_instance set
        link = Link(
            id=LINK_ID,
            name=LINK_NAME,
            parent_id=PARENT_ID,
            target_id=TARGET_ID,
        )
        link._set_last_persistent_instance()

        # WHEN we modify the link
        link.description = "Something new"

        # THEN has_changed should be True
        assert link.has_changed is True

    # -------------------------------------------------------------------------
    # _set_last_persistent_instance tests
    # -------------------------------------------------------------------------
    def test_set_last_persistent_instance(self) -> None:
        # GIVEN a Link with data
        link = Link(
            id=LINK_ID,
            name=LINK_NAME,
            parent_id=PARENT_ID,
            target_id=TARGET_ID,
            annotations={"key": ["value"]},
        )

        # WHEN we set the last persistent instance
        link._set_last_persistent_instance()

        # THEN it should be a copy of the current state
        assert link._last_persistent_instance is not None
        assert link._last_persistent_instance.id == LINK_ID
        assert link._last_persistent_instance.name == LINK_NAME
        assert link._last_persistent_instance.parent_id == PARENT_ID
        assert link._last_persistent_instance.target_id == TARGET_ID

        # AND annotations should be a deep copy
        assert link._last_persistent_instance.annotations == {"key": ["value"]}
        assert link._last_persistent_instance.annotations is not link.annotations
