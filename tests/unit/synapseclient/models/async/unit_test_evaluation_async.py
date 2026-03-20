"""Async unit tests for the synapseclient.models.Evaluation class."""

from copy import deepcopy
from typing import Dict, List, Union
from unittest.mock import AsyncMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.models.evaluation import Evaluation, RequestType

EVALUATION_ID = "9614112"
EVALUATION_NAME = "My Challenge Evaluation"
EVALUATION_DESCRIPTION = "Evaluation for my data challenge"
EVALUATION_ETAG = "etag-abc-123"
OWNER_ID = "123456"
CREATED_ON = "2023-01-01T10:00:00.000Z"
CONTENT_SOURCE = "syn123456"
SUBMISSION_INSTRUCTIONS = "Submit CSV files only"
SUBMISSION_RECEIPT = "Thank you for your submission!"
PROJECT_ID = "syn789012"
PRINCIPAL_ID = "999888"
TEAM_PRINCIPAL_ID = "777666"


class TestEvaluationAsync:
    """Async tests for the synapseclient.models.Evaluation class."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def get_example_evaluation_response(self) -> Dict[str, str]:
        """Get a complete example evaluation response from the REST API."""
        return {
            "id": EVALUATION_ID,
            "etag": EVALUATION_ETAG,
            "name": EVALUATION_NAME,
            "description": EVALUATION_DESCRIPTION,
            "ownerId": OWNER_ID,
            "createdOn": CREATED_ON,
            "contentSource": CONTENT_SOURCE,
            "submissionInstructionsMessage": SUBMISSION_INSTRUCTIONS,
            "submissionReceiptMessage": SUBMISSION_RECEIPT,
        }

    def get_minimal_evaluation_response(self) -> Dict[str, str]:
        """Get a minimal example evaluation response from the REST API."""
        return {
            "id": EVALUATION_ID,
            "etag": EVALUATION_ETAG,
            "name": EVALUATION_NAME,
        }

    def get_example_acl_response(self) -> Dict[str, Union[str, List]]:
        """Get an example ACL response from the REST API."""
        return {
            "id": EVALUATION_ID,
            "resourceAccess": [
                {
                    "principalId": int(OWNER_ID),
                    "accessType": [
                        "READ",
                        "UPDATE",
                        "DELETE",
                        "CHANGE_PERMISSIONS",
                    ],
                },
            ],
            "etag": EVALUATION_ETAG,
        }

    def get_example_permissions_response(self) -> Dict[str, Union[str, List, bool]]:
        """Get an example permissions response from the REST API."""
        return {
            "canPublicRead": False,
            "ownerPrincipalId": int(OWNER_ID),
            "canView": True,
            "canEdit": True,
            "canMove": False,
            "canAddChild": False,
            "canCertifiedUserEdit": True,
            "canCertifiedUserAddChild": False,
            "isCertifiedUser": True,
            "canChangePermissions": True,
            "canChangeSettings": True,
            "canDelete": True,
            "canDownload": True,
            "canUpload": False,
            "canEnableInheritance": False,
            "canModerate": False,
        }

    def _build_evaluation_with_all_fields(self) -> Evaluation:
        """Build an Evaluation instance with all required fields for store."""
        return Evaluation(
            name=EVALUATION_NAME,
            description=EVALUATION_DESCRIPTION,
            content_source=CONTENT_SOURCE,
            submission_instructions_message=SUBMISSION_INSTRUCTIONS,
            submission_receipt_message=SUBMISSION_RECEIPT,
        )

    # ------------------------------------------------------------------ #
    #  fill_from_dict
    # ------------------------------------------------------------------ #

    def test_fill_from_dict_complete_data(self) -> None:
        # GIVEN a complete evaluation response from the REST API
        response = self.get_example_evaluation_response()

        # WHEN I call fill_from_dict with the example evaluation response
        evaluation = Evaluation().fill_from_dict(response)

        # THEN the Evaluation object should be filled with all the data
        assert evaluation.id == EVALUATION_ID
        assert evaluation.etag == EVALUATION_ETAG
        assert evaluation.name == EVALUATION_NAME
        assert evaluation.description == EVALUATION_DESCRIPTION
        assert evaluation.owner_id == OWNER_ID
        assert evaluation.created_on == CREATED_ON
        assert evaluation.content_source == CONTENT_SOURCE
        assert evaluation.submission_instructions_message == SUBMISSION_INSTRUCTIONS
        assert evaluation.submission_receipt_message == SUBMISSION_RECEIPT

    def test_fill_from_dict_minimal_data(self) -> None:
        # GIVEN a minimal evaluation response from the REST API
        response = self.get_minimal_evaluation_response()

        # WHEN I call fill_from_dict with only required fields
        evaluation = Evaluation().fill_from_dict(response)

        # THEN the Evaluation object should have set fields and None for missing
        assert evaluation.id == EVALUATION_ID
        assert evaluation.etag == EVALUATION_ETAG
        assert evaluation.name == EVALUATION_NAME
        assert evaluation.description is None
        assert evaluation.owner_id is None
        assert evaluation.created_on is None
        assert evaluation.content_source is None
        assert evaluation.submission_instructions_message is None
        assert evaluation.submission_receipt_message is None

    def test_fill_from_dict_empty_dict(self) -> None:
        # GIVEN an empty dictionary
        # WHEN I call fill_from_dict with an empty dict
        evaluation = Evaluation().fill_from_dict({})

        # THEN all fields should be None
        assert evaluation.id is None
        assert evaluation.etag is None
        assert evaluation.name is None

    # ------------------------------------------------------------------ #
    #  to_synapse_request
    # ------------------------------------------------------------------ #

    def test_to_synapse_request_create(self) -> None:
        # GIVEN an Evaluation with all required fields for creation
        evaluation = self._build_evaluation_with_all_fields()

        # WHEN I call to_synapse_request with CREATE type
        request_body = evaluation.to_synapse_request(request_type=RequestType.CREATE)

        # THEN the request body should contain all required fields
        assert request_body["name"] == EVALUATION_NAME
        assert request_body["description"] == EVALUATION_DESCRIPTION
        assert request_body["contentSource"] == CONTENT_SOURCE
        assert request_body["submissionInstructionsMessage"] == SUBMISSION_INSTRUCTIONS
        assert request_body["submissionReceiptMessage"] == SUBMISSION_RECEIPT
        # AND should not contain id or etag for CREATE
        assert "id" not in request_body
        assert "etag" not in request_body

    def test_to_synapse_request_update(self) -> None:
        # GIVEN an Evaluation with all required fields for update including id and etag
        evaluation = self._build_evaluation_with_all_fields()
        evaluation.id = EVALUATION_ID
        evaluation.etag = EVALUATION_ETAG

        # WHEN I call to_synapse_request with UPDATE type
        request_body = evaluation.to_synapse_request(request_type=RequestType.UPDATE)

        # THEN the request body should contain all fields including id and etag
        assert request_body["id"] == EVALUATION_ID
        assert request_body["etag"] == EVALUATION_ETAG
        assert request_body["name"] == EVALUATION_NAME
        assert request_body["description"] == EVALUATION_DESCRIPTION

    def test_to_synapse_request_create_missing_field_raises_value_error(self) -> None:
        # GIVEN an Evaluation missing a required field (description)
        evaluation = Evaluation(
            name=EVALUATION_NAME,
            content_source=CONTENT_SOURCE,
            submission_instructions_message=SUBMISSION_INSTRUCTIONS,
            submission_receipt_message=SUBMISSION_RECEIPT,
        )

        # WHEN I call to_synapse_request with CREATE type
        # THEN a ValueError should be raised
        with pytest.raises(ValueError, match="description"):
            evaluation.to_synapse_request(request_type=RequestType.CREATE)

    def test_to_synapse_request_update_missing_id_raises_value_error(self) -> None:
        # GIVEN an Evaluation with all CREATE fields but missing id for UPDATE
        evaluation = self._build_evaluation_with_all_fields()
        evaluation.etag = EVALUATION_ETAG

        # WHEN I call to_synapse_request with UPDATE type
        # THEN a ValueError should be raised for missing id
        with pytest.raises(ValueError, match="id"):
            evaluation.to_synapse_request(request_type=RequestType.UPDATE)

    # ------------------------------------------------------------------ #
    #  has_changed
    # ------------------------------------------------------------------ #

    def test_has_changed_no_persistent_instance(self) -> None:
        # GIVEN a brand new Evaluation without any persistent instance
        evaluation = Evaluation(name=EVALUATION_NAME)

        # WHEN I check has_changed
        # THEN it should return True because there is no last persistent instance
        assert evaluation.has_changed is True

    def test_has_changed_after_set_persistent_instance(self) -> None:
        # GIVEN an Evaluation that has been persisted
        evaluation = Evaluation(name=EVALUATION_NAME)
        evaluation._set_last_persistent_instance()

        # WHEN I check has_changed without modifying anything
        # THEN it should return False
        assert evaluation.has_changed is False

    def test_has_changed_after_modification(self) -> None:
        # GIVEN an Evaluation that has been persisted
        evaluation = Evaluation(name=EVALUATION_NAME)
        evaluation._set_last_persistent_instance()

        # WHEN I modify a field
        evaluation.name = "Modified Name"

        # THEN has_changed should return True
        assert evaluation.has_changed is True

    # ------------------------------------------------------------------ #
    #  store_async - create (no ID)
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_store_async_create_new_evaluation(self) -> None:
        # GIVEN an Evaluation with required fields but no ID (new evaluation)
        evaluation = self._build_evaluation_with_all_fields()

        # WHEN I call store_async with a mocked API response
        with patch(
            "synapseclient.api.evaluation_services.create_or_update_evaluation",
            new_callable=AsyncMock,
            return_value=self.get_example_evaluation_response(),
        ) as mock_create:
            result = await evaluation.store_async(synapse_client=self.syn)

            # THEN the API should be called with the CREATE request body
            mock_create.assert_called_once()
            call_kwargs = mock_create.call_args[1]
            assert "id" not in call_kwargs["request_body"]
            assert call_kwargs["request_body"]["name"] == EVALUATION_NAME

            # AND the evaluation should be populated from the response
            assert result.id == EVALUATION_ID
            assert result.etag == EVALUATION_ETAG
            assert result.name == EVALUATION_NAME
            assert result.owner_id == OWNER_ID
            assert result.created_on == CREATED_ON

            # AND the persistent instance should be set
            assert result._last_persistent_instance is not None
            assert result.has_changed is False

    # ------------------------------------------------------------------ #
    #  store_async - update (has ID, has_changed=True)
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_store_async_update_existing_evaluation(self) -> None:
        # GIVEN an Evaluation that was previously retrieved from Synapse
        evaluation = self._build_evaluation_with_all_fields()
        evaluation.id = EVALUATION_ID
        evaluation.etag = EVALUATION_ETAG
        evaluation.owner_id = OWNER_ID
        evaluation.created_on = CREATED_ON
        evaluation._set_last_persistent_instance()

        # AND the description has been changed
        evaluation.description = "Updated description"

        updated_response = self.get_example_evaluation_response()
        updated_response["description"] = "Updated description"

        # WHEN I call store_async
        with patch(
            "synapseclient.api.evaluation_services.create_or_update_evaluation",
            new_callable=AsyncMock,
            return_value=updated_response,
        ) as mock_update:
            result = await evaluation.store_async(synapse_client=self.syn)

            # THEN the API should be called with the UPDATE request body
            mock_update.assert_called_once()
            call_kwargs = mock_update.call_args[1]
            assert call_kwargs["request_body"]["id"] == EVALUATION_ID
            assert call_kwargs["request_body"]["etag"] == EVALUATION_ETAG

            # AND the evaluation should be updated
            assert result.description == "Updated description"
            assert result.has_changed is False

    # ------------------------------------------------------------------ #
    #  store_async - skip update (has_changed=False)
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_store_async_skip_update_when_not_changed(self) -> None:
        # GIVEN an Evaluation that was previously retrieved and has NOT been modified
        evaluation = self._build_evaluation_with_all_fields()
        evaluation.id = EVALUATION_ID
        evaluation.etag = EVALUATION_ETAG
        evaluation._set_last_persistent_instance()

        # WHEN I call store_async without any changes
        with patch(
            "synapseclient.api.evaluation_services.create_or_update_evaluation",
            new_callable=AsyncMock,
        ) as mock_api:
            result = await evaluation.store_async(synapse_client=self.syn)

            # THEN the API should NOT be called
            mock_api.assert_not_called()

            # AND the same evaluation should be returned unchanged
            assert result is evaluation
            assert result.id == EVALUATION_ID

    # ------------------------------------------------------------------ #
    #  get_async - by ID
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_get_async_by_id(self) -> None:
        # GIVEN an Evaluation with an ID set
        evaluation = Evaluation(id=EVALUATION_ID)

        # WHEN I call get_async with a mocked API response
        with patch(
            "synapseclient.api.evaluation_services.get_evaluation",
            new_callable=AsyncMock,
            return_value=self.get_example_evaluation_response(),
        ) as mock_get:
            result = await evaluation.get_async(synapse_client=self.syn)

            # THEN the API should be called with the evaluation ID
            mock_get.assert_called_once_with(
                evaluation_id=EVALUATION_ID,
                name=None,
                synapse_client=self.syn,
            )

            # AND the evaluation should be populated from the response
            assert result.id == EVALUATION_ID
            assert result.name == EVALUATION_NAME
            assert result.description == EVALUATION_DESCRIPTION
            assert result.owner_id == OWNER_ID

            # AND the persistent instance should be set
            assert result._last_persistent_instance is not None
            assert result.has_changed is False

    # ------------------------------------------------------------------ #
    #  get_async - by name
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_get_async_by_name(self) -> None:
        # GIVEN an Evaluation with a name set but no ID
        evaluation = Evaluation(name=EVALUATION_NAME)

        # WHEN I call get_async with a mocked API response
        with patch(
            "synapseclient.api.evaluation_services.get_evaluation",
            new_callable=AsyncMock,
            return_value=self.get_example_evaluation_response(),
        ) as mock_get:
            result = await evaluation.get_async(synapse_client=self.syn)

            # THEN the API should be called with the name
            mock_get.assert_called_once_with(
                evaluation_id=None,
                name=EVALUATION_NAME,
                synapse_client=self.syn,
            )

            # AND the evaluation should be populated from the response
            assert result.id == EVALUATION_ID
            assert result.name == EVALUATION_NAME

    # ------------------------------------------------------------------ #
    #  get_async - missing both ID and name raises ValueError
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_get_async_missing_id_and_name_raises_value_error(self) -> None:
        # GIVEN an Evaluation with neither ID nor name
        evaluation = Evaluation()

        # WHEN I call get_async
        # THEN a ValueError should be raised
        with pytest.raises(
            ValueError, match="Either id or name must be set to get an evaluation"
        ):
            await evaluation.get_async(synapse_client=self.syn)

    # ------------------------------------------------------------------ #
    #  delete_async - with ID
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_delete_async_with_id(self) -> None:
        # GIVEN an Evaluation with an ID and a persistent instance
        evaluation = Evaluation(id=EVALUATION_ID, name=EVALUATION_NAME)
        evaluation._set_last_persistent_instance()

        # WHEN I call delete_async
        with patch(
            "synapseclient.api.evaluation_services.delete_evaluation",
            new_callable=AsyncMock,
        ) as mock_delete:
            await evaluation.delete_async(synapse_client=self.syn)

            # THEN the API should be called with the evaluation ID
            mock_delete.assert_called_once_with(
                evaluation_id=EVALUATION_ID,
                synapse_client=self.syn,
            )

            # AND the persistent instance should be cleared
            assert evaluation._last_persistent_instance is None

    # ------------------------------------------------------------------ #
    #  delete_async - missing ID raises ValueError
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_delete_async_missing_id_raises_value_error(self) -> None:
        # GIVEN an Evaluation with no ID
        evaluation = Evaluation(name=EVALUATION_NAME)

        # WHEN I call delete_async
        # THEN a ValueError should be raised
        with pytest.raises(ValueError, match="id must be set to delete an evaluation"):
            await evaluation.delete_async(synapse_client=self.syn)

    # ------------------------------------------------------------------ #
    #  get_acl_async - success
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_get_acl_async_success(self) -> None:
        # GIVEN an Evaluation with an ID
        evaluation = Evaluation(id=EVALUATION_ID)
        expected_acl = self.get_example_acl_response()

        # WHEN I call get_acl_async
        with patch(
            "synapseclient.api.evaluation_services.get_evaluation_acl",
            new_callable=AsyncMock,
            return_value=expected_acl,
        ) as mock_get_acl:
            result = await evaluation.get_acl_async(synapse_client=self.syn)

            # THEN the API should be called with the evaluation ID
            mock_get_acl.assert_called_once_with(
                evaluation_id=EVALUATION_ID,
                synapse_client=self.syn,
            )

            # AND the ACL should be returned
            assert result["id"] == EVALUATION_ID
            assert len(result["resourceAccess"]) == 1
            assert result["resourceAccess"][0]["principalId"] == int(OWNER_ID)

    # ------------------------------------------------------------------ #
    #  get_acl_async - missing ID raises ValueError
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_get_acl_async_missing_id_raises_value_error(self) -> None:
        # GIVEN an Evaluation with no ID
        evaluation = Evaluation(name=EVALUATION_NAME)

        # WHEN I call get_acl_async
        # THEN a ValueError should be raised
        with pytest.raises(ValueError, match="id must be set to get evaluation ACL"):
            await evaluation.get_acl_async(synapse_client=self.syn)

    # ------------------------------------------------------------------ #
    #  update_acl_async - with principal_id and access_type
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_update_acl_async_with_principal_id_and_access_type(self) -> None:
        # GIVEN an Evaluation with an ID
        evaluation = Evaluation(id=EVALUATION_ID)
        current_acl = self.get_example_acl_response()
        updated_acl = deepcopy(current_acl)
        updated_acl["resourceAccess"].append(
            {"principalId": int(PRINCIPAL_ID), "accessType": ["READ", "SUBMIT"]}
        )

        # WHEN I call update_acl_async with a principal_id and access_type
        with patch(
            "synapseclient.api.evaluation_services.get_evaluation_acl",
            new_callable=AsyncMock,
            return_value=current_acl,
        ) as mock_get_acl, patch(
            "synapseclient.api.evaluation_services.update_evaluation_acl",
            new_callable=AsyncMock,
            return_value=updated_acl,
        ) as mock_update_acl:
            result = await evaluation.update_acl_async(
                principal_id=PRINCIPAL_ID,
                access_type=["READ", "SUBMIT"],
                synapse_client=self.syn,
            )

            # THEN get_acl should be called first to fetch current ACL
            mock_get_acl.assert_called_once_with(
                evaluation_id=EVALUATION_ID,
                synapse_client=self.syn,
            )

            # AND update_acl should be called with the modified ACL
            mock_update_acl.assert_called_once()
            call_kwargs = mock_update_acl.call_args[1]
            acl_sent = call_kwargs["acl"]
            # The new principal should be in the resourceAccess
            principal_ids = [ra["principalId"] for ra in acl_sent["resourceAccess"]]
            assert int(PRINCIPAL_ID) in principal_ids

            # AND the updated ACL should be returned
            assert result == updated_acl

    # ------------------------------------------------------------------ #
    #  update_acl_async - with full acl dict
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_update_acl_async_with_full_acl_dict(self) -> None:
        # GIVEN an Evaluation with an ID
        evaluation = Evaluation(id=EVALUATION_ID)
        full_acl = self.get_example_acl_response()
        full_acl["resourceAccess"].append(
            {"principalId": int(PRINCIPAL_ID), "accessType": ["READ"]}
        )

        # WHEN I call update_acl_async with a complete ACL dictionary
        with patch(
            "synapseclient.api.evaluation_services.update_evaluation_acl",
            new_callable=AsyncMock,
            return_value=full_acl,
        ) as mock_update_acl:
            result = await evaluation.update_acl_async(
                acl=full_acl,
                synapse_client=self.syn,
            )

            # THEN the API should be called directly with the provided ACL
            mock_update_acl.assert_called_once_with(
                acl=full_acl,
                synapse_client=self.syn,
            )

            # AND get_acl should NOT be called (no need to fetch current ACL)
            # AND the updated ACL should be returned
            assert result == full_acl

    # ------------------------------------------------------------------ #
    #  update_acl_async - add new principal
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_update_acl_async_adds_new_principal(self) -> None:
        # GIVEN an Evaluation with an ID and an existing ACL with one principal
        evaluation = Evaluation(id=EVALUATION_ID)
        current_acl = self.get_example_acl_response()

        expected_updated_acl = deepcopy(current_acl)
        expected_updated_acl["resourceAccess"].append(
            {"principalId": int(TEAM_PRINCIPAL_ID), "accessType": ["READ", "SUBMIT"]}
        )

        # WHEN I call update_acl_async with a NEW principal_id
        with patch(
            "synapseclient.api.evaluation_services.get_evaluation_acl",
            new_callable=AsyncMock,
            return_value=current_acl,
        ), patch(
            "synapseclient.api.evaluation_services.update_evaluation_acl",
            new_callable=AsyncMock,
            return_value=expected_updated_acl,
        ) as mock_update_acl:
            result = await evaluation.update_acl_async(
                principal_id=TEAM_PRINCIPAL_ID,
                access_type=["READ", "SUBMIT"],
                synapse_client=self.syn,
            )

            # THEN the ACL passed to update should contain the new principal
            call_kwargs = mock_update_acl.call_args[1]
            acl_sent = call_kwargs["acl"]
            assert len(acl_sent["resourceAccess"]) == 2
            new_entry = [
                ra
                for ra in acl_sent["resourceAccess"]
                if ra["principalId"] == int(TEAM_PRINCIPAL_ID)
            ]
            assert len(new_entry) == 1
            assert new_entry[0]["accessType"] == ["READ", "SUBMIT"]

    # ------------------------------------------------------------------ #
    #  update_acl_async - update existing principal
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_update_acl_async_updates_existing_principal(self) -> None:
        # GIVEN an Evaluation with an existing ACL containing the OWNER_ID principal
        evaluation = Evaluation(id=EVALUATION_ID)
        current_acl = self.get_example_acl_response()

        expected_updated_acl = deepcopy(current_acl)
        expected_updated_acl["resourceAccess"][0]["accessType"] = ["READ"]

        # WHEN I call update_acl_async to update the existing principal's permissions
        with patch(
            "synapseclient.api.evaluation_services.get_evaluation_acl",
            new_callable=AsyncMock,
            return_value=current_acl,
        ), patch(
            "synapseclient.api.evaluation_services.update_evaluation_acl",
            new_callable=AsyncMock,
            return_value=expected_updated_acl,
        ) as mock_update_acl:
            result = await evaluation.update_acl_async(
                principal_id=OWNER_ID,
                access_type=["READ"],
                synapse_client=self.syn,
            )

            # THEN the ACL sent to the API should have updated permissions for the owner
            call_kwargs = mock_update_acl.call_args[1]
            acl_sent = call_kwargs["acl"]
            owner_entry = [
                ra
                for ra in acl_sent["resourceAccess"]
                if ra["principalId"] == int(OWNER_ID)
            ]
            assert len(owner_entry) == 1
            assert owner_entry[0]["accessType"] == ["READ"]

            # AND should still have only one resource access entry
            assert len(acl_sent["resourceAccess"]) == 1

    # ------------------------------------------------------------------ #
    #  update_acl_async - remove principal (empty access_type)
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_update_acl_async_removes_principal_with_empty_access_type(
        self,
    ) -> None:
        # GIVEN an Evaluation with an existing ACL containing the OWNER_ID principal
        evaluation = Evaluation(id=EVALUATION_ID)
        current_acl = self.get_example_acl_response()

        expected_updated_acl = deepcopy(current_acl)
        expected_updated_acl["resourceAccess"] = []

        # WHEN I call update_acl_async with an empty access_type list
        with patch(
            "synapseclient.api.evaluation_services.get_evaluation_acl",
            new_callable=AsyncMock,
            return_value=current_acl,
        ), patch(
            "synapseclient.api.evaluation_services.update_evaluation_acl",
            new_callable=AsyncMock,
            return_value=expected_updated_acl,
        ) as mock_update_acl:
            result = await evaluation.update_acl_async(
                principal_id=OWNER_ID,
                access_type=[],
                synapse_client=self.syn,
            )

            # THEN the ACL sent to the API should have the principal removed
            call_kwargs = mock_update_acl.call_args[1]
            acl_sent = call_kwargs["acl"]
            owner_entries = [
                ra
                for ra in acl_sent["resourceAccess"]
                if ra["principalId"] == int(OWNER_ID)
            ]
            assert len(owner_entries) == 0

    # ------------------------------------------------------------------ #
    #  update_acl_async - missing ID raises ValueError
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_update_acl_async_missing_id_raises_value_error(self) -> None:
        # GIVEN an Evaluation with no ID
        evaluation = Evaluation(name=EVALUATION_NAME)

        # WHEN I call update_acl_async
        # THEN a ValueError should be raised
        with pytest.raises(ValueError, match="id must be set to update evaluation ACL"):
            await evaluation.update_acl_async(
                principal_id=PRINCIPAL_ID,
                access_type=["READ"],
                synapse_client=self.syn,
            )

    # ------------------------------------------------------------------ #
    #  update_acl_async - missing both principal and acl raises ValueError
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_update_acl_async_missing_principal_and_acl_raises_value_error(
        self,
    ) -> None:
        # GIVEN an Evaluation with an ID
        evaluation = Evaluation(id=EVALUATION_ID)

        # WHEN I call update_acl_async without principal_id, access_type, or acl
        # THEN a ValueError should be raised
        with pytest.raises(
            ValueError,
            match="Either \\(principal_id and access_type\\) or acl must be provided",
        ):
            await evaluation.update_acl_async(synapse_client=self.syn)

    # ------------------------------------------------------------------ #
    #  update_acl_async - access_type uppercased
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_update_acl_async_uppercases_access_type(self) -> None:
        # GIVEN an Evaluation with an ID
        evaluation = Evaluation(id=EVALUATION_ID)
        current_acl = self.get_example_acl_response()

        # WHEN I call update_acl_async with lowercase access_type values
        with patch(
            "synapseclient.api.evaluation_services.get_evaluation_acl",
            new_callable=AsyncMock,
            return_value=current_acl,
        ), patch(
            "synapseclient.api.evaluation_services.update_evaluation_acl",
            new_callable=AsyncMock,
            return_value=current_acl,
        ) as mock_update_acl:
            await evaluation.update_acl_async(
                principal_id=TEAM_PRINCIPAL_ID,
                access_type=["read", "submit"],
                synapse_client=self.syn,
            )

            # THEN the access_type in the ACL should be uppercased
            call_kwargs = mock_update_acl.call_args[1]
            acl_sent = call_kwargs["acl"]
            new_entry = [
                ra
                for ra in acl_sent["resourceAccess"]
                if ra["principalId"] == int(TEAM_PRINCIPAL_ID)
            ]
            assert len(new_entry) == 1
            assert new_entry[0]["accessType"] == ["READ", "SUBMIT"]

    # ------------------------------------------------------------------ #
    #  get_permissions_async - success
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_get_permissions_async_success(self) -> None:
        # GIVEN an Evaluation with an ID
        evaluation = Evaluation(id=EVALUATION_ID)
        expected_permissions = self.get_example_permissions_response()

        # WHEN I call get_permissions_async
        with patch(
            "synapseclient.api.evaluation_services.get_evaluation_permissions",
            new_callable=AsyncMock,
            return_value=expected_permissions,
        ) as mock_get_perms:
            result = await evaluation.get_permissions_async(synapse_client=self.syn)

            # THEN the API should be called with the evaluation ID
            mock_get_perms.assert_called_once_with(
                evaluation_id=EVALUATION_ID,
                synapse_client=self.syn,
            )

            # AND the permissions should be returned
            assert result["canView"] is True
            assert result["canEdit"] is True
            assert result["canDelete"] is True
            assert result["canChangePermissions"] is True

    # ------------------------------------------------------------------ #
    #  get_permissions_async - missing ID raises ValueError
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_get_permissions_async_missing_id_raises_value_error(self) -> None:
        # GIVEN an Evaluation with no ID
        evaluation = Evaluation(name=EVALUATION_NAME)

        # WHEN I call get_permissions_async
        # THEN a ValueError should be raised
        with pytest.raises(
            ValueError, match="id must be set to get evaluation permissions"
        ):
            await evaluation.get_permissions_async(synapse_client=self.syn)

    # ------------------------------------------------------------------ #
    #  get_all_evaluations_async - static method with pagination params
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_get_all_evaluations_async_default(self) -> None:
        # GIVEN a mocked API that returns a paginated result with two evaluations
        eval_response_1 = self.get_example_evaluation_response()
        eval_response_2 = self.get_example_evaluation_response()
        eval_response_2["id"] = "9614113"
        eval_response_2["name"] = "Second Evaluation"

        api_response = {
            "results": [eval_response_1, eval_response_2],
            "totalNumberOfResults": 2,
        }

        # WHEN I call get_all_evaluations_async with no parameters
        with patch(
            "synapseclient.api.evaluation_services.get_all_evaluations",
            new_callable=AsyncMock,
            return_value=api_response,
        ) as mock_get_all:
            results = await Evaluation.get_all_evaluations_async(
                synapse_client=self.syn
            )

            # THEN the API should be called with default parameters
            mock_get_all.assert_called_once_with(
                access_type=None,
                active_only=None,
                evaluation_ids=None,
                offset=None,
                limit=None,
                synapse_client=self.syn,
            )

            # AND the results should be a list of Evaluation objects
            assert len(results) == 2
            assert isinstance(results[0], Evaluation)
            assert results[0].id == EVALUATION_ID
            assert results[0].name == EVALUATION_NAME
            assert isinstance(results[1], Evaluation)
            assert results[1].id == "9614113"
            assert results[1].name == "Second Evaluation"

    @pytest.mark.asyncio
    async def test_get_all_evaluations_async_with_pagination_params(self) -> None:
        # GIVEN a mocked API that returns a single evaluation
        api_response = {
            "results": [self.get_example_evaluation_response()],
            "totalNumberOfResults": 1,
        }

        # WHEN I call get_all_evaluations_async with pagination and filter params
        with patch(
            "synapseclient.api.evaluation_services.get_all_evaluations",
            new_callable=AsyncMock,
            return_value=api_response,
        ) as mock_get_all:
            results = await Evaluation.get_all_evaluations_async(
                access_type="SUBMIT",
                active_only=True,
                evaluation_ids=[EVALUATION_ID],
                offset=5,
                limit=20,
                synapse_client=self.syn,
            )

            # THEN the API should be called with the specified parameters
            mock_get_all.assert_called_once_with(
                access_type="SUBMIT",
                active_only=True,
                evaluation_ids=[EVALUATION_ID],
                offset=5,
                limit=20,
                synapse_client=self.syn,
            )

            # AND the results should contain one Evaluation
            assert len(results) == 1
            assert results[0].id == EVALUATION_ID

    @pytest.mark.asyncio
    async def test_get_all_evaluations_async_empty_results(self) -> None:
        # GIVEN a mocked API that returns an empty list
        api_response = {
            "results": [],
            "totalNumberOfResults": 0,
        }

        # WHEN I call get_all_evaluations_async
        with patch(
            "synapseclient.api.evaluation_services.get_all_evaluations",
            new_callable=AsyncMock,
            return_value=api_response,
        ):
            results = await Evaluation.get_all_evaluations_async(
                synapse_client=self.syn
            )

            # THEN the results should be an empty list
            assert results == []

    # ------------------------------------------------------------------ #
    #  get_available_evaluations_async - static method
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_get_available_evaluations_async_default(self) -> None:
        # GIVEN a mocked API that returns a list of available evaluations
        api_response = {
            "results": [self.get_example_evaluation_response()],
            "totalNumberOfResults": 1,
        }

        # WHEN I call get_available_evaluations_async
        with patch(
            "synapseclient.api.evaluation_services.get_available_evaluations",
            new_callable=AsyncMock,
            return_value=api_response,
        ) as mock_get_available:
            results = await Evaluation.get_available_evaluations_async(
                synapse_client=self.syn
            )

            # THEN the API should be called with default parameters
            mock_get_available.assert_called_once_with(
                active_only=None,
                evaluation_ids=None,
                offset=None,
                limit=None,
                synapse_client=self.syn,
            )

            # AND the results should contain Evaluation objects
            assert len(results) == 1
            assert isinstance(results[0], Evaluation)
            assert results[0].id == EVALUATION_ID

    @pytest.mark.asyncio
    async def test_get_available_evaluations_async_with_params(self) -> None:
        # GIVEN a mocked API response
        api_response = {
            "results": [self.get_example_evaluation_response()],
            "totalNumberOfResults": 1,
        }

        # WHEN I call get_available_evaluations_async with filtering parameters
        with patch(
            "synapseclient.api.evaluation_services.get_available_evaluations",
            new_callable=AsyncMock,
            return_value=api_response,
        ) as mock_get_available:
            results = await Evaluation.get_available_evaluations_async(
                active_only=True,
                evaluation_ids=[EVALUATION_ID],
                offset=0,
                limit=5,
                synapse_client=self.syn,
            )

            # THEN the API should be called with the specified parameters
            mock_get_available.assert_called_once_with(
                active_only=True,
                evaluation_ids=[EVALUATION_ID],
                offset=0,
                limit=5,
                synapse_client=self.syn,
            )

            assert len(results) == 1

    @pytest.mark.asyncio
    async def test_get_available_evaluations_async_empty_results(self) -> None:
        # GIVEN a mocked API that returns an empty list
        api_response = {
            "results": [],
            "totalNumberOfResults": 0,
        }

        # WHEN I call get_available_evaluations_async
        with patch(
            "synapseclient.api.evaluation_services.get_available_evaluations",
            new_callable=AsyncMock,
            return_value=api_response,
        ):
            results = await Evaluation.get_available_evaluations_async(
                synapse_client=self.syn
            )

            # THEN the results should be an empty list
            assert results == []

    # ------------------------------------------------------------------ #
    #  get_evaluations_by_project_async - static method with project_id
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_get_evaluations_by_project_async_default(self) -> None:
        # GIVEN a mocked API that returns evaluations for a project
        api_response = {
            "results": [self.get_example_evaluation_response()],
            "totalNumberOfResults": 1,
        }

        # WHEN I call get_evaluations_by_project_async
        with patch(
            "synapseclient.api.evaluation_services.get_evaluations_by_project",
            new_callable=AsyncMock,
            return_value=api_response,
        ) as mock_get_by_project:
            results = await Evaluation.get_evaluations_by_project_async(
                project_id=PROJECT_ID,
                synapse_client=self.syn,
            )

            # THEN the API should be called with the project_id and defaults
            mock_get_by_project.assert_called_once_with(
                project_id=PROJECT_ID,
                access_type=None,
                active_only=None,
                evaluation_ids=None,
                offset=None,
                limit=None,
                synapse_client=self.syn,
            )

            # AND the results should contain Evaluation objects
            assert len(results) == 1
            assert isinstance(results[0], Evaluation)
            assert results[0].id == EVALUATION_ID
            assert results[0].content_source == CONTENT_SOURCE

    @pytest.mark.asyncio
    async def test_get_evaluations_by_project_async_with_params(self) -> None:
        # GIVEN a mocked API response
        api_response = {
            "results": [self.get_example_evaluation_response()],
            "totalNumberOfResults": 1,
        }

        # WHEN I call get_evaluations_by_project_async with all optional params
        with patch(
            "synapseclient.api.evaluation_services.get_evaluations_by_project",
            new_callable=AsyncMock,
            return_value=api_response,
        ) as mock_get_by_project:
            results = await Evaluation.get_evaluations_by_project_async(
                project_id=PROJECT_ID,
                access_type="SUBMIT",
                active_only=True,
                evaluation_ids=[EVALUATION_ID],
                offset=10,
                limit=50,
                synapse_client=self.syn,
            )

            # THEN the API should be called with all specified parameters
            mock_get_by_project.assert_called_once_with(
                project_id=PROJECT_ID,
                access_type="SUBMIT",
                active_only=True,
                evaluation_ids=[EVALUATION_ID],
                offset=10,
                limit=50,
                synapse_client=self.syn,
            )

            assert len(results) == 1

    @pytest.mark.asyncio
    async def test_get_evaluations_by_project_async_empty_results(self) -> None:
        # GIVEN a mocked API that returns an empty list for the project
        api_response = {
            "results": [],
            "totalNumberOfResults": 0,
        }

        # WHEN I call get_evaluations_by_project_async
        with patch(
            "synapseclient.api.evaluation_services.get_evaluations_by_project",
            new_callable=AsyncMock,
            return_value=api_response,
        ):
            results = await Evaluation.get_evaluations_by_project_async(
                project_id=PROJECT_ID,
                synapse_client=self.syn,
            )

            # THEN the results should be an empty list
            assert results == []

    @pytest.mark.asyncio
    async def test_get_evaluations_by_project_async_multiple_results(self) -> None:
        # GIVEN a mocked API that returns multiple evaluations for a project
        eval_response_1 = self.get_example_evaluation_response()
        eval_response_2 = self.get_example_evaluation_response()
        eval_response_2["id"] = "9614113"
        eval_response_2["name"] = "Second Project Evaluation"

        api_response = {
            "results": [eval_response_1, eval_response_2],
            "totalNumberOfResults": 2,
        }

        # WHEN I call get_evaluations_by_project_async
        with patch(
            "synapseclient.api.evaluation_services.get_evaluations_by_project",
            new_callable=AsyncMock,
            return_value=api_response,
        ):
            results = await Evaluation.get_evaluations_by_project_async(
                project_id=PROJECT_ID,
                synapse_client=self.syn,
            )

            # THEN the results should contain two Evaluation objects
            assert len(results) == 2
            assert results[0].id == EVALUATION_ID
            assert results[1].id == "9614113"
            assert results[1].name == "Second Project Evaluation"

    # ------------------------------------------------------------------ #
    #  _update_acl_permissions (helper method)
    # ------------------------------------------------------------------ #

    def test_update_acl_permissions_add_new_principal(self) -> None:
        # GIVEN an Evaluation and an ACL with one existing principal
        evaluation = Evaluation(id=EVALUATION_ID)
        acl = self.get_example_acl_response()

        # WHEN I call _update_acl_permissions for a new principal
        result = evaluation._update_acl_permissions(
            principal_id=TEAM_PRINCIPAL_ID,
            access_type=["READ", "SUBMIT"],
            acl=acl,
            synapse_client=self.syn,
        )

        # THEN the new principal should be added to resourceAccess
        assert len(result["resourceAccess"]) == 2
        new_entry = [
            ra
            for ra in result["resourceAccess"]
            if ra["principalId"] == int(TEAM_PRINCIPAL_ID)
        ]
        assert len(new_entry) == 1
        assert new_entry[0]["accessType"] == ["READ", "SUBMIT"]

    def test_update_acl_permissions_update_existing_principal(self) -> None:
        # GIVEN an Evaluation and an ACL with the OWNER_ID principal
        evaluation = Evaluation(id=EVALUATION_ID)
        acl = self.get_example_acl_response()

        # WHEN I call _update_acl_permissions to update the existing principal
        result = evaluation._update_acl_permissions(
            principal_id=OWNER_ID,
            access_type=["READ"],
            acl=acl,
            synapse_client=self.syn,
        )

        # THEN the existing principal's access type should be updated
        assert len(result["resourceAccess"]) == 1
        assert result["resourceAccess"][0]["accessType"] == ["READ"]

    def test_update_acl_permissions_remove_principal(self) -> None:
        # GIVEN an Evaluation and an ACL with the OWNER_ID principal
        evaluation = Evaluation(id=EVALUATION_ID)
        acl = self.get_example_acl_response()

        # WHEN I call _update_acl_permissions with empty access_type
        result = evaluation._update_acl_permissions(
            principal_id=OWNER_ID,
            access_type=[],
            acl=acl,
            synapse_client=self.syn,
        )

        # THEN the principal should be removed from resourceAccess
        assert len(result["resourceAccess"]) == 0

    def test_update_acl_permissions_remove_nonexistent_principal(self) -> None:
        # GIVEN an Evaluation and an ACL with the OWNER_ID principal
        evaluation = Evaluation(id=EVALUATION_ID)
        acl = self.get_example_acl_response()

        # WHEN I call _update_acl_permissions to remove a principal that is not in the ACL
        result = evaluation._update_acl_permissions(
            principal_id=TEAM_PRINCIPAL_ID,
            access_type=[],
            acl=acl,
            synapse_client=self.syn,
        )

        # THEN the ACL should remain unchanged (the non-existent principal is simply not there)
        assert len(result["resourceAccess"]) == 1
        assert result["resourceAccess"][0]["principalId"] == int(OWNER_ID)

    # ------------------------------------------------------------------ #
    #  store_async - create populates all response fields
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_store_async_create_populates_all_fields(self) -> None:
        # GIVEN an Evaluation with required fields for creation
        evaluation = self._build_evaluation_with_all_fields()

        # WHEN I call store_async and the API returns a full response
        with patch(
            "synapseclient.api.evaluation_services.create_or_update_evaluation",
            new_callable=AsyncMock,
            return_value=self.get_example_evaluation_response(),
        ):
            result = await evaluation.store_async(synapse_client=self.syn)

            # THEN all fields should be populated from the API response
            assert result.id == EVALUATION_ID
            assert result.etag == EVALUATION_ETAG
            assert result.name == EVALUATION_NAME
            assert result.description == EVALUATION_DESCRIPTION
            assert result.owner_id == OWNER_ID
            assert result.created_on == CREATED_ON
            assert result.content_source == CONTENT_SOURCE
            assert result.submission_instructions_message == SUBMISSION_INSTRUCTIONS
            assert result.submission_receipt_message == SUBMISSION_RECEIPT

    # ------------------------------------------------------------------ #
    #  store_async - returns self
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_store_async_returns_self(self) -> None:
        # GIVEN an Evaluation object
        evaluation = self._build_evaluation_with_all_fields()

        # WHEN I call store_async
        with patch(
            "synapseclient.api.evaluation_services.create_or_update_evaluation",
            new_callable=AsyncMock,
            return_value=self.get_example_evaluation_response(),
        ):
            result = await evaluation.store_async(synapse_client=self.syn)

            # THEN the result should be the same object (self)
            assert result is evaluation

    # ------------------------------------------------------------------ #
    #  get_async - returns self
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_get_async_returns_self(self) -> None:
        # GIVEN an Evaluation with an ID
        evaluation = Evaluation(id=EVALUATION_ID)

        # WHEN I call get_async
        with patch(
            "synapseclient.api.evaluation_services.get_evaluation",
            new_callable=AsyncMock,
            return_value=self.get_example_evaluation_response(),
        ):
            result = await evaluation.get_async(synapse_client=self.syn)

            # THEN the result should be the same object (self)
            assert result is evaluation

    # ------------------------------------------------------------------ #
    #  get_async - sets persistent instance for change tracking
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_get_async_sets_persistent_instance(self) -> None:
        # GIVEN an Evaluation with an ID but no persistent instance
        evaluation = Evaluation(id=EVALUATION_ID)
        assert evaluation._last_persistent_instance is None

        # WHEN I call get_async
        with patch(
            "synapseclient.api.evaluation_services.get_evaluation",
            new_callable=AsyncMock,
            return_value=self.get_example_evaluation_response(),
        ):
            result = await evaluation.get_async(synapse_client=self.syn)

            # THEN the persistent instance should be set
            assert result._last_persistent_instance is not None
            assert result.has_changed is False

    # ------------------------------------------------------------------ #
    #  delete_async - returns None
    # ------------------------------------------------------------------ #

    @pytest.mark.asyncio
    async def test_delete_async_returns_none(self) -> None:
        # GIVEN an Evaluation with an ID
        evaluation = Evaluation(id=EVALUATION_ID)

        # WHEN I call delete_async
        with patch(
            "synapseclient.api.evaluation_services.delete_evaluation",
            new_callable=AsyncMock,
        ):
            result = await evaluation.delete_async(synapse_client=self.syn)

            # THEN the result should be None
            assert result is None
