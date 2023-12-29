"""
Unit test for synapseclient.client.get_permissions
"""
from unittest.mock import patch

import pytest

from synapseclient.models import Permissions
from synapseclient.entity import Entity
from synapseclient.evaluation import Evaluation
from typing import Dict

return_value = {
    "canEdit": True,
    "canView": True,
    "canMove": True,
    "canAddChild": True,
    "canCertifiedUserEdit": False,
    "canCertifiedUserAddChild": False,
    "isCertifiedUser": True,
    "canChangePermissions": True,
    "canChangeSettings": True,
    "canDelete": True,
    "canDownload": True,
    "canUpload": True,
    "canEnableInheritance": True,
    "ownerPrincipalId": 1111,
    "canPublicRead": True,
    "canModerate": True,
    "isCertificationRequired": True,
    "isEntityOpenData": False,
}


class TestGetPermissionsForCaller:
    @pytest.fixture(autouse=True, scope="function")
    def setup_method(self, syn) -> None:
        self.syn = syn
        self.syn.restGET = patch.object(
            self.syn, "restGET", return_value=return_value
        ).start()

    def teardown_method(self) -> None:
        self.syn.restGET.stop()

    def assert_entity_permission(self, d: Dict[str, bool], e: Permissions):
        """check if the values match between API output and function output"""
        assert d["canView"] == e.can_view
        assert d["canEdit"] == e.can_edit
        assert d["canMove"] == e.can_move
        assert d["canAddChild"] == e.can_add_child
        assert d["canCertifiedUserEdit"] == e.can_certified_user_edit
        assert d["canCertifiedUserAddChild"] == e.can_certified_user_add_child
        assert d["isCertifiedUser"] == e.is_certified_user
        assert d["canChangePermissions"] == e.can_change_permissions
        assert d["canChangeSettings"] == e.can_change_settings
        assert d["canDelete"] == e.can_delete
        assert d["canDownload"] == e.can_download
        assert d["canUpload"] == e.can_upload
        assert d["canEnableInheritance"] == e.can_enable_inheritance
        assert d["ownerPrincipalId"] == e.owner_principal_id
        assert d["canPublicRead"] == e.can_public_read
        assert d["canModerate"] == e.can_moderate
        assert d["isCertificationRequired"] == e.is_certification_required
        assert d["isEntityOpenData"] == e.is_entity_open_data

    def test_get_permissions_with_input_as_str(self) -> None:
        """test that entity permission object is created correctly from a dictionary"""
        entity_id = "syn123"
        result = self.syn.get_permissions(entity_id)
        self.syn.restGET.assert_called_once_with(f"/entity/{entity_id}/permissions")
        assert isinstance(result, Permissions)
        self.assert_entity_permission(return_value, result)

    def test_get_permissions_with_input_as_Entity(self) -> None:
        """test that entity permission object is created correctly from a dictionary"""
        entity = Entity(parentId="parent", id="fake")
        result = self.syn.get_permissions(entity)
        self.syn.restGET.assert_called_once_with("/entity/fake/permissions")
        assert isinstance(result, Permissions)
        self.assert_entity_permission(return_value, result)

    def test_get_permissions_with_input_as_Mapping(self) -> None:
        """test that entity permission object is created correctly from a dictionary"""
        entity = {"parentId": "parent", "id": "fake"}
        result = self.syn.get_permissions(entity)
        self.syn.restGET.assert_called_once_with("/entity/fake/permissions")
        assert isinstance(result, Permissions)
        self.assert_entity_permission(return_value, result)

    def test_get_permissions_with_input_as_Evaluation(self) -> None:
        """test that entity permission object is created correctly from a dictionary"""
        entity = Evaluation(contentSource="syn1234", id="fake")
        result = self.syn.get_permissions(entity)
        self.syn.restGET.assert_called_once_with("/entity/fake/permissions")
        assert isinstance(result, Permissions)
        self.assert_entity_permission(return_value, result)
