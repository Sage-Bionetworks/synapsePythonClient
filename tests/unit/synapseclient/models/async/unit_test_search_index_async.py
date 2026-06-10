"""Unit tests for the SearchIndex entity model."""

from unittest.mock import patch

import pytest

from synapseclient import Synapse
from synapseclient.core.constants import concrete_types
from synapseclient.models import SearchIndex
from synapseclient.models.mixins.table_components import (
    DeleteMixin,
    GetMixin,
    TableStoreMixin,
)


class TestSearchIndexBasics:
    """Round-trip serialization and field-level behavior."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_default_fields_are_none_or_empty(self):
        index = SearchIndex()
        assert index.id is None
        assert index.name is None
        assert index.parent_id is None
        assert index.defining_sql is None
        assert index.search_configuration_id is None
        assert index.annotations == {}
        assert index.activity is None

    def test_fill_from_dict_maps_camelcase_to_snakecase(self):
        index = SearchIndex().fill_from_dict(
            {
                "id": "syn1",
                "name": "Idx",
                "description": "d",
                "parentId": "syn2",
                "etag": "e",
                "createdOn": "2024-01-01",
                "modifiedOn": "2024-01-02",
                "createdBy": "u1",
                "modifiedBy": "u2",
                "versionNumber": 3,
                "versionLabel": "v3",
                "versionComment": "c",
                "isLatestVersion": True,
                "definingSQL": "SELECT * FROM syn99",
                "searchConfigurationId": "42",
            }
        )
        assert index.id == "syn1"
        assert index.name == "Idx"
        assert index.description == "d"
        assert index.parent_id == "syn2"
        assert index.etag == "e"
        assert index.created_on == "2024-01-01"
        assert index.modified_on == "2024-01-02"
        assert index.created_by == "u1"
        assert index.modified_by == "u2"
        assert index.version_number == 3
        assert index.version_label == "v3"
        assert index.version_comment == "c"
        assert index.is_latest_version is True
        assert index.defining_sql == "SELECT * FROM syn99"
        assert index.search_configuration_id == "42"

    def test_fill_from_dict_skips_annotations_when_flag_false(self):
        index = SearchIndex(annotations={"x": ["1"]})
        index.fill_from_dict(
            {"id": "syn1", "annotations": {"y": ["2"]}}, set_annotations=False
        )
        assert index.annotations == {"x": ["1"]}

    def test_to_synapse_request_emits_concrete_type_and_drops_none(self):
        index = SearchIndex(
            name="Idx",
            parent_id="syn1",
            defining_sql="SELECT * FROM syn2",
        )
        body = index.to_synapse_request()
        assert body == {
            "entity": {
                "name": "Idx",
                "parentId": "syn1",
                "concreteType": concrete_types.SEARCH_INDEX_ENTITY,
                "definingSQL": "SELECT * FROM syn2",
            }
        }

    def test_to_synapse_request_includes_search_configuration_id(self):
        index = SearchIndex(
            name="Idx",
            parent_id="syn1",
            defining_sql="SELECT * FROM syn2",
            search_configuration_id="cfg42",
        )
        body = index.to_synapse_request()["entity"]
        assert body["searchConfigurationId"] == "cfg42"

    def test_has_changed_uses_last_persistent_instance(self):
        index = SearchIndex(name="Idx", parent_id="syn1", defining_sql="SELECT 1")
        assert index.has_changed is True
        index._set_last_persistent_instance()
        assert index.has_changed is False
        index.name = "renamed"
        assert index.has_changed is True

    def test_set_last_persistent_instance_deepcopies_annotations(self):
        index = SearchIndex(
            name="Idx",
            parent_id="syn1",
            defining_sql="SELECT 1",
            annotations={"k": ["v"]},
        )
        index._set_last_persistent_instance()
        index.annotations["k"].append("v2")
        assert index._last_persistent_instance.annotations == {"k": ["v"]}


class TestSearchIndexStoreAsync:
    """`store_async` validation + super-delegation behavior."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_missing_defining_sql_raises_valueerror(self):
        index = SearchIndex(name="Idx", parent_id="syn1")

        with patch.object(TableStoreMixin, "store_async") as mock_super_store:
            with pytest.raises(
                ValueError,
                match="The defining_sql attribute must be set for a SearchIndex.",
            ):
                await index.store_async(synapse_client=self.syn)
            mock_super_store.assert_not_called()

    async def test_with_defining_sql_calls_super_store(self):
        index = SearchIndex(
            name="Idx", parent_id="syn1", defining_sql="SELECT * FROM syn2"
        )

        with patch.object(TableStoreMixin, "store_async") as mock_super_store:
            mock_super_store.return_value = index
            result = await index.store_async(synapse_client=self.syn)

            mock_super_store.assert_called_once_with(
                dry_run=False, job_timeout=600, synapse_client=self.syn
            )
            assert result is index


class TestSearchIndexGetDelete:
    """`get_async` and `delete_async` thin pass-throughs."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_get_async_delegates_to_get_mixin(self):
        index = SearchIndex(id="syn1")

        with patch.object(GetMixin, "get_async") as mock_super_get:
            mock_super_get.return_value = index
            result = await index.get_async(
                include_columns=False,
                include_activity=True,
                synapse_client=self.syn,
            )
            mock_super_get.assert_called_once_with(
                include_columns=False,
                include_activity=True,
                synapse_client=self.syn,
            )
            assert result is index

    async def test_delete_async_delegates_to_delete_mixin(self):
        index = SearchIndex(id="syn1")

        with patch.object(DeleteMixin, "delete_async") as mock_super_delete:
            await index.delete_async(synapse_client=self.syn)
            mock_super_delete.assert_called_once_with(synapse_client=self.syn)
