"""Unit tests for DataModelRelationships"""

import pytest

from synapseclient.extensions.curator.schema_generation import DataModelRelationships


class TestDataModelRelationships:
    """Tests for DataModelRelationships class"""

    def test_get_relationship_value(self, dmr: DataModelRelationships) -> None:
        """Test for DataModelRelationships.get_relationship_value"""
        assert (
            dmr.get_relationship_value("displayName", "jsonld_key") == "sms:displayName"
        )

    def test_get_relationship_value_return_none(
        self, dmr: DataModelRelationships
    ) -> None:
        """
        Test for DataModelRelationships.get_relationship_value when field is missing and
          none_if_missing is True
        """
        assert (
            dmr.get_relationship_value("displayName", "edge_dir", none_if_missing=True)
            is None
        )

    def test_get_relationship_value_exceptions(
        self, dmr: DataModelRelationships
    ) -> None:
        """
        Test for DataModelRelationships.get_relationship_value when field is missing and
          none_if_missing is False
        """
        with pytest.raises(
            ValueError,
            match="Value: 'edge_dir' not in relationship dictionary",
        ):
            dmr.get_relationship_value("displayName", "edge_dir")

    def test_get_allowed_values(self, dmr: DataModelRelationships) -> None:
        """Tests for DataModelRelationships.get_allowed_values"""
        result = dmr.get_allowed_values("columnType")
        assert sorted(result) == sorted(
            [
                "string",
                "number",
                "integer",
                "boolean",
                "string_list",
                "integer_list",
                "boolean_list",
            ]
        )
