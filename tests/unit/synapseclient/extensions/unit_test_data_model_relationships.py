"""Unit tests for DataModelRelationships"""

import pytest

from synapseclient.extensions.curator.schema_generation import DataModelRelationships


class TestDataModelRelationships2:
    """Tests for DataModelRelationships class"""

    def test_define_data_model_relationships(self, dmr: DataModelRelationships):
        """Tests relationships_dictionary created has correct keys"""
        required_keys = [
            "jsonld_key",
            "csv_header",
            "type",
            "edge_rel",
            "required_header",
        ]
        required_edge_keys = ["edge_key", "edge_dir"]
        required_node_keys = ["node_label", "node_attr_dict"]

        relationships = dmr.relationships_dictionary

        for relationship in relationships.values():
            for key in required_keys:
                assert key in relationship.keys()
            if relationship["edge_rel"]:
                for key in required_edge_keys:
                    assert key in relationship.keys()
            else:
                for key in required_node_keys:
                    assert key in relationship.keys()

    def test_define_required_csv_headers(self, dmr: DataModelRelationships):
        """Tests method returns correct values"""
        assert dmr.define_required_csv_headers() == [
            "Attribute",
            "Description",
            "Valid Values",
            "DependsOn",
            "Required",
            "Parent",
            "Validation Rules",
        ]

    @pytest.mark.parametrize("edge", [True, False], ids=["True", "False"])
    def test_retrieve_rel_headers_dict(self, dmr: DataModelRelationships, edge: bool):
        """Tests method returns correct values"""
        if edge:
            assert dmr.retrieve_rel_headers_dict(edge=edge) == {
                "rangeIncludes": "Valid Values",
                "requiresDependency": "DependsOn",
                "requiresComponent": "DependsOn Component",
                "subClassOf": "Parent",
                "domainIncludes": "Properties",
            }
        else:
            assert dmr.retrieve_rel_headers_dict(edge=edge) == {
                "columnType": "ColumnType",
                "displayName": "Attribute",
                "format": "Format",
                "label": None,
                "comment": "Description",
                "required": "Required",
                "validationRules": "Validation Rules",
                "isPartOf": None,
                "id": "Source",
                "maximum": "Maximum",
                "minimum": "Minimum",
                "pattern": "Pattern",
            }

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
