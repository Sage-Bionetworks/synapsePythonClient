import tempfile
from time import sleep
from typing import Any, Union

import numpy as np
import pandas as pd
import pytest

from synapseclient.extensions.curator.schema_generation import (
    DataModelCSVParser,
    DataModelJSONLDParser,
    DataModelParser,
    load_df,
    load_json,
)


class TestDataModelParser:
    def test_get_model_type_with_csv(
        self, dmp: DataModelParser, dmp_jsonld: DataModelParser
    ):
        assert dmp.model_type == "CSV"
        assert dmp_jsonld.model_type == "JSONLD"

    @pytest.mark.parametrize("data_model", ["dmp", "dmp_jsonld"])
    def test_parse_model(self, data_model: str, request):
        """Test that the correct parser is called and that a dictionary is returned in the expected structure."""
        data_model_parser = request.getfixturevalue(data_model)
        attr_rel_dictionary = data_model_parser.parse_model()
        attribute_key = list(attr_rel_dictionary.keys())[0]

        # Check that the structure of the model dictionary conforms to expectations.
        assert isinstance(attr_rel_dictionary, dict)
        assert attribute_key in attr_rel_dictionary.keys()
        assert "Relationships" in attr_rel_dictionary[attribute_key]
        assert "Attribute" in attr_rel_dictionary[attribute_key]["Relationships"]


class TestDataModelCsvParser:
    def test_gather_csv_attributes_relationships(
        self, helpers, csv_dmp: DataModelCSVParser
    ):
        """The output of the function is a attributes relationship dictionary, check that it is formatted properly."""
        path_to_data_model = helpers.get_schema_file_path(
            "data_models/example.model.csv"
        )
        model_df = load_df(path_to_data_model, data_model=True)

        attr_rel_dict = csv_dmp.gather_csv_attributes_relationships(model_df=model_df)
        attribute_key = list(attr_rel_dict.keys())[0]

        # Check that the structure of the model dictionary conforms to expectations.
        assert isinstance(attr_rel_dict, dict)
        assert attribute_key in attr_rel_dict.keys()
        assert "Relationships" in attr_rel_dict[attribute_key]
        assert "Attribute" in attr_rel_dict[attribute_key]["Relationships"]

    def test_parse_csv_model(self, helpers, csv_dmp: DataModelCSVParser):
        """The output of the function is a attributes relationship dictionary, check that it is formatted properly."""
        path_to_data_model = helpers.get_schema_file_path(
            "data_models/example.model.csv"
        )

        attr_rel_dictionary = csv_dmp.parse_csv_model(
            path_to_data_model=path_to_data_model
        )
        attribute_key = list(attr_rel_dictionary.keys())[0]

        # Check that the structure of the model dictionary conforms to expectations.
        assert type(attr_rel_dictionary) == dict
        assert attribute_key in attr_rel_dictionary.keys()
        assert "Relationships" in attr_rel_dictionary[attribute_key]
        assert "Attribute" in attr_rel_dictionary[attribute_key]["Relationships"]

    @pytest.mark.parametrize(
        "attribute_dict, expected_dict",
        [
            ({}, {}),
            ({"x": "y"}, {}),
            ({"Format": np.nan}, {}),
            ({"Format": "date"}, {"Format": "date"}),
            ({"Format": "date", "x": "y"}, {"Format": "date"}),
        ],
    )
    def test_parse_format(
        self,
        csv_dmp: DataModelCSVParser,
        attribute_dict: dict[str, Any],
        expected_dict: dict[str, str],
    ) -> None:
        assert csv_dmp.parse_format(attribute_dict) == expected_dict

    @pytest.mark.parametrize(
        "attribute_dict, expected_dict",
        [
            ({}, {}),
            ({"Pattern": np.nan}, {}),
            ({"Pattern": "^[a-b]"}, {"Pattern": "^[a-b]"}),
            ({"Pattern": "  [a-b]  "}, {"Pattern": "[a-b]"}),
        ],
    )
    def test_parse_regex_pattern(
        self,
        csv_dmp: DataModelCSVParser,
        attribute_dict: dict[str, Any],
        expected_dict: dict[str, str],
    ) -> None:
        assert csv_dmp.parse_pattern(attribute_dict) == expected_dict

    @pytest.mark.parametrize(
        "attribute_dict, relationship, expected_dict",
        [
            ({"Minimum": 10.0}, "Minimum", {"Minimum": 10.0}),
            ({"Minimum": 0.0}, "Minimum", {"Minimum": 0.0}),
            ({"Maximum": 10.0}, "Maximum", {"Maximum": 10.0}),
            ({"Minimum": 10.5}, "Minimum", {"Minimum": 10.5}),
            ({"Maximum": 10.5}, "Maximum", {"Maximum": 10.5}),
            ({"Minimum": "random_string"}, "Minimum", ValueError),
            ({"Maximum": "random_string"}, "Maximum", ValueError),
            ({"Maximum": True}, "Maximum", ValueError),
            ({"Minimum": False}, "Minimum", ValueError),
            ({"Maximum": 10, "Minimum": 200}, "Maximum", ValueError),
            ({"Maximum": 20, "Minimum": 2000}, "Minimum", ValueError),
        ],
        ids=[
            "minimum_integer",
            "minimum_zero",
            "maximum_integer",
            "minimum_float",
            "maximum_float",
            "minimum_wrong_type_string",
            "maximum_wrong_type_string",
            "maximum_wrong_type_boolean",
            "minimum_wrong_type_boolean",
            "maximum_smaller_than_minimum_1",
            "maximum_smaller_than_minimum_2",
        ],
    )
    def test_parse_minimum_maximum(
        self,
        csv_dmp: DataModelCSVParser,
        attribute_dict: dict[str, Any],
        relationship: str,
        expected_dict: Union[dict, type],
    ) -> None:
        if expected_dict == ValueError:
            with pytest.raises(ValueError):
                csv_dmp.parse_minimum_maximum(attribute_dict, relationship)
        else:
            assert (
                csv_dmp.parse_minimum_maximum(attribute_dict, relationship)
                == expected_dict
            )


class TestDataModelJsonLdParser:
    def test_gather_jsonld_attributes_relationships(
        self,
        csv_dmp: DataModelCSVParser,
        attribute_dict: dict[str, Any],
        expected_dict: dict[str, Union[float, int]],
    ) -> None:
        assert csv_dmp.parse_minimum_maximum(attribute_dict, "Minimum") == expected_dict
        assert csv_dmp.parse_minimum_maximum(attribute_dict, "Maximum") == expected_dict

    def test_gather_jsonld_attributes_relationships(
        self,
        helpers,
        jsonld_dmp: DataModelJSONLDParser,
    ):
        """The output of the function is a attributes relationship dictionary, check that it is formatted properly."""
        path_to_data_model = helpers.get_schema_file_path(
            "data_models_jsonld/example.model.jsonld"
        )
        model_jsonld = load_json(path_to_data_model)
        attr_rel_dict = jsonld_dmp.gather_jsonld_attributes_relationships(
            model_jsonld=model_jsonld["@graph"],
        )
        attribute_key = list(attr_rel_dict.keys())[0]

        # Check that the structure of the model dictionary conforms to expectations.
        assert type(attr_rel_dict) == dict
        assert attribute_key in attr_rel_dict.keys()
        assert "Relationships" in attr_rel_dict[attribute_key]
        assert "Attribute" in attr_rel_dict[attribute_key]["Relationships"]

    def test_parse_jsonld_model(
        self,
        helpers,
        jsonld_dmp: DataModelJSONLDParser,
    ):
        """The output of the function is a attributes relationship dictionary, check that it is formatted properly."""
        path_to_data_model = helpers.get_schema_file_path(
            "data_models_jsonld/example.model.jsonld"
        )
        attr_rel_dictionary = jsonld_dmp.parse_jsonld_model(
            path_to_data_model=path_to_data_model,
        )
        attribute_key = list(attr_rel_dictionary.keys())[0]

        # Check that the structure of the model dictionary conforms to expectations.
        assert type(attr_rel_dictionary) == dict
        assert attribute_key in attr_rel_dictionary.keys()
        assert "Relationships" in attr_rel_dictionary[attribute_key]
        assert "Attribute" in attr_rel_dictionary[attribute_key]["Relationships"]
