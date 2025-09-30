"""Unit tests for SchemaOrganization and JSONSchema classes"""
from typing import Any

import pytest

from synapseclient.models import JSONSchema, SchemaOrganization


class TestSchemaOrganization:
    """Synchronous unit tests for SchemaOrganization."""

    @pytest.mark.parametrize(
        "name",
        ["AAAAAAA", "A12345", "A....."],
        ids=["Just letters", "Numbers", "Periods"],
    )
    def test_init(self, name: str) -> None:
        "Tests that legal names don't raise a ValueError on init"
        assert SchemaOrganization(name)

    @pytest.mark.parametrize(
        "name",
        ["1AAAAAA", ".AAAAAA", "AAAAAA!"],
        ids=["Starts with a number", "Starts with a period", "Special character"],
    )
    def test_init_name_exceptions(self, name: str) -> None:
        "Tests that illegal names raise a ValueError on init"
        with pytest.raises(ValueError, match="Organization name must start with"):
            SchemaOrganization(name)

    @pytest.mark.parametrize(
        "response",
        [
            {
                "name": "AAAAAAA",
                "id": "abc",
                "createdOn": "9-30-25",
                "createdBy": "123",
            }
        ],
    )
    def test_from_response(self, response: dict[str, Any]):
        "Tests that legal Synapse API responses result in created objects."
        assert SchemaOrganization.from_response(response)

    @pytest.mark.parametrize(
        "response",
        [
            {
                "name": None,
                "id": None,
                "createdOn": None,
                "createdBy": None,
            }
        ],
    )
    def test_from_response_with_exception(self, response: dict[str, Any]):
        "Tests that illegal Synapse API responses cause exceptions"
        with pytest.raises(TypeError):
            SchemaOrganization.from_response(response)


class TestJSONSchema:
    """Synchronous unit tests for JSONSchema."""

    @pytest.mark.parametrize(
        "name",
        ["AAAAAAA", "A12345", "A....."],
        ids=["Just letters", "Numbers", "Periods"],
    )
    def test_init(self, name: str) -> None:
        "Tests that legal names don't raise a ValueError on init"
        assert JSONSchema(name, "org.name")

    @pytest.mark.parametrize(
        "name",
        ["1AAAAAA", ".AAAAAA", "AAAAAA!"],
        ids=["Starts with a number", "Starts with a period", "Special character"],
    )
    def test_init_name_exceptions(self, name: str) -> None:
        "Tests that illegal names raise a ValueError on init"
        with pytest.raises(ValueError, match="Schema name must start with"):
            JSONSchema(name, "org.name")

    @pytest.mark.parametrize(
        "uri",
        ["ORG.NAME-SCHEMA.NAME", "ORG.NAME-SCHEMA.NAME-0.0.1"],
        ids=["Non-semantic URI", "Semantic URI"],
    )
    def test_from_uri(self, uri: str):
        "Tests that legal schema URIs result in created objects."
        assert JSONSchema.from_uri(uri)

    @pytest.mark.parametrize(
        "uri",
        ["ORG.NAME", "ORG.NAME-SCHEMA.NAME-0.0.1-extra.part"],
        ids=["No dashes", "Too many dashes"],
    )
    def test_from_uri_with_exceptions(self, uri: str):
        "Tests that illegal schema URIs result in an exception."
        with pytest.raises(ValueError, match="The URI must be in the form of"):
            JSONSchema.from_uri(uri)

    @pytest.mark.parametrize(
        "response",
        [
            {
                "createdOn": "9-30-25",
                "createdBy": "123",
                "organizationId": "123",
                "organizationName": "org.name",
                "schemaId": "123",
                "schemaName": "schema.name",
            }
        ],
    )
    def test_from_response(self, response: dict[str, Any]):
        "Tests that legal Synapse API responses result in created objects."
        assert JSONSchema.from_response(response)

    @pytest.mark.parametrize(
        "response",
        [
            {
                "createdOn": None,
                "createdBy": None,
                "organizationId": None,
                "organizationName": None,
                "schemaId": None,
                "schemaName": None,
            }
        ],
    )
    def test_from_response_with_exception(self, response: dict[str, Any]):
        "Tests that illegal Synapse API responses cause exceptions"
        with pytest.raises(TypeError):
            JSONSchema.from_response(response)
