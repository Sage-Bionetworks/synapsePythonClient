"""Unit tests for SchemaOrganization and JSONSchema classes"""
from typing import Any

import pytest

from synapseclient.models import JSONSchema, SchemaOrganization
from synapseclient.models.schema_organization import CreateSchemaRequest


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
        with pytest.raises(
            ValueError,
            match="Name must start with a letter and contain only letters numbers and periods",
        ):
            SchemaOrganization(name)

    def test_fill_from_dict(self) -> None:
        "Tests that fill_from_dict fills in all fields"
        organization = SchemaOrganization()
        assert organization.name is None
        assert organization.id is None
        assert organization.created_by is None
        assert organization.created_on is None
        organization.fill_from_dict(
            {
                "name": "org.name",
                "id": "org.id",
                "createdOn": "1",
                "createdBy": "2",
            }
        )
        assert organization.name == "org.name"
        assert organization.id == "org.id"
        assert organization.created_on == "1"
        assert organization.created_by == "2"


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
        with pytest.raises(
            ValueError,
            match="Name must start with a letter and contain only letters numbers and periods",
        ):
            JSONSchema(name, "org.name")

    @pytest.mark.parametrize(
        "uri",
        ["ORG.NAME-SCHEMA.NAME", "ORG.NAME-SCHEMA.NAME-0.0.1"],
        ids=["Non-semantic URI", "Semantic URI"],
    )
    def test_from_uri(self, uri: str) -> None:
        "Tests that legal schema URIs result in created objects."
        assert JSONSchema.from_uri(uri)

    @pytest.mark.parametrize(
        "uri",
        ["ORG.NAME", "ORG.NAME-SCHEMA.NAME-0.0.1-extra.part"],
        ids=["No dashes", "Too many dashes"],
    )
    def test_from_uri_with_exceptions(self, uri: str) -> None:
        "Tests that illegal schema URIs result in an exception."
        with pytest.raises(ValueError, match="The URI must be in the form of"):
            JSONSchema.from_uri(uri)

    def test_fill_from_dict(self) -> None:
        "Tests that fill_from_dict fills in all fields"
        js = JSONSchema()
        assert js.name is None
        assert js.organization_name is None
        assert js.id is None
        assert js.organization_id is None
        assert js.created_on is None
        assert js.created_by is None
        assert js.uri is None
        js.fill_from_dict(
            {
                "organizationId": "org.id",
                "organizationName": "org.name",
                "schemaId": "id",
                "schemaName": "name",
                "createdOn": "1",
                "createdBy": "2",
            }
        )
        assert js.name == "name"
        assert js.organization_name == "org.name"
        assert js.id == "id"
        assert js.organization_id == "org.id"
        assert js.created_on == "1"
        assert js.created_by == "2"
        assert js.uri == "org.name-name"


class TestCreateSchemaRequest:
    @pytest.mark.parametrize(
        "name",
        ["AAAAAAA", "A12345", "A....."],
        ids=["Just letters", "Numbers", "Periods"],
    )
    def test_init_name(self, name: str) -> None:
        "Tests that legal names don't raise a ValueError on init"
        assert CreateSchemaRequest(schema={}, name=name, organization_name="org.name")

    @pytest.mark.parametrize(
        "name",
        ["1AAAAAA", ".AAAAAA", "AAAAAA!"],
        ids=["Starts with a number", "Starts with a period", "Special character"],
    )
    def test_init_name_exceptions(self, name: str) -> None:
        "Tests that illegal names raise a ValueError on init"
        with pytest.raises(
            ValueError,
            match="Name must start with a letter and contain only letters numbers and periods",
        ):
            CreateSchemaRequest(schema={}, name=name, organization_name="org.name")

    @pytest.mark.parametrize(
        "name",
        ["AAAAAAA", "A12345", "A....."],
        ids=["Just letters", "Numbers", "Periods"],
    )
    def test_init_org_name(self, name: str) -> None:
        "Tests that legal org names don't raise a ValueError on init"
        assert CreateSchemaRequest(
            schema={}, name="schema.name", organization_name=name
        )

    @pytest.mark.parametrize(
        "name",
        ["1AAAAAA", ".AAAAAA", "AAAAAA!"],
        ids=["Starts with a number", "Starts with a period", "Special character"],
    )
    def test_init_org_name_exceptions(self, name: str) -> None:
        "Tests that illegal org names raise a ValueError on init"
        with pytest.raises(
            ValueError,
            match="Name must start with a letter and contain only letters numbers and periods",
        ):
            CreateSchemaRequest(schema={}, name="schema.name", organization_name=name)

    @pytest.mark.parametrize(
        "version",
        ["0.0.1", "1.0.0"],
    )
    def test_init_version(self, version: str) -> None:
        "Tests that legal versions don't raise a ValueError on init"
        assert CreateSchemaRequest(
            schema={}, name="schema.name", organization_name="org.name", version=version
        )

    @pytest.mark.parametrize(
        "version",
        ["1", "1.0", "0.0.0.1", "0.0.0"],
    )
    def test_init_version_exceptions(self, version: str) -> None:
        "Tests that illegal versions raise a ValueError on init"
        with pytest.raises(
            ValueError,
            match="Schema version must be a semantic version starting at 0.0.1",
        ):
            CreateSchemaRequest(
                schema={},
                name="schema.name",
                organization_name="org.name",
                version=version,
            )
