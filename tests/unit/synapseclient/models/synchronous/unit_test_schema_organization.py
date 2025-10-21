"""Unit tests for SchemaOrganization and JSONSchema classes"""
import pytest

from synapseclient.models import JSONSchema, SchemaOrganization
from synapseclient.models.schema_organization import CreateSchemaRequest, _check_name


class TestSchemaOrganization:
    """Synchronous unit tests for SchemaOrganization."""

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

    @pytest.mark.parametrize("version", ["1.0.0", "0.0.1", "0.1.0"])
    def test_check_semantic_version(self, version: str) -> None:
        "Tests that only correct versions are allowed"
        js = JSONSchema()
        js._check_semantic_version(version)

    def test_check_semantic_version_with_exceptions(self) -> None:
        "Tests that only correct versions are allowed"
        js = JSONSchema()
        with pytest.raises(
            ValueError, match="Schema version must start at '0.0.1' or higher"
        ):
            js._check_semantic_version("0.0.0")
        with pytest.raises(
            ValueError,
            match="Schema version must be a semantic version with no letters and a major, minor and patch version",
        ):
            js._check_semantic_version("0.0.1.rc")


class TestCreateSchemaRequest:
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


class TestCheckName:
    """Tests for check name helper function"""

    @pytest.mark.parametrize(
        "name",
        ["aaaaaaa", "aaaaaa1", "aa.aa.aa", "a1.a1.a1"],
    )
    def test_check_name(self, name: str):
        """Checks that legal names don't raise an exception"""
        _check_name(name)

    @pytest.mark.parametrize(
        "name",
        [
            "a",
            "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        ],
    )
    def test_check_length_exception(self, name: str):
        """Checks that names that are too short or long raise an exception"""
        with pytest.raises(
            ValueError, match="The name must be of length 6 to 250 characters"
        ):
            _check_name(name)

    @pytest.mark.parametrize(
        "name",
        [
            "sagebionetworks",
            "asagebionetworks",
            "sagebionetworksa",
            "aaa.sagebionetworks.aaa",
            "SAGEBIONETWORKS",
            "SageBionetworks",
        ],
    )
    def test_check_sage_exception(self, name: str):
        """Checks that names that contain 'sagebionetworks' raise an exception"""
        with pytest.raises(
            ValueError, match="The name must not contain 'sagebionetworks'"
        ):
            _check_name(name)

    @pytest.mark.parametrize(
        "name",
        ["1AAAAA", "AAA.1AAA", "AAA.AAA.1AAA", ".AAAAAAA", "AAAAAAAA!"],
        ids=[
            "Starts with number",
            "Part2 starts with number",
            "Part3 starts with number",
            "Starts with period",
            "Contains special characters",
        ],
    )
    def test_check_content_exception(self, name: str):
        """Checks that names that contain special characters(besides periods) or have parts that start with numbers raise an exception"""
        with pytest.raises(
            ValueError,
            match="Name may be separated by periods, but each part must start with a letter and contain only letters and numbers",
        ):
            _check_name(name)
