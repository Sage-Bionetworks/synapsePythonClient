"""Unit tests for the SchemaOrganization and JSONSchema models."""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from synapseclient import Synapse
from synapseclient.models.mixins.json_schema import JSONSchemaVersionInfo
from synapseclient.models.schema_organization import (
    CreateSchemaRequest,
    JSONSchema,
    SchemaOrganization,
    _check_name,
)

ORG_NAME = "mytest.organization"
ORG_ID = "1075"
CREATED_ON = "2024-01-01T00:00:00.000Z"
CREATED_BY = "111111"
SCHEMA_NAME = "mytest.schemaname"
SCHEMA_ID = "5001"
SCHEMA_URI = f"{ORG_NAME}-{SCHEMA_NAME}"
ORG_ID_INT = 1075
VERSION = "1.0.0"
VERSION_ID = "9001"
JSON_SHA_HEX = "abc123sha256"
PRINCIPAL_ID_1 = 100
PRINCIPAL_ID_2 = 200
REPO_ENDPOINT = "https://repo-prod.prod.sagebase.org/repo/v1"

SCHEMA_BODY = {
    "properties": {
        "Component": {
            "description": "TBD",
            "not": {"type": "null"},
            "title": "Component",
        }
    }
}


def _get_organization_response(**overrides):
    """Return a mock organization API response."""
    response = {
        "name": ORG_NAME,
        "id": ORG_ID,
        "createdOn": CREATED_ON,
        "createdBy": CREATED_BY,
    }
    response.update(overrides)
    return response


def _get_json_schema_list_response(**overrides):
    """Return a mock JSON schema list item response."""
    response = {
        "organizationId": ORG_ID_INT,
        "organizationName": ORG_NAME,
        "schemaId": SCHEMA_ID,
        "schemaName": SCHEMA_NAME,
        "createdOn": CREATED_ON,
        "createdBy": CREATED_BY,
    }
    response.update(overrides)
    return response


def _get_schema_version_response(semantic_version=VERSION, **overrides):
    """Return a mock JSON schema version info response."""
    response = {
        "organizationId": ORG_ID_INT,
        "organizationName": ORG_NAME,
        "schemaId": SCHEMA_ID,
        "$id": f"{ORG_NAME}-{SCHEMA_NAME}-{semantic_version}",
        "schemaName": SCHEMA_NAME,
        "versionId": VERSION_ID,
        "semanticVersion": semantic_version,
        "jsonSHA256Hex": JSON_SHA_HEX,
        "createdOn": CREATED_ON,
        "createdBy": CREATED_BY,
    }
    response.update(overrides)
    return response


def _get_acl_response():
    """Return a mock ACL response."""
    return {
        "id": ORG_ID,
        "etag": "acl-etag-123",
        "resourceAccess": [
            {
                "principalId": PRINCIPAL_ID_1,
                "accessType": ["READ", "CREATE"],
            }
        ],
    }


class TestCheckName:
    """Tests for the _check_name validation function."""

    def test_valid_name(self) -> None:
        # GIVEN a valid name
        # WHEN I check the name
        # THEN no exception should be raised
        _check_name("mytest.organization")

    def test_name_too_short(self) -> None:
        # GIVEN a name that is too short
        # WHEN I check the name
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="length 6 to 250"):
            _check_name("abc")

    def test_name_too_long(self) -> None:
        # GIVEN a name that is too long
        # WHEN I check the name
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="length 6 to 250"):
            _check_name("a" * 251)

    def test_name_contains_sagebionetworks(self) -> None:
        # GIVEN a name containing 'sagebionetworks'
        # WHEN I check the name
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="sagebionetworks"):
            _check_name("my.sagebionetworks.test")

    def test_name_part_starts_with_number(self) -> None:
        # GIVEN a name where a part starts with a number
        # WHEN I check the name
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must start with a letter"):
            _check_name("mytest.1invalid")


class TestSchemaOrganization:
    """Unit tests for the SchemaOrganization model."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_fill_from_dict(self) -> None:
        # GIVEN an organization API response
        response = _get_organization_response()

        # WHEN I fill a SchemaOrganization from the response
        org = SchemaOrganization()
        org.fill_from_dict(response)

        # THEN all fields should be populated
        assert org.name == ORG_NAME
        assert org.id == ORG_ID
        assert org.created_on == CREATED_ON
        assert org.created_by == CREATED_BY

    async def test_store_async(self) -> None:
        # GIVEN a SchemaOrganization with a name
        org = SchemaOrganization(name=ORG_NAME)

        # WHEN I call store_async
        with patch(
            "synapseclient.models.schema_organization.create_organization",
            new_callable=AsyncMock,
            return_value=_get_organization_response(),
        ) as mock_create:
            result = await org.store_async(synapse_client=self.syn)

            # THEN the create API should be called with the name
            mock_create.assert_called_once_with(ORG_NAME, synapse_client=self.syn)

            # AND the result should be populated
            assert result.name == ORG_NAME
            assert result.id == ORG_ID
            assert result.created_on == CREATED_ON

    async def test_store_async_without_name_raises(self) -> None:
        # GIVEN a SchemaOrganization without a name
        org = SchemaOrganization()

        # WHEN I call store_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must have a name"):
            await org.store_async(synapse_client=self.syn)

    async def test_get_async(self) -> None:
        # GIVEN a SchemaOrganization with a name
        org = SchemaOrganization(name=ORG_NAME)

        # WHEN I call get_async
        with patch(
            "synapseclient.models.schema_organization.get_organization",
            new_callable=AsyncMock,
            return_value=_get_organization_response(),
        ) as mock_get:
            result = await org.get_async(synapse_client=self.syn)

            # THEN the get API should be called with the name
            mock_get.assert_called_once_with(ORG_NAME, synapse_client=self.syn)

            # AND the result should be populated
            assert result.name == ORG_NAME
            assert result.id == ORG_ID

    async def test_get_async_without_name_raises(self) -> None:
        # GIVEN a SchemaOrganization without a name
        org = SchemaOrganization()

        # WHEN I call get_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must have a name"):
            await org.get_async(synapse_client=self.syn)

    async def test_delete_async_with_id(self) -> None:
        # GIVEN a SchemaOrganization with an id
        org = SchemaOrganization(name=ORG_NAME, id=ORG_ID)

        # WHEN I call delete_async
        with patch(
            "synapseclient.models.schema_organization.delete_organization",
            new_callable=AsyncMock,
            return_value=None,
        ) as mock_delete:
            await org.delete_async(synapse_client=self.syn)

            # THEN the delete API should be called with the id
            mock_delete.assert_called_once_with(
                organization_id=ORG_ID, synapse_client=self.syn
            )

    async def test_delete_async_without_id_triggers_get(self) -> None:
        # GIVEN a SchemaOrganization with only a name (no id)
        org = SchemaOrganization(name=ORG_NAME)

        # WHEN I call delete_async
        with patch(
            "synapseclient.models.schema_organization.get_organization",
            new_callable=AsyncMock,
            return_value=_get_organization_response(),
        ) as mock_get, patch(
            "synapseclient.models.schema_organization.delete_organization",
            new_callable=AsyncMock,
            return_value=None,
        ) as mock_delete:
            await org.delete_async(synapse_client=self.syn)

            # THEN get should be called first to obtain the id
            mock_get.assert_called_once_with(ORG_NAME, synapse_client=self.syn)

            # AND then delete should be called with the obtained id
            mock_delete.assert_called_once_with(
                organization_id=ORG_ID, synapse_client=self.syn
            )

    async def test_get_json_schemas_async(self) -> None:
        # GIVEN a SchemaOrganization with a name
        org = SchemaOrganization(name=ORG_NAME)

        schema_response_1 = _get_json_schema_list_response()
        schema_response_2 = _get_json_schema_list_response(
            schemaName="another.schema", schemaId="5002"
        )

        async def mock_list(*args, **kwargs):
            yield schema_response_1
            yield schema_response_2

        # WHEN I call get_json_schemas_async
        with patch(
            "synapseclient.models.schema_organization.list_json_schemas",
            return_value=mock_list(),
        ):
            results = []
            async for schema in org.get_json_schemas_async(synapse_client=self.syn):
                results.append(schema)

            # THEN I should get two JSONSchema objects
            assert len(results) == 2
            assert isinstance(results[0], JSONSchema)
            assert results[0].name == SCHEMA_NAME
            assert results[0].organization_name == ORG_NAME
            assert results[0].id == SCHEMA_ID
            assert results[1].name == "another.schema"

    async def test_get_json_schemas_async_without_name_raises(self) -> None:
        # GIVEN a SchemaOrganization without a name
        org = SchemaOrganization()

        # WHEN I call get_json_schemas_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must have a name"):
            async for _ in org.get_json_schemas_async(synapse_client=self.syn):
                pass  # pragma: no cover

    async def test_get_acl_async(self) -> None:
        # GIVEN a SchemaOrganization with an id
        org = SchemaOrganization(name=ORG_NAME, id=ORG_ID)

        acl_response = _get_acl_response()

        # WHEN I call get_acl_async
        with patch(
            "synapseclient.models.schema_organization.get_organization_acl",
            new_callable=AsyncMock,
            return_value=acl_response,
        ) as mock_get_acl:
            result = await org.get_acl_async(synapse_client=self.syn)

            # THEN the ACL API should be called with the id
            mock_get_acl.assert_called_once_with(ORG_ID, synapse_client=self.syn)

            # AND the result should contain the ACL data
            assert result["etag"] == "acl-etag-123"
            assert len(result["resourceAccess"]) == 1
            assert result["resourceAccess"][0]["principalId"] == PRINCIPAL_ID_1

    async def test_get_acl_async_without_id_triggers_get(self) -> None:
        # GIVEN a SchemaOrganization with only a name
        org = SchemaOrganization(name=ORG_NAME)

        acl_response = _get_acl_response()

        # WHEN I call get_acl_async (id will be fetched first)
        with patch(
            "synapseclient.models.schema_organization.get_organization",
            new_callable=AsyncMock,
            return_value=_get_organization_response(),
        ) as mock_get, patch(
            "synapseclient.models.schema_organization.get_organization_acl",
            new_callable=AsyncMock,
            return_value=acl_response,
        ) as mock_get_acl:
            result = await org.get_acl_async(synapse_client=self.syn)

            # THEN get should be called first to obtain the id
            mock_get.assert_called_once()

            # AND get_acl should be called with the obtained id
            mock_get_acl.assert_called_once_with(ORG_ID, synapse_client=self.syn)

    async def test_update_acl_async_add_new_principal(self) -> None:
        # GIVEN a SchemaOrganization with an id
        org = SchemaOrganization(name=ORG_NAME, id=ORG_ID)

        acl_response = _get_acl_response()

        # WHEN I call update_acl_async with a new principal
        with patch(
            "synapseclient.models.schema_organization.get_organization_acl",
            new_callable=AsyncMock,
            return_value=acl_response,
        ), patch(
            "synapseclient.models.schema_organization.update_organization_acl",
            new_callable=AsyncMock,
            return_value=None,
        ) as mock_update:
            await org.update_acl_async(
                principal_id=PRINCIPAL_ID_2,
                access_type=["READ"],
                synapse_client=self.syn,
            )

            # THEN the update API should be called with the new principal added
            mock_update.assert_called_once()
            call_kwargs = mock_update.call_args[1]
            resource_access = call_kwargs["resource_access"]

            # AND the resource_access should contain both principals
            assert len(resource_access) == 2
            principal_ids = [ra["principalId"] for ra in resource_access]
            assert PRINCIPAL_ID_1 in principal_ids
            assert PRINCIPAL_ID_2 in principal_ids

            # AND the new principal should have the correct access type
            new_entry = next(
                ra for ra in resource_access if ra["principalId"] == PRINCIPAL_ID_2
            )
            assert new_entry["accessType"] == ["READ"]

    async def test_update_acl_async_update_existing_principal(self) -> None:
        # GIVEN a SchemaOrganization with an id
        org = SchemaOrganization(name=ORG_NAME, id=ORG_ID)

        acl_response = _get_acl_response()

        # WHEN I call update_acl_async for an existing principal with new permissions
        with patch(
            "synapseclient.models.schema_organization.get_organization_acl",
            new_callable=AsyncMock,
            return_value=acl_response,
        ), patch(
            "synapseclient.models.schema_organization.update_organization_acl",
            new_callable=AsyncMock,
            return_value=None,
        ) as mock_update:
            await org.update_acl_async(
                principal_id=PRINCIPAL_ID_1,
                access_type=["READ", "CREATE", "DELETE"],
                synapse_client=self.syn,
            )

            # THEN the update API should be called
            mock_update.assert_called_once()
            call_kwargs = mock_update.call_args[1]
            resource_access = call_kwargs["resource_access"]

            # AND the resource_access should still have one entry
            assert len(resource_access) == 1

            # AND the existing principal should have updated access types
            assert resource_access[0]["principalId"] == PRINCIPAL_ID_1
            assert resource_access[0]["accessType"] == ["READ", "CREATE", "DELETE"]

            # AND the etag should be passed
            assert call_kwargs["etag"] == "acl-etag-123"


class TestJSONSchema:
    """Unit tests for the JSONSchema model."""

    @pytest.fixture(autouse=True, scope="function")
    def init_syn(self, syn: Synapse) -> None:
        self.syn = syn

    def test_post_init_sets_uri(self) -> None:
        # GIVEN a JSONSchema with name and organization_name
        schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)

        # THEN the uri should be set
        assert schema.uri == SCHEMA_URI

    def test_post_init_no_uri_without_both_names(self) -> None:
        # GIVEN a JSONSchema without organization_name
        schema = JSONSchema(name=SCHEMA_NAME)

        # THEN the uri should be None
        assert schema.uri is None

    def test_fill_from_dict(self) -> None:
        # GIVEN a JSON schema API response
        response = _get_json_schema_list_response()

        # WHEN I fill a JSONSchema from the response
        schema = JSONSchema()
        schema.fill_from_dict(response)

        # THEN all fields should be populated
        assert schema.organization_id == ORG_ID_INT
        assert schema.organization_name == ORG_NAME
        assert schema.id == SCHEMA_ID
        assert schema.name == SCHEMA_NAME
        assert schema.created_on == CREATED_ON
        assert schema.created_by == CREATED_BY
        assert schema.uri == SCHEMA_URI

    def test_from_uri_non_semantic(self) -> None:
        # GIVEN a non-semantic URI
        uri = f"{ORG_NAME}-{SCHEMA_NAME}"

        # WHEN I create a JSONSchema from the URI
        schema = JSONSchema.from_uri(uri)

        # THEN the schema should have the correct fields
        assert schema.organization_name == ORG_NAME
        assert schema.name == SCHEMA_NAME

    def test_from_uri_semantic(self) -> None:
        # GIVEN a semantic URI (three parts)
        uri = f"{ORG_NAME}-{SCHEMA_NAME}-1.0.0"

        # WHEN I create a JSONSchema from the URI
        schema = JSONSchema.from_uri(uri)

        # THEN the schema should have the correct fields
        assert schema.organization_name == ORG_NAME
        assert schema.name == SCHEMA_NAME

    def test_from_uri_invalid_raises(self) -> None:
        # GIVEN an invalid URI with too many parts
        uri = "a-b-c-d"

        # WHEN I try to create a JSONSchema from the URI
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must be in the form"):
            JSONSchema.from_uri(uri)

    def test_from_uri_too_few_parts_raises(self) -> None:
        # GIVEN an invalid URI with too few parts
        uri = "onlyonepart"

        # WHEN I try to create a JSONSchema from the URI
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must be in the form"):
            JSONSchema.from_uri(uri)

    def test_check_semantic_version_valid(self) -> None:
        # GIVEN a JSONSchema
        schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)

        # WHEN I check a valid semantic version
        # THEN no exception should be raised
        schema._check_semantic_version("1.0.0")
        schema._check_semantic_version("0.0.1")
        schema._check_semantic_version("10.20.30")

    def test_check_semantic_version_zero_raises(self) -> None:
        # GIVEN a JSONSchema
        schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)

        # WHEN I check version 0.0.0
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must start at '0.0.1'"):
            schema._check_semantic_version("0.0.0")

    def test_check_semantic_version_letters_raises(self) -> None:
        # GIVEN a JSONSchema
        schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)

        # WHEN I check a version with letters
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="semantic version"):
            schema._check_semantic_version("1.0.0-beta")

    async def test_store_async_with_schema_body(self) -> None:
        # GIVEN a JSONSchema with name and organization_name
        schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)

        mock_version_info = JSONSchemaVersionInfo(
            organization_id=ORG_ID_INT,
            organization_name=ORG_NAME,
            schema_id=SCHEMA_ID,
            id=f"{ORG_NAME}-{SCHEMA_NAME}",
            schema_name=SCHEMA_NAME,
            version_id=VERSION_ID,
            semantic_version=VERSION,
            json_sha256_hex=JSON_SHA_HEX,
            created_on=CREATED_ON,
            created_by=CREATED_BY,
        )

        mock_completed_request = MagicMock()
        mock_completed_request.new_version_info = mock_version_info

        # WHEN I call store_async with a schema body
        with patch.object(
            CreateSchemaRequest,
            "send_job_and_wait_async",
            new_callable=AsyncMock,
            return_value=mock_completed_request,
        ):
            self.syn.repoEndpoint = REPO_ENDPOINT
            result = await schema.store_async(
                schema_body=SCHEMA_BODY.copy(),
                synapse_client=self.syn,
            )

            # THEN the result should have updated fields from the version info
            assert result.organization_id == ORG_ID_INT
            assert result.created_by == CREATED_BY
            assert result.created_on == CREATED_ON

    async def test_store_async_with_version(self) -> None:
        # GIVEN a JSONSchema with name and organization_name
        schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)

        mock_version_info = JSONSchemaVersionInfo(
            organization_id=ORG_ID_INT,
            organization_name=ORG_NAME,
            schema_id=SCHEMA_ID,
            id=f"{ORG_NAME}-{SCHEMA_NAME}-{VERSION}",
            schema_name=SCHEMA_NAME,
            version_id=VERSION_ID,
            semantic_version=VERSION,
            json_sha256_hex=JSON_SHA_HEX,
            created_on=CREATED_ON,
            created_by=CREATED_BY,
        )

        mock_completed_request = MagicMock()
        mock_completed_request.new_version_info = mock_version_info

        # WHEN I call store_async with a version
        with patch.object(
            CreateSchemaRequest,
            "send_job_and_wait_async",
            new_callable=AsyncMock,
            return_value=mock_completed_request,
        ):
            self.syn.repoEndpoint = REPO_ENDPOINT
            result = await schema.store_async(
                schema_body=SCHEMA_BODY.copy(),
                version=VERSION,
                synapse_client=self.syn,
            )

            # THEN the result should be populated
            assert result.organization_id == ORG_ID_INT

    async def test_store_async_dry_run(self) -> None:
        # GIVEN a JSONSchema with name and organization_name
        schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)

        mock_version_info = JSONSchemaVersionInfo(
            organization_id=ORG_ID_INT,
            organization_name=ORG_NAME,
            schema_id=SCHEMA_ID,
            id=f"{ORG_NAME}-{SCHEMA_NAME}",
            schema_name=SCHEMA_NAME,
            version_id=VERSION_ID,
            semantic_version=VERSION,
            json_sha256_hex=JSON_SHA_HEX,
            created_on=CREATED_ON,
            created_by=CREATED_BY,
        )

        mock_completed_request = MagicMock()
        mock_completed_request.new_version_info = mock_version_info

        # WHEN I call store_async with dry_run=True
        with patch.object(
            CreateSchemaRequest,
            "send_job_and_wait_async",
            new_callable=AsyncMock,
            return_value=mock_completed_request,
        ) as mock_send:
            self.syn.repoEndpoint = REPO_ENDPOINT
            result = await schema.store_async(
                schema_body=SCHEMA_BODY.copy(),
                dry_run=True,
                synapse_client=self.syn,
            )

            # THEN send_job_and_wait_async should be called
            mock_send.assert_called_once()

    async def test_store_async_without_name_raises(self) -> None:
        # GIVEN a JSONSchema without a name
        schema = JSONSchema(organization_name=ORG_NAME)

        # WHEN I call store_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must have a name"):
            await schema.store_async(schema_body=SCHEMA_BODY, synapse_client=self.syn)

    async def test_store_async_without_org_name_raises(self) -> None:
        # GIVEN a JSONSchema without an organization_name
        schema = JSONSchema(name=SCHEMA_NAME)

        # WHEN I call store_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must have a organization_name"):
            await schema.store_async(schema_body=SCHEMA_BODY, synapse_client=self.syn)

    async def test_get_async(self) -> None:
        # GIVEN a JSONSchema with name and organization_name
        schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)

        schema_response = _get_json_schema_list_response()

        async def mock_list(*args, **kwargs):
            yield schema_response

        # WHEN I call get_async (org exists and schema is found)
        with patch(
            "synapseclient.models.schema_organization.get_organization",
            new_callable=AsyncMock,
            return_value=_get_organization_response(),
        ), patch(
            "synapseclient.models.schema_organization.list_json_schemas",
            return_value=mock_list(),
        ):
            result = await schema.get_async(synapse_client=self.syn)

            # THEN the result should be populated
            assert result.name == SCHEMA_NAME
            assert result.organization_name == ORG_NAME
            assert result.id == SCHEMA_ID

    async def test_get_async_schema_not_found_raises(self) -> None:
        # GIVEN a JSONSchema with a name that does not exist in the org
        schema = JSONSchema(name="nonexistent.schema", organization_name=ORG_NAME)

        # Mock list_json_schemas to return schemas that don't match
        other_schema_response = _get_json_schema_list_response(
            schemaName="different.schema"
        )

        async def mock_list(*args, **kwargs):
            yield other_schema_response

        # WHEN I call get_async
        with patch(
            "synapseclient.models.schema_organization.get_organization",
            new_callable=AsyncMock,
            return_value=_get_organization_response(),
        ), patch(
            "synapseclient.models.schema_organization.list_json_schemas",
            return_value=mock_list(),
        ):
            # THEN it should raise ValueError
            with pytest.raises(ValueError, match="does not contain a schema with name"):
                await schema.get_async(synapse_client=self.syn)

    async def test_get_async_without_name_raises(self) -> None:
        # GIVEN a JSONSchema without a name
        schema = JSONSchema(organization_name=ORG_NAME)

        # WHEN I call get_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must have a name"):
            await schema.get_async(synapse_client=self.syn)

    async def test_get_async_without_org_name_raises(self) -> None:
        # GIVEN a JSONSchema without an organization_name
        schema = JSONSchema(name=SCHEMA_NAME)

        # WHEN I call get_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must have a organization_name"):
            await schema.get_async(synapse_client=self.syn)

    async def test_delete_async_without_version(self) -> None:
        # GIVEN a JSONSchema with name and organization_name
        schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)

        # WHEN I call delete_async without a version
        with patch(
            "synapseclient.models.schema_organization.delete_json_schema",
            new_callable=AsyncMock,
            return_value=None,
        ) as mock_delete:
            await schema.delete_async(synapse_client=self.syn)

            # THEN the delete API should be called with the base URI
            mock_delete.assert_called_once_with(SCHEMA_URI, synapse_client=self.syn)

    async def test_delete_async_with_version(self) -> None:
        # GIVEN a JSONSchema with name and organization_name
        schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)

        # WHEN I call delete_async with a specific version
        with patch(
            "synapseclient.models.schema_organization.delete_json_schema",
            new_callable=AsyncMock,
            return_value=None,
        ) as mock_delete:
            await schema.delete_async(version=VERSION, synapse_client=self.syn)

            # THEN the delete API should be called with the versioned URI
            expected_uri = f"{SCHEMA_URI}-{VERSION}"
            mock_delete.assert_called_once_with(expected_uri, synapse_client=self.syn)

    async def test_delete_async_without_name_raises(self) -> None:
        # GIVEN a JSONSchema without a name
        schema = JSONSchema(organization_name=ORG_NAME)

        # WHEN I call delete_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must have a name"):
            await schema.delete_async(synapse_client=self.syn)

    async def test_delete_async_without_org_name_raises(self) -> None:
        # GIVEN a JSONSchema without an organization_name
        schema = JSONSchema(name=SCHEMA_NAME)

        # WHEN I call delete_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must have a organization_name"):
            await schema.delete_async(synapse_client=self.syn)

    async def test_get_versions_async(self) -> None:
        # GIVEN a JSONSchema with name and organization_name
        schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)

        version_response_1 = _get_schema_version_response(semantic_version="1.0.0")
        version_response_2 = _get_schema_version_response(semantic_version="2.0.0")

        async def mock_list(*args, **kwargs):
            yield version_response_1
            yield version_response_2

        # WHEN I call get_versions_async
        with patch(
            "synapseclient.models.schema_organization.list_json_schema_versions",
            return_value=mock_list(),
        ):
            results = []
            async for version_info in schema.get_versions_async(
                synapse_client=self.syn
            ):
                results.append(version_info)

            # THEN I should get two JSONSchemaVersionInfo objects
            assert len(results) == 2
            assert isinstance(results[0], JSONSchemaVersionInfo)
            assert results[0].semantic_version == "1.0.0"
            assert results[1].semantic_version == "2.0.0"

    async def test_get_versions_async_filters_non_semantic(self) -> None:
        # GIVEN a JSONSchema with name and organization_name
        schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)

        # One version has a semantic version, one does not
        version_with_semantic = _get_schema_version_response(semantic_version="1.0.0")
        version_without_semantic = {
            "organizationId": ORG_ID_INT,
            "organizationName": ORG_NAME,
            "schemaId": SCHEMA_ID,
            "$id": f"{ORG_NAME}-{SCHEMA_NAME}",
            "schemaName": SCHEMA_NAME,
            "versionId": "8888",
            # Note: no "semanticVersion" key
            "jsonSHA256Hex": JSON_SHA_HEX,
            "createdOn": CREATED_ON,
            "createdBy": CREATED_BY,
        }

        async def mock_list(*args, **kwargs):
            yield version_with_semantic
            yield version_without_semantic

        # WHEN I call get_versions_async
        with patch(
            "synapseclient.models.schema_organization.list_json_schema_versions",
            return_value=mock_list(),
        ):
            results = []
            async for version_info in schema.get_versions_async(
                synapse_client=self.syn
            ):
                results.append(version_info)

            # THEN only the version with semantic version should be returned
            assert len(results) == 1
            assert results[0].semantic_version == "1.0.0"

    async def test_get_body_async_latest(self) -> None:
        # GIVEN a JSONSchema with name and organization_name
        schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)

        expected_body = {"type": "object", "properties": {"name": {"type": "string"}}}

        # WHEN I call get_body_async without a version (latest)
        with patch(
            "synapseclient.models.schema_organization.get_json_schema_body",
            new_callable=AsyncMock,
            return_value=expected_body,
        ) as mock_get_body:
            result = await schema.get_body_async(synapse_client=self.syn)

            # THEN the API should be called with the base URI
            mock_get_body.assert_called_once_with(SCHEMA_URI, synapse_client=self.syn)

            # AND the result should be the schema body
            assert result == expected_body

    async def test_get_body_async_with_version(self) -> None:
        # GIVEN a JSONSchema with name and organization_name
        schema = JSONSchema(name=SCHEMA_NAME, organization_name=ORG_NAME)

        expected_body = {"type": "object"}

        # WHEN I call get_body_async with a specific version
        with patch(
            "synapseclient.models.schema_organization.get_json_schema_body",
            new_callable=AsyncMock,
            return_value=expected_body,
        ) as mock_get_body:
            result = await schema.get_body_async(
                version=VERSION, synapse_client=self.syn
            )

            # THEN the API should be called with the versioned URI
            expected_uri = f"{SCHEMA_URI}-{VERSION}"
            mock_get_body.assert_called_once_with(expected_uri, synapse_client=self.syn)

            # AND the result should be the schema body
            assert result == expected_body

    async def test_get_body_async_without_name_raises(self) -> None:
        # GIVEN a JSONSchema without a name
        schema = JSONSchema(organization_name=ORG_NAME)

        # WHEN I call get_body_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must have a name"):
            await schema.get_body_async(synapse_client=self.syn)

    async def test_get_body_async_without_org_name_raises(self) -> None:
        # GIVEN a JSONSchema without an organization_name
        schema = JSONSchema(name=SCHEMA_NAME)

        # WHEN I call get_body_async
        # THEN it should raise ValueError
        with pytest.raises(ValueError, match="must have a organization_name"):
            await schema.get_body_async(synapse_client=self.syn)


class TestCreateSchemaRequest:
    """Tests for the CreateSchemaRequest helper dataclass."""

    def test_post_init_sets_fields(self) -> None:
        # GIVEN schema request parameters
        body = SCHEMA_BODY.copy()

        # WHEN I create a CreateSchemaRequest
        request = CreateSchemaRequest(
            schema=body,
            name=SCHEMA_NAME,
            organization_name=ORG_NAME,
            synapse_schema_url=f"{REPO_ENDPOINT}/schema/type/registered/",
        )

        # THEN the fields should be set correctly
        assert request.uri == SCHEMA_URI
        assert request.id == f"{REPO_ENDPOINT}/schema/type/registered/{SCHEMA_URI}"
        assert body["$id"] == request.id

    def test_post_init_with_version(self) -> None:
        # GIVEN schema request parameters with a version
        body = SCHEMA_BODY.copy()

        # WHEN I create a CreateSchemaRequest with version
        request = CreateSchemaRequest(
            schema=body,
            name=SCHEMA_NAME,
            organization_name=ORG_NAME,
            version=VERSION,
            synapse_schema_url=f"{REPO_ENDPOINT}/schema/type/registered/",
        )

        # THEN the URI should include the version
        expected_uri = f"{SCHEMA_URI}-{VERSION}"
        assert request.uri == expected_uri

    def test_to_synapse_request(self) -> None:
        # GIVEN a CreateSchemaRequest
        body = SCHEMA_BODY.copy()
        request = CreateSchemaRequest(
            schema=body,
            name=SCHEMA_NAME,
            organization_name=ORG_NAME,
            dry_run=True,
            synapse_schema_url=f"{REPO_ENDPOINT}/schema/type/registered/",
        )

        # WHEN I convert it to a synapse request
        result = request.to_synapse_request()

        # THEN it should have the correct structure
        assert "concreteType" in result
        assert result["schema"] == body
        assert result["dryRun"] is True

    def test_fill_from_dict(self) -> None:
        # GIVEN a CreateSchemaRequest and an API response
        body = SCHEMA_BODY.copy()
        request = CreateSchemaRequest(
            schema=body,
            name=SCHEMA_NAME,
            organization_name=ORG_NAME,
            synapse_schema_url=f"{REPO_ENDPOINT}/schema/type/registered/",
        )

        response = {
            "newVersionInfo": _get_schema_version_response(),
            "validationSchema": {"$id": "validated", "type": "object"},
        }

        # WHEN I fill from the response
        request.fill_from_dict(response)

        # THEN the new_version_info should be populated
        assert request.new_version_info is not None
        assert request.new_version_info.organization_id == ORG_ID_INT
        assert request.new_version_info.semantic_version == VERSION
        assert request.schema == {"$id": "validated", "type": "object"}
