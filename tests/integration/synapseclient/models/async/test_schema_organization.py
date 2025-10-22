"""Integration tests for SchemaOrganization and JSONSchema classes"""
import uuid
from typing import Any, Optional

import pytest
import pytest_asyncio

from synapseclient import Synapse
from synapseclient.core.constants.concrete_types import CREATE_SCHEMA_REQUEST
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import JSONSchema, SchemaOrganization
from synapseclient.models.schema_organization import (
    SYNAPSE_SCHEMA_URL,
    CreateSchemaRequest,
    JSONSchemaVersionInfo,
    list_json_schema_organizations,
)


def create_test_entity_name():
    """Creates a random string for naming orgs and schemas in Synapse for testing

    Returns:
        A legal Synapse org/schema name
    """
    random_string = "".join(i for i in str(uuid.uuid4()) if i.isalpha())
    return f"SYNPY.TEST.{random_string}"


async def org_exists(name: str, synapse_client: Optional[Synapse] = None) -> bool:
    """
    Checks if any organizations exists with the given name

    Args:
        name: the name to check
        syn: Synapse client

    Returns:
        bool: True if an organization match the given name
    """
    matching_orgs = [
        org
        for org in list_json_schema_organizations(synapse_client=synapse_client)
        if org.name == name
    ]
    return len(matching_orgs) == 1


@pytest.fixture(name="module_organization", scope="module")
def fixture_module_organization(syn: Synapse, request) -> SchemaOrganization:
    """
    Returns a created organization at the module scope. Used to hold JSON Schemas created by tests.
    """
    name = create_test_entity_name()
    org = SchemaOrganization(name)
    org.store(synapse_client=syn)

    def delete_org():
        for schema in org.get_json_schemas(synapse_client=syn):
            schema.delete()
        org.delete(synapse_client=syn)

    request.addfinalizer(delete_org)

    return org


@pytest.fixture(name="json_schema", scope="function")
def fixture_json_schema(module_organization: SchemaOrganization) -> JSONSchema:
    """
    Returns a JSON Schema
    """
    name = create_test_entity_name()
    js = JSONSchema(name, module_organization.name)
    return js


@pytest_asyncio.fixture(name="organization", loop_scope="function", scope="function")
async def fixture_organization(syn: Synapse, request) -> SchemaOrganization:
    """
    Returns a Synapse organization.
    """
    name = create_test_entity_name()
    org = SchemaOrganization(name)

    async def delete_org():
        exists = await org_exists(name, syn)
        if exists:
            org.delete()

    request.addfinalizer(delete_org)

    return org


@pytest_asyncio.fixture(
    name="organization_with_schema", loop_scope="function", scope="function"
)
async def fixture_organization_with_schema(request) -> SchemaOrganization:
    """
    Returns a Synapse organization.
    As Cleanup it checks for JSON Schemas and deletes them"""

    name = create_test_entity_name()
    org = SchemaOrganization(name)
    org.store()
    js1 = JSONSchema("schema1", name)
    js2 = JSONSchema("schema2", name)
    js3 = JSONSchema("schema3", name)
    js1.store({})
    js2.store({})
    js3.store({})

    def delete_org():
        for schema in org.get_json_schemas():
            schema.delete()
        org.delete()

    request.addfinalizer(delete_org)

    return org


class TestSchemaOrganization:
    """Asynchronous integration tests for SchemaOrganization."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_create_and_get(self, organization: SchemaOrganization) -> None:
        # GIVEN an initialized organization object that hasn't been stored in Synapse
        # THEN it shouldn't have any metadata besides it's name
        assert organization.name is not None
        assert organization.id is None
        assert organization.created_by is None
        assert organization.created_on is None
        # AND it shouldn't exist in Synapse
        exists = await org_exists(organization.name, synapse_client=self.syn)
        assert not exists
        # WHEN I store the organization the metadata will be saved
        await organization.store_async(synapse_client=self.syn)
        assert organization.name is not None
        assert organization.id is not None
        assert organization.created_by is not None
        assert organization.created_on is not None
        # AND it should exist in Synapse
        exists = await org_exists(organization.name, synapse_client=self.syn)
        assert exists
        # AND it should be getable by future instances with the same name
        org2 = SchemaOrganization(organization.name)
        await org2.get_async(synapse_client=self.syn)
        assert organization.name is not None
        assert organization.id is not None
        assert organization.created_by is not None
        assert organization.created_on is not None
        # WHEN I try to store an organization that exists in Synapse
        # THEN I should get an exception
        with pytest.raises(SynapseHTTPError):
            org2.store()

    async def test_get_json_schemas_async(
        self,
        organization: SchemaOrganization,
        organization_with_schema: SchemaOrganization,
    ) -> None:
        # GIVEN an organization with no schemas and one with 3 schemas
        await organization.store_async(synapse_client=self.syn)
        # THEN get_json_schema_list should return the correct list of schemas
        schema_list = []
        async for item in organization.get_json_schemas_async(synapse_client=self.syn):
            schema_list.append(item)
        assert len(schema_list) == 0
        schema_list2 = []
        async for item in organization_with_schema.get_json_schemas_async(
            synapse_client=self.syn
        ):
            schema_list2.append(item)
        assert len(schema_list2) == 3

    async def test_get_acl_and_update_acl(
        self, organization: SchemaOrganization
    ) -> None:
        # GIVEN an organization that has been initialized, but not created
        # THEN get_acl should raise an error
        with pytest.raises(
            SynapseHTTPError, match="404 Client Error: Organization with name"
        ):
            await organization.get_acl_async(synapse_client=self.syn)
        # GIVEN an organization that has been created
        await organization.store_async(synapse_client=self.syn)
        acl = await organization.get_acl_async(synapse_client=self.syn)
        resource_access: list[dict[str, Any]] = acl["resourceAccess"]
        # THEN the resource access should be have one principal
        assert len(resource_access) == 1
        # WHEN adding another principal to the resource access
        # AND updating the acl
        await organization.update_acl_async(1, ["READ"], synapse_client=self.syn)
        # THEN the resource access should be have two principals
        acl = await organization.get_acl_async(synapse_client=self.syn)
        assert len(acl["resourceAccess"]) == 2


class TestJSONSchema:
    """Asynchronous integration tests for JSONSchema."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_store_and_get(self, json_schema: JSONSchema) -> None:
        # GIVEN an initialized schema object that hasn't been created in Synapse
        # THEN it shouldn't have any metadata besides it's name and organization name, and uri
        assert json_schema.name
        assert json_schema.organization_name
        assert json_schema.uri
        assert not json_schema.organization_id
        assert not json_schema.id
        assert not json_schema.created_by
        assert not json_schema.created_on
        # WHEN the object is stored
        # THEN the Synapse metadata is filled out
        await json_schema.store_async({}, synapse_client=self.syn)
        assert json_schema.name
        assert json_schema.organization_name
        assert json_schema.uri
        assert json_schema.organization_id
        assert json_schema.id
        assert json_schema.created_by
        assert json_schema.created_on
        # AND it should be getable by future instances using the same name
        js2 = JSONSchema(json_schema.name, json_schema.organization_name)
        await js2.get_async(synapse_client=self.syn)
        assert js2.name
        assert js2.organization_name
        assert js2.uri
        assert js2.organization_id
        assert js2.id
        assert js2.created_by
        assert js2.created_on

    async def test_delete(self, organization_with_schema: SchemaOrganization) -> None:
        # GIVEN an organization with 3 schema
        schemas: list[JSONSchema] = []
        async for item in organization_with_schema.get_json_schemas_async():
            schemas.append(item)
        assert len(schemas) == 3
        # WHEN deleting one of those schemas
        schema = schemas[0]
        await schema.delete_async()
        # THEN there should be only two left
        schemas2: list[JSONSchema] = []
        async for item in organization_with_schema.get_json_schemas_async():
            schemas2.append(item)
        assert len(schemas2) == 2

    async def test_delete_version(self, json_schema: JSONSchema) -> None:
        # GIVEN an organization and a JSONSchema
        await json_schema.store_async(schema_body={}, version="0.0.1")
        # THEN that schema should have one version
        js_versions: list[JSONSchemaVersionInfo] = []
        async for item in json_schema.get_versions_async():
            js_versions.append(item)
        assert len(js_versions) == 1
        # WHEN storing a second version
        await json_schema.store_async(schema_body={}, version="0.0.2")
        # THEN that schema should have two versions
        js_versions = []
        async for item in json_schema.get_versions_async():
            js_versions.append(item)
        assert len(js_versions) == 2
        # AND they should be the ones stored
        versions = [js_version.semantic_version for js_version in js_versions]
        assert versions == ["0.0.1", "0.0.2"]
        # WHEN deleting the first schema version
        await json_schema.delete_async(version="0.0.1")
        # THEN there should only be one version left
        js_versions = []
        async for item in json_schema.get_versions_async():
            js_versions.append(item)
        assert len(js_versions) == 1
        # AND it should be the second version
        versions = [js_version.semantic_version for js_version in js_versions]
        assert versions == ["0.0.2"]

    async def test_get_versions(self, json_schema: JSONSchema) -> None:
        # GIVEN an schema that hasn't been created
        # THEN get_versions should return an empty list
        versions = []
        async for item in json_schema.get_versions_async(synapse_client=self.syn):
            versions.append(item)
        assert len(versions) == 0
        # WHEN creating a schema with no version
        await json_schema.store_async(schema_body={}, synapse_client=self.syn)
        # THEN get_versions should return an empty list
        versions = []
        async for item in json_schema.get_versions_async(synapse_client=self.syn):
            versions.append(item)
        assert len(versions) == 0
        # WHEN creating a schema with a version
        await json_schema.store_async(
            schema_body={}, version="0.0.1", synapse_client=self.syn
        )
        # THEN get_versions should return that version
        versions = []
        async for item in json_schema.get_versions_async(synapse_client=self.syn):
            versions.append(item)
        assert len(versions) == 1
        assert versions[0].semantic_version == "0.0.1"

    async def test_get_body(self, json_schema: JSONSchema) -> None:
        # GIVEN a schema
        # WHEN storing 2 versions of the schema
        first_body = {}
        latest_body = {"description": ""}
        await json_schema.store_async(
            schema_body=first_body, version="0.0.1", synapse_client=self.syn
        )
        await json_schema.store_async(
            schema_body=latest_body, version="0.0.2", synapse_client=self.syn
        )
        # WHEN get_body has no version argument
        body0 = await json_schema.get_body_async(synapse_client=self.syn)
        # THEN the body should be the latest version
        assert body0 == {
            "description": "",
            "$id": f"https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/{json_schema.organization_name}-{json_schema.name}",
        }
        # WHEN get_body has a version argument
        body1 = await json_schema.get_body_async(
            version="0.0.1", synapse_client=self.syn
        )
        body2 = await json_schema.get_body_async(
            version="0.0.2", synapse_client=self.syn
        )
        # THEN the appropriate body should be returned
        assert body1 == {
            "$id": f"https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/{json_schema.organization_name}-{json_schema.name}-0.0.1",
        }
        assert body2 == {
            "description": "",
            "$id": f"https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/{json_schema.organization_name}-{json_schema.name}-0.0.2",
        }


class TestCreateSchemaRequest:
    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_create_schema_request_no_version(
        self, module_organization: SchemaOrganization
    ) -> None:
        # GIVEN an organization
        # WHEN creating a CreateSchemaRequest with no version given
        schema_name = create_test_entity_name()
        request = CreateSchemaRequest(
            schema={}, name=schema_name, organization_name=module_organization.name
        )
        # THEN id in schema will not include version
        assert request.schema == {
            "$id": f"{SYNAPSE_SCHEMA_URL}{module_organization.name}-{schema_name}"
        }
        assert request.name == schema_name
        assert request.organization_name == module_organization.name
        # AND version will be None
        assert request.version is None
        assert request.dry_run is False
        assert request.concrete_type == CREATE_SCHEMA_REQUEST
        # AND URI and id will not include a version
        assert request.uri == f"{module_organization.name}-{schema_name}"
        assert (
            request.id
            == f"{SYNAPSE_SCHEMA_URL}{module_organization.name}-{schema_name}"
        )
        assert not request.new_version_info
        # THEN the Schema should not be part of the organization yet
        assert request.uri not in [
            schema.uri for schema in module_organization.get_json_schemas()
        ]

        # WHEN sending the CreateSchemaRequest
        completed_request = await request.send_job_and_wait_async(
            synapse_client=self.syn
        )
        assert completed_request.new_version_info
        # THEN the Schema should be part of the organization
        # assert completed_request.uri in [
        #    schema.uri for schema in module_organization.get_json_schema_list()
        # ]

    async def test_create_schema_request_with_version(
        self, module_organization: SchemaOrganization
    ) -> None:
        # GIVEN an organization
        # WHEN creating a CreateSchemaRequest with no version given
        schema_name = create_test_entity_name()
        version = "0.0.1"
        request = CreateSchemaRequest(
            schema={},
            name=schema_name,
            organization_name=module_organization.name,
            version=version,
        )
        # THEN id in schema will include version
        assert request.schema == {
            "$id": f"{SYNAPSE_SCHEMA_URL}{module_organization.name}-{schema_name}-{version}"
        }
        assert request.name == schema_name
        assert request.organization_name == module_organization.name
        # AND version will be set
        assert request.version == version
        assert request.dry_run is False
        assert request.concrete_type == CREATE_SCHEMA_REQUEST
        # AND URI and id will include a version
        assert request.uri == f"{module_organization.name}-{schema_name}-{version}"
        assert (
            request.id
            == f"{SYNAPSE_SCHEMA_URL}{module_organization.name}-{schema_name}-{version}"
        )
        assert not request.new_version_info
        # THEN the Schema should not be part of the organization yet
        assert f"{module_organization.name}-{schema_name}" not in [
            schema.uri for schema in module_organization.get_json_schemas()
        ]

        # WHEN sending the CreateSchemaRequest
        completed_request = await request.send_job_and_wait_async(
            synapse_client=self.syn
        )
        assert completed_request.new_version_info
        # THEN the Schema (minus version) should be part of the organization yet
        schemas = [
            schema
            for schema in module_organization.get_json_schemas()
            if schema.uri == f"{module_organization.name}-{schema_name}"
        ]
        assert len(schemas) == 1
        schema = schemas[0]
        # AND schema version should have matching full uri
        assert completed_request.uri in [
            version.id for version in schema.get_versions()
        ]
