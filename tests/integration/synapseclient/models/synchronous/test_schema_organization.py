"""Integration tests for SchemaOrganization and JSONSchema classes"""
import uuid
from typing import Any, Optional

import pytest
import pytest_asyncio

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.models import JSONSchema, SchemaOrganization
from synapseclient.models.schema_organization import list_json_schema_organizations


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
def fixture_module_organization(request) -> SchemaOrganization:
    """
    Returns a created organization at the module scope. Used to hold JSON Schemas created by tests.
    """
    org = SchemaOrganization(create_test_entity_name())
    org.store()

    def delete_org():
        for schema in org.get_json_schemas():
            schema.delete()
        org.delete()

    request.addfinalizer(delete_org)

    return org


@pytest.fixture(name="json_schema", scope="function")
def fixture_json_schema(module_organization: SchemaOrganization) -> JSONSchema:
    """
    Returns a JSON Schema
    """
    js = JSONSchema(create_test_entity_name(), module_organization.name)
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


@pytest.fixture(name="organization_with_schema", scope="function")
def fixture_organization_with_schema(request) -> SchemaOrganization:
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
    """Synchronous integration tests for SchemaOrganization."""

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
        organization.store(synapse_client=self.syn)
        assert organization.name is not None
        assert organization.id is not None
        assert organization.created_by is not None
        assert organization.created_on is not None
        # AND it should exist in Synapse
        exists = await org_exists(organization.name, synapse_client=self.syn)
        assert exists
        # AND it should be getable by future instances with the same name
        org2 = SchemaOrganization(organization.name)
        org2.get(synapse_client=self.syn)
        assert organization.name is not None
        assert organization.id is not None
        assert organization.created_by is not None
        assert organization.created_on is not None
        # WHEN I try to store an organization that exists in Synapse
        # THEN I should get an exception
        with pytest.raises(SynapseHTTPError):
            org2.store()

    async def test_get_json_schemas(
        self,
        organization: SchemaOrganization,
        organization_with_schema: SchemaOrganization,
    ) -> None:
        # GIVEN an organization with no schemas and one with 3 schemas
        organization.store(synapse_client=self.syn)
        # THEN get_json_schema_list should return the correct list of schemas
        assert len(list(organization.get_json_schemas(synapse_client=self.syn))) == 0
        assert (
            len(
                list(organization_with_schema.get_json_schemas(synapse_client=self.syn))
            )
            == 3
        )

    async def test_get_acl_and_update_acl(
        self, organization: SchemaOrganization
    ) -> None:
        # GIVEN an organization that has been initialized, but not created
        # THEN get_acl should raise an error
        with pytest.raises(
            SynapseHTTPError, match="404 Client Error: Organization with name"
        ):
            organization.get_acl(synapse_client=self.syn)
        # GIVEN an organization that has been created
        organization.store(synapse_client=self.syn)
        acl = organization.get_acl(synapse_client=self.syn)
        resource_access: list[dict[str, Any]] = acl["resourceAccess"]
        # THEN the resource access should be have one principal
        assert len(resource_access) == 1
        # WHEN adding another principal to the resource access
        # AND updating the acl
        organization.update_acl(1, ["READ"], synapse_client=self.syn)
        # THEN the resource access should be have two principals
        acl = organization.get_acl(synapse_client=self.syn)
        assert len(acl["resourceAccess"]) == 2


class TestJSONSchema:
    """Synchronous integration tests for JSONSchema."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse) -> None:
        self.syn = syn

    async def test_store_and_get(self, json_schema: JSONSchema) -> None:
        # GIVEN an initialized schema object that hasn't been stored in Synapse
        # THEN it shouldn't have any metadata besides it's name and organization name, and uri
        assert json_schema.name
        assert json_schema.organization_name
        assert json_schema.uri
        assert not json_schema.organization_id
        assert not json_schema.id
        assert not json_schema.created_by
        assert not json_schema.created_on
        # WHEN the object is created
        json_schema.store({}, synapse_client=self.syn)
        assert json_schema.name
        assert json_schema.organization_name
        assert json_schema.uri
        assert json_schema.organization_id
        assert json_schema.id
        assert json_schema.created_by
        assert json_schema.created_on
        # AND it should be getable by future instances with the same name
        js2 = JSONSchema(json_schema.name, json_schema.organization_name)
        js2.get(synapse_client=self.syn)
        assert js2.name
        assert js2.organization_name
        assert js2.uri
        assert js2.organization_id
        assert js2.id
        assert js2.created_by
        assert js2.created_on

    async def test_delete(self, organization_with_schema: SchemaOrganization) -> None:
        # GIVEN an organization with 3 schema
        schemas = list(organization_with_schema.get_json_schemas())
        assert len(schemas) == 3
        # WHEN deleting one of those schemas
        schema = schemas[0]
        schema.delete()
        # THEN there should be only two left
        schemas = list(organization_with_schema.get_json_schemas())
        assert len(schemas) == 2

    async def test_delete_version(self, json_schema: JSONSchema) -> None:
        # GIVEN an organization and a JSONSchema
        json_schema.store(schema_body={}, version="0.0.1")
        # THEN that schema should have one version
        js_versions = list(json_schema.get_versions())
        assert len(js_versions) == 1
        # WHEN storing a second version
        json_schema.store(schema_body={}, version="0.0.2")
        # THEN that schema should have two versions
        js_versions = list(json_schema.get_versions())
        assert len(js_versions) == 2
        # AND they should be the ones stored
        versions = [js_version.semantic_version for js_version in js_versions]
        assert versions == ["0.0.1", "0.0.2"]
        # WHEN deleting the first schema version
        json_schema.delete(version="0.0.1")
        # THEN there should only be one version left
        js_versions = list(json_schema.get_versions())
        assert len(js_versions) == 1
        # AND it should be the second version
        versions = [js_version.semantic_version for js_version in js_versions]
        assert versions == ["0.0.2"]

    async def test_get_versions(self, json_schema: JSONSchema) -> None:
        # GIVEN an schema that hasn't been created
        # THEN get_versions should return an empty list
        assert len(list(json_schema.get_versions(synapse_client=self.syn))) == 0
        # WHEN creating a schema with no version
        json_schema.store(schema_body={}, synapse_client=self.syn)
        # THEN get_versions should return an empty list
        assert len(list(json_schema.get_versions(synapse_client=self.syn))) == 0
        # WHEN creating a schema with a version
        json_schema.store(schema_body={}, version="0.0.1", synapse_client=self.syn)
        # THEN get_versions should return that version
        schemas = list(json_schema.get_versions(synapse_client=self.syn))
        assert len(schemas) == 1
        assert schemas[0].semantic_version == "0.0.1"

    async def test_get_body(self, json_schema: JSONSchema) -> None:
        # GIVEN an schema that hasn't been created
        # WHEN creating a schema with 2 version
        first_body = {}
        latest_body = {"description": ""}
        json_schema.store(
            schema_body=first_body, version="0.0.1", synapse_client=self.syn
        )
        json_schema.store(
            schema_body=latest_body, version="0.0.2", synapse_client=self.syn
        )
        # WHEN get_body has no version argument
        body0 = json_schema.get_body(synapse_client=self.syn)
        # THEN the body should be the latest version
        assert body0 == {
            "description": "",
            "$id": f"https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/{json_schema.organization_name}-{json_schema.name}",
        }
        # WHEN get_body has a version argument
        body1 = json_schema.get_body(version="0.0.1", synapse_client=self.syn)
        body2 = json_schema.get_body(version="0.0.2", synapse_client=self.syn)
        # THEN the appropriate body should be returned
        assert body1 == {
            "$id": f"https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/{json_schema.organization_name}-{json_schema.name}-0.0.1",
        }
        assert body2 == {
            "description": "",
            "$id": f"https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/{json_schema.organization_name}-{json_schema.name}-0.0.2",
        }
