"""Integration tests for SchemaOrganization and JSONSchema classes"""
import uuid
from typing import Any, Optional

import pytest

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


def org_exists(name: str, synapse_client: Optional[Synapse] = None) -> bool:
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
async def fixture_module_organization(request) -> SchemaOrganization:
    """
    Returns a created organization at the module scope. Used to hold JSON Schemas created by tests.
    """
    name = create_test_entity_name()
    org = SchemaOrganization(name)
    await org.store_async()

    async def delete_org():
        for schema in org.get_json_schema_list():
            await schema.delete_async()
        await org.delete_async()

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


@pytest.fixture(name="organization", scope="function")
async def fixture_organization(syn: Synapse, request) -> SchemaOrganization:
    """
    Returns a Synapse organization.
    """
    name = create_test_entity_name()
    org = SchemaOrganization(name)

    async def delete_org():
        if org_exists(name, syn):
            org.delete_async()

    request.addfinalizer(delete_org)

    return org


@pytest.fixture(name="organization_with_schema", scope="function")
async def fixture_organization_with_schema(request) -> SchemaOrganization:
    """
    Returns a Synapse organization.
    As Cleanup it checks for JSON Schemas and deletes them"""

    name = create_test_entity_name()
    org = SchemaOrganization(name)
    await org.store_async()
    js1 = JSONSchema("schema1", name)
    js2 = JSONSchema("schema2", name)
    js3 = JSONSchema("schema3", name)
    # TODO: Change to create_async when method is working
    js1.store({})
    js2.store({})
    js3.store({})

    async def delete_org():
        for schema in org.get_json_schema_list_async():
            await schema.delete_async()
        await org.delete_async()

    request.addfinalizer(delete_org)

    return org


class TestSchemaOrganization:
    """Synchronous integration tests for SchemaOrganization."""

    @pytest.fixture(autouse=True, scope="function")
    def init(self, syn: Synapse) -> None:
        self.syn = syn

    @pytest.mark.asyncio
    async def test_create_and_get(self, organization: SchemaOrganization) -> None:
        # GIVEN an initialized organization object that hasn't been created in Synapse
        # THEN it shouldn't have any metadata besides it's name
        assert organization.name is not None
        assert organization.id is None
        assert organization.created_by is None
        assert organization.created_on is None
        # AND it shouldn't exists in Synapse
        assert not org_exists(organization.name, synapse_client=self.syn)
        # WHEN I create the organization the metadata will be saved
        await organization.store_async(synapse_client=self.syn)
        assert organization.name is not None
        assert organization.id is not None
        assert organization.created_by is not None
        assert organization.created_on is not None
        # AND it should exist in Synapse
        assert org_exists(organization.name, synapse_client=self.syn)
        # AND it should be getable by future instances with the same name
        org2 = SchemaOrganization(organization.name)
        await org2.get_async(synapse_client=self.syn)
        assert organization.name is not None
        assert organization.id is not None
        assert organization.created_by is not None
        assert organization.created_on is not None

    @pytest.mark.asyncio
    async def test_get_json_schema_list(
        self,
        organization: SchemaOrganization,
        organization_with_schema: SchemaOrganization,
    ) -> None:
        # GIVEN an organization with no schemas and one with 3 schemas
        await organization.store_async(synapse_client=self.syn)
        # THEN get_json_schema_list should return the correct list of schemas
        schema_list = await organization.get_json_schema_list_async(
            synapse_client=self.syn
        )
        assert not schema_list
        schema_list2 = await organization_with_schema.get_json_schema_list_async(
            synapse_client=self.syn
        )
        assert len(schema_list2) == 3

    @pytest.mark.asyncio
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
        resource_access.append({"principalId": 1, "accessType": ["READ"]})
        etag = acl["etag"]
        # AND updating the acl
        await organization.update_acl_async(
            resource_access, etag, synapse_client=self.syn
        )
        # THEN the resource access should be have two principals
        acl = await organization.get_acl_async(synapse_client=self.syn)
        assert len(acl["resourceAccess"]) == 2


# TODO: Add JSONSchema async tests once create_async is working
