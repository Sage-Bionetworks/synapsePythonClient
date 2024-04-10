from random import randint
from time import sleep
import uuid

import pytest

import synapseclient
from synapseclient.core.exceptions import SynapseHTTPError


def test_available_services(syn):
    services = syn.get_available_services()  # Output: ['json_schema']
    available_services = ["json_schema"]
    assert set(services) == set(available_services)


@pytest.fixture
def js(syn):
    return syn.service("json_schema")


@pytest.mark.flaky(reruns=3, only_rerun=["SynapseHTTPError"])
def test_json_schema_organization(js):
    # Schema organizations must start with a string
    js_org = "a" + str(uuid.uuid4()).replace("-", "")
    # Create, manage, and delete a JSON Schema organization
    my_org = js.JsonSchemaOrganization(js_org)
    my_org.create()
    created_org = js.get_organization(js_org)
    assert created_org["name"] == js_org

    original_acl = my_org.get_acl()
    new_acl = my_org.update_acl(principal_ids=[273949], access_type=["READ"])
    assert len(new_acl["resourceAccess"]) == 2
    remove_public_acl = my_org.set_acl(
        principal_ids=[js.synapse.getUserProfile()["ownerId"]]
    )
    # Use set to reorder the resource access control list
    original_acl["resourceAccess"][0]["accessType"] = set(
        original_acl["resourceAccess"][0]["accessType"]
    )
    remove_public_acl["resourceAccess"][0]["accessType"] = set(
        remove_public_acl["resourceAccess"][0]["accessType"]
    )
    assert original_acl["resourceAccess"] == remove_public_acl["resourceAccess"]

    my_org.delete()
    # the org should be deleted
    with pytest.raises(
        SynapseHTTPError, match=f"Organization with name: '{js_org}' not found"
    ):
        js.get_organization(js_org)
    # Retrieve existing organization and associated JSON schemas
    orgs = js.list_organizations()
    assert orgs is not None


class TestJsonSchemaSchemas:
    @pytest.fixture(autouse=True, scope="function")
    def setup(self, js):
        self.js_org = "a" + str(uuid.uuid4()).replace("-", "")
        # Create, manage, and delete a JSON Schema organization
        self.my_org = js.JsonSchemaOrganization(self.js_org)
        self.my_org.create()
        self.schema_name = "my.schema" + str(uuid.uuid4()).replace("-", "")
        self.rint = randint(0, 100000)
        self.simple_schema = {
            "$id": "https://example.com/person.schema.json",
            "$schema": "https://json-schema.org/draft/2020-12/schema",
            "title": "Person",
            "type": "object",
            "properties": {
                "firstName": {
                    "type": "string",
                    "description": "The person's first name.",
                },
                "lastName": {
                    "type": "string",
                    "description": "The person's last name.",
                },
                "age": {
                    "description": "Age in years which must be equal to or greater than zero.",
                    "type": "integer",
                    "minimum": 0,
                },
            },
        }

    def teardown(self):
        self.my_org.delete()

    def test_json_schema_schemas_org_create_schema(self):
        # Create json schema
        new_version = self.my_org.create_json_schema(
            self.simple_schema, self.schema_name, f"0.{self.rint}.1"
        )
        schemas = self.my_org.list_json_schemas()
        schema1 = next(schemas)
        schema2 = self.my_org.get_json_schema(schema1.name)
        assert schema1 is schema2
        # Manage a specific version of a JSON schema
        versions = schema1.list_versions()
        version1 = next(versions)
        raw_body = version1.body
        full_body = version1.expand()
        assert raw_body["properties"] == self.simple_schema["properties"]
        assert full_body["properties"] == self.simple_schema["properties"]
        new_version.delete()

    def test_json_schema_schemas_js_create_schema(self, js):
        # Create json schema
        # Version 2 of creating json schema
        my_schema = js.JsonSchema(self.my_org, self.schema_name)
        new_version = my_schema.create(self.simple_schema, f"0.{self.rint}.2")
        schemas = self.my_org.list_json_schemas()
        schema1 = next(schemas)
        schema2 = self.my_org.get_json_schema(schema1.name)
        assert schema1 is schema2
        new_version.delete()

    def test_json_schema_schemas_js_version_create_schema(self, js):
        # Create json schema
        # Version 3 of creating json schema
        my_version = js.JsonSchemaVersion(
            self.my_org, self.schema_name, f"0.{self.rint}.3"
        )
        new_version = my_version.create(self.simple_schema)
        schemas = self.my_org.list_json_schemas()
        schema1 = next(schemas)
        schema2 = self.my_org.get_json_schema(schema1.name)
        assert schema1 is schema2
        new_version.delete()

    def test_json_schema_validate(self, js, syn, schedule_for_cleanup):
        project_name = str(uuid.uuid4()).replace("-", "")
        project = synapseclient.Project(name=project_name)
        project.firstName = "test"
        project = syn.store(project)
        synapse_id = project.id
        schedule_for_cleanup(project)

        new_version = self.my_org.create_json_schema(
            self.simple_schema, self.schema_name, f"0.{self.rint}.1"
        )
        js.bind_json_schema(new_version.uri, synapse_id)
        sleep(3)
        # TODO: look into why this doesn't work
        # js.validate(synapse_id)
        js.validate_children(synapse_id)
        stats = js.validation_stats(synapse_id)
        assert stats["containerId"] == synapse_id
        assert stats["totalNumberOfChildren"] == 0
        assert stats["numberOfValidChildren"] == 0
        assert stats["numberOfInvalidChildren"] == 0
        assert stats["numberOfUnknownChildren"] == 0

        js.unbind_json_schema(synapse_id)
        new_version.delete()
