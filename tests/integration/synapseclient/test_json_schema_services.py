from random import randint
# from time import sleep
import uuid

import pytest

# import synapseclient
from synapseclient.core.exceptions import SynapseHTTPError


def test_available_services(syn):
    services = syn.get_available_services()  # Output: ['json_schema']
    available_services = ["json_schema"]
    assert set(services) == set(available_services)


@pytest.fixture
def js(syn):
    return syn.service("json_schema")


def test_json_schema_organization(js):
    # Schema organizations must start with a string
    js_org = "a" + uuid.uuid4().hex
    # Create, manage, and delete a JSON Schema organization
    my_org = js.JsonSchemaOrganization(js_org)
    my_org.create()
    created_org = js.get_organization(js_org)
    assert created_org['name'] == js_org

    original_acl = my_org.get_acl()
    new_acl = my_org.update_acl(
        principal_ids=[273949],
        access_type=['READ']
    )
    assert len(new_acl['resourceAccess']) == 2
    remove_public_acl = my_org.set_acl(principal_ids=[js.synapse.getUserProfile()['ownerId']])
    # Use set to reorder the resource access control list
    original_acl['resourceAccess'][0]['accessType'] = set(original_acl['resourceAccess'][0]['accessType'])
    remove_public_acl['resourceAccess'][0]['accessType'] = set(remove_public_acl['resourceAccess'][0]['accessType'])
    assert original_acl['resourceAccess'] == remove_public_acl['resourceAccess']

    my_org.delete()
    # the org should be deleted
    with pytest.raises(SynapseHTTPError, match=f"Organization with name: '{js_org}' not found"):
        js.get_organization(js_org)
    # Retrieve existing organization and associated JSON schemas
    orgs = js.list_organizations()
    assert orgs is not None


def test_json_schema_schemas(js):
    js_org = "a" + uuid.uuid4().hex
    # Create, manage, and delete a JSON Schema organization
    my_org = js.JsonSchemaOrganization(js_org)
    my_org.create()
    schema_name = "my.schema"
    rint = randint(0, 100000)
    simple_schema = {
        "$id": "https://example.com/person.schema.json",
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "title": "Person",
        "type": "object",
        "properties": {
            "firstName": {
                "type": "string",
                "description": "The person's first name."
            },
            "lastName": {
                "type": "string",
                "description": "The person's last name."
            },
            "age": {
                "description": "Age in years which must be equal to or greater than zero.",
                "type": "integer",
                "minimum": 0
            }
        }
    }
    # Create json schema
    new_version1 = my_org.create_json_schema(simple_schema, schema_name, f"0.{rint}.1")

    schemas = my_org.list_json_schemas()
    schema1 = next(schemas)
    schema2 = my_org.get_json_schema(schema1.name)
    assert schema1 is schema2
    # Manage a specific version of a JSON schema
    versions = schema1.list_versions()
    version1 = next(versions)
    raw_body = version1.body
    full_body = version1.expand()
    assert raw_body['properties'] == simple_schema['properties']
    assert full_body['properties'] == simple_schema['properties']

    new_version1.delete()
    my_org.delete()

    # # Create a new JSON schema version for an existing organization
    # rint = randint(0, 100000)
    # org_name = "bgrande.test"
    # schema_name = "my.schema"

    # # Method 1
    # my_org          = js.JsonSchemaOrganization(org_name)
    # new_version1    = my_org.create_json_schema(raw_body, schema_name, f"0.{rint}.1")

    # # Method 2
    # my_schema    = js.JsonSchema(my_org, schema_name)
    # new_version2 = my_schema.create(raw_body, f"0.{rint}.2")

    # # Method 3
    # my_version   = js.JsonSchemaVersion(my_org, schema_name, f"0.{rint}.3")
    # new_version3 = my_version.create(raw_body)

    # # Test validation on a Synapse entity
    # synapse_id = "syn25922647"
    # project_name = uuid.uuid4().hex
    # project = synapseclient.Project(name=project_name)
    # project = syn.store(project)
    # synapse_id = project.id
    # schedule_for_cleanup(project)

    # js.bind_json_schema(new_version1.uri, synapse_id)
    # js.get_json_schema(synapse_id)
    # sleep(3)

    # js.validate(synapse_id)
    # js.validate_children(synapse_id)
    # js.validation_stats(synapse_id)
    # js.unbind_json_schema(synapse_id)
