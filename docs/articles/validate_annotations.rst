********************
Validate Annotations
********************

.. warning::
    This is a beta implementation and is subject to change.  Use at your own risk.

Validate annotations on your Synapse entities by leveraging the JSON schema services.
Here are the steps you must take to set up the JSON Schema service.


Create a JSON Schema organization
=================================

Set up Synapse client and JSON Schema service::

    import synapseclient
    syn = synapseclient.login()
    syn.get_available_services()  # Output: ['json_schema']
    js = syn.service("json_schema")

Create, manage, and delete a JSON Schema organization::

    my_org = js.JsonSchemaOrganization("test.new")
    my_org  # Output: JsonSchemaOrganization(name='test.new')
    my_org.create()
    my_org.get_acl()
    my_org.set_acl([3413689])
    my_org.update_acl([3324230])
    my_org.delete()

Retrieve existing organization and associated JSON schemas::

    orgs     = js.list_organizations()
    sage_org = js.JsonSchemaOrganization("sage.annotations")
    schemas  = sage_org.list_json_schemas()
    schema1  = next(schemas)
    schema2  = sage_org.get_json_schema(schema1.name)
    assert schema1 is schema2  # True
    schema1  # Output: JsonSchema(org='sage.annotations', name='analysis.alignmentMethod')

Manage a specific version of a JSON schema::

    versions  = schema1.list_versions()
    version1  = next(versions)
    raw_body  = version1.body
    full_body = version1.expand()
    version1  # Output: JsonSchemaVersion(org='sage.annotations', name='analysis.alignmentMethod', version='0.0.2')


Create a new JSON schema version for an existing organization::

    from random import randint
    rint = randint(0, 100000)
    org_name    = "bgrande.test"
    schema_name = "my.schema"

    # Method 1
    my_org          = js.JsonSchemaOrganization(org_name)
    new_version1    = my_org.create_json_schema(raw_body, schema_name, f"0.{rint}.1")

    # Method 2
    my_schema    = js.JsonSchema(my_org, schema_name)
    new_version2 = my_schema.create(raw_body, f"0.{rint}.2")

    # Method 3
    my_version   = js.JsonSchemaVersion(my_org, schema_name, f"0.{rint}.3")
    new_version3 = my_version.create(raw_body)

Test validation on a Synapse entity::

    from time import sleep
    synapse_id = "syn25922647"
    js.bind_json_schema(new_version1.uri, synapse_id)
    js.get_json_schema(synapse_id)
    sleep(3)
    js.validate(synapse_id)
    js.validate_children(synapse_id)
    js.validation_stats(synapse_id)
    js.unbind_json_schema(synapse_id)

Access to low-level API functions::

    js.create_organization(organization_name)
    js.get_organization(organization_name)
    js.list_organizations()
    js.delete_organization(organization_id)
    js.get_organization_acl(organization_id)
    js.update_organization_acl(organization_id, resource_access, etag)
    js.list_json_schemas(organization_name)
    js.list_json_schema_versions(organization_name, json_schema_name)
    js.create_json_schema(json_schema_body, dry_run)
    js.get_json_schema_body(json_schema_uri)
    js.delete_json_schema(json_schema_uri)
    js.json_schema_validation(json_schema_uri)
    js.bind_json_schema_to_entity(synapse_id, json_schema_uri)
    js.get_json_schema_from_entity(synapse_id)
    js.delete_json_schema_from_entity(synapse_id)
    js.validate_entity_with_json_schema(synapse_id)
    js.get_json_schema_validation_statistics(synapse_id)
    js.get_invalid_json_schema_validation(synapse_id)
