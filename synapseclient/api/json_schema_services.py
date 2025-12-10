"""
JSON Schema Services for Synapse

This module provides functions for interacting with JSON schemas in Synapse,
including managing organizations, schemas, and entity bindings.
"""

import json
from typing import (
    TYPE_CHECKING,
    Any,
    AsyncGenerator,
    Dict,
    Generator,
    List,
    Mapping,
    Optional,
    Sequence,
    Union,
)

from synapseclient.api.api_client import rest_post_paginated_async

if TYPE_CHECKING:
    from synapseclient import Synapse


async def bind_json_schema_to_entity(
    synapse_id: str,
    json_schema_uri: str,
    *,
    enable_derived_annotations: bool = False,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    <https://rest-docs.synapse.org/rest/PUT/entity/id/schema/binding.html>
    Bind a JSON schema to a Synapse entity.


    This creates a binding between an entity and a JSON schema, which enables
    schema validation for the entity. When bound, the entity's annotations
    will be validated against the schema requirements.

    Arguments:
        synapse_id: The Synapse ID of the entity to bind the schema to
        json_schema_uri: The $id URI of the JSON schema to bind (e.g., "my.org-schema.name-1.0.0")
        enable_derived_annotations: If True, enables automatic generation of derived annotations
                                   from the schema for this entity. Defaults to False.
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        A JsonSchemaObjectBinding object containing the binding details.
        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchemaObjectBinding.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    request_body = {
        "entityId": synapse_id,
        "schema$id": json_schema_uri,
        "enableDerivedAnnotations": enable_derived_annotations,
    }
    return await client.rest_put_async(
        uri=f"/entity/{synapse_id}/schema/binding", body=json.dumps(request_body)
    )


async def get_json_schema_from_entity(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Any]:
    """
    Get the JSON schema binding for a Synapse entity.

    <https://rest-docs.synapse.org/rest/GET/entity/id/schema/binding.html>


    Retrieves information about any JSON schema that is currently bound to the specified entity.

    Arguments:
        synapse_id: The Synapse ID of the entity to check for schema bindings
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        A JsonSchemaObjectBinding object if a schema is bound, containing:
        - entityId: The entity ID
        - schema$id: The URI of the bound schema
        - enableDerivedAnnotations: Whether derived annotations are enabled

        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchemaObjectBinding.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/entity/{synapse_id}/schema/binding")


async def delete_json_schema_from_entity(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> None:
    """
    <https://rest-docs.synapse.org/rest/DELETE/entity/id/schema/binding.html>

    Remove the JSON schema binding from a Synapse entity.

    This unbinds any JSON schema from the specified entity, removing schema validation
    requirements and stopping the generation of derived annotations.

    Arguments:
        synapse_id: The Synapse ID of the entity to unbind the schema from
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_delete_async(uri=f"/entity/{synapse_id}/schema/binding")


async def validate_entity_with_json_schema(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Union[str, bool]]:
    """
    <https://rest-docs.synapse.org/rest/GET/entity/id/schema/validation.html>

    Validate a Synapse entity against its bound JSON schema.

    Checks whether the entity's annotations conform to the requirements of its bound JSON schema.
    The entity must have a schema binding for this operation to work.

    Arguments:
        synapse_id: The Synapse ID of the entity to validate
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/ValidationResults.html>

        A ValidationResults object containing:
        - objectId: The entity ID that was validated
        - objectType: The type of object (typically "entity")
        - isValid: Boolean indicating if the entity passes validation
        - validatedOn: Timestamp of when validation was performed
        - validationErrorMessage: Error details if validation failed
        - validationException: Exception details if validation failed

    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/entity/{synapse_id}/schema/validation")


async def get_json_schema_validation_statistics(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Union[str, int]]:
    """
    Get validation statistics for a container entity (Project or Folder).

    Returns summary statistics about JSON schema validation results for all child entities
    of the specified container that have schema bindings.

    <https://rest-docs.synapse.org/rest/GET/entity/id/schema/validation/statistics.html>

    Arguments:
        synapse_id: The Synapse ID of the container entity (Project or Folder)
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        A ValidationSummaryStatistics object containing:
        - containerId: The container entity ID
        - totalNumberOfChildren: Total child entities in the container
        - numberOfValidChildren: Number of children that pass validation
        - numberOfInvalidChildren: Number of children that fail validation
        - numberOfUnknownChildren: Number of children with unknown validation status
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(
        uri=f"/entity/{synapse_id}/schema/validation/statistics"
    )


async def get_invalid_json_schema_validation(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    <https://rest-docs.synapse.org/rest/POST/entity/id/schema/validation/invalid.html>


    Get all invalid JSON schema validation results for a container entity.

    Returns detailed validation results for all child entities of the specified container
    that fail their JSON schema validation. Results are paginated automatically.

    Arguments:
        synapse_id: The Synapse ID of the container entity (Project or Folder)
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Yields:
        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/ValidationResults.html>

        ValidationResults objects for each invalid child entity, containing:
        - objectId: The child entity ID
        - objectType: The type of object
        - isValid: Always False for this endpoint
        - validationErrorMessage: Details about why validation failed
        - validationException: Exception details
    """
    request_body = {"containerId": synapse_id}
    response = rest_post_paginated_async(
        f"/entity/{synapse_id}/schema/validation/invalid",
        body=request_body,
        synapse_client=synapse_client,
    )
    async for item in response:
        yield item


def get_invalid_json_schema_validation_sync(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> Generator[Dict[str, Any], None, None]:
    """
    <https://rest-docs.synapse.org/rest/POST/entity/id/schema/validation/invalid.html>

    Get a single page of invalid JSON schema validation results for a container entity
    (Project or Folder).


    Arguments:
        synapse_id: The Synapse ID of the container entity (Project or Folder)
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Yields:
        ValidationResults objects for each invalid child entity.
        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/ValidationResults.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {"containerId": synapse_id}
    response = client._POST_paginated(
        f"/entity/{synapse_id}/schema/validation/invalid", request_body
    )
    for item in response:
        yield item


async def get_json_schema_derived_keys(
    synapse_id: str, *, synapse_client: Optional["Synapse"] = None
) -> List[str]:
    """
    <https://rest-docs.synapse.org/rest/GET/entity/id/derivedKeys.html>

    Get the derived annotation keys for a Synapse entity with a bound JSON schema.

    When an entity has a JSON schema binding with derived annotations enabled,
    Synapse can automatically generate annotation keys based on the schema structure.
    This function retrieves those derived keys.

    Arguments:
        synapse_id: The Synapse ID of the entity to get derived keys for
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        A Keys object containing a list of derived annotation key names.
        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/annotation/v2/Keys.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    return await client.rest_get_async(uri=f"/entity/{synapse_id}/derivedKeys")


async def create_organization(
    organization_name: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Any]:
    """
    Create a new JSON schema organization.

    Creates a new organization with a unique name that will serve as a namespace for JSON schemas.
    The new organization will have an auto-generated AccessControlList (ACL) granting the caller
    all relevant permissions. Organization names must be at least 6 characters and follow specific
    naming conventions.

    Arguments:
        organization_name: Unique name for the organization. Must be at least 6 characters,
                          cannot start with a number, and should follow dot-separated alphanumeric
                          format (e.g., "my.organization")
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        An Organization object containing:
        - id: The numeric identifier of the organization
        - name: The organization name
        - createdOn: Creation timestamp
        - createdBy: ID of the user who created the organization

        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/Organization.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {"organizationName": organization_name}

    return await client.rest_post_async(
        uri="/schema/organization", body=json.dumps(request_body)
    )


async def get_organization(
    organization_name: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Any]:
    """
    Get an organization by name.

    Looks up an existing JSON schema organization by its unique name.

    Arguments:
        organization_name: The name of the organization to retrieve
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        An Organization object containing:
        - id: The numeric identifier of the organization
        - name: The organization name
        - createdOn: Creation timestamp
        - createdBy: ID of the user who created the organization

        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/Organization.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    return await client.rest_get_async(
        uri=f"/schema/organization?name={organization_name}"
    )


async def list_organizations(
    *, synapse_client: Optional["Synapse"] = None
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Generator to list all JSON schema organizations.

    Retrieves a list of all organizations that are visible to the caller. This operation
    does not require authentication and will return all publicly visible organizations.

    Arguments:
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        A generator of Organization objects, each containing:
        - id: The numeric identifier of the organization
        - name: The organization name
        - createdOn: Creation timestamp
        - createdBy: ID of the user who created the organization

        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/Organization.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {}

    async for item in rest_post_paginated_async(
        "/schema/organization/list", body=request_body, synapse_client=client
    ):
        yield item


def list_organizations_sync(
    *, synapse_client: Optional["Synapse"] = None
) -> Generator[Dict[str, Any], None, None]:
    """
    Generator to list all JSON schema organizations.

    Retrieves a list of all organizations that are visible to the caller. This operation
    does not require authentication and will return all publicly visible organizations.

    Arguments:
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        A generator of Organization objects, each containing:
        - id: The numeric identifier of the organization
        - name: The organization name
        - createdOn: Creation timestamp
        - createdBy: ID of the user who created the organization

        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/Organization.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {}

    for item in client._POST_paginated("/schema/organization/list", body=request_body):
        yield item


async def delete_organization(
    organization_id: str, *, synapse_client: Optional["Synapse"] = None
) -> None:
    """
    Delete a JSON schema organization.

    Deletes the specified organization. All schemas defined within the organization's
    namespace must be deleted first before the organization can be deleted. The caller
    must have ACCESS_TYPE.DELETE permission on the organization.

    Arguments:
        organization_id: The numeric identifier of the organization to delete
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    await client.rest_delete_async(uri=f"/schema/organization/{organization_id}")


async def get_organization_acl(
    organization_id: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Any]:
    """
    Get the Access Control List (ACL) for a JSON schema organization.

    Retrieves the permissions associated with the specified organization. The caller
    must have ACCESS_TYPE.READ permission on the organization to view its ACL.

    Arguments:
        organization_id: The numeric identifier of the organization
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        An AccessControlList object containing:
        - id: The organization ID
        - creationDate: The date the ACL was created
        - etag: The etag for concurrency control
        - resourceAccess: List of ResourceAccess objects with principalId and accessType arrays matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/ResourceAccess.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    return await client.rest_get_async(
        uri=f"/schema/organization/{organization_id}/acl"
    )


async def update_organization_acl(
    organization_id: str,
    resource_access: Sequence[Mapping[str, Sequence[str]]],
    etag: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Dict[str, Any]:
    """
    Update the Access Control List (ACL) for a JSON schema organization.

    Updates the permissions for the specified organization. The caller must have
    ACCESS_TYPE.CHANGE_PERMISSIONS permission on the organization. The etag from
    a previous get_organization_acl() call is required for concurrency control.

    Arguments:
        organization_id: The numeric identifier of the organization
        resource_access: List of ResourceAccess objects, each containing:
                        - principalId: The user or team ID
                        - accessType: List of permission types (e.g., ["READ", "CREATE", "DELETE"])
        etag: The etag from get_organization_acl() for concurrency control
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        The updated AccessControlList object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/AccessControlList.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {"resourceAccess": resource_access, "etag": etag}

    return await client.rest_put_async(
        uri=f"/schema/organization/{organization_id}/acl", body=json.dumps(request_body)
    )


async def list_json_schemas(
    organization_name: str, *, synapse_client: Optional["Synapse"] = None
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    List all JSON schemas for an organization.

    Retrieves all JSON schemas that belong to the specified organization. This operation
    does not require authentication and will return all publicly visible schemas.

    Arguments:
        organization_name: The name of the organization to list schemas for
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        A generator of JsonSchemaInfo objects, each containing:
        - organizationId: The Synapse issued numeric identifier for the organization.
        - organizationName: The name of the organization to which this schema belongs.
        - schemaId: The Synapse issued numeric identifier for the schema.
        - schemaName: The name of the this schema.
        - createdOn: The date this JSON schema was created.
        - createdBy: The ID of the user that created this JSON schema.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {"organizationName": organization_name}

    async for item in rest_post_paginated_async(
        "/schema/list", body=request_body, synapse_client=client
    ):
        yield item


def list_json_schemas_sync(
    organization_name: str, *, synapse_client: Optional["Synapse"] = None
) -> Generator[Dict[str, Any], None, None]:
    """
    List all JSON schemas for an organization.

    Retrieves all JSON schemas that belong to the specified organization. This operation
    does not require authentication and will return all publicly visible schemas.

    Arguments:
        organization_name: The name of the organization to list schemas for
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        A generator of JsonSchemaInfo objects, each containing:
        - organizationId: The Synapse issued numeric identifier for the organization.
        - organizationName: The name of the organization to which this schema belongs.
        - schemaId: The Synapse issued numeric identifier for the schema.
        - schemaName: The name of the this schema.
        - createdOn: The date this JSON schema was created.
        - createdBy: The ID of the user that created this JSON schema.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {"organizationName": organization_name}

    for item in client._POST_paginated("/schema/list", body=request_body):
        yield item


async def list_json_schema_versions(
    organization_name: str,
    json_schema_name: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    List version information for a JSON schema.

    Retrieves version information for all versions of the specified JSON schema within
    an organization. This shows the history and available versions of a schema.

    Arguments:
        organization_name: The name of the organization containing the schema
        json_schema_name: The name of the JSON schema to list versions for
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        A generator of JsonSchemaVersionInfo objects, each containing:
        - organizationId: The Synapse issued numeric identifier for the organization.
        - organizationName: The name of the organization to which this schema belongs.
        - schemaName: The name of the this schema.
        - schemaId: The Synapse issued numeric identifier for the schema.
        - versionId: The Synapse issued numeric identifier for this version.
        - $id: The full '$id' of this schema version
        - semanticVersion: The semantic version label provided when this version was created. Can be null if a semantic version was not provided when this version was created.
        - createdOn: The date this JSON schema version was created.
        - createdBy: The ID of the user that created this JSON schema version.
        - jsonSHA256Hex: The SHA-256 hexadecimal hash of the UTF-8 encoded JSON schema.

        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchemaVersionInfo.html>
    """
    request_body = {
        "organizationName": organization_name,
        "schemaName": json_schema_name,
    }

    async for item in rest_post_paginated_async(
        "/schema/version/list", body=request_body, synapse_client=synapse_client
    ):
        yield item


def list_json_schema_versions_sync(
    organization_name: str,
    json_schema_name: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Generator[Dict[str, Any], None, None]:
    """
    List version information for a JSON schema.

    Retrieves version information for all versions of the specified JSON schema within
    an organization. This shows the history and available versions of a schema.

    Arguments:
        organization_name: The name of the organization containing the schema
        json_schema_name: The name of the JSON schema to list versions for
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        A generator of JsonSchemaVersionInfo objects, each containing:
        - organizationId: The Synapse issued numeric identifier for the organization.
        - organizationName: The name of the organization to which this schema belongs.
        - schemaName: The name of the this schema.
        - schemaId: The Synapse issued numeric identifier for the schema.
        - versionId: The Synapse issued numeric identifier for this version.
        - $id: The full '$id' of this schema version
        - semanticVersion: The semantic version label provided when this version was created. Can be null if a semantic version was not provided when this version was created.
        - createdOn: The date this JSON schema version was created.
        - createdBy: The ID of the user that created this JSON schema version.
        - jsonSHA256Hex: The SHA-256 hexadecimal hash of the UTF-8 encoded JSON schema.

        Object matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchemaVersionInfo.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {
        "organizationName": organization_name,
        "schemaName": json_schema_name,
    }

    for item in client._POST_paginated("/schema/version/list", body=request_body):
        yield item


async def get_json_schema_body(
    json_schema_uri: str, *, synapse_client: Optional["Synapse"] = None
) -> Dict[str, Any]:
    """
    Get a registered JSON schema by its $id URI.

    Retrieves the full JSON schema content using its unique $id identifier. This operation
    does not require authentication for publicly registered schemas.

    Arguments:
        json_schema_uri: The relative $id of the JSON schema to get (e.g., "my.org-schema.name-1.0.0")
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        The complete JSON schema object as a dictionary, matching <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchema.html>
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    return await client.rest_get_async(uri=f"/schema/type/registered/{json_schema_uri}")


async def delete_json_schema(
    json_schema_uri: str, *, synapse_client: Optional["Synapse"] = None
) -> None:
    """
    Delete a JSON schema by its $id URI.

    Deletes the specified JSON schema. If the $id excludes a semantic version, all versions
    of the schema will be deleted. If the $id includes a semantic version, only that specific
    version will be deleted. The caller must have ACCESS_TYPE.DELETE permission on the
    schema's organization.

    Arguments:
        json_schema_uri: The $id URI of the schema to delete. Examples:
                        - "my.org-schema.name" (deletes all versions)
                        - "my.org-schema.name-1.0.0" (deletes only version 1.0.0)
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    await client.rest_delete_async(uri=f"/schema/type/registered/{json_schema_uri}")
