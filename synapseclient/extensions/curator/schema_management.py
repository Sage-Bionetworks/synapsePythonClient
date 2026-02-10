"""
Wrapper functions for JSON Schema registration and binding operations.

This module provides convenience functions for CLI commands that interact with
the Synapse JSON Schema OOP models.
"""

import json
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse


def register_jsonschema(
    schema_path: str,
    organization_name: str,
    schema_name: str,
    schema_version: Optional[str] = None,
    synapse_client: Optional["Synapse"] = None,
) -> tuple[str, str]:
    """
    Register a JSON schema to a Synapse organization.

    This function loads a JSON schema from a file and registers it with a specified
    organization in Synapse using the JSONSchema OOP model.

    Arguments:
        schema_path: Path to the JSON schema file to register
        organization_name: Name of the organization to register the schema under
        schema_name: The name of the JSON schema
        schema_version: Optional version of the schema (e.g., '0.0.1').
                       If not specified, a version will be auto-generated.
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        A tuple of (schema_uri, message) where:
        - schema_uri is the full URI of the registered schema
        - message is a success message

    Example: Register a JSON schema
        ```python
        from synapseclient import Synapse
        from synapseclient.extensions.curator import register_jsonschema

        syn = Synapse()
        syn.login()

        schema_uri, message = register_jsonschema(
            schema_path="/path/to/schema.json",
            organization_name="my.org",
            schema_name="my.schema",
            schema_version="0.0.1",
            synapse_client=syn
        )
        print(message)
        print(f"Schema URI: {schema_uri}")
        ```
    """
    from synapseclient.models.schema_organization import JSONSchema

    # Load the schema from file
    with open(schema_path, "r") as f:
        schema_body = json.load(f)

    # Create JSONSchema instance
    json_schema = JSONSchema(name=schema_name, organization_name=organization_name)

    # Store the schema with optional version
    json_schema.store(
        schema_body=schema_body,
        version=schema_version,
        synapse_client=synapse_client,
    )

    # Get the schema URI from the JSONSchema object
    schema_uri = json_schema.uri

    message = f"Successfully registered schema '{schema_name}' to organization '{organization_name}'"

    return schema_uri, message


def bind_jsonschema(
    entity_id: str,
    json_schema_uri: str,
    enable_derived_annotations: bool = False,
    synapse_client: Optional["Synapse"] = None,
) -> dict:
    """
    Bind a JSON schema to a Synapse entity.

    This function binds a JSON schema to a Synapse entity using the Entity OOP model's
    bind_schema method.

    Arguments:
        entity_id: The Synapse ID of the entity to bind the schema to (e.g., syn12345678)
        json_schema_uri: The URI of the JSON Schema to bind (e.g., 'my.org-schema.name-1.0.0')
        enable_derived_annotations: If true, enable derived annotations. Defaults to False.
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        A dictionary containing the binding details

    Example: Bind a JSON schema to an entity
        ```python
        from synapseclient import Synapse
        from synapseclient.extensions.curator import bind_jsonschema

        syn = Synapse()
        syn.login()

        result = bind_jsonschema(
            entity_id="syn12345678",
            json_schema_uri="my.org-my.schema-0.0.1",
            enable_derived_annotations=True,
            synapse_client=syn
        )
        print(f"Successfully bound schema: {result}")
        ```
    """
    from synapseclient import Synapse

    syn = Synapse.get_client(synapse_client=synapse_client)

    # Get the entity to determine its type and use its bind_schema method
    entity = syn.get(entity_id, downloadFile=False)

    # Use the entity's bind_schema method if available (new OOP models)
    if hasattr(entity, "bind_schema"):
        result = entity.bind_schema(
            json_schema_uri=json_schema_uri,
            enable_derived_annotations=enable_derived_annotations,
            synapse_client=syn,
        )
    else:
        # Fallback to direct API call for old-style entities
        from synapseclient.api.json_schema_services import bind_json_schema_to_entity
        import asyncio

        result = asyncio.run(
            bind_json_schema_to_entity(
                synapse_id=entity_id,
                json_schema_uri=json_schema_uri,
                enable_derived_annotations=enable_derived_annotations,
                synapse_client=syn,
            )
        )

    # Convert result to dictionary format for consistent return type
    if hasattr(result, "__dict__"):
        return vars(result)
    return result
