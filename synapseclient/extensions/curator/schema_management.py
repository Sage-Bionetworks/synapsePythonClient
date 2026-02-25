"""
Wrapper functions for JSON Schema registration and binding operations.

This module provides convenience functions for CLI commands that interact with
the Synapse JSON Schema OOP models.
"""

import json
import re
from typing import TYPE_CHECKING, Optional

from synapseclient.core.async_utils import wrap_async_to_sync

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models.mixins.json_schema import JSONSchemaBinding
    from synapseclient.models.schema_organization import JSONSchema


def register_jsonschema(
    schema_path: str,
    organization_name: str,
    schema_name: str,
    fix_schema_name: bool = False,
    schema_version: Optional[str] = None,
    synapse_client: Optional["Synapse"] = None,
) -> "JSONSchema":
    """
    Register a JSON schema to a Synapse organization.

    This function loads a JSON schema from a file and registers it with a specified
    organization in Synapse using the JSONSchema OOP model.

    Arguments:
        schema_path: Path to the JSON schema file to register
        organization_name: Name of the organization to register the schema under
        schema_name: Name of the JSON schema
        fix_schema_name: If True, fixes the schema name to meet Synapse requirements by replacing
            dashes and underscores with periods. Defaults to False.
        schema_version: Optional version of the schema (e.g., '0.0.1').
                       If not specified, a version will be auto-generated.
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        The registered JSONSchema object

    Example: Register a JSON schema
        ```python
        from synapseclient import Synapse
        from synapseclient.extensions.curator import register_jsonschema

        syn = Synapse()
        syn.login()

        json_schema = register_jsonschema(
            schema_path="/path/to/schema.json",
            organization_name="my.org",
            schema_name="my.schema",
            fix_schema_name=True,
            schema_version="0.0.1",
            synapse_client=syn
        )
        print(f"Registered schema URI: {json_schema.uri}")
        print(f"Schema version: {json_schema.version}")
        ```
    """
    return wrap_async_to_sync(
        coroutine=register_jsonschema_async(
            schema_path=schema_path,
            organization_name=organization_name,
            fix_schema_name=fix_schema_name,
            schema_name=schema_name,
            schema_version=schema_version,
            synapse_client=synapse_client,
        )
    )


async def register_jsonschema_async(
    schema_path: str,
    organization_name: str,
    schema_name: str,
    fix_schema_name: bool = False,
    schema_version: Optional[str] = None,
    synapse_client: Optional["Synapse"] = None,
) -> "JSONSchema":
    """
    Register a JSON schema to a Synapse organization asynchronously.

    This function loads a JSON schema from a file and registers it with a specified
    organization in Synapse using the JSONSchema OOP model.

    Arguments:
        schema_path: Path to the JSON schema file to register
        organization_name: Name of the organization to register the schema under
        schema_name: The name of the JSON schema
        fix_schema_name: If True, fixes the schema name to meet Synapse requirements by replacing
            dashes and underscores with periods. Defaults to False.
        schema_version: Optional version of the schema (e.g., '0.0.1').
                       If not specified, a version will be auto-generated.
        synapse_client: If not passed in and caching was not disabled by
                       `Synapse.allow_client_caching(False)` this will use the last created
                       instance from the Synapse class constructor

    Returns:
        The registered JSONSchema object

    Example: Register a JSON schema
        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.extensions.curator import register_jsonschema_async

        syn = Synapse()
        syn.login()

        json_schema = asyncio.run(register_jsonschema_async(
            schema_path="/path/to/schema.json",
            organization_name="my.org",
            schema_name="my.schema",
            fix_schema_name=True,
            schema_version="0.0.1",
            synapse_client=syn
        ))
        print(f"Registered schema URI: {json_schema.uri}")
        print(f"Schema version: {json_schema.version}")
        ```
    """
    from synapseclient import Synapse
    from synapseclient.models.schema_organization import JSONSchema

    syn = Synapse.get_client(synapse_client=synapse_client)

    if fix_schema_name:
        old_name = schema_name
        schema_name = fix_name(schema_name)
        syn.logger.info(f"Changed schema name from '{old_name}' to '{schema_name}' ")

    with open(schema_path, "r") as f:
        schema_body = json.load(f)

    json_schema = JSONSchema(name=schema_name, organization_name=organization_name)

    await json_schema.store_async(
        schema_body=schema_body,
        version=schema_version,
        synapse_client=syn,
    )

    syn.logger.info(
        f"Successfully registered schema '{schema_name}' to organization '{organization_name}'"
    )
    syn.logger.info(f"Schema URI: {json_schema.uri}")

    return json_schema


def fix_name(name: str) -> str:
    """
    Fixes a schema name to meet Synapse requirements by:
      - replacing dashes and underscores with periods.
      - collapsing multiple consecutive periods into a single period.

    Arguments:
        name: The original schema name

    Returns:
        The fixed schema name

    """
    name = name.replace("-", ".").replace("_", ".")
    name = re.sub(r"\.+", ".", name)
    return name


def bind_jsonschema(
    entity_id: str,
    json_schema_uri: str,
    enable_derived_annotations: bool = False,
    synapse_client: Optional["Synapse"] = None,
) -> "JSONSchemaBinding":
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
        The JSONSchemaBinding object containing the binding details

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
    return wrap_async_to_sync(
        coroutine=bind_jsonschema_async(
            entity_id=entity_id,
            json_schema_uri=json_schema_uri,
            enable_derived_annotations=enable_derived_annotations,
            synapse_client=synapse_client,
        )
    )


async def bind_jsonschema_async(
    entity_id: str,
    json_schema_uri: str,
    enable_derived_annotations: bool = False,
    synapse_client: Optional["Synapse"] = None,
) -> "JSONSchemaBinding":
    """
    Bind a JSON schema to a Synapse entity asynchronously.

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
        The JSONSchemaBinding object containing the binding details

    Example: Bind a JSON schema to an entity
        ```python
        import asyncio
        from synapseclient import Synapse
        from synapseclient.extensions.curator import bind_jsonschema_async

        syn = Synapse()
        syn.login()

        result = asyncio.run(bind_jsonschema_async(
            entity_id="syn12345678",
            json_schema_uri="my.org-my.schema-0.0.1",
            enable_derived_annotations=True,
            synapse_client=syn
        ))
        print(f"Successfully bound schema: {result}")
        ```
    """
    from synapseclient import Synapse
    from synapseclient.operations import FileOptions, get_async

    syn = Synapse.get_client(synapse_client=synapse_client)

    entity = await get_async(
        file_options=FileOptions(download_file=False),
        synapse_id=entity_id,
        synapse_client=syn,
    )

    result = await entity.bind_schema_async(
        json_schema_uri=json_schema_uri,
        enable_derived_annotations=enable_derived_annotations,
        synapse_client=syn,
    )

    return result
