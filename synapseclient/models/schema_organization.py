"""
This file contains the SchemaOrganization and JSONSchema classes.
These are used to manage Organization and JSON Schema entities in Synapse.
"""

import re
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Mapping, Optional, Sequence

from synapseclient.api import (
    create_organization,
    delete_json_schema,
    delete_organization,
    get_json_schema_body,
    get_organization,
    get_organization_acl,
    list_json_schema_versions,
    list_json_schemas,
    list_organizations_sync,
    update_organization_acl,
)
from synapseclient.core.async_utils import async_to_sync
from synapseclient.models.mixins.json_schema import JSONSchemaVersionInfo
from synapseclient.models.protocols.schema_organization_protocol import (
    JSONSchemaProtocol,
    SchemaOrganizationProtocol,
)

if TYPE_CHECKING:
    from synapseclient import Synapse


@dataclass()
@async_to_sync
class SchemaOrganization(SchemaOrganizationProtocol):
    """
    Represents an [Organization](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/Organization.html).

    Attributes:
        id: The ID of the organization
        name: The name of the organization
        created_on: The date this organization was created
        created_by: The ID of the user that created this organization

    Example:
        from synapseclient.models import SchemaOrganization
        from synapseclient import Synapse

        syn = Synapse()
        syn.login()

        # Create a new org
        org = SchemaOrganization("my.new.org.name")
        org.create()

        # Get the metadata and JSON Schemas for an existing org
        org = SchemaOrganization("my.org.name")
        org.get_async()
        print(org)
        schemas = org.get_json_schema_list()
        print(schemas)
    """

    name: str
    """The name of the organization"""

    id: Optional[str] = None
    """The ID of the organization"""

    created_on: Optional[str] = None
    """The date this organization was created"""

    created_by: Optional[str] = None
    """The ID of the user that created this organization"""

    def __post_init__(self) -> None:
        self._check_name(self.name)

    async def get_async(self, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Gets the metadata from Synapse for this organization

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example:
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            asyncio.run(org.get_async())

        """
        if self.id:
            return
        response = await get_organization(self.name, synapse_client=synapse_client)
        self._fill_from_dict(response)

    # Should this be named 'store_async'?
    async def create_async(self, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Creates this organization in Synapse

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example:
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            asyncio.run(org.create())

        """
        if self.id:
            await self.get_async(synapse_client=synapse_client)
        response = await create_organization(self.name, synapse_client=synapse_client)
        self._fill_from_dict(response)

    async def delete_async(self, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Deletes this organization in Synapse

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example:
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            asyncio.run(org.delete())

        """
        if not self.id:
            await self.get_async(synapse_client=synapse_client)
        await delete_organization(self.id, synapse_client=synapse_client)

    async def get_json_schema_list_async(
        self, synapse_client: Optional["Synapse"] = None
    ) -> list["JSONSchema"]:
        """
        Gets the list of JSON Schemas that are part of this organization

        Returns: A list of JSONSchema objects

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example:
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            asyncio.run(org.get_json_schema_list_async())

        """
        response = list_json_schemas(self.name, synapse_client=synapse_client)
        schemas = []
        async for item in response:
            schemas.append(JSONSchema.from_response(item))
        return schemas

    async def get_acl_async(
        self, synapse_client: Optional["Synapse"] = None
    ) -> dict[str, Any]:
        """
        Gets the ACL for this organization

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            A dictionary in the form of this response:
              https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/AccessControlList.html

        Example:
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            asyncio.run(org.get_acl_async())
        """
        if not self.id:
            await self.get_async()
        response = await get_organization_acl(self.id, synapse_client=synapse_client)
        return response

    async def update_acl_async(
        self,
        resource_access: Sequence[Mapping[str, Sequence[str]]],
        etag: str,
        synapse_client: Optional["Synapse"] = None,
    ) -> None:
        """
        Updates the ACL for this organization

        Arguments:
            resource_access: List of ResourceAccess objects, each containing:
                - principalId: The user or team ID
                - accessType: List of permission types (e.g., ["READ", "CREATE", "DELETE"])
                see:
                  https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/ResourceAccess.html
            etag: The etag from get_organization_acl() for concurrency control
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example:
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            current acl = asyncio.run(org.get_acl_async())
            resource_access = current_acl["resourceAccess"]
            resource_access.append({"principalId": 1, "accessType": ["READ"]})
            etag = current_acl["etag"]
            asyncio.run(org.update_acl_async(resource_access, etag))

        """
        if not self.id:
            await self.get_async()
        await update_organization_acl(
            organization_id=self.id,
            resource_access=resource_access,
            etag=etag,
            synapse_client=synapse_client,
        )

    def _check_name(self, name) -> None:
        """
        Checks that the input name is a valid Synapse organization name
        - Must start with a letter
        - Must contains only letters, numbers and periods.

        Arguments:
            name: The name of the organization to be checked

        Raises:
            ValueError: When the name isn't valid
        """
        if not re.match("^([A-Za-z])([A-Za-z]|\d|\.)*$", name):
            raise ValueError(
                "Organization name must start with a letter and contain "
                f"only letters numbers, and periods: {name}"
            )

    def _fill_from_dict(self, response: dict[str, Any]) -> None:
        """
        Fills in this classes attributes using a Synapse API response

        Args:
            response: A response from this endpoint:
              https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/Organization.html
        """
        self.name = response.get("name")
        self.id = response.get("id")
        self.created_on = response.get("createdOn")
        self.created_by = response.get("createdBy")

    @classmethod
    def from_response(cls, response: dict[str, Any]) -> "SchemaOrganization":
        """
        Creates an SchemaOrganization object using a Synapse API response

        Arguments:
            response: A response from this endpoint:
              https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/Organization.html

        Returns:
            A SchemaOrganization object using the input response

        Example:
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio
            from synapseclient.api  import get_organization

            syn = Synapse()
            syn.login()

            response = asyncio.run(get_organization("my.org.name"))
            org = SchemaOrganization.from_response(response)

        """
        org = SchemaOrganization(response.get("name"))
        org._fill_from_dict(response)
        return org


@dataclass()
@async_to_sync
class JSONSchema(JSONSchemaProtocol):
    """
    Represents a:
      [JSON Schema](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchemaInfo.html)

    Attributes:
        name: The name of the schema
        organization_name: The name of the organization the schema belongs to
        organization_id: the id of the organization the schema belongs to
        id: The ID of the schema
        created_on: The date this schema was created
        created_by: The ID of the user that created this schema
        uri: The uri of this schema

    Example:
        from synapseclient.models import JSON Schema
        from synapseclient import Synapse

        syn = Synapse()
        syn.login()

    """

    name: str
    """The name of the schema"""

    organization_name: str
    """The name of the organization the schema belongs to"""

    organization_id: Optional[int] = None
    """The id of the organization the schema belongs to"""

    id: Optional[str] = None
    """The ID of the schema"""

    created_on: Optional[str] = None
    """The date this schema was created"""

    created_by: Optional[str] = None
    """The ID of the user that created this schema"""

    uri: str = field(init=False)
    """The uri of this schema"""

    def __post_init__(self) -> None:
        self.uri = f"{self.organization_name}-{self.name}"
        self._check_name(self.name)

    async def get_async(self, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Gets this JSON Schemas metadata

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Raises:
            ValueError: This JSONSchema doesn't exist in its organization

        Example:
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            asyncio.run(js.get_async())
        """
        if self.id:
            return

        # Check that the org exists,
        #  if it doesn't list_json_schemas will unhelpfully return an empty generator.
        org = SchemaOrganization(self.organization_name)
        await org.get_async()

        org_schemas = list_json_schemas(
            self.organization_name, synapse_client=synapse_client
        )
        async for schema in org_schemas:
            if schema["schemaName"] == self.name:
                self._fill_from_dict(schema)
                return
        raise ValueError(
            (
                f"Organization: '{self.organization_name}' does not contain a schema with "
                f"name: '{self.name}'"
            )
        )

    # Should this ba named store?
    # TODO: crate api function, and async version of method, write docstring
    def create(
        self,
        body: dict[str, Any],
        version: Optional[str] = None,
        dry_run: bool = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> None:
        uri = self.uri
        if version:
            self._check_semantic_version(version)
            uri = f"{uri}-{version}"
        body["$id"] = uri

        request_body = {
            "concreteType": "org.sagebionetworks.repo.model.schema.CreateSchemaRequest",
            "schema": body,
            "dryRun": dry_run,
        }
        if not synapse_client:
            from synapseclient import Synapse

            synapse_client = Synapse()
            synapse_client.login()

        response = synapse_client._waitForAsync(
            "/schema/type/create/async", request_body
        )
        self._fill_from_dict(response["newVersionInfo"])

    async def delete_async(self, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Deletes this JSONSchema

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example:
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            asyncio.run(js.delete_async())
        """
        await delete_json_schema(self.uri, synapse_client=synapse_client)

    async def get_versions_async(
        self, synapse_client: Optional["Synapse"] = None
    ) -> list[JSONSchemaVersionInfo]:
        """
        Gets a list of all versions of this JSONSchema

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            A JSONSchemaVersionInfo for each version of this schema

        Example:
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            versions = asyncio.run(get_versions_async())
        """
        all_schemas = list_json_schema_versions(
            self.organization_name, self.name, synapse_client=synapse_client
        )
        versions = []
        async for schema in all_schemas:
            # Schemas created without a semantic version will be returned from the API call.
            # Those won't be returned here since they aren't really versions.
            # JSONSchemaVersionInfo.semantic_version could also be changed to optional.
            if "semanticVersion" in schema:
                versions.append(self._create_json_schema_version_from_response(schema))
        return versions

    async def get_body_async(
        self, version: Optional[str] = None, synapse_client: Optional["Synapse"] = None
    ) -> dict[str, Any]:
        """
        Gets the JSON body for the schema.

        Arguments:
            version: Defaults to None.
            - If a version is supplied, that versions body will be returned.
            - If no version is supplied the most recent version will be returned.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            The JSON Schema body

        Example:
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            # Get latest version
            latest = asyncio.run(js.get_body_async())
            # Get specific version
            first = asyncio.run(js.get_body_async("0.0.1"))
        """
        uri = self.uri
        if version:
            self._check_semantic_version(version)
            uri = f"{uri}-{version}"
        response = await get_json_schema_body(uri, synapse_client=synapse_client)
        return response

    @classmethod
    def from_uri(cls, uri: str) -> "JSONSchema":
        """
        Creates a JSONSchema from a URI.
        The URI can either be a semantic version or not
        - non-semantic: ORGANIZATION.NAME-SCHEMA.NAME
        - semantic: ORGANIZATION.NAME-SCHEMA.NAME-MAJOR.MINOR.PATCH

        Arguments:
            uri: The URI to the JSON Schema in Synapse

        Raises:
            ValueError:  If the URI isn't in the correct form.

        Returns:
            A JSONSchema object

        Example:
            from synapseclient.models import JSONSchema

            # Non-semantic URI
            js1 = JSONSchema.from_uri("my.org-my.schema")

            # Semantic URI
            js2 = JSONSchema.from_uri("my.org-my.schema-0.0.1")

        """
        uri_parts = uri.split("-")
        if len(uri_parts) > 3 or len(uri_parts) < 2:
            msg = (
                "The URI must be in the form of "
                "'<ORGANIZATION>-<NAME>' or '<ORGANIZATION>-<NAME>-<VERSION>': "
                f"{uri}"
            )
            raise ValueError(msg)
        return JSONSchema(name=uri_parts[1], organization_name=uri_parts[0])

    @classmethod
    def from_response(cls, response: dict[str, Any]) -> "JSONSchema":
        """
        Creates a JSONSchema object using a Synapse API response

        Arguments:
            response: A response from this endpoint:
              [JSON Schema](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchemaInfo.html)

        Returns:
            A JSONSchema object from the API response

        Example:
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            from synapseclient.api import list_json_schemas
            import asyncio

            syn = Synapse()
            syn.login()

            async def get_first_response():
                async_gen = list_json_schemas("my.org.name")
                responses = []
                async for item in async_gen:
                    responses.append(item)
                return responses[1]

            response = asyncio.run(get_first_response())
            JSONSchema.from_response(response)

        """
        js = JSONSchema(response.get("schemaName"), response.get("organizationName"))
        js._fill_from_dict(response)
        return js

    @staticmethod
    def _create_json_schema_version_from_response(
        response: dict[str, Any]
    ) -> JSONSchemaVersionInfo:
        """
        Creates a JSONSchemaVersionInfo object from a Synapse API response

        Arguments:
            response: This Synapse API object:
                [JsonSchemaVersionInfo]https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchemaVersionInfo.html

        Returns:
            A JSONSchemaVersionInfo object
        """
        return JSONSchemaVersionInfo(
            organization_id=response.get("organizationId"),
            organization_name=response.get("organizationName"),
            schema_id=response.get("schemaId"),
            id=response.get("$id"),
            schema_name=response.get("schemaName"),
            version_id=response.get("versionId"),
            semantic_version=response.get("semanticVersion"),
            json_sha256_hex=response.get("jsonSHA256Hex"),
            created_on=response.get("createdOn"),
            created_by=response.get("createdBy"),
        )

    def _fill_from_dict(self, response: dict[str, Any]) -> None:
        """
        Fills in this classes attributes using a Synapse API response

        Arguments:
            response: This Synapse API object:
              [JsonSchema]https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchema.html
        """
        self.organization_id = response.get("organizationId")
        self.organization_name = response.get("organizationName")
        self.id = response.get("schemaId")
        self.name = response.get("schemaName")
        self.created_on = response.get("createdOn")
        self.created_by = response.get("createdBy")
        self.uri = f"{self.organization_name}-{self.name}"

    def _check_semantic_version(self, version: str) -> None:
        """
        Checks that the semantic version is correctly formatted

        Args:
            version: A semantic version(ie. `1.0.0`) to be checked

        Raises:
            ValueError: If the string is not a correct semantic version
        """
        if not re.match("^(\d+)\.(\d+)\.([1-9]\d*)$", version):
            raise ValueError(
                (
                    "Schema version must be a semantic version with no letters "
                    "and a major, minor and patch version, such as 0.0.1: "
                    f"{version}"
                )
            )

    def _check_name(self, name) -> None:
        """
        Checks that the input name is a valid Synapse JSONSchema name
        - Must start with a letter
        - Must contains only letters, numbers and periods.

        Arguments:
            name: The name of the organization to be checked

        Raises:
            ValueError: When the name isn't valid
        """
        if not re.match("^([A-Za-z])([A-Za-z]|\d|\.)*$", name):
            raise ValueError(
                (
                    "Schema name must start with a letter and contain "
                    f"only letters numbers and periods: {name}"
                )
            )


# TODO: Move to a utils module
def list_json_schema_organizations(
    synapse_client: Optional["Synapse"] = None,
) -> list[SchemaOrganization]:
    """
    Lists all the Schema Organizations currently in Synapse

    Arguments:
        synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

    Returns:
        A list of SchemaOrganizations
    """
    all_orgs = [
        SchemaOrganization.from_response(org)
        for org in list_organizations_sync(synapse_client=synapse_client)
    ]
    return all_orgs
