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
from synapseclient.core.constants.concrete_types import CREATE_SCHEMA_REQUEST
from synapseclient.models.mixins.asynchronous_job import AsynchronousCommunicator
from synapseclient.models.mixins.json_schema import JSONSchemaVersionInfo
from synapseclient.models.protocols.schema_organization_protocol import (
    JSONSchemaProtocol,
    SchemaOrganizationProtocol,
)

if TYPE_CHECKING:
    from synapseclient import Synapse

SYNAPSE_SCHEMA_URL = (
    "https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/"
)


@dataclass()
@async_to_sync
class SchemaOrganization(SchemaOrganizationProtocol):
    """
    Represents an [Organization](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/Organization.html).
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

    async def get_async(
        self, synapse_client: Optional["Synapse"] = None
    ) -> "SchemaOrganization":
        """
        Gets the metadata from Synapse for this organization

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            Itself

        Example: Get an existing SchemaOrganization
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("dpetest")
            asyncio.run(org.get_async())
            ```
        """
        if self.id:
            return self
        response = await get_organization(self.name, synapse_client=synapse_client)
        self._fill_from_dict(response)
        return self

    async def store_async(
        self, synapse_client: Optional["Synapse"] = None
    ) -> "SchemaOrganization":
        """
        Stores this organization in Synapse

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            Itself

        Example: Store a new SchemaOrganization
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            asyncio.run(org.store_async())
            ```
        """
        response = await create_organization(self.name, synapse_client=synapse_client)
        self._fill_from_dict(response)
        return self

    async def delete_async(self, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Deletes this organization in Synapse

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example: Delete a SchemaOrganization
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            asyncio.run(org.delete_async())
            ```
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

        Example: Get a list of JSONSchemas that belong to the SchemaOrganization
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("dpetest")
            schemas = asyncio.run(org.get_json_schema_list_async())
            ```

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

        Example: Get the ACL for a SchemaOrganization
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("dpetest")
            acl = asyncio.run(org.get_acl_async())
            ```
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

        Example: Update the ACL or a SchemaOrganization
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            # Store a new org
            org = SchemaOrganization("my.org.name")
            asyncio.run(org.store_async())

            # Get and modify the ACL
            current acl = asyncio.run(org.get_acl_async())
            resource_access = current_acl["resourceAccess"]
            resource_access.append({"principalId": 1, "accessType": ["READ"]})
            etag = current_acl["etag"]

            # Update the ACL
            asyncio.run(org.update_acl_async(resource_access, etag))
            ```

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

        Example: Create a SchemaOrganization form an API response
            &nbsp;

            ```python
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse
            import asyncio
            from synapseclient.api  import get_organization

            syn = Synapse()
            syn.login()

            response = asyncio.run(get_organization("my.org.name"))
            org = SchemaOrganization.from_response(response)
            ```

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

    async def get_async(
        self, synapse_client: Optional["Synapse"] = None
    ) -> "JSONSchema":
        """
        Gets this JSON Schemas metadata

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Raises:
            ValueError: This JSONSchema doesn't exist in its organization

        Returns:
            Itself

        Example: Get an existing JSONSchema
            &nbsp;

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            asyncio.run(js.get_async())
            ```
        """
        if self.id:
            return self

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
                return self
        raise ValueError(
            (
                f"Organization: '{self.organization_name}' does not contain a schema with "
                f"name: '{self.name}'"
            )
        )

    async def store_async(
        self,
        schema_body: dict[str, Any],
        version: Optional[str] = None,
        dry_run: bool = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> "JSONSchema":
        """
        Stores this JSONSchema in Synapse

        Arguments:
            schema_body: The body of the JSONSchema to store
            version: The version of the JSONSchema body to store
            dry_run: Whether or not to do a dry-run
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            Itself

        Example: Store a JSON Schema in Synapse
            &nbsp;

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            asyncio.run(js.store_async())
            ```
        """
        request = CreateSchemaRequest(
            schema=schema_body,
            name=self.name,
            organization_name=self.organization_name,
            version=version,
            dry_run=dry_run,
        )
        completed_request: CreateSchemaRequest = await request.send_job_and_wait_async(
            synapse_client=synapse_client
        )
        new_version_info = completed_request.new_version_info
        self.organization_id = new_version_info.organization_id
        self.id = new_version_info.id
        self.created_by = new_version_info.created_by
        self.created_on = new_version_info.created_on
        return self

    async def delete_async(self, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Deletes this JSONSchema

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example: Delete an existing JSONSchema
            &nbsp;

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            asyncio.run(js.delete_async())
            ```
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

        Example: Get all the versions of the JSONSchema
            &nbsp;

            ```python
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse
            import asyncio

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            versions = asyncio.run(get_versions_async())
            ```
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

        Example: Get the body of the JSONSchema
            &nbsp;

            ```python
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
            ```
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

        Example: Create a JSONSchema from a URI
            &nbsp;

            ```python
            from synapseclient.models import JSONSchema

            # Non-semantic URI
            js1 = JSONSchema.from_uri("my.org-my.schema")

            # Semantic URI
            js2 = JSONSchema.from_uri("my.org-my.schema-0.0.1")
            ```

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

        Example: Create a JSONSchema from an API response
            &nbsp;

            ```python
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
            ```

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
                [JsonSchemaVersionInfo](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchemaVersionInfo.html)

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
              [JsonSchema](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchema.html)
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

        Arguments:
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


@dataclass
class CreateSchemaRequest(AsynchronousCommunicator):
    """
    This result is modeled from: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/QueryBundleRequest.html>
    """

    schema: dict[str, Any]
    """The JSON Schema to be stored"""

    name: str
    """The name of the schema being stored"""

    organization_name: str
    """The name of the organization the schema to store the schema in"""

    version: Optional[str] = None
    """The version to store the schema as if given"""

    dry_run: bool = False
    """Whether or not to do the request as a dry-run"""

    concrete_type: str = field(init=False)
    """The concrete type of the request"""

    uri: str = field(init=False)
    """The URI of this schema"""

    id: str = field(init=False)
    """The ID/URL of this schema"""

    new_version_info: JSONSchemaVersionInfo = None
    """Info from the API response"""

    def __post_init__(self) -> None:
        self.concrete_type = CREATE_SCHEMA_REQUEST
        self._check_name(self.name)
        self._check_name(self.organization_name)
        uri = f"{self.organization_name}-{self.name}"
        if self.version:
            self._check_semantic_version(self.version)
            uri = f"{uri}-{self.version}"
        self.uri = uri
        self.id = f"{SYNAPSE_SCHEMA_URL}{uri}"
        self.schema["$id"] = self.id

    def to_synapse_request(self) -> dict[str, Any]:
        """
        Create a CreateSchemaRequest from attributes
        https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/CreateSchemaRequest.html
        """

        result = {
            "concreteType": self.concrete_type,
            "schema": self.schema,
            "dryRun": self.dry_run,
        }

        return result

    def fill_from_dict(self, synapse_response: dict[str, Any]) -> "CreateSchemaRequest":
        """
        Set attributes from CreateSchemaResponse
        https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/CreateSchemaResponse.html
        """
        self.new_version_info = self._create_json_schema_version_from_response(
            synapse_response.get("newVersionInfo")
        )
        self.schema = synapse_response.get("validationSchema")

        return self

    def _check_semantic_version(self, version: str) -> None:
        """
        Checks that the semantic version is correctly formatted

        Args:
            version: A semantic version(ie. `1.0.0`) to be checked

        Raises:
            ValueError: If the string is not a correct semantic version
        """
        if not re.match("^(\d+)\.(\d+)\.(\d*)$", version) or version == "0.0.0":
            raise ValueError(
                (
                    "Schema version must be a semantic version starting at 0.0.1 with no letters "
                    "and a major, minor and patch version "
                    f"{version}"
                )
            )

    def _check_name(self, name: str) -> None:
        """
        Checks that the input name is a valid Synapse JSONSchema or Organization name
        - Must start with a letter
        - Must contains only letters, numbers and periods.

        Arguments:
            name: The name of the organization/schema to be checked

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
