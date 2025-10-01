"""Protocols for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Any, Mapping, Optional, Protocol, Sequence

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models.mixins.json_schema import JSONSchemaVersionInfo
    from synapseclient.models.schema_organization import JSONSchema


class SchemaOrganizationProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def get(self, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Gets the metadata from Synapse for this organization

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example:
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            org.get()

        """
        return self

    def store(self, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Stores this organization in Synapse

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example:
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            org.store()

        """
        return self

    def delete(self, synapse_client: Optional["Synapse"] = None) -> None:
        """
        Deletes this organization in Synapse

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example:
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            org.delete()

        """
        return None

    def get_json_schema_list(
        self, synapse_client: Optional["Synapse"] = None
    ) -> list["JSONSchema"]:
        """
        Gets the list of JSON Schemas that are part of this organization

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns: A list of JSONSchema objects

        Example:
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            org.get_json_schema_list()

        """
        return []

    def get_acl(self, synapse_client: Optional["Synapse"] = None) -> dict[str, Any]:
        """
        Gets the ACL for this organization

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Returns:
            A dictionary in the form of this response:
              https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/AccessControlList.html

        Example:
            from synapseclient.models import SchemaOrganization
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            org.get_acl()
        """
        return {}

    def update_acl(
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

            syn = Synapse()
            syn.login()

            org = SchemaOrganization("my.org.name")
            current acl = org.get_acl()
            resource_access = current_acl["resourceAccess"]
            resource_access.append({"principalId": 1, "accessType": ["READ"]})
            etag = current_acl["etag"]
            org.update_acl(resource_access, etag)

        """
        return None


class JSONSchemaProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def get(self, synapse_client: Optional["Synapse"] = None) -> None:
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

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            js.get()
        """
        return self

    def store(
        self,
        body: dict[str, Any],
        version: Optional[str] = None,
        dry_run: bool = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> None:
        return self

    def delete(self) -> None:
        """
        Deletes this JSON Schema

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor

        Example:
            from synapseclient.models import JSONSchema
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            js.delete()
        """
        return None

    def get_versions(
        self, synapse_client: Optional["Synapse"] = None
    ) -> list["JSONSchemaVersionInfo"]:
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

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            versions = get_versions()
        """
        return []

    def get_body(
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

            syn = Synapse()
            syn.login()

            js = JSONSchema("my.schema.name", "my.org.name")
            # Get latest version
            latest = js.get_body()
            # Get specific version
            first = ajs.get_body("0.0.1")
        """
        return {}
