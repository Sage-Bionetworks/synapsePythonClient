"""
***********
JSON Schema
***********

.. warning::
    This is a beta implementation and is subject to change.  Use at your own risk.
"""

from __future__ import annotations

import json
from functools import wraps
from typing import Mapping, Sequence, Union

from synapseclient.client import Synapse
from synapseclient.core.exceptions import SynapseAuthenticationError, SynapseHTTPError
from synapseclient.core.utils import id_of
from synapseclient.entity import Entity

DEFAULT_ACCESS = ("CHANGE_PERMISSIONS", "DELETE", "READ", "CREATE", "UPDATE")


class JsonSchemaVersion:
    """Json schema version response object

    :param organization:     JSON schema organization.
    :type organization:      JsonSchemaOrganization
    :param name:             Name of the JSON schema.
    :type name:              str
    :param semantic_version: Version of JSON schema. Defaults to None.
    :type semantic_version:  str, optional
    """

    def __init__(
        self,
        organization: JsonSchemaOrganization,
        name: str,
        semantic_version: str = None,
    ) -> None:
        self.organization = organization
        self.name = name
        self.semantic_version = semantic_version
        self.uri = None
        self.version_id = None
        self.created_on = None
        self.created_by = None
        self.json_sha256_hex = None
        self.set_service(self.organization.service)

    def __repr__(self):
        string = (
            f"JsonSchemaVersion(org={self.organization.name!r}, name={self.name!r}, "
            f"version={self.semantic_version!r})"
        )
        return string

    def set_service(self, service):
        self.service = service

    @property
    def raw(self):
        self.must_get()
        return self._raw

    def parse_response(self, response):
        self._raw = response
        self.uri = response["$id"]
        self.version_id = response["versionId"]
        self.created_on = response["createdOn"]
        self.created_by = response["createdBy"]
        self.json_sha256_hex = response["jsonSHA256Hex"]

    @classmethod
    def from_response(cls, organization, response):
        semver = response.get("semanticVersion")
        version = cls(organization, response["schemaName"], semver)
        version.parse_response(response)
        return version

    def get(self):
        """Get the JSON Schema Version"""
        if self.uri is not None:
            return True
        json_schema = self.organization.get_json_schema(self.name)
        if json_schema is None:
            return False
        raw_version = json_schema.get_version(self.semantic_version, raw=True)
        if raw_version is None:
            return False
        self.parse_response(raw_version)
        return True

    def must_get(self):
        already_exists = self.get()
        assert already_exists, (
            "This operation requires that the JSON Schema name is created first."
            "Call the 'create_version()' method to trigger the creation."
        )

    def create(
        self,
        json_schema_body: dict,
        dry_run: bool = False,
    ):
        """Create JSON schema version

        :param json_schema_body: JSON schema body
        :type json_schema_body:  dict
        :param dry_run:          Do not store to Synapse. Defaults to False.
        :type dry_run:           bool, optional
        :returns: JSON Schema
        """
        uri = f"{self.organization.name}-{self.name}"
        if self.semantic_version:
            uri = f"{uri}-{self.semantic_version}"
        json_schema_body["$id"] = uri
        response = self.service.create_json_schema(json_schema_body, dry_run)
        if dry_run:
            return response
        raw_version = response["newVersionInfo"]
        self.parse_response(raw_version)
        return self

    def delete(self):
        """Delete the JSON schema version"""
        self.must_get()
        response = self.service.delete_json_schema(self.uri)
        return response

    @property
    def body(self):
        self.must_get()
        json_schema_body = self.service.get_json_schema_body(self.uri)
        return json_schema_body

    def expand(self):
        """Validate entities with schema"""
        self.must_get()
        response = self.service.json_schema_validation(self.uri)
        json_schema_body = response["validationSchema"]
        return json_schema_body

    def bind_to_object(self, synapse_id: str):
        """Bind schema to an entity

        :param synapse_id: Synapse Id to bind json schema to.
        :type synapse_id:  str
        """
        self.must_get()
        response = self.service.bind_json_schema_to_entity(synapse_id, self.uri)
        return response


class JsonSchema:
    """Json schema response object

    :param organization:     JSON schema organization.
    :type organization:      JsonSchemaOrganization
    :param name:             Name of the JSON schema.
    :type name:              str
    """

    def __init__(self, organization: JsonSchemaOrganization, name: str) -> None:
        self.organization = organization
        self.name = name
        self.id = None
        self.created_on = None
        self.created_by = None
        self._versions = dict()
        self.set_service(self.organization.service)

    def __repr__(self):
        string = f"JsonSchema(org={self.organization.name!r}, name={self.name!r})"
        return string

    def set_service(self, service):
        self.service = service

    @property
    def raw(self):
        self.must_get()
        return self._raw

    def parse_response(self, response):
        self._raw = response
        self.id = response["schemaId"]
        self.created_on = response["createdOn"]
        self.created_by = response["createdBy"]

    @classmethod
    def from_response(cls, organization, response):
        json_schema = cls(organization, response["schemaName"])
        json_schema.parse_response(response)
        return json_schema

    def get(self):
        """Get Json schema"""
        if self.id is not None:
            return True
        response = self.organization.get_json_schema(self.name, raw=True)
        if response is None:
            return False
        self.parse_response(response)
        return True

    def must_get(self):
        already_exists = self.get()
        assert already_exists, (
            "This operation requires that the JSON Schema name is created first."
            "Call the 'create_version()' method to trigger the creation."
        )

    def list_versions(self):
        """List versions of the json schema"""
        self.must_get()
        self._versions = dict()
        response = self.service.list_json_schema_versions(
            self.organization.name, self.name
        )
        for raw_version in response:
            semver = raw_version.get("semanticVersion")
            version = JsonSchemaVersion.from_response(self.organization, raw_version)
            # Handle that multiple versions can have None/null as their semver
            if semver is None:
                update_none_version = (
                    # Is this the first null version?
                    semver not in self._versions
                    # Or is the version ID higher (i.e.,  more recent)?
                    or version.version_id > self._versions[semver].version_id
                )
                if update_none_version:
                    self._versions[semver] = (raw_version, version)
            else:
                self._versions[semver] = (raw_version, version)
            # Skip versions w/o semver until the end
            if semver is not None:
                yield version
        # Return version w/o semver now (if applicable) to ensure latest is returned
        if None in self._versions:
            yield self._versions[None]

    def get_version(self, semantic_version: str = None, raw: bool = False):
        self.must_get()
        if semantic_version not in self._versions:
            list(self.list_versions())
        raw_version, version = self._versions.get(semantic_version, [None, None])
        return raw_version if raw else version

    def create(
        self,
        json_schema_body: dict,
        semantic_version: str = None,
        dry_run: bool = False,
    ):
        """Create JSON schema

        :param json_schema_body: JSON schema body
        :type json_schema_body:  dict
        :param semantic_version: Version of JSON schema. Defaults to None.
        :type semantic_version:  str, optional
        :param dry_run:          Do not store to Synapse. Defaults to False.
        :type dry_run:           bool, optional
        """
        uri = f"{self.organization.name}-{self.name}"
        if semantic_version:
            uri = f"{uri}-{semantic_version}"
        json_schema_body["$id"] = uri
        response = self.service.create_json_schema(json_schema_body, dry_run)
        if dry_run:
            return response
        raw_version = response["newVersionInfo"]
        version = JsonSchemaVersion.from_response(self.organization, raw_version)
        self._versions[semantic_version] = (raw_version, version)
        return version


class JsonSchemaOrganization:
    """Json Schema Organization

    :param name: Name of JSON schema organization
    :type name:  str
    """

    def __init__(self, name: str) -> None:
        self.name = name
        self.id = None
        self.created_on = None
        self.created_by = None
        self._json_schemas = dict()
        self._raw_json_schemas = dict()

    def __repr__(self):
        string = f"JsonSchemaOrganization(name={self.name!r})"
        return string

    def set_service(self, service):
        self.service = service

    def get(self):
        """Gets Json Schema organization"""
        if self.id is not None:
            return True
        try:
            response = self.service.get_organization(self.name)
        except SynapseHTTPError as e:
            error_msg = str(e)
            if "not found" in error_msg:
                return False
            else:
                raise e
        self.id = response["id"]
        self.created_on = response["createdOn"]
        self.created_by = response["createdBy"]
        return True

    def must_get(self):
        already_exists = self.get()
        assert already_exists, (
            "This operation requires that the organization is created first. "
            "Call the 'create()' method to trigger the creation."
        )

    @property
    def name(self):
        return self._name

    @name.setter
    def name(self, value):
        if len(value) < 6:
            raise ValueError("Name must be at least 6 characters.")
        if len(value) > 250:
            raise ValueError("Name cannot exceed 250 characters. ")
        if value[0].isdigit():
            raise ValueError("Name must not start with a number.")
        self._name = value

    @property
    def raw(self):
        self.must_get()
        return self._raw

    def parse_response(self, response):
        self._raw = response
        self.id = response["id"]
        self.created_on = response["createdOn"]
        self.created_by = response["createdBy"]

    @classmethod
    def from_response(cls, response):
        organization = cls(response["name"])
        organization.parse_response(response)
        return organization

    def create(self):
        """Create the JSON schema organization"""
        already_exists = self.get()
        if already_exists:
            return
        response = self.service.create_organization(self.name)
        self.parse_response(response)
        return self

    def delete(self):
        """Delete the JSON schema organization"""
        self.must_get()
        response = self.service.delete_organization(self.id)
        return response

    def get_acl(self):
        """Get ACL of JSON schema organization"""
        self.must_get()
        response = self.service.get_organization_acl(self.id)
        return response

    def set_acl(
        self,
        principal_ids: Sequence[int],
        access_type: Sequence[str] = DEFAULT_ACCESS,
        etag: str = None,
    ):
        """Set ACL of JSON schema organization

        :param principal_ids: List of Synapse user or team ids.
        :type principal_ids:  list
        :param access_type:   Access control list. Defaults to ["CHANGE_PERMISSIONS", "DELETE", "READ", "CREATE", "UPDATE"].
        :type access_type:    list, optional
        :param etag:          Etag. Defaults to None.
        :type etag:           str, optional
        """
        self.must_get()
        if etag is None:
            acl = self.get_acl()
            etag = acl["etag"]
        resource_access = [
            {"principalId": principal_id, "accessType": access_type}
            for principal_id in principal_ids
        ]
        response = self.service.update_organization_acl(self.id, resource_access, etag)
        return response

    def update_acl(
        self,
        principal_ids: Sequence[int],
        access_type: Sequence[str] = DEFAULT_ACCESS,
        etag: str = None,
    ):
        """Update ACL of JSON schema organization

        :param principal_ids: List of Synapse user or team ids.
        :type principal_ids:  list
        :param access_type:   Access control list. Defaults to ["CHANGE_PERMISSIONS", "DELETE", "READ", "CREATE", "UPDATE"].
        :type access_type:    list, optional
        :param etag:          Etag. Defaults to None.
        :type etag:           str, optional
        """
        self.must_get()
        principal_ids = set(principal_ids)
        acl = self.get_acl()
        resource_access = acl["resourceAccess"]
        if etag is None:
            etag = acl["etag"]
        for entry in resource_access:
            if entry["principalId"] in principal_ids:
                entry["accessType"] = access_type
                principal_ids.remove(entry["principalId"])
        for principal_id in principal_ids:
            entry = {
                "principalId": principal_id,
                "accessType": access_type,
            }
            resource_access.append(entry)
        response = self.service.update_organization_acl(self.id, resource_access, etag)
        return response

    def list_json_schemas(self):
        """List JSON schemas available from the organization"""
        self.must_get()
        response = self.service.list_json_schemas(self.name)
        for raw_json_schema in response:
            json_schema = JsonSchema.from_response(self, raw_json_schema)
            self._raw_json_schemas[json_schema.name] = raw_json_schema
            self._json_schemas[json_schema.name] = json_schema
            yield json_schema

    def get_json_schema(self, json_schema_name: str, raw: bool = False):
        """Get JSON schema

        :param json_schema_name: Name of JSON schema.
        :type json_schema_name:  str
        :param raw:              Return raw JSON schema. Default is False.
        :type raw:               bool, optional
        """
        self.must_get()
        if json_schema_name not in self._json_schemas:
            list(self.list_json_schemas())
        if raw:
            json_schema = self._raw_json_schemas.get(json_schema_name)
        else:
            json_schema = self._json_schemas.get(json_schema_name)
        return json_schema

    def create_json_schema(
        self,
        json_schema_body: dict,
        name: str = None,
        semantic_version: str = None,
        dry_run: bool = False,
    ):
        """Create JSON schema

        :param json_schema_body: JSON schema dict
        :type json_schema_body:  dict
        :param name:             Name of JSON schema. Defaults to None.
        :type name:              str, optional
        :param semantic_version: Version of JSON schema. Defaults to None.
        :type semantic_version:  str, optional
        :param dry_run:          Don't store to Synapse. Defaults to False.
        :type dry_run:           bool, optional
        """
        if name:
            uri = f"{self.name}-{name}"
            if semantic_version:
                uri = f"{uri}-{semantic_version}"
            json_schema_body["$id"] = uri
        else:
            assert (
                semantic_version is not None
            ), "Specify both the name and the semantic version (not just the latter)"
        response = self.service.create_json_schema(json_schema_body, dry_run)
        if dry_run:
            return response
        raw_version = response["newVersionInfo"]
        json_schema = JsonSchemaVersion.from_response(self, raw_version)
        return json_schema


class JsonSchemaService:
    """Json Schema Service

    :param synapse: Synapse connection
    :type synapse:  Synapse
    """

    def __init__(self, synapse: Synapse = None) -> None:
        self.synapse = synapse

    @wraps(Synapse.login)
    def login(self, *args, **kwargs):
        synapse = Synapse()
        synapse.login(*args, **kwargs)
        self.synapse = synapse

    @wraps(JsonSchemaOrganization)
    def JsonSchemaOrganization(self, *args, **kwargs):
        instance = JsonSchemaOrganization(*args, **kwargs)
        instance.set_service(self)
        return instance

    @wraps(JsonSchemaVersion)
    def JsonSchemaVersion(self, *args, **kwargs):
        instance = JsonSchemaVersion(*args, **kwargs)
        instance.set_service(self)
        return instance

    @wraps(JsonSchema)
    def JsonSchema(self, *args, **kwargs):
        instance = JsonSchema(*args, **kwargs)
        instance.set_service(self)
        return instance

    def authentication_required(func):
        @wraps(func)
        def wrapper(self, *args, **kwargs):
            msg = (
                f"`JsonSchemaService.{func.__name__}()` requests must be authenticated."
                " Login using the `login()` method on the existing `JsonSchemaService`"
                " instance (e.g., `js.login()` or `js.login(authToken=...)`)."
            )
            assert self.synapse is not None, msg
            try:
                result = func(self, *args, **kwargs)
            except SynapseAuthenticationError as e:
                raise SynapseAuthenticationError(msg).with_traceback(e.__traceback__)
            return result

        return wrapper

    @authentication_required
    def create_organization(self, organization_name: str):
        """Create a new organization

        :param organization_name: JSON schema organization name
        :type organization_name:  str
        """
        request_body = {"organizationName": organization_name}
        response = self.synapse.restPOST(
            "/schema/organization", body=json.dumps(request_body)
        )
        return response

    @authentication_required
    def get_organization(self, organization_name: str):
        """Get a organization

        :param organization_name: JSON schema organization name
        :type organization_name:  str
        """
        response = self.synapse.restGET(
            f"/schema/organization?name={organization_name}"
        )
        return response

    def list_organizations(self):
        """List organizations"""
        request_body = {}
        response = self.synapse._POST_paginated(
            "/schema/organization/list", request_body
        )
        return response

    @authentication_required
    def delete_organization(self, organization_id: str):
        """Delete organization

        :param organization_id: JSON schema organization Id
        :type organization_id:  str
        """
        response = self.synapse.restDELETE(f"/schema/organization/{organization_id}")
        return response

    @authentication_required
    def get_organization_acl(self, organization_id: str):
        """Get ACL associated with Organization

        :param organization_id: JSON schema organization Id
        :type organization_id:  str
        """
        response = self.synapse.restGET(f"/schema/organization/{organization_id}/acl")
        return response

    @authentication_required
    def update_organization_acl(
        self,
        organization_id: str,
        resource_access: Sequence[Mapping[str, Sequence[str]]],
        etag: str,
    ):
        """Get ACL associated with Organization

        :param organization_id: JSON schema organization Id
        :type organization_id:  str
        :param resource_access: Resource access array
        :type resource_access:  list
        :param etag:            Etag
        :type etag:             str
        """
        request_body = {"resourceAccess": resource_access, "etag": etag}
        response = self.synapse.restPUT(
            f"/schema/organization/{organization_id}/acl", body=json.dumps(request_body)
        )
        return response

    def list_json_schemas(self, organization_name: str):
        """List JSON schemas for an organization

        :param organization_name: JSON schema organization name
        :type organization_name:  str
        """
        request_body = {"organizationName": organization_name}
        response = self.synapse._POST_paginated("/schema/list", request_body)
        return response

    def list_json_schema_versions(self, organization_name: str, json_schema_name: str):
        """List version information for each JSON schema

        :param organization_name: JSON schema organization name
        :type organization_name:  str
        :param json_schema_name:  JSON schema name
        :type json_schema_name:   str
        """
        request_body = {
            "organizationName": organization_name,
            "schemaName": json_schema_name,
        }
        response = self.synapse._POST_paginated("/schema/version/list", request_body)
        return response

    @authentication_required
    def create_json_schema(self, json_schema_body: dict, dry_run: bool = False):
        """Create a JSON schema

        :param json_schema_body: JSON schema body
        :type json_schema_body:  dict
        :param dry_run:          Don't store to Synapse. Default to False.
        :type dry_run:           bool, optional
        """
        request_body = {
            "concreteType": "org.sagebionetworks.repo.model.schema.CreateSchemaRequest",
            "schema": json_schema_body,
            "dryRun": dry_run,
        }
        response = self.synapse._waitForAsync("/schema/type/create/async", request_body)
        return response

    def get_json_schema_body(self, json_schema_uri: str):
        """Get registered JSON schema with its $id

        :param json_schema_uri: JSON schema URI
        :type json_schema_uri:  str
        """
        response = self.synapse.restGET(f"/schema/type/registered/{json_schema_uri}")
        return response

    @authentication_required
    def delete_json_schema(self, json_schema_uri: str):
        """Delete the given schema using its $id

        :param json_schema_uri: JSON schema URI
        :type json_schema_uri:  str
        """
        response = self.synapse.restDELETE(f"/schema/type/registered/{json_schema_uri}")
        return response

    @authentication_required
    def json_schema_validation(self, json_schema_uri: str):
        """Use a JSON schema for validation

        :param json_schema_uri: JSON schema URI
        :type json_schema_uri:  str
        """
        request_body = {
            "concreteType": (
                "org.sagebionetworks.repo.model.schema.GetValidationSchemaRequest"
            ),
            "$id": json_schema_uri,
        }
        response = self.synapse._waitForAsync(
            "/schema/type/validation/async", request_body
        )
        return response

    @authentication_required
    def bind_json_schema_to_entity(self, synapse_id: str, json_schema_uri: str):
        """Bind a JSON schema to an entity

        :param synapse_id:      Synapse Id
        :type synapse_id:       str
        :param json_schema_uri: JSON schema URI
        :type json_schema_uri:  str
        """
        request_body = {"entityId": synapse_id, "schema$id": json_schema_uri}
        response = self.synapse.restPUT(
            f"/entity/{synapse_id}/schema/binding", body=json.dumps(request_body)
        )
        return response

    @authentication_required
    def get_json_schema_from_entity(self, synapse_id: str):
        """Get bound schema from entity

        :param synapse_id:      Synapse Id
        :type synapse_id:       str
        """
        response = self.synapse.restGET(f"/entity/{synapse_id}/schema/binding")
        return response

    @authentication_required
    def delete_json_schema_from_entity(self, synapse_id: str):
        """Delete bound schema from entity

        :param synapse_id:      Synapse Id
        :type synapse_id:       str
        """
        response = self.synapse.restDELETE(f"/entity/{synapse_id}/schema/binding")
        return response

    @authentication_required
    def validate_entity_with_json_schema(self, synapse_id: str):
        """Get validation results of an entity against bound JSON schema

        :param synapse_id:      Synapse Id
        :type synapse_id:       str
        """
        response = self.synapse.restGET(f"/entity/{synapse_id}/schema/validation")
        return response

    @authentication_required
    def get_json_schema_validation_statistics(self, synapse_id: str):
        """Get the summary statistic of json schema validation results for
        a container entity

        :param synapse_id:      Synapse Id
        :type synapse_id:       str
        """
        response = self.synapse.restGET(
            f"/entity/{synapse_id}/schema/validation/statistics"
        )
        return response

    @authentication_required
    def get_invalid_json_schema_validation(self, synapse_id: str):
        """Get a single page of invalid JSON schema validation results for a container Entity
        (Project or Folder).

        :param synapse_id:      Synapse Id
        :type synapse_id:       str
        """
        request_body = {"containerId": synapse_id}
        response = self.synapse._POST_paginated(
            f"/entity/{synapse_id}/schema/validation/invalid", request_body
        )
        return response

    # The methods below are here until they are integrated with Synapse/Entity

    def bind_json_schema(self, json_schema_uri: str, entity: Union[str, Entity]):
        """Bind a JSON schema to an entity

        :param json_schema_uri: JSON schema URI
        :type json_schema_uri:  str
        :param entity:          Synapse Entity or Synapse Id
        :type entity:           str, Entity
        """
        synapse_id = id_of(entity)
        response = self.bind_json_schema_to_entity(synapse_id, json_schema_uri)
        return response

    def get_json_schema(self, entity: Union[str, Entity]):
        """Get a JSON schema associated to an Entity

        :param entity:          Synapse Entity or Synapse Id
        :type entity:           str, Entity
        """
        synapse_id = id_of(entity)
        response = self.get_json_schema_from_entity(synapse_id)
        return response

    def unbind_json_schema(self, entity: Union[str, Entity]):
        """Unbind a JSON schema from an entity

        :param entity:          Synapse Entity or Synapse Id
        :type entity:           str, Entity
        """
        synapse_id = id_of(entity)
        response = self.delete_json_schema_from_entity(synapse_id)
        return response

    def validate(self, entity: Union[str, Entity]):
        """Validate an entity based on the bound JSON schema

        :param entity:          Synapse Entity or Synapse Id
        :type entity:           str, Entity
        """
        synapse_id = id_of(entity)
        response = self.validate_entity_with_json_schema(synapse_id)
        return response

    def validation_stats(self, entity: Union[str, Entity]):
        """Get validation statistics of an entity based on the bound JSON schema

        :param entity:          Synapse Entity or Synapse Id
        :type entity:           str, Entity
        """
        synapse_id = id_of(entity)
        response = self.get_json_schema_validation_statistics(synapse_id)
        return response

    def validate_children(self, entity: Union[str, Entity]):
        """Validate an entity and it's children based on the bound JSON schema

        :param entity:          Synapse Entity or Synapse Id of a project or folder.
        :type entity:           str, Entity
        """
        synapse_id = id_of(entity)
        response = self.get_invalid_json_schema_validation(synapse_id)
        return response
