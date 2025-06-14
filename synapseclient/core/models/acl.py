"""
Access Control List (ACL) models for Synapse entities.

These dataclasses provide structured representations of ACL data returned from
the list_acl_async method, replacing the raw dictionary format with type-safe
and self-documenting objects.
"""

from dataclasses import dataclass, field
from typing import Dict, List, Optional


@dataclass
class AclEntry:
    """
    Represents an Access Control List entry for a single principal (user or team).

    This dataclass encapsulates the permissions that a specific principal has
    on an entity, providing a structured alternative to raw string-based
    principal ID and permission lists.

    Attributes:
        principal_id: The ID of the principal (user or team) as a string.
        permissions: List of permission strings granted to this principal.
            Common permissions include: 'READ', 'DOWNLOAD', 'UPDATE', 'DELETE',
            'CREATE', 'CHANGE_PERMISSIONS', 'CHANGE_SETTINGS', 'MODERATE'.
    """

    principal_id: str
    """The ID of the principal (user or team) as a string."""

    permissions: List[str] = field(default_factory=list)
    """List of permission strings granted to this principal."""


@dataclass
class EntityAcl:
    """
    Represents the complete Access Control List for a single Synapse entity.

    Attributes:
        entity_id: The Synapse ID of the entity (e.g., "syn123").
        acl_entries: List of ACL entries, each representing permissions for a principal.
    """

    entity_id: str
    """The Synapse ID of the entity (e.g., "syn123")."""

    acl_entries: List[AclEntry] = field(default_factory=list)
    """List of ACL entries, each representing permissions for a principal."""

    @classmethod
    def from_dict(cls, entity_id: str, acl_dict: Dict[str, List[str]]) -> "EntityAcl":
        """
        Create an EntityAcl from the dictionary format returned by the API.

        Arguments:
            entity_id: The Synapse ID of the entity.
            acl_dict: Dictionary mapping principal IDs to permission lists.

        Returns:
            A new EntityAcl instance.

        Example:
            ```python
            acl_data = {"273948": ["READ", "DOWNLOAD"], "12345": ["READ", "UPDATE"]}
            entity_acl = EntityAcl.from_dict("syn123", acl_data)
            ```
        """
        acl_entries = [
            AclEntry(principal_id=principal_id, permissions=permissions)
            for principal_id, permissions in acl_dict.items()
        ]
        return cls(entity_id=entity_id, acl_entries=acl_entries)

    def to_dict(self) -> Dict[str, List[str]]:
        """
        Convert this EntityAcl to the dictionary format used by the API.

        Returns:
            Dictionary mapping principal IDs to permission lists.

        Example:
            ```python
            entity_acl = EntityAcl(entity_id="syn123", acl_entries=[...])
            acl_dict = entity_acl.to_dict()
            # Returns: {"273948": ["READ", "DOWNLOAD"], "12345": ["READ", "UPDATE"]}
            ```
        """
        return {entry.principal_id: entry.permissions for entry in self.acl_entries}


@dataclass
class AclListResult:
    """
    Represents the complete result of listing ACLs, potentially across multiple entities.

    This is the top-level dataclass returned by the list_acl_async method, containing
    ACL information for one or more entities. It provides convenient methods for
    accessing and manipulating ACL data across the entire result set.

    Attributes:
        entity_acls: List of EntityAcl objects, each representing the ACL for one entity.
    """

    entity_acls: List[EntityAcl] = field(default_factory=list)
    """List of EntityAcl objects, each representing the ACL for one entity."""

    ascii_tree: Optional[str] = None
    """Optional ASCII tree representation of the ACLs. This is only populated when
    `log_tree` is set to True when calling `list_acl_async`."""

    @classmethod
    def from_dict(cls, acl_dict: Dict[str, Dict[str, List[str]]]) -> "AclListResult":
        """
        Create an AclListResult from the nested dictionary format returned by the API.

        Arguments:
            acl_dict: Nested dictionary where:
                - Keys are entity IDs (e.g., "syn123")
                - Values are dictionaries mapping principal IDs to permission lists

        Returns:
            A new AclListResult instance.

        Example:
            ```python
            api_response = {
                "syn123": {"273948": ["READ", "DOWNLOAD"], "12345": ["READ", "UPDATE"]},
                "syn456": {"273948": ["READ"], "67890": ["READ", "DELETE"]}
            }
            result = AclListResult.from_dict(api_response)
            ```
        """
        entity_acls = [
            EntityAcl.from_dict(entity_id, entity_acl_dict)
            for entity_id, entity_acl_dict in acl_dict.items()
        ]
        return cls(entity_acls=entity_acls)

    def to_dict(self) -> Dict[str, Dict[str, List[str]]]:
        """
        Convert this AclListResult to the nested dictionary format used by the API.

        Returns:
            Nested dictionary where:
            - Keys are entity IDs (e.g., "syn123")
            - Values are dictionaries mapping principal IDs to permission lists

        Example:
            ```python
            result = AclListResult(entity_acls=[...])
            api_format = result.to_dict()
            # Returns: {"syn123": {"273948": ["READ", "DOWNLOAD"]}, ...}
            ```
        """
        return {
            entity_acl.entity_id: entity_acl.to_dict()
            for entity_acl in self.entity_acls
        }
