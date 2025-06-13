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

    def has_permission(self, permission: str) -> bool:
        """
        Check if this ACL entry includes a specific permission.

        Arguments:
            permission: The permission string to check for (e.g., 'READ', 'DOWNLOAD').

        Returns:
            True if the permission is granted, False otherwise.
        """
        return permission in self.permissions

    def add_permission(self, permission: str) -> None:
        """
        Add a permission to this ACL entry if not already present.

        Arguments:
            permission: The permission string to add.
        """
        if permission not in self.permissions:
            self.permissions.append(permission)

    def remove_permission(self, permission: str) -> None:
        """
        Remove a permission from this ACL entry if present.

        Arguments:
            permission: The permission string to remove.
        """
        if permission in self.permissions:
            self.permissions.remove(permission)


@dataclass
class EntityAcl:
    """
    Represents the complete Access Control List for a single Synapse entity.

    This dataclass contains all ACL entries for an entity, organizing them
    by principal ID and providing convenient methods for ACL management.

    Attributes:
        entity_id: The Synapse ID of the entity (e.g., "syn123").
        acl_entries: List of ACL entries, each representing permissions for a principal.
    """

    entity_id: str
    """The Synapse ID of the entity (e.g., "syn123")."""

    acl_entries: List[AclEntry] = field(default_factory=list)
    """List of ACL entries, each representing permissions for a principal."""

    def get_acl_entry(self, principal_id: str) -> Optional[AclEntry]:
        """
        Get the ACL entry for a specific principal.

        Arguments:
            principal_id: The ID of the principal to look up.

        Returns:
            The AclEntry for the principal, or None if not found.
        """
        for entry in self.acl_entries:
            if entry.principal_id == principal_id:
                return entry
        return None

    def get_principals(self) -> List[str]:
        """
        Get a list of all principal IDs that have ACL entries for this entity.

        Returns:
            List of principal ID strings.
        """
        return [entry.principal_id for entry in self.acl_entries]

    def get_permissions_for_principal(self, principal_id: str) -> List[str]:
        """
        Get the permissions list for a specific principal.

        Arguments:
            principal_id: The ID of the principal to look up.

        Returns:
            List of permission strings, or empty list if principal not found.
        """
        entry = self.get_acl_entry(principal_id)
        return entry.permissions if entry else []

    def add_acl_entry(self, acl_entry: AclEntry) -> None:
        """
        Add an ACL entry to this entity ACL.

        If an entry for the same principal already exists, it will be replaced.

        Arguments:
            acl_entry: The ACL entry to add.
        """
        # Remove existing entry for the same principal if it exists
        self.acl_entries = [
            entry
            for entry in self.acl_entries
            if entry.principal_id != acl_entry.principal_id
        ]
        self.acl_entries.append(acl_entry)

    def remove_acl_entry(self, principal_id: str) -> bool:
        """
        Remove the ACL entry for a specific principal.

        Arguments:
            principal_id: The ID of the principal whose ACL entry should be removed.

        Returns:
            True if an entry was removed, False if the principal was not found.
        """
        original_length = len(self.acl_entries)
        self.acl_entries = [
            entry for entry in self.acl_entries if entry.principal_id != principal_id
        ]
        return len(self.acl_entries) < original_length

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

    def get_entity_acl(self, entity_id: str) -> Optional[EntityAcl]:
        """
        Get the ACL for a specific entity.

        Arguments:
            entity_id: The Synapse ID of the entity to look up.

        Returns:
            The EntityAcl for the entity, or None if not found.
        """
        for entity_acl in self.entity_acls:
            if entity_acl.entity_id == entity_id:
                return entity_acl
        return None

    def get_entity_ids(self) -> List[str]:
        """
        Get a list of all entity IDs included in this ACL result.

        Returns:
            List of entity ID strings.
        """
        return [entity_acl.entity_id for entity_acl in self.entity_acls]

    def get_permissions_for_entity_and_principal(
        self, entity_id: str, principal_id: str
    ) -> List[str]:
        """
        Get the permissions for a specific principal on a specific entity.

        Arguments:
            entity_id: The Synapse ID of the entity.
            principal_id: The ID of the principal.

        Returns:
            List of permission strings, or empty list if entity or principal not found.
        """
        entity_acl = self.get_entity_acl(entity_id)
        if entity_acl:
            return entity_acl.get_permissions_for_principal(principal_id)
        return []

    def add_entity_acl(self, entity_acl: EntityAcl) -> None:
        """
        Add an EntityAcl to this result.

        If an ACL for the same entity already exists, it will be replaced.

        Arguments:
            entity_acl: The EntityAcl to add.
        """
        # Remove existing ACL for the same entity if it exists
        self.entity_acls = [
            acl for acl in self.entity_acls if acl.entity_id != entity_acl.entity_id
        ]
        self.entity_acls.append(entity_acl)

    def remove_entity_acl(self, entity_id: str) -> bool:
        """
        Remove the ACL for a specific entity.

        Arguments:
            entity_id: The Synapse ID of the entity whose ACL should be removed.

        Returns:
            True if an ACL was removed, False if the entity was not found.
        """
        original_length = len(self.entity_acls)
        self.entity_acls = [
            acl for acl in self.entity_acls if acl.entity_id != entity_id
        ]
        return len(self.entity_acls) < original_length

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

    def get_all_principals(self) -> List[str]:
        """
        Get a list of all unique principal IDs across all entities in this result.

        Returns:
            List of unique principal ID strings.
        """
        all_principals = set()
        for entity_acl in self.entity_acls:
            all_principals.update(entity_acl.get_principals())
        return list(all_principals)

    def get_entities_for_principal(self, principal_id: str) -> List[str]:
        """
        Get a list of entity IDs where the specified principal has any permissions.

        Arguments:
            principal_id: The ID of the principal to look up.

        Returns:
            List of entity ID strings where the principal has permissions.
        """
        entity_ids = []
        for entity_acl in self.entity_acls:
            if entity_acl.get_acl_entry(principal_id):
                entity_ids.append(entity_acl.entity_id)
        return entity_ids

    def is_empty(self) -> bool:
        """
        Check if this result contains any ACL data.

        Returns:
            True if there are no entity ACLs, False otherwise.
        """
        return len(self.entity_acls) == 0

    def __len__(self) -> int:
        """
        Get the number of entities in this ACL result.

        Returns:
            The number of entities with ACL data.
        """
        return len(self.entity_acls)
