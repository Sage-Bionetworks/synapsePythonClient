# Sharing Settings (ACL Management) in Synapse

Access Control Lists (ACLs) in Synapse determine who can access your data and what actions they can perform. By default, entities inherit permissions from their parent container, but you can create local sharing settings to provide more granular control over access to specific folders and files.

This tutorial demonstrates how to use the Python client to manage permissions and sharing settings for your Synapse entities using the new object-oriented models.

[Read more about Access Control](../../explanations/access_control.md)

## Tutorial Purpose
In this tutorial you will:

1. Retrieve an existing project or create a new one
2. Create a main folder to set custom sharing settings
3. Examine current permissions using `get_permissions()` and `get_acl()`
4. Get ACL for specific principal using `get_acl()`
5. Set specific permissions using `set_permissions()`
6. Create a sub-folder with different permissions than its parent
7. List all ACLs in a hierarchy using `list_acl()`
8. Manage permissions with advanced options
9. Remove permissions and delete ACLs using `delete_permissions()`

## Prerequisites
* Make sure that you have completed the [Installation](../installation.md) and [Authentication](../authentication.md) setup.
* **IMPORTANT**: You must set a valid `PRINCIPAL_ID` in the tutorial script before running it. This should be a Synapse user ID or team ID that you want to grant permissions to.

## Understanding Permission Types

Before we begin, it's important to understand the different permission types available in Synapse:

- **READ**: View entity metadata and download files
- **DOWNLOAD**: Download files (requires READ)
- **UPDATE**: Modify entity metadata and upload new file versions
- **CREATE**: Create new entities within containers
- **DELETE**: Delete entities
- **CHANGE_PERMISSIONS**: Modify ACL permissions
- **CHANGE_SETTINGS**: Modify entity settings
- **MODERATE**: Moderate forum discussions (for projects)

## Special Principal IDs

- **273948**: All authenticated Synapse users
- **273949**: Public access (anyone on the internet)
- **Specific user ID**: Individual Synapse user (e.g., 123456)
- **Team ID**: Synapse team

## Permission Inheritance and Benefactors

Understanding how permissions work in Synapse is crucial for effective ACL management:

### Benefactor Concept
Every entity in Synapse has a **benefactor** - the entity from which it inherits its permissions. When checking permissions for an entity:

1. **Start at the current entity**: Check if it has local sharing settings (its own ACL)
2. **Traverse up the hierarchy**: If no local ACL exists, continue checking parent containers
3. **Find the benefactor**: The search continues up the tree until an entity with permissions/ACL is found, or until reaching the Project level
4. **Inherit permissions**: The entity inherits all permissions from its benefactor

### Default Inheritance Behavior
- **New Projects**: Become their own benefactor with default permissions
- **New Folders/Files**: Initially inherit from their containing Project or parent Folder
- **Local ACLs**: When you set local sharing settings, the entity becomes its own benefactor

### Applying Permissions to Other Entities
While this tutorial focuses on Folders, you can apply the same permission management methods (`get_permissions()`, `get_acl()`, `set_permissions()`, `list_acl()`, and `delete_permissions()`) to other Synapse entities including:
- **Projects**: Top-level containers that are typically their own benefactors
- **Files**: Individual data files that can have their own sharing settings
- **Tables**: Structured data entities with their own permission requirements
- **Views**: Query-based entities that can have custom access controls

The inheritance and benefactor concepts apply consistently across all entity types in Synapse.

## 1. Set Up and Get Project

**⚠️ IMPORTANT**: Before running the tutorial, you MUST edit the script to set a valid `PRINCIPAL_ID`.

```python
{!docs/tutorials/python/tutorial_scripts/sharing_settings.py!lines=17-34}
```

## 2. Create a main folder to set custom sharing settings

```python
{!docs/tutorials/python/tutorial_scripts/sharing_settings.py!lines=36-45}
```

## 3. Examine Current Permissions

The `get_permissions()` method returns the permissions that the current user has on an entity

```python
{!docs/tutorials/python/tutorial_scripts/sharing_settings.py!lines=46-49}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
=== Step 3: Getting current user's permissions ===
Current user permissions on main folder: ['READ', 'UPDATE', 'CREATE', 'DELETE', 'DOWNLOAD', 'MODERATE', 'CHANGE_PERMISSIONS', 'CHANGE_SETTINGS']
```
</details>

## 4. Get ACL for Specific Principal

The `get_acl()` method gets the specific permissions for a given principal (user or team):

```python
{!docs/tutorials/python/tutorial_scripts/sharing_settings.py!lines=51-62}
```

Depending on if you've already given permissions to given user/team your may see
different results in the output.

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
=== Step 4: Getting ACL for specific principal ===
Principal ######## current permissions (including inherited): []
Principal ######## permissions on folder only: []
```
</details>

## 5. Set Custom Permissions

Use `set_permissions()` to grant specific permissions to a user or team:

```python
{!docs/tutorials/python/tutorial_scripts/sharing_settings.py!lines=63-79}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
=== Step 5: Setting permissions on main folder ===
[WARNING] Creating an ACL for entity syn#######, which formerly inherited access control from a benefactor entity, "My uniquely named project about Alzheimer's Disease" (syn########).

Set permissions for principal ####### on main folder: ['READ', 'DOWNLOAD']
Verified new permissions: ['DOWNLOAD', 'READ']
```
</details>

## 6. Create Sub-folder with Different Permissions

Create a sub-folder and give it more restrictive permissions than its parent:

```python
{!docs/tutorials/python/tutorial_scripts/sharing_settings.py!lines=80-101}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
=== Step 6: Creating sub-folder with different permissions ===
Created sub-folder: Sensitive Analysis (ID: syn#######)
[WARNING] Creating an ACL for entity syn#######, which formerly inherited access control from a benefactor entity, "Research Data" (syn#######).

Set more restrictive permissions for principal ####### on sub-folder: ['READ']
```
</details>

## 7. List All ACLs

Use `list_acl()` to see all the permissions in your hierarchy. By specifying
the `log_tree=True` argument for the method we can get an ascii tree representation of
the ACLs on the project. Alternatively you will also be able to loop over the data.

```python
{!docs/tutorials/python/tutorial_scripts/sharing_settings.py!lines=102-117}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
=== Step 7: Listing ACLs ===
ACL Tree Structure:
🔍 Legend:
  🔒 = No local ACL (inherits from parent)
  🔑 = Local ACL (custom permissions)

└── 🔑 Research Data (syn#######) [Folder]
       └── user_name_1 (#######): CHANGE_PERMISSIONS, DOWNLOAD, CHANGE_SETTINGS, MODERATE, READ, DELETE, CREATE, UPDATE
       └── team_name_1 (#######): DOWNLOAD, READ
    └── 🔑 Sensitive Analysis (syn#######) [Folder]
           └── user_name_1 (#######): CHANGE_PERMISSIONS, UPDATE, READ, DELETE, MODERATE, CHANGE_SETTINGS, DOWNLOAD, CREATE
           └── team_name_1 (#######): READ
Folder (syn#######) ACL entries:

Entity syn####### ACL:
  Principal #######: ['CHANGE_PERMISSIONS', 'DOWNLOAD', 'CHANGE_SETTINGS', 'MODERATE', 'READ', 'DELETE', 'CREATE', 'UPDATE']
  Principal #######: ['DOWNLOAD', 'READ']

Entity syn####### ACL:
  Principal #######: ['CHANGE_PERMISSIONS', 'UPDATE', 'READ', 'DELETE', 'MODERATE', 'CHANGE_SETTINGS', 'DOWNLOAD', 'CREATE']
  Principal #######: ['READ']
```
</details>

## 8. Advanced Permission Management

Using `overwrite=False` will allow you to add Permissions Non-destructively

```python
{!docs/tutorials/python/tutorial_scripts/sharing_settings.py!lines=118-132}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
=== Step 8: Demonstrating additional permission options ===
Adding additional permissions non-destructively...
Updated permissions after adding UPDATE: ['READ', 'DOWNLOAD', 'UPDATE']
```
</details>

## 9. Remove Permissions

Remove permissions for a specific principal by setting an empty access type list:

```python
{!docs/tutorials/python/tutorial_scripts/sharing_settings.py!lines=133-147}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
=== Step 9: Removing specific permissions ===
Before removal - Principal ####### permissions: ['READ', 'DOWNLOAD', 'UPDATE']
After removal - Principal ####### permissions: []
```
</details>

## 10. Delete Entire ACLs

Use `delete_permissions()` to remove entire ACLs and revert to inheritance:

```python
{!docs/tutorials/python/tutorial_scripts/sharing_settings.py!lines=149-167}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
=== Step 10: Demonstrating delete_permissions() ===
Re-added permissions to sub-folder for demonstration

Dry run - showing what would be deleted:
=== DRY RUN: Permission Deletion Impact Analysis ===
📊 Summary: 1 entities with local ACLs to delete, 1 entities will change inheritance

🔍 Legend:
  ⚠️  = Local ACL will be deleted
  🚫 = Local ACL will NOT be deleted
  ↗️ = Currently inherits permissions
  🔒 = No local ACL (inherits from parent)
  🔑 = Permission that will be removed
  📍 = New inheritance after deletion

Sensitive Analysis (syn#######) [Folder] ⚠️  WILL DELETE LOCAL ACL → Will inherit from syn#######
├── 🔑 user_name_1 (#######): ['CHANGE_SETTINGS', 'MODERATE', 'DELETE', 'READ', 'DOWNLOAD', 'UPDATE', 'CHANGE_PERMISSIONS', 'CREATE'] → WILL BE REMOVED
└── 🔑 team_name_1 (#######): ['READ', 'UPDATE'] → WILL BE REMOVED
=== End of Dry Run Analysis ===

Actually deleting ACL from sub-folder:
Sub-folder now inherits permissions: []
```
</details>

## 11. Final Overview

Get a complete view of your permission structure:

```python
{!docs/tutorials/python/tutorial_scripts/sharing_settings.py!lines=168-172}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
=== Step 11: Final ACL overview ===
ACL Tree Structure:
🔍 Legend:
  🔒 = No local ACL (inherits from parent)
  🔑 = Local ACL (custom permissions)

└── 🔑 Research Data (syn#######) [Folder]
       └── user_name_1 (#######): CHANGE_PERMISSIONS, DOWNLOAD, CHANGE_SETTINGS, MODERATE, READ, DELETE, CREATE, UPDATE
```
</details>

## Key Concepts Covered

### Inheritance vs Local ACLs
- **Inheritance**: Entities inherit permissions from their parent by default
- **Local ACLs**: Create specific permissions that override inheritance
- **Benefactor**: The entity from which permissions are inherited

### Permission Management Functions
- **[`get_permissions()`][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.get_permissions]**: Get current user's effective permissions
- **[`get_acl()`][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.get_acl]**: Get specific principal's permissions on an entity
- **[`set_permissions()`][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.set_permissions]**: Set permissions for a principal
- **[`list_acl()`][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.list_acl]**: List all ACLs in a hierarchy
- **[`delete_permissions()`][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.delete_permissions]**: Remove ACLs and revert to inheritance

### Best Practices
1. Use inheritance when possible for easier management
2. Create local ACLs only when you need different permissions than the parent
3. Use teams for managing group permissions
4. Test with `dry_run=True` before deleting permissions
5. Use `list_acl()` to audit your permission structure

## Common Use Cases

### Research Collaboration
- Give collaborators READ/DOWNLOAD on data folders
- Restrict UPDATE/DELETE to data owners
- Use teams for lab group permissions

### Data Sharing
- Use authenticated users (273948) for internal sharing
- Use public access (273949) for open data
- Create different permission levels for different data sensitivity

### Hierarchical Access
- Set broad permissions at project level
- Override with restrictive permissions for sensitive sub-folders
- Use `delete_permissions()` to revert to parent permissions when needed

## Source code for this tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/sharing_settings.py!}
```
</details>

## References used in this tutorial

- [Folder][synapseclient.models.Folder]
- [Project][synapseclient.models.Project]
- [AccessControllable Protocol][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol]
- [get_permissions][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.get_permissions]
- [get_acl][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.get_acl]
- [set_permissions][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.set_permissions]
- [list_acl][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.list_acl]
- [delete_permissions][synapseclient.models.protocols.access_control_protocol.AccessControllableSynchronousProtocol.delete_permissions]
- [syn.login][synapseclient.Synapse.login]
