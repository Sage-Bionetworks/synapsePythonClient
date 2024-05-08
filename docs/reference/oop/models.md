Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## Sample Scripts:

<details class="quote">
  <summary>Working with a project</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_project.py!}
```
</details>

<details class="quote">
  <summary>Working with folders</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_folder.py!}
```
</details>

<details class="quote">
  <summary>Working with files</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_file.py!}
```
</details>

<details class="quote">
  <summary>Working with tables</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_table.py!}
```
</details>

<details class="quote">
  <summary>Current Synapse interface for working with a project</summary>

```python
{!docs/scripts/object_orientated_programming_poc/synapse_project.py!}
```
</details>

<details class="quote">
  <summary>Working with activities</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_activity.py!}
```
</details>

<details class="quote">
  <summary>Working with teams</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_team.py!}
```
</details>

## API reference

::: synapseclient.models.Project
    options:
        inherited_members: true
        members:
        - get
        - store
        - delete
        - sync_from_synapse
        - get_permissions
        - get_acl
        - set_permissions
---
::: synapseclient.models.Folder
    options:
        inherited_members: true
        members:
        - get
        - store
        - delete
        - copy
        - sync_from_synapse
        - get_permissions
        - get_acl
        - set_permissions
---
::: synapseclient.models.File
    options:
        inherited_members: true
        members:
        - get
        - store
        - copy
        - delete
        - from_id
        - from_path
        - change_metadata
        - get_permissions
        - get_acl
        - set_permissions
::: synapseclient.models.file.FileHandle
    options:
      filters:
      - "!"
---
::: synapseclient.models.Table
    options:
        inherited_members: true
        members:
        - get
        - store_schema
        - store_rows_from_csv
        - delete_rows
        - query
        - delete
        - get_permissions
        - get_acl
        - set_permissions
---
::: synapseclient.models.Activity
    options:
      members:
      - from_parent
      - store
      - delete

::: synapseclient.models.UsedEntity
    options:
      filters:
      - "!"
::: synapseclient.models.UsedURL
    options:
      filters:
      - "!"
---
::: synapseclient.models.Team
    options:
        members:
        - create
        - delete
        - from_id
        - from_name
        - members
        - invite
        - open_invitations
---
::: synapseclient.models.UserProfile
    options:
      members:
      - get
      - from_id
      - from_username
      - is_certified
::: synapseclient.models.UserPreference
---
::: synapseclient.models.Annotations
    options:
      members:
      - from_dict
---
::: synapseclient.models.mixins.AccessControllable
---

::: synapseclient.models.mixins.StorableContainer
---
::: synapseclient.models.FailureStrategy
