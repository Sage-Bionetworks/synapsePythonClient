Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

These APIs also introduce [AsyncIO](https://docs.python.org/3/library/asyncio.html) to
the client.

## API reference

::: synapseclient.models.Project
    options:
        members:
        - get
        - store
        - delete
---
::: synapseclient.models.Folder
    options:
        members:
        - get
        - store
        - delete
---
::: synapseclient.models.File
    options:
        members:
        - get
        - store
        - delete
---
::: synapseclient.models.Table
    options:
        members:
        - get
        - store_schema
        - store_rows_from_csv
        - delete_rows
        - query
        - delete
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
  <summary>Working with activities</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_activity.py!}
```
</details>

<details class="quote">
  <summary>Current Synapse interface for working with a project</summary>

```python
{!docs/scripts/object_orientated_programming_poc/synapse_project.py!}
```
</details>
