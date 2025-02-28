# Table

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## Example Script

<details class="quote">
  <summary>Working with tables</summary>

```python
{!docs/scripts/object_orientated_programming_poc/oop_poc_table.py!}
```
</details>

## API Reference


::: synapseclient.models.Table
    options:
        inherited_members: true
        members:
        - get
        - store
        - delete
        - query
        - query_part_mask
        - store_rows
        - upsert_rows
        - delete_rows
        - snapshot
        - delete_column
        - add_column
        - reorder_column
        - get_permissions
        - get_acl
        - set_permissions

::: synapseclient.models.Column
    options:
        members:

::: synapseclient.models.SchemaStorageStrategy
::: synapseclient.models.ColumnExpansionStrategy

::: synapseclient.models.FacetType
::: synapseclient.models.ColumnType
::: synapseclient.models.JsonSubColumn


::: synapseclient.models.ColumnChange
::: synapseclient.models.PartialRow
::: synapseclient.models.PartialRowSet
::: synapseclient.models.TableSchemaChangeRequest
::: synapseclient.models.AppendableRowSetRequest
::: synapseclient.models.UploadToTableRequest
::: synapseclient.models.TableUpdateTransaction
::: synapseclient.models.CsvTableDescriptor
::: synapseclient.models.mixins.table_operator.csv_to_pandas_df
