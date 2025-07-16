[](){ #table-reference-sync }
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
        - list_acl
        - set_permissions
        - bind_schema
        - get_schema
        - unbind_schema
        - validate_schema
        - get_schema_derived_keys
        - delete_permissions

[](){ #column-reference-sync }
::: synapseclient.models.Column
    options:
        inherited_members: true
        members:
        - get
        - list

[](){ #schema-storage-strategy-reference-sync }
::: synapseclient.models.SchemaStorageStrategy
[](){ #column-expansion-strategy-reference-sync }
::: synapseclient.models.ColumnExpansionStrategy

[](){ #facet-type-reference-sync }
::: synapseclient.models.FacetType
[](){ #column-type-reference-sync }
::: synapseclient.models.ColumnType
[](){ #json-sub-column-reference-sync }
::: synapseclient.models.JsonSubColumn


[](){ #column-change-reference-sync }
::: synapseclient.models.ColumnChange
[](){ #partial-row-reference-sync }
::: synapseclient.models.PartialRow
[](){ #partial-row-set-reference-sync }
::: synapseclient.models.PartialRowSet
[](){ #table-schema-change-request-reference-sync }
::: synapseclient.models.TableSchemaChangeRequest
[](){ #appendable-row-set-request-reference-sync }
::: synapseclient.models.AppendableRowSetRequest
[](){ #upload-to-table-request-reference-sync }
::: synapseclient.models.UploadToTableRequest
[](){ #table-update-transaction-reference-sync }
::: synapseclient.models.TableUpdateTransaction
[](){ #csv-table-descriptor-reference-sync }
::: synapseclient.models.CsvTableDescriptor
[](){ #csv-to-pandas-df-reference-sync }
::: synapseclient.models.mixins.table_components.csv_to_pandas_df
