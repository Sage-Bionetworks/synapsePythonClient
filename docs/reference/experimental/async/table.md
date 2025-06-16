# Table

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## API Reference

[](){ #table-reference-async }
::: synapseclient.models.Table
    options:
        inherited_members: true
        members:
        - get_async
        - store_async
        - delete_async
        - query_async
        - query_part_mask_async
        - store_rows_async
        - upsert_rows_async
        - delete_rows_async
        - snapshot_async
        - delete_column
        - add_column
        - reorder_column
        - get_permissions_async
        - get_acl_async
        - set_permissions_async
        - delete_permissions_async
        - list_acl_async

[](){ #column-reference-async }
::: synapseclient.models.Column
    options:
        members:

[](){ #schema-storage-strategy-reference-async }
::: synapseclient.models.SchemaStorageStrategy
[](){ #column-expansion-strategy-reference-async }
::: synapseclient.models.ColumnExpansionStrategy

[](){ #facet-type-reference-async }
::: synapseclient.models.FacetType
[](){ #column-type-reference-async }
::: synapseclient.models.ColumnType
[](){ #json-sub-column-reference-async }
::: synapseclient.models.JsonSubColumn


[](){ #column-change-reference-async }
::: synapseclient.models.ColumnChange
[](){ #partial-row-reference-async }
::: synapseclient.models.PartialRow
[](){ #partial-row-set-reference-async }
::: synapseclient.models.PartialRowSet
[](){ #table-schema-change-request-reference-async }
::: synapseclient.models.TableSchemaChangeRequest
[](){ #appendable-row-set-request-reference-async }
::: synapseclient.models.AppendableRowSetRequest
[](){ #upload-to-table-request-reference-async }
::: synapseclient.models.UploadToTableRequest
[](){ #table-update-transaction-reference-async }
::: synapseclient.models.TableUpdateTransaction
[](){ #csv-table-descriptor-reference-async }
::: synapseclient.models.CsvTableDescriptor
[](){ #csv-to-pandas-df-reference-async }
::: synapseclient.models.mixins.table_components.csv_to_pandas_df
