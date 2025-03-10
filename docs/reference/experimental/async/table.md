# Table

Contained within this file are experimental interfaces for working with the Synapse Python
Client. Unless otherwise noted these interfaces are subject to change at any time. Use
at your own risk.

## API Reference

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
::: synapseclient.models.mixins.table_components.csv_to_pandas_df
