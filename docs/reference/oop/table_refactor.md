Contained within this file are proposed changes for interacting with Tables via this
client.



::: synapseclient.models.Table
    options:
        inherited_members: true
        members:
        - get_async
        - store_async
        - delete_async
        - query_async
        - store_rows_async
        - upsert_rows_async
        - delete_rows_async
        - snapshot_async
        - delete_column
        - add_column
        - reorder_column
        - set_columns
        - get_permissions
        - get_acl
        - set_permissions

::: synapseclient.models.table.SchemaStorageStrategy
::: synapseclient.models.table.ColumnExpansionStrategy

::: synapseclient.models.FacetType
::: synapseclient.models.ColumnType
::: synapseclient.models.table.JsonSubColumn

::: synapseclient.models.Column
    options:
        members:

::: synapseclient.models.table.ColumnChange
::: synapseclient.models.table.PartialRow
::: synapseclient.models.table.PartialRowSet
::: synapseclient.models.table.TableSchemaChangeRequest
::: synapseclient.models.table.AppendableRowSetRequest
::: synapseclient.models.table.UploadToTableRequest
::: synapseclient.models.table.TableUpdateTransactionRequest
::: synapseclient.models.table.CsvTableDescriptor
::: synapseclient.models.table.infer_column_type_from_data
::: synapseclient.models.table.csv_to_pandas_df
