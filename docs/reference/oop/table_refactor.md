Contained within this file are proposed changes for interacting with Tables via this
client.



::: synapseclient.models.Table
    options:
        inherited_members: true
        members:
        - get_async-
        - store_async-
        - delete_async-
        - query_async-
        - store_rows_async-
        - upsert_rows_async-
        - delete_rows_async-
        - snapshot_async-
        - delete_column-
        - add_column-
        - reorder_column-
        - set_columns-
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

::: synapseclient.models.ColumnChange
::: synapseclient.models.PartialRow
::: synapseclient.models.PartialRowSet
::: synapseclient.models.TableSchemaChangeRequest
::: synapseclient.models.AppendableRowSetRequest
::: synapseclient.models.UploadToTableRequest
::: synapseclient.models.TableUpdateTransactionRequest
::: synapseclient.models.CsvTableDescriptor
::: synapseclient.models.infer_column_type_from_data
::: synapseclient.models.csv_to_pandas_df
