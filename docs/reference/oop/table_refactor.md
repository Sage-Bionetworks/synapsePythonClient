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

::: synapseclient.models.mixins.table_operator.SchemaStorageStrategy
::: synapseclient.models.mixins.table_operator.ColumnExpansionStrategy

::: synapseclient.models.mixins.table_operator.FacetType
::: synapseclient.models.ColumnType
::: synapseclient.models.mixins.table_operator.JsonSubColumn

::: synapseclient.models.Column
    options:
        members:

::: synapseclient.models.mixins.table_operator.ColumnChange
::: synapseclient.models.mixins.table_operator.PartialRow
::: synapseclient.models.mixins.table_operator.PartialRowSet
::: synapseclient.models.mixins.table_operator.TableSchemaChangeRequest
::: synapseclient.models.mixins.table_operator.AppendableRowSetRequest
::: synapseclient.models.mixins.table_operator.UploadToTableRequest
::: synapseclient.models.mixins.table_operator.TableUpdateTransaction
::: synapseclient.models.mixins.table_operator.CsvTableDescriptor
::: synapseclient.models.mixins.table_operator.infer_column_type_from_data
::: synapseclient.models.mixins.table_operator.csv_to_pandas_df
