"""
Constant variables for Synapse's concreteType
"""

# Concrete types for StorageLocationSettings
SYNAPSE_S3_STORAGE_LOCATION_SETTING = (
    "org.sagebionetworks.repo.model.project.S3StorageLocationSetting"
)
EXTERNAL_S3_STORAGE_LOCATION_SETTING = (
    "org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting"
)
# EXTERNAL_GCP_STORAGE_LOCATION_SETTING = 'org.sagebionetworks.repo.model.project.ExternalGoogleCloudStorageLocationSetting'  # noqa: E501

# Concrete types for UploadDestinations
SYNAPSE_S3_UPLOAD_DESTINATION = (
    "org.sagebionetworks.repo.model.file.S3UploadDestination"
)
EXTERNAL_UPLOAD_DESTINATION = (
    "org.sagebionetworks.repo.model.file.ExternalUploadDestination"
)
EXTERNAL_S3_UPLOAD_DESTINATION = (
    "org.sagebionetworks.repo.model.file.ExternalS3UploadDestination"
)
EXTERNAL_GCP_UPLOAD_DESTINATION = (
    "org.sagebionetworks.repo.model.file.ExternalGoogleCloudUploadDestination"
)
EXTERNAL_OBJECT_STORE_UPLOAD_DESTINATION = (
    "org.sagebionetworks.repo.model.file.ExternalObjectStoreUploadDestination"
)

# Concrete types for FileHandles
EXTERNAL_OBJECT_STORE_FILE_HANDLE = (
    "org.sagebionetworks.repo.model.file.ExternalObjectStoreFileHandle"
)
EXTERNAL_FILE_HANDLE = "org.sagebionetworks.repo.model.file.ExternalFileHandle"
S3_FILE_HANDLE = "org.sagebionetworks.repo.model.file.S3FileHandle"

# Concrete types for Tables
ROW_REFERENCE_SET_RESULTS = (
    "org.sagebionetworks.repo.model.table.RowReferenceSetResults"
)
ENTITY_UPDATE_RESULTS = "org.sagebionetworks.repo.model.table.EntityUpdateResults"
TABLE_SCHEMA_CHANGE_RESPONSE = (
    "org.sagebionetworks.repo.model.table.TableSchemaChangeResponse"
)
TABLE_SCHEMA_CHANGE_REQUEST = (
    "org.sagebionetworks.repo.model.table.TableSchemaChangeRequest"
)
TABLE_UPDATE_TRANSACTION_REQUEST = (
    "org.sagebionetworks.repo.model.table.TableUpdateTransactionRequest"
)
UPLOAD_TO_TABLE_REQUEST = "org.sagebionetworks.repo.model.table.UploadToTableRequest"
UPLOAD_TO_TABLE_RESULT = "org.sagebionetworks.repo.model.table.UploadToTableResult"

PARTIAL_ROW_SET = "org.sagebionetworks.repo.model.table.PartialRowSet"
APPENDABLE_ROWSET_REQUEST = (
    "org.sagebionetworks.repo.model.table.AppendableRowSetRequest"
)

COLUMN_MODEL = "org.sagebionetworks.repo.model.table.ColumnModel"
COLUMN_CHANGE = "org.sagebionetworks.repo.model.table.ColumnChange"

# EntityTypes
FILE_ENTITY = "org.sagebionetworks.repo.model.FileEntity"
FOLDER_ENTITY = "org.sagebionetworks.repo.model.Folder"
LINK_ENTITY = "org.sagebionetworks.repo.model.Link"
PROJECT_ENTITY = "org.sagebionetworks.repo.model.Project"
RECORD_SET_ENTITY = "org.sagebionetworks.repo.model.RecordSet"
TABLE_ENTITY = "org.sagebionetworks.repo.model.table.TableEntity"
DATASET_ENTITY = "org.sagebionetworks.repo.model.table.Dataset"
DATASET_COLLECTION_ENTITY = "org.sagebionetworks.repo.model.table.DatasetCollection"
ENTITY_VIEW = "org.sagebionetworks.repo.model.table.EntityView"
MATERIALIZED_VIEW = "org.sagebionetworks.repo.model.table.MaterializedView"
SUBMISSION_VIEW = "org.sagebionetworks.repo.model.table.SubmissionView"
VIRTUAL_TABLE = "org.sagebionetworks.repo.model.table.VirtualTable"

# upload requests
MULTIPART_UPLOAD_REQUEST = "org.sagebionetworks.repo.model.file.MultipartUploadRequest"
MULTIPART_UPLOAD_COPY_REQUEST = (
    "org.sagebionetworks.repo.model.file.MultipartUploadCopyRequest"
)

# Activity/Provenance
USED_URL = "org.sagebionetworks.repo.model.provenance.UsedURL"
USED_ENTITY = "org.sagebionetworks.repo.model.provenance.UsedEntity"

# Agent
AGENT_CHAT_REQUEST = "org.sagebionetworks.repo.model.agent.AgentChatRequest"

# JSON Schema
GET_VALIDATION_SCHEMA_REQUEST = (
    "org.sagebionetworks.repo.model.schema.GetValidationSchemaRequest"
)
CREATE_SCHEMA_REQUEST = "org.sagebionetworks.repo.model.schema.CreateSchemaRequest"

# Query Table as a CSV
# https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/DownloadFromTableResult.html
QUERY_TABLE_CSV_REQUEST = (
    "org.sagebionetworks.repo.model.table.DownloadFromTableRequest"
)

# Query Table Bundle Request
# https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/QueryBundleRequest.html
QUERY_BUNDLE_REQUEST = "org.sagebionetworks.repo.model.table.QueryBundleRequest"

QUERY_RESULT = "org.sagebionetworks.repo.model.table.QueryResult"

QUERY_TABLE_CSV_RESULT = "org.sagebionetworks.repo.model.table.DownloadFromTableResult"

# Curation Task Types
CURATION_TASK = "org.sagebionetworks.repo.model.curation.CurationTask"
FILE_BASED_METADATA_TASK_PROPERTIES = (
    "org.sagebionetworks.repo.model.curation.metadata.FileBasedMetadataTaskProperties"
)
RECORD_BASED_METADATA_TASK_PROPERTIES = (
    "org.sagebionetworks.repo.model.curation.metadata.RecordBasedMetadataTaskProperties"
)

# Grid Session Types
CREATE_GRID_REQUEST = "org.sagebionetworks.repo.model.grid.CreateGridRequest"
GRID_RECORD_SET_EXPORT_REQUEST = (
    "org.sagebionetworks.repo.model.grid.GridRecordSetExportRequest"
)
LIST_GRID_SESSIONS_REQUEST = (
    "org.sagebionetworks.repo.model.grid.ListGridSessionsRequest"
)
LIST_GRID_SESSIONS_RESPONSE = (
    "org.sagebionetworks.repo.model.grid.ListGridSessionsResponse"
)
