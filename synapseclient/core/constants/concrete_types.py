"""
Constant variables for Synapse's concreteType
"""

# Concrete types for StorageLocationSettings
SYNAPSE_S3_STORAGE_LOCATION_SETTING = 'org.sagebionetworks.repo.model.project.S3StorageLocationSetting'
EXTERNAL_S3_STORAGE_LOCATION_SETTING = 'org.sagebionetworks.repo.model.project.ExternalS3StorageLocationSetting'

# Concrete types for UploadDestinations
SYNAPSE_S3_UPLOAD_DESTINATION = 'org.sagebionetworks.repo.model.file.S3UploadDestination'
EXTERNAL_UPLOAD_DESTINATION = 'org.sagebionetworks.repo.model.file.ExternalUploadDestination'
EXTERNAL_S3_UPLOAD_DESTINATION = 'org.sagebionetworks.repo.model.file.ExternalS3UploadDestination'
EXTERNAL_OBJECT_STORE_UPLOAD_DESTINATION = 'org.sagebionetworks.repo.model.file.ExternalObjectStoreUploadDestination'

# Concrete types for FileHandles
EXTERNAL_OBJECT_STORE_FILE_HANDLE = "org.sagebionetworks.repo.model.file.ExternalObjectStoreFileHandle"
EXTERNAL_FILE_HANDLE = 'org.sagebionetworks.repo.model.file.ExternalFileHandle'
S3_FILE_HANDLE = "org.sagebionetworks.repo.model.file.S3FileHandle"

# Concrete types for TableUpdateResponse
ROW_REFERENCE_SET_RESULTS = 'org.sagebionetworks.repo.model.table.RowReferenceSetResults'
ENTITY_UPDATE_RESULTS = 'org.sagebionetworks.repo.model.table.EntityUpdateResults'
TABLE_SCHEMA_CHANGE_RESPONSE = 'org.sagebionetworks.repo.model.table.TableSchemaChangeResponse'
UPLOAD_TO_TABLE_RESULT = 'org.sagebionetworks.repo.model.table.UploadToTableResult'

PARTIAL_ROW_SET = 'org.sagebionetworks.repo.model.table.PartialRowSet'
APPENDABLE_ROWSET_REQUEST = 'org.sagebionetworks.repo.model.table.AppendableRowSetRequest'

COLUMN_MODEL = 'org.sagebionetworks.repo.model.table.ColumnModel'
