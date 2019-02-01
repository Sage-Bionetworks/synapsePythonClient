# These are exposed functions and objects from the `synapseclient.core.constants` package.
# However, these functions and objects are not public APIs for the Synapse Python client.
# The Synapse Engineering team is free to change their signatures and implementations anytime.
# Please use them at your own risk.

from .concrete_types import SYNAPSE_S3_UPLOAD_DESTINATION, EXTERNAL_UPLOAD_DESTINATION, EXTERNAL_S3_UPLOAD_DESTINATION,\
    EXTERNAL_OBJECT_STORE_UPLOAD_DESTINATION, EXTERNAL_OBJECT_STORE_FILE_HANDLE, S3_FILE_HANDLE,\
    ROW_REFERENCE_SET_RESULTS, ENTITY_UPDATE_RESULTS, TABLE_SCHEMA_CHANGE_RESPONSE, UPLOAD_TO_TABLE_RESULT,\
    PARTIAL_ROW_SET, APPENDABLE_ROWSET_REQUEST, COLUMN_MODEL
from .config_file_constants import AUTHENTICATION_SECTION_NAME