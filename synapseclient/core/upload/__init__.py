# These are exposed functions and objects from the `synapseclient.core.upload` package.
# However, these functions and objects are not public APIs for the Synapse Python client.
# The Synapse Engineering team is free to change their signatures and implementations anytime.
# Please use them at your own risk.

from .upload_functions import upload_file_handle, upload_synapse_s3
from .multipart_upload import multipart_upload_file, multipart_upload_string
