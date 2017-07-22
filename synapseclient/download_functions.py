from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from __future__ import unicode_literals

import os
from .utils import is_url, md5_for_file
from . import concrete_types
import sys
from .remote_file_connection import ClientS3Connection
from .multipart_upload import  multipart_upload
try:
    from urllib.parse import urlparse
    from urllib.parse import urlunparse
    from urllib.parse import quote
    from urllib.parse import unquote
    from urllib.request import urlretrieve
except ImportError:
    from urlparse import urlparse
    from urlparse import urlunparse
    from urllib import quote
    from urllib import unquote
    from urllib import urlretrieve


