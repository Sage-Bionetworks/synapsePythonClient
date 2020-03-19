import json

import pkg_resources

__version__ = json.load(pkg_resources.resource_stream(__name__,
                                                       'synapsePythonClient'))['latestVersion']
