import json
import pkg_resources
__version__ = json.loads(pkg_resources.resource_string('synapseclient', 'synapsePythonClient').decode())['latestVersion']

import requests
USER_AGENT = {'User-Agent':'synapseclient/%s %s' % (__version__, requests.utils.default_user_agent())}

