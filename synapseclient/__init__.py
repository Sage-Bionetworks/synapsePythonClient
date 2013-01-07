import json 
import pkg_resources

from client import Synapse
__version__=json.loads(pkg_resources.resource_string('synapseclient', 'synapsePythonClient'))['latestVersion']

