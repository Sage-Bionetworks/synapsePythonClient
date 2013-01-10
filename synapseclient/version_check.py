## Check for latest version and recommend upgrade
##
############################################################
from distutils.version import StrictVersion
from utils import downloadFile
import requests
import json
import sys
import traceback
import warnings
import pkg_resources


def version_check(current_version=None,
                  version_url="http://versions.synapse.sagebase.org/synapsePythonClient",
                  upgrade_url="https://github.com/Sage-Bionetworks/synapsePythonClient"):
    """
    Get the latest version information from version_url and check against
    the current version. Recommend upgrade, if a newer version exists.
    """

    try:
        if (not current_version):
            current_version = json.loads(pkg_resources.resource_string('synapseclient', 'synapsePythonClient'))['latestVersion']

        headers = { 'Accept': 'application/json' }
        version_info = requests.get(version_url, headers=headers).json()

        ## check blacklist
        if current_version in version_info['blacklist']:
            msg = "\nPLEASE UPGRADE YOUR CLIENT\n\nUpgrading your SynapseClient is required. Please visit:\n%s\n\n" % (upgrade_url,)
            raise SystemExit(msg)

        if 'message' in version_info:
            sys.stdout.write(version_info['message'] + '\n')

        ## check latest version
        if StrictVersion(current_version) < StrictVersion(version_info['latestVersion']):
            msg = "\nUPGRADE AVAILABLE\n\nA more recent version of the Synapse Client (%s) is available. Your version (%s) can be upgraded by visiting:\n%s\n\n" % (version_info['latestVersion'], current_version, upgrade_url,)
            sys.stdout.write(msg)
            if 'releaseNotes' in version_info:
                sys.stdout.write(version_info['releaseNotes'] + '\n')
            return False

    except Exception, e:
        ## don't prevent the client from running if something goes wrong
        sys.stderr.write("Exception in version check: %s" % (str(e),))
        return False

    return True

## if this file is run as a script, print current version
## then perform version check
if __name__ == "__main__":
    print("Python Synapse Client version: %s" % json.loads(pkg_resources.resource_string('synapseclient', 'synapsePythonClient'))['latestVersion'])
    if version_check():
        print("ok")
