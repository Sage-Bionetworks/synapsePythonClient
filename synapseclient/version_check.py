## Check for latest version and recommend upgrade
##
############################################################
from distutils.version import StrictVersion
import re
import requests
import json
import sys
import pkg_resources
import synapseclient


_VERSION_URL     = 'http://versions.synapse.sagebase.org/synapsePythonClient'
_DEV_VERSION_URL = 'http://dev-versions.synapse.sagebase.org/synapsePythonClient'
_GITHUB_URL      = 'https://github.com/Sage-Bionetworks/synapsePythonClient'


def version_check(current_version=None, version_url=_VERSION_URL):
    """
    Gets the latest version information from version_url and check against
    the current version.  Recommends upgrade, if a newer version exists.

    :returns: True if current version is the latest release (or higher) version,
              False otherwise.
    """

    try:
        if (not current_version):
            current_version = synapseclient.__version__

        if version_url is None:
            version_info = json.loads(pkg_resources.resource_string('synapseclient', 'synapsePythonClient'))
        else:
            headers = { 'Accept': 'application/json' }
            headers.update(synapseclient.USER_AGENT)
            version_info = requests.get(version_url, headers=headers).json()

        # strip off .devNN suffix, which StrictVersion doesn't like
        current_base_version = re.sub(r'\.dev\d+', '', current_version)

        # Check blacklist
        if current_base_version in version_info['blacklist']:
            msg = ("\nPLEASE UPGRADE YOUR CLIENT\n\nUpgrading your SynapseClient is required. "
                   "Please upgrade your client by typing:\n"
                   "    pip install --upgrade synapseclient\n\n")
            raise SystemExit(msg)

        if 'message' in version_info:
            sys.stdout.write(version_info['message'] + '\n')

        # Compare with latest version
        if StrictVersion(current_base_version) < StrictVersion(version_info['latestVersion']):
            msg = ("\nUPGRADE AVAILABLE\n\nA more recent version of the Synapse Client (%s) is available. "
                   "Your version (%s) can be upgraded by typing:\n"
                   "    pip install --upgrade synapseclient\n\n") % (version_info['latestVersion'], current_version,)
            sys.stdout.write(msg)
            if 'releaseNotes' in version_info:
                sys.stdout.write(version_info['releaseNotes'] + '\n')
            return False

    except Exception, e:
        # Don't prevent the client from running if something goes wrong
        sys.stderr.write("Exception in version check: %s\n" % (str(e),))
        return False

    return True


# If this file is run as a script, print current version
# then perform version check
if __name__ == "__main__":
    print "Version check"
    print "============="
    print("Python Synapse Client version: %s" % synapseclient.__version__)

    print("Check against production version:")
    if version_check():
        print("ok")

    print("Check against dev version:")
    if version_check(version_url=_DEV_VERSION_URL):
        print("ok")

    print("Check against local copy of version file:")
    if version_check(version_url=None):
        print("ok")

