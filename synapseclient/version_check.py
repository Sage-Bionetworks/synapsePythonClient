## Check for latest version and recommend upgrade
##
############################################################
import re
import requests
import json
import sys
import pkg_resources
import synapseclient


_VERSION_URL     = 'http://versions.synapse.sagebase.org/synapsePythonClient'
_DEV_VERSION_URL = 'http://dev-versions.synapse.sagebase.org/synapsePythonClient'
_GITHUB_URL      = 'https://github.com/Sage-Bionetworks/synapsePythonClient'


def version_check(current_version=None, version_url=_VERSION_URL, check_for_point_releases=False):
    """
    Gets the latest version information from version_url and check against
    the current version.  Recommends upgrade, if a newer version exists.

    :returns: True if current version is the latest release (or higher) version,
              False otherwise.
    """

    try:
        if (not current_version):
            current_version = synapseclient.__version__

        version_info = _get_version_info(version_url)

        current_base_version = _strip_dev_suffix(current_version)

        # Check blacklist
        if current_base_version in version_info['blacklist'] or current_version in version_info['blacklist']:
            msg = ("\nPLEASE UPGRADE YOUR CLIENT\n\nUpgrading your SynapseClient is required. "
                   "Please upgrade your client by typing:\n"
                   "    pip install --upgrade synapseclient\n\n")
            raise SystemExit(msg)

        if 'message' in version_info:
            sys.stdout.write(version_info['message'] + '\n')

        levels = 3 if check_for_point_releases else 2

        # Compare with latest version
        if _version_tuple(current_version, levels=levels) < _version_tuple(version_info['latestVersion'], levels=levels):
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


def check_for_updates(current_version=None):
    print('Python Synapse Client')
    print('currently running version:  %s' % synapseclient.__version__)

    release_version_info = _get_version_info(_VERSION_URL)
    print('latest release version:     %s' % release_version_info['latestVersion'])

    dev_version_info = _get_version_info(_DEV_VERSION_URL)
    print('latest development version: %s' % dev_version_info['latestVersion'])

    if _version_tuple(synapseclient.__version__, levels=3) < _version_tuple(release_version_info['latestVersion'], levels=3):
        msg = ("\nUPGRADE AVAILABLE\n\nA more recent version of the Synapse Client (%s) is available. "
               "Your version (%s) can be upgraded by typing:\n"
               "    pip install --upgrade synapseclient\n\n") % (release_version_info['latestVersion'], synapseclient.__version__,)
        sys.stdout.write(msg)
    else:
        sys.stdout.write('Your Synapse client is up to date!' + '\n')


def release_notes(version_url=_VERSION_URL):
    version_info = _get_version_info(_VERSION_URL)
    print('Python Synapse Client version %s' % version_info['latestVersion'])
    if 'releaseNotes' in version_info:
        sys.stdout.write(version_info['releaseNotes'] + '\n')


def _strip_dev_suffix(version):
    return re.sub(r'\.dev\d+', '', version)


def _version_tuple(version, levels=2):
    """
    Take a version number as a string delimited by periods and return a tuple
    with the desired number of levels. For example::

        print version_tuple('0.5.1.dev1', levels=2)
        ('0', '5')
    """
    v = _strip_dev_suffix(version).split('.')
    v = v[0:min(len(v),levels)]
    if len(v) < levels:
        v = v + ['0'] * (levels-len(v))
    return tuple(v)


def _get_version_info(version_url=_VERSION_URL):
    if version_url is None:
        return json.loads(pkg_resources.resource_string('synapseclient', 'synapsePythonClient'))
    else:
        headers = { 'Accept': 'application/json' }
        headers.update(synapseclient.USER_AGENT)
        return requests.get(version_url, headers=headers).json()


# If this file is run as a script, print current version
# then perform version check
if __name__ == "__main__":
    print "Version check"
    print "============="
    print("Python Synapse Client version %s" % synapseclient.__version__)

    print("Check against production version:")
    if version_check():
        print("ok")

    print("Check against dev version:")
    if version_check(version_url=_DEV_VERSION_URL):
        print("ok")

    print("Check against local copy of version file:")
    if version_check(version_url=None):
        print("ok")

