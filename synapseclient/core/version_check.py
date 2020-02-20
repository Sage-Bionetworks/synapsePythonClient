"""
*****************
Version Functions
*****************

Check for latest version and recommend upgrade::

    synapseclient.check_for_updates()

Print release notes for installed version of client::

    synapseclient.release_notes()

.. automethod:: synapseclient.version_check.check_for_updates
.. automethod:: synapseclient.version_check.release_notes

"""

import json
import pkg_resources
import re
import requests
import synapseclient
import sys


_VERSION_URL = 'https://raw.githubusercontent.com/Sage-Bionetworks/synapsePythonClient/master/synapseclient/synapsePythonClient'


def version_check(current_version=None, version_url=_VERSION_URL, check_for_point_releases=False):
    """
    Gets the latest version information from version_url and check against the current version.
    Recommends upgrade, if a newer version exists.

    :returns: True if current version is the latest release (or higher) version,
              False otherwise.
    """

    try:
        if not current_version:
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
            sys.stderr.write(version_info['message'] + '\n')

        levels = 3 if check_for_point_releases else 2

        # Compare with latest version
        if _version_tuple(current_version, levels=levels) < _version_tuple(version_info['latestVersion'],
                                                                           levels=levels):
            sys.stderr.write("\nUPGRADE AVAILABLE\n\nA more recent version of the Synapse Client (%s) "
                             "is available. Your version (%s) can be upgraded by typing:\n"
                             "    pip install --upgrade synapseclient\n\n" %
                             (version_info['latestVersion'], current_version,))
            if 'releaseNotes' in version_info:
                sys.stderr.write('Python Synapse Client version %s release notes\n\n'
                                 % version_info['latestVersion'])
                sys.stderr.write(version_info['releaseNotes'] + '\n\n')
            return False

    except Exception as e:
        # Don't prevent the client from running if something goes wrong
        sys.stderr.write("Exception in version check: %s\n" % (str(e),))
        return False

    return True


def check_for_updates():
    """
    Check for the existence of newer versions of the client, reporting both current release version and development
    version.

    For help installing development versions of the client, see the docs for
    :py:mod:`synapseclient` or the `README.md <https://github.com/Sage-Bionetworks/synapsePythonClient>`_.
    """
    sys.stderr.write('Python Synapse Client\n')
    sys.stderr.write('currently running version:  %s\n' % synapseclient.__version__)

    release_version_info = _get_version_info(_VERSION_URL)
    sys.stderr.write('latest release version:     %s\n' % release_version_info['latestVersion'])

    if _version_tuple(synapseclient.__version__, levels=3) < _version_tuple(release_version_info['latestVersion'],
                                                                            levels=3):
        print(("\nUPGRADE AVAILABLE\n\nA more recent version of the Synapse Client (%s) is available. "
               "Your version (%s) can be upgraded by typing:\n"
               "    pip install --upgrade synapseclient\n\n") % (release_version_info['latestVersion'],
                                                                 synapseclient.__version__,))
    else:
        sys.stderr.write('\nYour Synapse client is up to date!\n')


def release_notes(version_url=None):
    """
    Print release notes for the installed version of the client or latest release or development version if version_url
    is supplied.

    :param version_url: Defaults to None, meaning release notes for the installed version. Alternatives are:
                            - synapseclient.version_check._VERSION_URL
                            - synapseclient.version_check._DEV_VERSION_URL
    """
    version_info = _get_version_info(version_url)
    sys.stderr.write('Python Synapse Client version %s release notes\n\n' % version_info['latestVersion'])
    if 'releaseNotes' in version_info:
        sys.stderr.write(version_info['releaseNotes'] + '\n')


def _strip_dev_suffix(version):
    return re.sub(r'\.dev\d+', '', version)


def _version_tuple(version, levels=2):
    """
    Take a version number as a string delimited by periods and return a tuple with the desired number of levels.
    For example::

        print(version_tuple('0.5.1.dev1', levels=2))
        ('0', '5')
    """
    v = _strip_dev_suffix(version).split('.')
    v = v[0:min(len(v), levels)]
    if len(v) < levels:
        v = v + ['0'] * (levels-len(v))
    return tuple(v)


def _get_version_info(version_url=_VERSION_URL):
    if version_url is None:
        return json.loads(pkg_resources.resource_string('synapseclient', 'synapsePythonClient').decode())
    else:
        headers = {'Accept': 'application/json; charset=UTF-8'}
        headers.update(synapseclient.USER_AGENT)
        return requests.get(version_url, headers=headers).json()


# If this file is run as a script, print current version
# then perform version check
if __name__ == "__main__":
    print("Version check")
    print("=============")
    print("Python Synapse Client version %s" % synapseclient.__version__)

    print("Check against production version:")
    if version_check():
        print("ok")

    print("Check against local copy of version file:")
    if version_check(version_url=None):
        print("ok")
