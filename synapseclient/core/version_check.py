"""
# Version Functions

Check for latest version and recommend upgrade:

    synapseclient.check_for_updates()

Print release notes for installed version of client:

    synapseclient.release_notes()

"""

import importlib.resources
import json
import re
import sys
import urllib.request
from typing import Optional

import requests

import synapseclient

_VERSION_URL = "https://raw.githubusercontent.com/Sage-Bionetworks/synapsePythonClient/master/synapseclient/synapsePythonClient"  # noqa
_PYPI_JSON_URL = "https://pypi.org/pypi/synapseclient/json"
_RELEASE_NOTES_URL = "https://python-docs.synapse.org/news/"


def version_check(
    current_version: Optional[str] = None,
    check_for_point_releases: bool = False,
    use_local_metadata: bool = False,
) -> bool:
    """
    Gets the latest version information from version_url and check against the current version.
    Recommends upgrade, if a newer version exists.

    This wraps the _version_check function in a try except block.
    The purpose of this is so that no exception caught running the version check stops the client from running.

    Args:
        current_version (Optional[str], optional): The current version of the package.
          Defaults to None.
          This argument is mainly used for testing.
        check_for_point_releases (bool, optional):
          Defaults to False.
          If True, The whole package versions will be compared (ie. 1.0.0)
          If False, only the major and minor package version will be compared (ie. 1.0)
        use_local_metadata (bool, optional):
          Defaults to False.
          If True, importlib.resources will be used to get the latest version fo the package
          If False, the latest version fo the package will be taken from Pypi

    Returns:
        bool: True if current version is the latest release (or higher) version, otherwise False.
    """
    try:
        if not _version_check(
            current_version, check_for_point_releases, use_local_metadata
        ):
            return False

    except Exception as e:
        # Don't prevent the client from running if something goes wrong
        sys.stderr.write(f"Exception in version check: {str(e)}\n")
        return False

    return True


def _version_check(
    current_version: Optional[str] = None,
    check_for_point_releases: bool = False,
    use_local_metadata: bool = False,
) -> bool:
    """
    Gets the latest version information from version_url and check against the current version.
    Recommends upgrade, if a newer version exists.

    This has been split of from the version_check function to make testing easier.

    Args:
        current_version (Optional[str], optional): The current version of the package.
          Defaults to None.
          This argument is mainly used for testing.
        check_for_point_releases (bool, optional):
          Defaults to False.
          If True, The whole package versions will be compared (ie. 1.0.0)
          If False, only the major and minor package version will be compared (ie. 1.0)
        use_local_metadata (bool, optional):
          Defaults to False.
          If True, importlib.resources will be used to get the latest version fo the package
          If False, the latest version fo the package will be taken from Pypi

    Returns:
        bool: True if current version is the latest release (or higher) version, otherwise False.
    """
    if not current_version:
        current_version = synapseclient.__version__
    assert isinstance(current_version, str)

    if use_local_metadata:
        metadata = _get_local_package_metadata()
        latest_version = metadata["latestVersion"]
        assert isinstance(latest_version, str)
    else:
        latest_version = _get_version_info_from_pypi()

    levels = 3 if check_for_point_releases else 2

    if _is_current_version_behind(current_version, latest_version, levels):
        _write_package_behind_messages(current_version, latest_version)
        return False
    return True


def _get_version_info_from_pypi() -> str:
    """Gets the current release version from PyPi

    Returns:
        str: The current release version
    """
    with urllib.request.urlopen(_PYPI_JSON_URL) as url:
        data = json.load(url)
    version = data["info"]["version"]
    assert isinstance(version, str)
    return version


def _is_current_version_behind(
    current_version: str, latest_version: str, levels: int
) -> bool:
    """
    Tests if the current version of the package is behind the latest version.

    Args:
        current_version (str): The current version of a package
        latest_version (str): The latest version of a package
        levels (int): The levels of the packages to check. For example:
          level 1: major versions
          level 2: minor versions
          level 3: patch versions

    Returns:
        bool: True if current version of package is up to date
    """
    current_version_str_tuple = _version_tuple(current_version, levels=levels)
    latest_version_str_tuple = _version_tuple(latest_version, levels=levels)

    # strings are converted to ints because comparisons of versions of different magnitudes
    #  don't work as strings
    #  for example 10 > 2, but "10" <  "2"
    current_version_int_tuple = tuple(
        int(version_level) for version_level in current_version_str_tuple
    )
    latest_version_int_tuple = tuple(
        int(version_level) for version_level in latest_version_str_tuple
    )

    return current_version_int_tuple < latest_version_int_tuple


def _write_package_behind_messages(
    current_version: str,
    latest_version: str,
) -> None:
    """
    This writes the output message for when the installed package version is behind the
      most recent release.

    Args:
        current_version (str): The current version of a package
        latest_version (str): The latest version of a package
    """
    sys.stderr.write(
        "\nUPGRADE AVAILABLE\n\nA more recent version of the Synapse Client"
        f" ({latest_version}) is available."
        f" Your version ({current_version}) can be upgraded by typing:\n   "
        " pip install --upgrade synapseclient\n\n"
    )
    sys.stderr.write(
        f"Python Synapse Client version {latest_version}" " release notes\n\n"
    )
    sys.stderr.write(f"{_RELEASE_NOTES_URL}\n\n")


def check_for_updates():
    """
    Check for the existence of newer versions of the client, reporting both current release version and development
    version.

    For help installing development versions of the client,
    see the [README.md](https://github.com/Sage-Bionetworks/synapsePythonClient#installation).
    """
    sys.stderr.write("Python Synapse Client\n")
    sys.stderr.write("currently running version:  %s\n" % synapseclient.__version__)

    release_version_info = _get_version_info(_VERSION_URL)
    sys.stderr.write(
        "latest release version:     %s\n" % release_version_info["latestVersion"]
    )

    if _version_tuple(synapseclient.__version__, levels=3) < _version_tuple(
        release_version_info["latestVersion"], levels=3
    ):
        print(
            "\nUPGRADE AVAILABLE\n\nA more recent version of the Synapse Client (%s) is"
            " available. Your version (%s) can be upgraded by typing:\n    pip install"
            " --upgrade synapseclient\n\n"
            % (
                release_version_info["latestVersion"],
                synapseclient.__version__,
            )
        )
    else:
        sys.stderr.write("\nYour Synapse client is up to date!\n")


def release_notes(version_url=None):
    """
    Print release notes for the installed version of the client or latest release or development version if version_url
    is supplied.

    version_url: Defaults to None, meaning release notes for the installed version. Alternatives are:
                        - synapseclient.version_check._VERSION_URL
                        - synapseclient.version_check._DEV_VERSION_URL

    """
    version_info = _get_version_info(version_url)
    sys.stderr.write(
        "Python Synapse Client version %s release notes\n\n"
        % version_info["latestVersion"]
    )
    if "releaseNotes" in version_info:
        sys.stderr.write(version_info["releaseNotes"] + "\n")


def _strip_dev_suffix(version):
    return re.sub(r"\.dev\d+", "", version)


def _version_tuple(version: str, levels: int = 2) -> tuple:
    """
    Take a version number as a string delimited by periods and return a tuple with
      the desired number of levels.
    For example:

        print(version_tuple('0.5.1.dev1', levels=2))
        ('0', '5')

    First the version string is split into version levels.
    If the number of levels is greater than the levels argument(x),
      only x levels are returned.
    If the number of levels is lesser than the levels argument(x),
      "0" strings are used to pad out the return value.

    Args:
        version (str): A package version in string form such as "1.0.0"
        levels (int, optional):
          Defaults to 2.
          The number of levels deep in the package version to return. "1.0.0", for example:
            levels=1: only the major version ("1")
            levels=2: the major and minor version ("1", "0")
            levels=2: the major, minor, and patch version ("1", "0", "0")

    Returns:
        Tuple: A tuple of strings where the length is equal to the levels argument.
    """
    v = _strip_dev_suffix(version).split(".")
    v = v[0 : min(len(v), levels)]
    if len(v) < levels:
        v = v + ["0"] * (levels - len(v))
    return tuple(v)


def _get_version_info(version_url: Optional[str] = _VERSION_URL) -> dict:
    """
    Gets version info from the version_url argument, or locally
    By default this is the Github for the python client
    If the version_url argument is None the version info will be obtained locally.

    Args:
        version_url (str, optional):
          Defaults to _VERSION_URL.
          The url to get version info from

    Returns:
        dict: This will have various fields relating the version of the client
    """
    if version_url is None:
        return _get_local_package_metadata()
    headers = {"Accept": "application/json; charset=UTF-8"}
    headers.update(synapseclient.USER_AGENT)
    return requests.get(version_url, headers=headers).json()


def _get_local_package_metadata() -> dict:
    """Gets version info locally, using importlib.resources

    Returns:
        dict: This will have various fields relating the version of the client
    """
    ref = importlib.resources.files("synapseclient").joinpath("synapsePythonClient")
    with ref.open("r") as fp:
        pkg_metadata = json.loads(fp.read())
    return pkg_metadata


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
    if version_check(use_local_metadata=True):
        print("ok")
