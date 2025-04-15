"""
# Version Functions

Check for latest version and recommend upgrade:

    synapseclient.check_for_updates()

Print release notes for installed version of client:

    synapseclient.release_notes()

"""

import json
import logging
import re
import sys
from importlib.resources import files
from typing import Any, Optional

import httpx

import synapseclient

_PYPI_JSON_URL = "https://pypi.org/pypi/synapseclient/json"
_RELEASE_NOTES_URL = "https://python-docs.synapse.org/news/"


def version_check(
    current_version: Optional[str] = None,
    check_for_point_releases: bool = False,
    use_local_metadata: bool = False,
    logger: logging.Logger = None,
) -> bool:
    """
    Gets the latest version information from version_url and check against the current version.
    Recommends upgrade, if a newer version exists.

    This wraps the _version_check function in a try except block.
    The purpose of this is so that no exception caught running the version
      check stops the client from running.

    Arguments:
        current_version: The current version of the package.
          Defaults to None.
          This argument is mainly used for testing.
        check_for_point_releases:
          Defaults to False.
          If True, The whole package versions will be compared (ie. 1.0.0)
          If False, only the major and minor package version will be compared (ie. 1.0)
        use_local_metadata:
          Defaults to False.
          If True, importlib.resources will be used to get the latest version of the package
          If False, the latest version of the package will be taken from Pypi
        logger: a logger for logging output

    Returns:
        bool: True if current version is the latest release (or higher) version, otherwise False.
    """
    try:
        if not _version_check(
            current_version, check_for_point_releases, use_local_metadata, logger
        ):
            return False

    # Don't prevent the client from running if something goes wrong
    except Exception as e:
        msg = f"Exception in version check: {str(e)}\n"
        if logger:
            logger.info(msg)
        else:
            sys.stdout.write(msg)
            sys.stdout.flush()
        return False

    return True


def check_for_updates(logger: logging.Logger = None):
    """
    Check for the existence of newer versions of the client,
      reporting both current release version and development version.

    For help installing development versions of the client,
    see the [README.md](https://github.com/Sage-Bionetworks/synapsePythonClient#installation).

    Arguments:
        logger: a logger for logging output

    """
    current_version = synapseclient.__version__
    latest_version = _get_version_info_from_pypi()

    msg = (
        "Python Synapse Client\n"
        f"currently running version:  {current_version}\n"
        f"latest release version:     {latest_version}\n"
    )
    if _is_current_version_behind(current_version, latest_version, levels=3):
        msg = msg + _create_package_behind_message(current_version, latest_version)
    else:
        msg = msg + "\nYour Synapse client is up to date!\n"

    if logger:
        logger.info(msg)
    else:
        sys.stdout.write(msg)
        sys.stdout.flush()


def release_notes(logger: logging.Logger = None):
    """
    Print release notes for the latest release

    Arguments:
        logger: a logger for logging output
    """
    latest_version = _get_version_info_from_pypi()
    msg = _create_release_notes_message(latest_version)
    if logger:
        logger.info(msg)
    else:
        sys.stdout.write(msg)
        sys.stdout.flush()


def _version_check(
    current_version: Optional[str] = None,
    check_for_point_releases: bool = False,
    use_local_metadata: bool = False,
    logger: logging.Logger = None,
) -> bool:
    """
    Gets the latest version information from version_url and check against the current version.
    Recommends upgrade, if a newer version exists.

    This has been split of from the version_check function to make testing easier.

    Arguments:
        current_version: The current version of the package.
          Defaults to None.
          This argument is mainly used for testing.
        check_for_point_releases:
          Defaults to False.
          If True, The whole package versions will be compared (ie. 1.0.0)
          If False, only the major and minor package version will be compared (ie. 1.0)
        use_local_metadata:
          Defaults to False.
          If True, importlib.resources will be used to get the latest version of the package
        logger: a logger for logging output

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
        msg1 = _create_package_behind_message(current_version, latest_version)
        msg2 = _create_release_notes_message(latest_version)
        msg = msg1 + msg2
        if logger:
            logger.info(msg)
        else:
            sys.stdout.write(msg)
            sys.stdout.flush()
        return False
    return True


def _get_local_package_metadata() -> dict[str, Any]:
    """Gets version info locally, using importlib.resources

    Returns:
        dict[str, Any]: This will have various fields relating the version of the client
    """
    ref = files("synapseclient").joinpath("synapsePythonClient")
    with ref.open("r") as fp:
        pkg_metadata = json.loads(fp.read())
    return pkg_metadata


def _get_version_info_from_pypi() -> str:
    """Gets the current release version from PyPi

    Returns:
        str: The current release version
    """
    content = httpx.get(_PYPI_JSON_URL)
    data = json.load(content)
    version = data["info"]["version"]
    assert isinstance(version, str)
    return version


def _is_current_version_behind(
    current_version: str, latest_version: str, levels: int
) -> bool:
    """
    Tests if the current version of the package is behind the latest version.

    Arguments:
        current_version: The current version of a package
        latest_version: The latest version of a package
        levels: The levels of the packages to check. For example:
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


def _create_package_behind_message(current_version: str, latest_version: str) -> str:
    msg = (
        "\nUPGRADE AVAILABLE\n\n"
        f"A more recent version of the Synapse Client ({latest_version}) is available."
        f" Your version ({current_version}) can be upgraded by typing:\n   "
        "pip install --upgrade synapseclient\n\n"
    )
    return msg


def _create_release_notes_message(latest_version) -> str:
    msg = (
        f"Python Synapse Client version {latest_version} release notes\n\n"
        f"{_RELEASE_NOTES_URL}\n\n"
    )
    return msg


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

    Arguments:
        version: A package version in string form such as "1.0.0"
        levels:
          Defaults to 2.
          The number of levels deep in the package version to return. "1.0.0", for example:
            levels=1: only the major version ("1")
            levels=2: the major and minor version ("1", "0")
            levels=3: the major, minor, and patch version ("1", "0", "0")

    Returns:
        Tuple: A tuple of strings where the length is equal to the levels argument.
    """
    v = _strip_dev_suffix(version).split(".")
    v = v[0 : min(len(v), levels)]
    if len(v) < levels:
        v = v + ["0"] * (levels - len(v))
    return tuple(v)


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
