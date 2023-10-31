# Installation script for Synapse Client for Python
############################################################
import os
from setuptools import setup

import platform
import json

# make sure not to overwrite existing .synapseConfig with our example one
data_files = (
    [(os.path.expanduser("~"), ["synapseclient/.synapseConfig"])]
    if not os.path.exists(os.path.expanduser("~/.synapseConfig"))
    else []
)
# figure out the version
with open("synapseclient/synapsePythonClient") as config:
    __version__ = json.load(config)["latestVersion"]

install_requires = [
    # "requests>=2.22.0,<2.30.0; python_version<'3.10'",
    "requests>=2.22.0,<3.0",
    "urllib3>=1.26.18,<2",
    # "urllib3>=2; python_version>='3.10'",
    "keyring>=15,<23.5",
    "deprecated>=1.2.4,<2.0",
]

# on Linux specify a cryptography dependency that will not
# require a Rust compiler to compile from source (< 3.4).
# on Linux cryptography is a transitive dependency
# (keyring -> SecretStorage -> cryptography)
# SecretStorage doesn't pin a version so otherwise if cryptography
# is not already installed the dependency will resolve to the latest
# and will require Rust if a precompiled wheel cannot be used
# (e.g. old version of pip or no wheel available for an architecture).
# if a newer version of cryptography is already installed that is
# fine we don't want to trigger a downgrade, hence the conditional
# addition of the versioned dependency.
if platform.system() == "Linux":
    install_requires.append("keyrings.alt==3.1")
    try:
        import cryptography  # noqa

        # already installed, don't need to install (or downgrade)
    except ImportError:
        install_requires.append("cryptography<3.4")

setup(data_files=data_files, version=__version__, install_requires=install_requires)
