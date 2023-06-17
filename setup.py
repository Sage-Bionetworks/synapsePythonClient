# Installation script for Synapse Client for Python
############################################################
# import sys
import os

# import platform
import setuptools
import json

# figure out the version
__version__ = json.loads(open("synapseclient/synapsePythonClient").read())[
    "latestVersion"
]

# make sure not to overwrite existing .synapseConfig with our example one
data_files = (
    [(os.path.expanduser("~"), ["synapseclient/.synapseConfig"])]
    if not os.path.exists(os.path.expanduser("~/.synapseConfig"))
    else []
)

setuptools.setup(
    # basic
    version=__version__,
    data_files=data_files,
)
