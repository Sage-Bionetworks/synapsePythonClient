# Installation script for Synapse Client for Python
############################################################
import json
import os

from setuptools import setup

# make sure not to overwrite existing .synapseConfig with our example one
data_files = (
    [(os.path.expanduser("~"), ["synapseclient/.synapseConfig"])]
    if not os.path.exists(os.path.expanduser("~/.synapseConfig"))
    else []
)
# figure out the version
with open("synapseclient/synapsePythonClient") as config:
    __version__ = json.load(config)["latestVersion"]

setup(data_files=data_files, version=__version__)
