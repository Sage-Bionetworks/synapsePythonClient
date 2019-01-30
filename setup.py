# Installation script for Synapse Client for Python
############################################################
import sys
from os.path import expanduser, exists
from setuptools import setup, find_packages
import json

# check Python version, before we do anything
if sys.version_info[:2] not in [(3, 5), (3, 6), (3, 7)]:
    sys.stderr.write("The Synapse Client for Python requires Python 3.5, 3.6, or 3.7.\n")
    sys.stderr.write("Your Python appears to be version %d.%d.%d\n" % sys.version_info[:3])
    sys.exit(-1)

__version__=json.loads(open('synapseclient/synapsePythonClient').read())['latestVersion']

description = """A client for Synapse, a collaborative compute space 
that allows scientists to share and analyze data together.""".replace("\n", " ")

with open("README.md", "r") as fh:
    long_description = fh.read()

# make sure not to overwrite existing .synapseConfig with our example one
data_files = [(expanduser('~'), ['synapseclient/.synapseConfig'])] if not exists(expanduser('~/.synapseConfig')) else []

setup(
    # basic
    name='synapseclient',
    version=__version__,
    packages=find_packages(),

    # requirements
    python_requires='>=3.5*',
    install_requires=[
        'requests>=1.2',
        'keyring==12.0.2',
        'deprecated==1.2.4',
    ],
    extras_require={
        'pandas': ["pandas==0.23.0"],
        'pysftp': ["pysftp>=0.2.8"],
        'boto3' : ["boto3"],
        ':sys_platform=="linux2" or sys_platform=="linux"': ['keyrings.alt==3.1'],
    },

    # extra
    entry_points={
        'console_scripts': ['synapse = synapseclient.__main__:main']
    },
    package_data={'synapseclient': ['synapsePythonClient', '.synapseConfig']},
    data_files=data_files,
    zip_safe=False,

    # test
    test_suite='nose.collector',
    tests_require=['nose', 'mock'],

    # metadata to display on PyPI
    description=description,
    long_description=long_description,
    long_description_content_type="text/markdown",
    url='http://synapse.sagebase.org/',
    author='The Synapse Engineering Team',
    author_email='platform@sagebase.org',
    license='Apache',
    project_urls={
        "Documentation": "https://python-docs.synapse.org",
        "Source Code": "https://github.com/Sage-Bionetworks/synapsePythonClient",
        "Bug Tracker": "https://github.com/Sage-Bionetworks/synapsePythonClient/issues",
    },
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Software Development :: Libraries',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics'],
)
