# Installation script for Synapse Client for Python
############################################################
import sys
import os
import setuptools
import json

# check Python version, before we do anything
if sys.version_info.major < 3 and sys.version_info.minor < 6:
    sys.stderr.write("The Synapse Client for Python requires Python 3.6+\n")
    sys.stderr.write("Your Python appears to be version %d.%d.%d\n" % sys.version_info[:3])
    sys.exit(-1)

# figure out the version
__version__=json.loads(open('synapseclient/synapsePythonClient').read())['latestVersion']

description = """A client for Synapse, a collaborative compute space 
that allows scientists to share and analyze data together.""".replace("\n", " ")

with open("README.md", "r") as fh:
    long_description = fh.read()

# make sure not to overwrite existing .synapseConfig with our example one
data_files = [(os.path.expanduser('~'), ['synapseclient/.synapseConfig'])] if not os.path.exists(os.path.expanduser('~/.synapseConfig')) else []

setuptools.setup(
    # basic
    name='synapseclient',
    version=__version__,
    packages=setuptools.find_packages(exclude=["tests", "tests.*"]),

    # requirements
    python_requires='>=3.6.*',
    install_requires=[
        'requests>=2.22.0',
        'keyring==12.0.2',
        'deprecated==1.2.4',
    ],
    extras_require={
        'pandas': ["pandas==0.25.0"],
        'pysftp': ["pysftp>=0.2.8"],
        'boto3' : ["boto3"],
        ':sys_platform=="linux2" or sys_platform=="linux"': ['keyrings.alt==3.1'],
    },

    # command line
    entry_points={
        'console_scripts': ['synapse = synapseclient.__main__:main']
    },

    # data
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
        'Programming Language :: Python :: 3.6',
        'Programming Language :: Python :: 3.7',
        'Programming Language :: Python :: 3.8',
        'Operating System :: MacOS',
        'Operating System :: Microsoft :: Windows',
        'Operating System :: Unix',
        'Operating System :: POSIX :: Linux',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Software Development :: Libraries',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics'],
)
