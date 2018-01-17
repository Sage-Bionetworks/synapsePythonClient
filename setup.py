## Installation script for Synapse Client for Python
############################################################
import sys
from os.path import expanduser, exists

## check Python version, before we do anything
if sys.version_info < (2, 7, 0):
    sys.stderr.write("The Synapse Client for Python requires Python 2.7 or 3.4 or higher.\n")
    sys.stderr.write("Your Python appears to be version %d.%d.%d\n" % sys.version_info[:3])
    sys.exit(-1)

from setuptools import setup, find_packages
import json

description = """A client for Synapse, a collaborative compute space 
that allows scientists to share and analyze data together.""".replace("\n", " ")

long_description = """A client for Synapse, a collaborative compute
space that allows scientists to share and analyze data
together. Synapse brings together scientific data, tools, and disease
models into a commons that enables true collaborative research. The
platform consists of a web portal, web services, and integration with
data analysis tools such as R, python, Galaxy and Java.
""".replace("\n", " ")

__version__=json.loads(open('synapseclient/synapsePythonClient').read())['latestVersion']

#make sure not to overwrite existing .synapseConfig with our example one
data_files = [(expanduser('~'), ['.synapseConfig'])] if not exists(expanduser('~/.synapseConfig')) else []

setup(name='synapseclient',
    version=__version__,
    description=description,
    long_description=long_description,
    url='http://synapse.sagebase.org/',
    download_url="https://github.com/Sage-Bionetworks/synapsePythonClient",
    author='Synapse Team',
    author_email='platform@sagebase.org',
    license='Apache',
    packages=find_packages(),
    install_requires=[
        'requests>=1.2',
        'six',
        'future',
        'backports.csv'
    ],
    extras_require = {
        'pandas':  ["pandas"],
        'pysftp': ["pysftp>=0.2.8"],
        'boto3' : ["boto3"]
    },
    test_suite='nose.collector',
    tests_require=['nose', 'mock'],
    entry_points = {
        'console_scripts': ['synapse = synapseclient.__main__:main']
    },
    zip_safe=False,
    package_data={'synapseclient': ['synapsePythonClient']},
    data_files=data_files,
    classifiers=[
        'Development Status :: 5 - Production/Stable',
        'Programming Language :: Python',
        'Programming Language :: Python :: 2.7',
        'Programming Language :: Python :: 3.4',
        'Programming Language :: Python :: 3.5',
        'Intended Audience :: Developers',
        'Intended Audience :: Science/Research',
        'License :: OSI Approved :: Apache Software License',
        'Topic :: Software Development :: Libraries',
        'Topic :: Scientific/Engineering',
        'Topic :: Scientific/Engineering :: Bio-Informatics'],
    platforms=['any'],
)
