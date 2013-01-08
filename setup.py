## Installation script for Synapse Client for Python
############################################################

from setuptools import setup
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

setup(name='synapseclient',
    version=__version__,
    description=description,
    long_description=long_description,
    url='http://synapse.sagebase.org/',
    download_url="https://github.com/Sage-Bionetworks/synapsePythonClient",
    author='Synapse Team',
    author_email='platform@sagebase.org',
    license='Apache',
    packages=['synapseclient'],
    install_requires=[
        'requests>=1.0',
    ],
    test_suite='nose.collector',
    tests_require=['nose'],
    entry_points = {
        'console_scripts': ['synapse = synapseclient.__main__:main']
    },
    zip_safe=False,
    package_data={'synapseclient': ['synapsePythonClient']},
    classifiers=[
    	'Development Status :: 3 - Alpha',
			'Topic :: Software Development :: Libraries',
			'Topic :: Scientific/Engineering',
			'Topic :: Scientific/Engineering :: Bio-Informatics'],
    platforms=['any'],
)
