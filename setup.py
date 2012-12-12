## Installation script for Synapse Client for Python
############################################################

from setuptools import setup

description = """A client for the Sage Synapse, a collaborative compute space 
that allows scientists to share and analyze data together.""".replace("\n", " ")

long_description = """A client for the Sage Synapse, a collaborative compute
space that allows scientists to share and analyze data together. Synapse is an
innovation space that brings together scientific data, tools, and disease
models into a Commons that enables true collaborative research. The platform
consists of a web portal, web services, and integration with data analysis tools.
""".replace("\n", " ")

setup(name='SynapseClient',
    version='0.1.1',
    description=description,
    long_description=long_description,
    url='http://synapse.sagebase.org/',
    download_url="https://github.com/Sage-Bionetworks/synapsePythonClient",
    author='Synapse Team',
    author_email='platform@sagebase.org',
    license='GPL',
    packages=['synapse'],
    install_requires=[
        'requests',
    ],
    entry_points = {
        'console_scripts': ['synapse = synapse.__main__:main']
    },
    zip_safe=False,
    classifiers=[
    	'Development Status :: 3 - Alpha',
			'Topic :: Software Development :: Libraries',
			'Topic :: Scientific/Engineering',
			'Topic :: Scientific/Engineering :: Bio-Informatics'],
    platforms=['any'],
)
