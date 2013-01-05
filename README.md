Python Synapse Client
=====================

A python client for [Sage Synapse](https://synapse.sagebase.org/), a collaborative compute space that allows scientists to share and analyze data together. Please visit the [Getting Started Guide to Synapse](https://sagebionetworks.jira.com/wiki/x/R4B3AQ) page for more information.

The Python client can be used as a library for development of software that communicates with Synapse or as a command-line utility. See also the [Synapse R client](https://sagebionetworks.jira.com/wiki/display/SYNR/Home).


Installation
------------

The python synapse client has been tested on python 2.7.

### Install using pip

The [Python Synapse Client is on PyPI](http://pypi.python.org/pypi/SynapseClient) and can be installed with pip:

    pip install SynapseClient

### Install from source

Clone the [source code repository](https://github.com/Sage-Bionetworks/synapsePythonClient).

    git clone git://github.com/Sage-Bionetworks/synapsePythonClient.git
    cd synapsePythonClient
    python setup.py install


Command line usage
------------------

The synapse client can be used from the shell command prompt. Valid commands
include: query, get, cat, add, update, delete, and onweb. A few examples are
shown.

### querying for entities that are part of the [Synapse Commons Repository](https://synapse.sagebase.org/Portal.html#Synapse:syn150935)

    synapse -u me@nowhere.com -p secret query 'select id, name from entity where parentId=="syn150935"'

### querying for a test [entity](https://synapse.sagebase.org/Portal.html#Synapse:syn1528299)
The test entity is tagged with an attribute *test_data* whose value is "bogus". We'll use the ID
of this entity in the next example.

    synapse -u me@nowhere.com -p secret query 'select id, name, parentId from entity where test_data=="bogus"'

### downloading test data from synapse

    synapse -u me@nowhere.com -p secret get syn1528299

### getting help

    synapse -h

Note that a [synapse account](https://synapse.sagebase.org/#RegisterAccount:0) is required.


Usage as a library
------------------

The synapse client can be used to write software that interacts with the Sage Synapse repository.

### Example

    import synapseclient

    s = synapseclient.Synapse()
    s.login('me@nowhere.com', 'secret')

    e = s.getEntity('syn1528299')

    print(e['name'])
    print(e['description'])
    print(e['files'])

    ## assuming the file is a short text file, read it as a list of lines
    with open(e['files'][0]) as f:
  		lines = f.readlines()


License and Copyright
---------------------

&copy; Copyright 2012 Sage Bionetworks

This software is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
