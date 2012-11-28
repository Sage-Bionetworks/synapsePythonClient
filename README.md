synapsePythonClient
===================

A python client for [Sage Synapse](https://synapse.sagebase.org/), a collaborative compute space that allows scientists to share and analyze data together. Please visit the [Getting Started Guide to Synapse](https://sagebionetworks.jira.com/wiki/x/R4B3AQ) page for more information.


Installation
------------

		pip install SynapseClient

The python synapse client has been tested, so far, only on python 2.7.


Command line usage
------------------

The synapse client can be used from the shell command prompt. Valid commands
include: query, get, cat, add, update, delete, and onweb. A few examples are
shown.

### querying for a synapse entity

		synapse -u me@nowhere.com -p secret query 'select id, name, parentId from entity where test_data=="bogus"'

### downloading data from synapse

    synapse -u me@nowhere.com -p secret get syn1528299

### getting help

    synapse -h

Note that a [synapse account](https://synapse.sagebase.org/#RegisterAccount:0) is required.


Usage as a library
------------------

The synapse client can be used to write software that interacts with the Sage Synapse repository.

### Example

    import synapse.client

    s = synapse.client.Synapse()
    s.login('me@nowhere.com', 'secret')

    e = s.getEntity('syn1528299')

    print(e['name'])
    print(e['description'])
    print(e['files'])

    with open(e['files'][0]) as f:
  		lines = f.readlines()


License
-------

This software is licensed under the GPL.
