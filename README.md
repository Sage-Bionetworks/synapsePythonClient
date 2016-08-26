Python Synapse Client
=====================

 branch  | build status
 --------|--------------
 develop | [![Build Status develop branch](https://travis-ci.org/Sage-Bionetworks/synapsePythonClient.svg?branch=develop)](https://travis-ci.org/Sage-Bionetworks/synapsePythonClient)
 master  | [![Build Status master branch](https://travis-ci.org/Sage-Bionetworks/synapsePythonClient.svg?branch=master)](https://travis-ci.org/Sage-Bionetworks/synapsePythonClient)

[![Get the synapseclient from PyPI](https://img.shields.io/pypi/v/synapseclient.svg)](https://pypi.python.org/pypi/synapseclient/) [![Supported Python Versions](https://img.shields.io/pypi/pyversions/synapseclient.svg)](https://pypi.python.org/pypi/synapseclient/) [![Monthly downloads of synapseclient from PyPI](https://img.shields.io/pypi/dm/synapseclient.svg)](https://pypi.python.org/pypi/synapseclient/)

A Python client for [Sage Bionetwork's](https://www.sagebase.org) [Synapse](https://www.synapse.org/), a collaborative compute space that allows scientists to share and analyze data together. The Python client can be used as a library for development of software that communicates with Synapse or as a command-line utility.

There is also a [Synapse client for R](https://github.com/Sage-Bionetworks/rSynapseClient/).


Documentation
-------------

For more information about the Python client, see:

 * [Python client API docs](http://docs.synapse.org/python/) 

For more information about interacting with Synapse, see:

 * [Synapse API docs](http://docs.synapse.org/rest/)
 * [User guides (including Python examples)](http://docs.synapse.org/articles/)
 * [Getting Started Guide to Synapse](http://docs.synapse.org/articles/getting_started.html)


Installation
------------

The Python Synapse client has been tested on Python 2.7, 3.4 and 3.5 on Mac OS X, Ubuntu Linux and Windows.

### Install using pip

The [Python Synapse Client is on PyPI](https://pypi.python.org/pypi/synapseclient) and can be installed with pip:

    (sudo) pip install synapseclient[pandas,pysftp]

...or to upgrade an existing installation of the Synapse client:

    (sudo) pip install --upgrade synapseclient

The dependencies on `pandas` and `pysftp` are optional. Synapse [Tables](http://docs.synapse.org/python/#tables) integrate
with [Pandas](http://pandas.pydata.org/). The library `pysftp` is required for users of
[SFTP](http://docs.synapse.org/python/sftp.html) file storage. Both libraries require native code
to be compiled or installed separately from prebuilt binaries.

### Install from source

Clone the [source code repository](https://github.com/Sage-Bionetworks/synapsePythonClient).

    git clone git://github.com/Sage-Bionetworks/synapsePythonClient.git
    cd synapsePythonClient
    python setup.py install

#### Install develop branch

Installing the [develop](https://github.com/Sage-Bionetworks/synapsePythonClient/tree/develop) branch can be useful for testing or for access to the latest features, with the acceptance of an increased risk of experiencing bugs. Using [virtualenv](http://www.virtualenv.org/) to create an isolated test environment is a good idea.

    git clone git://github.com/Sage-Bionetworks/synapsePythonClient.git
    cd synapsePythonClient
    git checkout develop
    python setup.py install

Replace `python setup.py install` with `python setup.py develop` to make the installation follow the head without having to reinstall.

Pip will nicely install from a branch in one step:

    pip install git+https://github.com/Sage-Bionetworks/synapsePythonClient.git@develop

#### Installing a tagged version

Checking out a tagged version will ensure that [JIRA issues](https://sagebionetworks.jira.com/issues/?jql=project%20%3D%20SYNR%20AND%20component%20in%20%28EMPTY%2C%20%22Command%20Line%20Client%22%2C%20%22Python%20Client%22%29%20AND%20status%20%3D%20Resolved%20ORDER%20BY%20updatedDate%20DESC) are validated on the correct version of the client code. Instead of checking out the develop branch, check out the tag instead, for example:

    git checkout v1.0.dev2



Command line usage
------------------

The synapse client can be used from the shell command prompt. Valid commands
include: query, get, cat, add, update, delete, and onweb. A few examples are
shown.

### querying for entities that are part of the [Synapse Commons Repository](https://www.synapse.org/#!Synapse:syn150935)

    synapse -u me@nowhere.com -p secret query 'select id, name from entity where parentId=="syn150935"'

### querying for a test [entity](https://www.synapse.org/#!Synapse:syn1528299)
The test entity is tagged with an annotation *test_data* whose value is "bogus". We'll use the ID
of this entity in the next example.

    synapse -u me@nowhere.com -p secret query 'select id, name, parentId from entity where test_data=="bogus"'

### downloading test data from synapse

    synapse -u me@nowhere.com -p secret get syn1528299

### getting help

    synapse -h

Note that a [synapse account](https://www.synapse.org/#RegisterAccount:0) is required.


Usage as a library
------------------

The synapse client can be used to write software that interacts with the Sage Synapse repository.

### Example

    import synapseclient

    syn = synapseclient.Synapse()

    ## log in using cached API key
    syn.login('joeuser')

    ## retrieve a 100 by 4 matrix
    matrix = syn.get('syn1901033')

    ## inspect its properties
    print(matrix.name)
    print(matrix.description)
    print(matrix.path)

    ## load the data matrix into a dictionary with an entry for each column
    with open(matrix.path, 'r') as f:
        labels = f.readline().strip().split('\t')
        data = {label: [] for label in labels}
        for line in f:
            values = [float(x) for x in line.strip().split('\t')]
            for i in range(len(labels)):
                data[labels[i]].append(values[i])

    ## load the data matrix into a numpy array
    import numpy as np
    np.loadtxt(fname=matrix.path, skiprows=1)

### querying for my projects
    profile = syn.getUserProfile()
    query_results = syn.query('select id,name from project where project.createdByPrincipalId==%s' % profile['ownerId'])

### querying for entities that are part of the [Synapse Commons Repository](https://www.synapse.org/#!Synapse:syn150935)

    syn.query('select id, name from entity where parentId=="syn150935"')

### querying for entities that are part of [TCGA pancancer](https://www.synapse.org/#!Synapse:syn300013) that are also RNA-Seq data
    syn.query('select id, name from entity where freeze=="tcga_pancancer_v4" and platform=="IlluminaHiSeq_RNASeqV2"')




Authentication
--------------
Authentication toward [synapse](https://www.synapse.org/#RegisterAccount:0) can be accomplished in a few different ways. One is by passing username and password to the `syn.login` function.

    import synapseclient
    syn = synapseclient.Synapse()
    syn.login('me@nowhere.com', 'secret')

It is much more convenient to use an API key, which can be generated and cached locally by doing the following _once_:

    syn.login('me@nowhere.com', 'secret', rememberMe=True)

Then, in subsequent interactions, specifying username and password is optional and only needed to login as a different user. Calling `login` with no arguments uses cached credentials when they are available.

    syn.login('me@nowhere.com')

As a short-cut, creating the `Synapse` object and logging in can be done in one step:

    import synapseclient
    syn = synapseclient.login()

Caching credentials can also be done from the command line client:

    synapse login -u me@nowhere.com -p secret --rememberMe


Synapse Utilities (synapseutils)
--------------------------------

The synapse utils contain helper functions such as synu.copy().

### Example

    import synapseutils as synu
    import synapseclient
    syn = synapseclient.login()
    
    #COPY: copies all synapse entities to a destination location
    synu.copy(syn, "syn1234", destinationId = "syn2345")
    
    #COPY WIKI: copies the wiki from the entity to a destionation entity. Only a project can have subwikipages.
    synu.copyWiki(syn, "syn1234", destinationId = "syn2345")


    #WALK: Traverses through synapse directories, behaves exactly like os.walk()
    walkedPath = synu.walk(syn, "syn1234")

    for dirpath, dirname, filename in walkedPath:
        print(dirpath)
        print(dirname)
        print(filename)
        

License and Copyright
---------------------

&copy; Copyright 2013-15 Sage Bionetworks

This software is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
