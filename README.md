Python Synapse Client
=====================

A python client for [Sage Synapse](https://synapse.sagebase.org/), a collaborative compute space that allows scientists to share and analyze data together. Please visit these pages for more information: [Getting Started Guide to Synapse](https://www.synapse.org/#!Wiki:syn1669771/ENTITY/54546) and [Getting started with the Synapse python client](https://www.synapse.org/#!Synapse:syn1768504).

The Python client can be used as a library for development of software that communicates with Synapse or as a command-line utility. See also the [Synapse R client](https://sagebionetworks.jira.com/wiki/display/SYNR/Home).

Upgrade Python Client
---------------------
**Note**: The January 25th, 2013 release of the Synapse platform made changes that are incompatible with older versions of the Python Client. If you have a version prior to 0.2.1, you'll need to upgrade to use the latest features of the Synapse platform.


Installation
------------

The python synapse client has been tested on python 2.7 on Mac OS X, Ubuntu Linux and Windows 2008.

### Install using pip

The [Python Synapse Client is on PyPI](https://pypi.python.org/pypi/synapseclient) and can be installed with pip:

    pip install synapseclient

...or to upgrade an existing installation of the Synapse client:

    pip install --upgrade synapseclient

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

    syn = synapseclient.Synapse()
    syn.login('me@nowhere.com', 'secret')

    ## credentials may also be specified in .synapseConfig,
    ## in which case you can do:
    # syn.login()

    ## retrieve a 100 by 4 matrix
    matrix = syn.get('syn1901033')

    ## inspect its properties
    print matrix.name
    print matrix.description
    print matrix.path

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
    query_results = syn.query('select id,name from projects where project.createdByPrincipleId==%s' % profile['ownerId'])

### querying for entities that are part of the [Synapse Commons Repository](https://synapse.sagebase.org/Portal.html#Synapse:syn150935)

    syn.query('select id, name from entity where parentId=="syn150935"')

### querying for entities that are part of [TCGA pancancer](https://synapse.sagebase.org/Portal.html#Synapse:syn300013) that are also RNA-Seq data
    syn.query('select id, name from entity where freeze=="tcga_pancancer_v4" and platform=="IlluminaHiSeq_RNASeqV2"')




Authentication
--------------
Authentication toward [synapse](https://synapse.sagebase.org/#RegisterAccount:0) can be specified in three ways:

1. By passing username and password to the login function (or using the -u and -p parameters on the command line)

2. Creating a configuration file in the home directory called .synapseConfig that contains the username and password. See example below.

3. Using cached authentication. Everytime the client authenticates the credentials are cached for 24 hours.  Meaning that subsequent interactions do not need the username and password credentials.


###Example config file stored in ~/.synapseConfig
    [authentication]
    username: me@nowhere.com
    password: secret
    


License and Copyright
---------------------

&copy; Copyright 2013 Sage Bionetworks

This software is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
