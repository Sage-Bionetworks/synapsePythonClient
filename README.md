Python Synapse Client
=====================

 CI | Branch  | Build Status
 ---|---------|-------------
Travis | develop | [![Build Status develop branch](https://travis-ci.org/Sage-Bionetworks/synapsePythonClient.svg?branch=develop)](https://travis-ci.org/Sage-Bionetworks/synapsePythonClient)
Travis | master  | [![Build Status master branch](https://travis-ci.org/Sage-Bionetworks/synapsePythonClient.svg?branch=master)](https://travis-ci.org/Sage-Bionetworks/synapsePythonClient)
AppVeyor | develop | [![AppVeyor branch](https://img.shields.io/appveyor/ci/SageBionetworks/synapsePythonClient/develop.svg)](https://ci.appveyor.com/project/SageBionetworks/synapsepythonclient)
AppVeyor | master | [![AppVeyor branch](https://img.shields.io/appveyor/ci/SageBionetworks/synapsePythonClient/master.svg)](https://ci.appveyor.com/project/SageBionetworks/synapsepythonclient)


[![Get the synapseclient from PyPI](https://img.shields.io/pypi/v/synapseclient.svg)](https://pypi.python.org/pypi/synapseclient/) [![Supported Python Versions](https://img.shields.io/pypi/pyversions/synapseclient.svg)](https://pypi.python.org/pypi/synapseclient/) 

A Python client for [Sage Bionetworks'](https://www.sagebase.org) [Synapse](https://www.synapse.org/), a collaborative compute space that allows scientists to share and analyze data together. The Python client can be used as a library for development of software that communicates with Synapse or as a command-line utility.

There is also a [Synapse client for R](https://github.com/Sage-Bionetworks/synapser/).


Python 2 Support
----------------

The sun is setting on Python 2. Many major open source Python packages are moving to require Python 3.

The last version that works with Python 2 is Synapse Python client version 1.9.4. 
There are no plans for future maintenance of version 1.9.x.

**Starting from Synapse Python client version 2.0, Synapse Python client requires Python 3.6+**


Documentation
-------------

For more information about the Python client, see:

 * [Python client API docs](https://python-docs.synapse.org) 

For more information about interacting with Synapse, see:

 * [Synapse API docs](http://docs.synapse.org/rest/)
 * [User guides (including Python examples)](http://docs.synapse.org/articles/)
 * [Getting Started Guide to Synapse](http://docs.synapse.org/articles/getting_started.html)


Installation
------------

The Python Synapse client has been tested on 3.6, 3.7, and 3.8 on Mac OS X, Ubuntu Linux and Windows.

### Install using pip

The [Python Synapse Client is on PyPI](https://pypi.python.org/pypi/synapseclient) and can be installed with pip:

    (sudo) pip install synapseclient[pandas,pysftp]

...or to upgrade an existing installation of the Synapse client:

    (sudo) pip install --upgrade synapseclient

The dependencies on `pandas` and `pysftp` are optional. Synapse [Tables](http://python-docs.synapse.org/build/html/#tables) integrate
with [Pandas](http://pandas.pydata.org/). The library `pysftp` is required for users of
[SFTP](http://python-docs.synapse.org/build/html/sftp.html) file storage. Both libraries require native code
to be compiled or installed separately from prebuilt binaries.

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

### downloading test data from synapse

    synapse -u my_username -p my_password get syn1528299

### getting help

    synapse -h

Note that a [synapse account](https://www.synapse.org/#RegisterAccount:0) is required.


Usage as a library
------------------

The synapse client can be used to write software that interacts with the Sage Synapse repository.

### Example

    import synapseclient

    syn = synapseclient.Synapse()

    ## log in using username and password
    syn.login('my_username', 'my_password')

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


Authentication
--------------
Authentication toward [synapse](https://www.synapse.org/#RegisterAccount:0) can be accomplished in a few different ways. One is by passing username and password to the `syn.login` function.

    import synapseclient
    syn = synapseclient.Synapse()
    syn.login('my_username', 'my_password')

It is much more convenient to use an API key, which can be generated and cached locally by doing the following _once_:

    syn.login('my_username', 'my_password', rememberMe=True)

Then, in subsequent interactions, specifying username and password is optional and only needed to login as a different user. Calling `login` with no arguments uses cached credentials when they are available.

    syn.login('my_username')

As a short-cut, creating the `Synapse` object and logging in can be done in one step:

    import synapseclient
    syn = synapseclient.login()

Caching credentials can also be done from the command line client:

    synapse login -u my_username -p my_password --rememberMe


Synapse Utilities (synapseutils)
--------------------------------

The purpose of synapseutils is to create a space filled with convenience functions that includes traversing through large projects, copying entities, recursively downloading files and many more.

### Example

    import synapseutils
    import synapseclient
    syn = synapseclient.login()
    
    #COPY: copies all synapse entities to a destination location
    synapseutils.copy(syn, "syn1234", destinationId = "syn2345")
    
    #COPY WIKI: copies the wiki from the entity to a destination entity. Only a project can have sub wiki pages.
    synapseutils.copyWiki(syn, "syn1234", destinationId = "syn2345")


    #WALK: Traverses through synapse directories, behaves exactly like os.walk()
    walkedPath = synapseutils.walk(syn, "syn1234")

    for dirpath, dirname, filename in walkedPath:
        print(dirpath)
        print(dirname)
        print(filename)
        

License and Copyright
---------------------

&copy; Copyright 2013-19 Sage Bionetworks

This software is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
