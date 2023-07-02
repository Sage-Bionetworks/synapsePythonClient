Python Synapse Client
=====================

Branch  | Build Status
--------|-------------
develop | [![Build Status develop branch](https://github.com/Sage-Bionetworks/synapsePythonClient/workflows/build/badge.svg?branch=develop)](https://github.com/Sage-Bionetworks/synapsePythonClient/actions?query=branch%3Adevelop)
master  | [![Build Status master branch](https://github.com/Sage-Bionetworks/synapsePythonClient/workflows/build/badge.svg?branch=master)](https://github.com/Sage-Bionetworks/synapsePythonClient/actions?query=branch%3Amaster)

[![Get the synapseclient from PyPI](https://img.shields.io/pypi/v/synapseclient.svg)](https://pypi.python.org/pypi/synapseclient/) [![Supported Python Versions](https://img.shields.io/pypi/pyversions/synapseclient.svg)](https://pypi.python.org/pypi/synapseclient/)

A Python client for [Sage Bionetworks'](https://www.sagebase.org) [Synapse](https://www.synapse.org/), a collaborative, open-source research platform that allows teams to share data, track analyses, and collaborate. The Python client can be used as a library for development of software that communicates with Synapse or as a command-line utility.

There is also a [Synapse client for R](https://github.com/Sage-Bionetworks/synapser/).

Documentation
-------------

For more information about the Python client, see:

 * [Python client API docs](https://python-docs.synapse.org)

For more information about interacting with Synapse, see:

 * [Synapse API docs](https://rest-docs.synapse.org/rest/)
 * [User cases](https://help.synapse.org/docs/Use-Cases.1985151645.html)
 * [Getting Started Guide to Synapse](https://help.synapse.org/docs/Getting-Started.2055471150.html)

For release information, see:

 * [Release notes](https://python-docs.synapse.org/build/html/news.html)

<!-- Subscribe to release and other announcements [here](https://groups.google.com/a/sagebase.org/forum/#!forum/python-announce)
or by sending an email to [python-announce+subscribe@sagebase.org](mailto:python-announce+subscribe@sagebase.org) -->


Installation
------------

The Python Synapse client has been tested on 3.8, 3.9, 3.10 and 3.11 on Mac OS X, Ubuntu Linux and Windows.

**Starting from Synapse Python client version 3.0, Synapse Python client requires Python >= 3.8**

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

The Synapse client can be used from the shell command prompt. Valid commands
include: query, get, cat, add, update, delete, and onweb. A few examples are
shown.

### downloading test data from Synapse

    synapse -p auth_token get syn1528299

### getting help

    synapse -h

Note that a [Synapse account](https://www.synapse.org/#RegisterAccount:0) is required.


Usage as a library
------------------

The Synapse client can be used to write software that interacts with the Sage Bionetworks Synapse repository.

### Example

    import synapseclient

    syn = synapseclient.Synapse()

    ## log in using username and password
    syn.login(authToken='auth_token')

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
Authentication toward [Synapse](https://www.synapse.org/#RegisterAccount:0) can be accomplished with the clients using personal access tokens. Learn more about [Synapse personal access tokens](https://help.synapse.org/docs/Managing-Your-Account.2055405596.html#ManagingYourAccount-PersonalAccessTokens)

Authentication via passwords and API keys will be deprecated early 2024.


Synapse Utilities (synapseutils)
--------------------------------

The purpose of synapseutils is to create a space filled with convenience functions that includes traversing through large projects, copying entities, recursively downloading files and many more.

### Example

    import synapseutils
    import synapseclient
    syn = synapseclient.login()

    # copies all Synapse entities to a destination location
    synapseutils.copy(syn, "syn1234", destinationId = "syn2345")

    # copies the wiki from the entity to a destination entity. Only a project can have sub wiki pages.
    synapseutils.copyWiki(syn, "syn1234", destinationId = "syn2345")


    # Traverses through Synapse directories, behaves exactly like os.walk()
    walkedPath = synapseutils.walk(syn, "syn1234")

    for dirpath, dirname, filename in walkedPath:
        print(dirpath)
        print(dirname)
        print(filename)


License and Copyright
---------------------

&copy; Copyright 2013-23 Sage Bionetworks

This software is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
