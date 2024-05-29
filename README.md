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
 * [Use cases](https://help.synapse.org/docs/Use-Cases.1985151645.html)
 * [Getting Started Guide to Synapse](https://help.synapse.org/docs/Getting-Started.2055471150.html)

For release information, see:

 * [Release notes](https://python-docs.synapse.org/news/)

<!-- Subscribe to release and other announcements [here](https://groups.google.com/a/sagebase.org/forum/#!forum/python-announce)
or by sending an email to [python-announce+subscribe@sagebase.org](mailto:python-announce+subscribe@sagebase.org) -->


Installation
------------

The Python Synapse client has been tested on 3.8, 3.9, 3.10 and 3.11 on Mac OS X, Ubuntu Linux and Windows.

**Starting from Synapse Python client version 3.0, Synapse Python client requires Python >= 3.8**

### Install using pip

The [Python Synapse Client is on PyPI](https://pypi.python.org/pypi/synapseclient) and can be installed with pip:

    # Here are a few ways to install the client. Choose the one that fits your use-case
    # sudo may optionally be needed depending on your setup

    pip install --upgrade synapseclient
    pip install --upgrade "synapseclient[pandas]"
    pip install --upgrade "synapseclient[pandas, pysftp, boto3]"

...or to upgrade an existing installation of the Synapse client:

    # sudo may optionally be needed depending on your setup
    pip install --upgrade synapseclient

The dependencies on `pandas`, `pysftp`, and `boto3` are optional. Synapse
[Tables](https://python-docs.synapse.org/reference/tables/) integrate
with [Pandas](http://pandas.pydata.org/). The library `pysftp` is required for users of
[SFTP](https://python-docs.synapse.org/guides/data_storage/#sftp) file storage. All
libraries require native code to be compiled or installed separately from prebuilt
binaries.

### Install from source

Clone the [source code repository](https://github.com/Sage-Bionetworks/synapsePythonClient).

    git clone git://github.com/Sage-Bionetworks/synapsePythonClient.git
    cd synapsePythonClient
    pip install .

Alternatively, you can use pip to install a particular branch, commit, or other git reference:

    pip install git+https://github.com/Sage-Bionetworks/synapsePythonClient@master

or

    pip install git+https://github.com/Sage-Bionetworks/synapsePythonClient@my-commit-hash

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

The Synapse client can be used to write software that interacts with the Sage Bionetworks Synapse repository. More examples can be found in the Tutorial section found [here](https://python-docs.synapse.org/tutorials/home/)

### Examples

#### Log-in and create a Synapse object
```
import synapseclient

syn = synapseclient.Synapse()
## You may optionally specify the debug flag to True to print out debug level messages.
## A debug level may help point to issues in your own code, or uncover a bug within ours.
# syn = synapseclient.Synapse(debug=True)

## log in using auth token
syn.login(authToken='auth_token')
```

#### Sync a local directory to synapse
This is the recommended way of synchronizing more than one file or directory to a synapse project through the use of `synapseutils`. Using this library allows us to handle scheduling everything required to sync an entire directory tree. Read more about the manifest file format in [`synapseutils.syncToSynapse`](https://python-docs.synapse.org/reference/synapse_utils/#synapseutils.sync.syncToSynapse)
```
import synapseclient
import synapseutils
import os

syn = synapseclient.Synapse()

## log in using auth token
syn.login(authToken='auth_token')

path = os.path.expanduser("~/synapse_project")
manifest_path = f"{path}/my_project_manifest.tsv"
project_id = "syn1234"

# Create the manifest file on disk
with open(manifest_path, "w", encoding="utf-8") as f:
    pass

# Walk the specified directory tree and create a TSV manifest file
synapseutils.generate_sync_manifest(
    syn,
    directory_path=path,
    parent_id=project_id,
    manifest_path=manifest_path,
)

# Using the generated manifest file, sync the files to Synapse
synapseutils.syncToSynapse(
    syn,
    manifestFile=manifest_path,
    sendMessages=False,
)
```

#### Store a Project to Synapse
```
import synapseclient
from synapseclient.entity import Project

syn = synapseclient.Synapse()

## log in using auth token
syn.login(authToken='auth_token')

project = Project('My uniquely named project')
project = syn.store(project)

print(project.id)
print(project)
```

#### Store a Folder to Synapse (Does not upload files within the folder)
```
import synapseclient

syn = synapseclient.Synapse()

## log in using auth token
syn.login(authToken='auth_token')

folder = Folder(name='my_folder', parent="syn123")
folder = syn.store(folder)

print(folder.id)
print(folder)

```

#### Store a File to Synapse
```
import synapseclient

syn = synapseclient.Synapse()

## log in using auth token
syn.login(authToken='auth_token')

file = File(
    path=filepath,
    parent="syn123",
)
file = syn.store(file)

print(file.id)
print(file)
```

#### Get a data matrix
```
import synapseclient

syn = synapseclient.Synapse()

## log in using auth token
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
```


Authentication
--------------
Authentication toward [Synapse](https://www.synapse.org/#RegisterAccount:0) can be accomplished with the clients using personal access tokens. Learn more about [Synapse personal access tokens](https://help.synapse.org/docs/Managing-Your-Account.2055405596.html#ManagingYourAccount-PersonalAccessTokens)

Learn about the [multiple ways one can login to Synapse](https://python-docs.synapse.org/tutorials/authentication/).


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

OpenTelemetry (OTEL)
--------------------------------
[OpenTelemetry](https://opentelemetry.io/) helps support the analysis of traces and spans which can provide insights into latency, errors, and other performance metrics. The synapseclient is ready to provide traces should you want them. The Synapse Python client supports OTLP Exports and can be configured via environment variables as defined [here](https://opentelemetry-python.readthedocs.io/en/stable/exporter/otlp/otlp.html).

Read more about OpenTelemetry in Python [here](https://opentelemetry.io/docs/instrumentation/python/)

### Quick-start
The following shows an example of setting up [jaegertracing](https://www.jaegertracing.io/docs/1.50/deployment/#all-in-one) via docker and executing a simple python script that implements the Synapse Python client.

#### Running the jaeger docker container
Start a docker container with the following options:
```
docker run --name jaeger \
  -e COLLECTOR_OTLP_ENABLED=true \
  -p 16686:16686 \
  -p 4318:4318 \
  jaegertracing/all-in-one:latest
```
Explanation of ports:
* `4318` HTTP
* `16686` Jaeger UI

Once the docker container is running you can access the Jaeger UI via: `http://localhost:16686`

#### Example
By default the OTEL exporter sends trace data to `http://localhost:4318/v1/traces`, however you may override this by setting the `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT` environment variable.

```
import synapseclient
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

trace.set_tracer_provider(
    TracerProvider(
        resource=Resource(attributes={SERVICE_NAME: "my_own_code_above_synapse_client"})
    )
)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
tracer = trace.get_tracer("my_tracer")

@tracer.start_as_current_span("my_span_name")
def main():
    syn = synapseclient.Synapse()
    syn.login()
    my_entity = syn.get("syn52569429")
    print(my_entity)

main()
```




License and Copyright
---------------------

&copy; Copyright 2013-23 Sage Bionetworks

This software is licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0).
