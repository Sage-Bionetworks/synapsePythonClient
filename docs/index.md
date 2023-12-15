# Synapse Python/Command Line Client Documentation

[![Supported Python Versions](https://img.shields.io/pypi/pyversions/synapseclient.svg)](https://pypi.org/project/synapseclient/)

The `synapseclient` package provides an interface to [Synapse](http://www.synapse.org), a collaborative, open-source research platform that allows teams to share data, track analyses, and collaborate, providing support for:

- integrated presentation of data, code and text
- fine grained access control
- provenance tracking

The `synapseclient` package lets you communicate with the cloud-hosted Synapse service to access data and create shared data analysis projects from within Python scripts or at the interactive Python console. Other Synapse clients exist for [R](https://r-docs.synapse.org/), [Java](https://github.com/Sage-Bionetworks/Synapse-Repository-Services/tree/develop/client/synapseJavaClient), and the [web](https://www.synapse.org/). The Python client can also be used from the [command line](tutorials/command_line_client.md).

Installing this package will install `synapseclient`, `synapseutils` and the command line client. `synapseutils` contains beta features and the behavior of these features are subject to change.

If you're just getting started with Synapse, have a look at the Getting Started guides for [Synapse](https://help.synapse.org/docs/Getting-Started.2055471150.html).
