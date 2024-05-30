# Synapse Python/Command Line Client Documentation

[![Supported Python Versions](https://img.shields.io/pypi/pyversions/synapseclient.svg)](https://pypi.org/project/synapseclient/)

### **Notice for the upcoming v5.0 release:**

- The upcoming v5.0 release will include a number of breaking changes. Take a look at
this [pubpub](https://sagebionetworks.pubpub.org/pub/828a3x4k/release/1) article
detailing some of the changes.
- A release date has not been set. A number of these changes will be available within
the 4.x.x versions hidden behind optional feature flags or different import paths. Any
breaking changes will not be included until v5.0.

The `synapseclient` package provides an interface to [Synapse](http://www.synapse.org), a collaborative, open-source research platform that allows teams to share data, track analyses, and collaborate, providing support for:

- integrated presentation of data, code and text
- fine grained access control
- provenance tracking

The `synapseclient` package lets you communicate with the cloud-hosted Synapse service to access data and create shared data analysis projects from within Python scripts or at the interactive Python console. Other Synapse clients exist for [R](https://r-docs.synapse.org/), [Java](https://github.com/Sage-Bionetworks/Synapse-Repository-Services/tree/develop/client/synapseJavaClient), and the [web](https://www.synapse.org/). The Python client can also be used from the [command line](tutorials/command_line_client.md).

Installing this package will install `synapseclient`, `synapseutils` and the command line client. `synapseutils` contains beta features and the behavior of these features are subject to change.

## What’s on this docs site for you?
* [Installation](./tutorials/installation.md), [Authentication](./tutorials/authentication.md), and [Configuration](./tutorials/configuration.md) of the `synapseclient`
* [Tutorials](./tutorials/home.md) to get you and your team sharing, organizing, and discussing your scientific research
* [How-To Guides](./guides/home.md) showcasing the full power and functionality available to you
* [API Reference](./reference/client.md) of the programatic interfaces
* [Further Reading](./explanations/home.md) to gain a deeper understanding of best practices and advanced use cases
* Our [release notes](./news.md)

## Additional Background

* Read [about Synapse](https://help.synapse.org/docs/About-Synapse.2058846607.html)—how it got started and how it fits into the bigger data-sharing picture
* Gain a better understanding of [Sage Bionetworks](https://help.synapse.org/docs/About-Synapse.2058846607.html#AboutSynapse-WhomanagesSynapse?) (that’s us—the nonprofit organization that created Synapse) and our other platforms that coincide with Synapse (such as portals)
* Learn about [Synapse governance](https://help.synapse.org/docs/Synapse-Governance.2004255211.html) and how it protects data privacy
* Look up an unfamiliar term or acronym in our [glossary](https://help.synapse.org/docs/Glossary.2667938103.html)
* See our [help section](https://help.synapse.org/docs/Help.2650865669.html) for further assistance via the FAQ page, discussion forum, or contact information to get in touch
