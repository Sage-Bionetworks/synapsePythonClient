================================================
Synapse Python/Command Line Client Documentation
================================================

The ``synapseclient`` package provides an interface to `Synapse <http://www.synapse.org>`__, a collaborative,
open-source research platform that allows teams to share data, track analyses,
and collaborate, providing support for:

- integrated presentation of data, code and text
- fine grained access control
- provenance tracking

The ``synapseclient`` package lets you communicate with the cloud-hosted Synapse service to access data and create
shared data analysis projects from within Python scripts or at the interactive Python console. Other Synapse clients
exist for `R <https://r-docs.synapse.org/>`_,
`Java <https://github.com/Sage-Bionetworks/Synapse-Repository-Services/tree/develop/client/synapseJavaClient>`_, and
the `web <https://www.synapse.org/>`_. The Python client can also be used from the
`command line <getting_started/cli.html>`_.

Installing this package will install `synapseclient`, `synapseutils` and the command line client.
`synapseutils` contains beta features and the behavior of these features are subject to change.

If you're just getting started with Synapse, have a look at the Getting Started guides for
`Synapse <https://help.synapse.org/docs/Getting-Started.2055471150.html>`__.


.. Hidden TOCs


.. toctree::
   :caption: Getting Started
   :maxdepth: 1
   :hidden:

   getting_started/installation
   getting_started/credentials
   getting_started/basics
   getting_started/cli

.. toctree::
   :caption: Articles
   :maxdepth: 1
   :hidden:

   Multipart_upload
   S3Storage
   Upload
   reticulate
   sftp
   synapseutils

.. toctree::
   :caption: Python API
   :maxdepth: 1
   :hidden:

   api/Client
   api/Entity
   api/Evaluation
   api/Activity
   api/Annotations
   api/Wiki
   api/Utilities
   api/Versions
   api/Table
   api/Team
   api/Views

.. toctree::
   :caption: News
   :maxdepth: 2
   :hidden:

   news
