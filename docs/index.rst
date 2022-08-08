================================================
Synapse Python/Command Line Client Documentation
================================================

.. .. automodule:: synapseclient
..     :members:
..     :noindex:

The ``synapseclient`` package provides an interface to `Synapse <http://www.synapse.org>`__, a collaborative,
open-source research platform that allows teams to share data, track analyses,
and collaborate, providing support for:

- integrated presentation of data, code and text
- fine grained access control
- provenance_ tracking

The ``synapseclient`` package lets you communicate with the cloud-hosted Synapse service to access data and create
shared data analysis projects from within Python scripts or at the interactive Python console. Other Synapse clients
exist for `R <https://r-docs.synapse.org/>`_,
`Java <https://github.com/Sage-Bionetworks/Synapse-Repository-Services/tree/develop/client/synapseJavaClient>`_, and
the `web <https://www.synapse.org/>`_. The Python client can also be used from the
`command line <CommandLineClient.html>`_.

If you're just getting started with Synapse, have a look at the Getting Started guides for
`Synapse <https://docs.synapse.org/articles/getting_started.html>`__.

.. Hidden TOCs


.. toctree::
   :caption: Getting Started
   :maxdepth: 1
   :hidden:

   getting_started/installation
   getting_started/credentials
   getting_started/basics


.. toctree::
   :caption: Command Line Client
   :maxdepth: 1
   :hidden:
   
   CommandLineClient

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
   

.. .. toctree::
..    :caption: Articles
..    :maxdepth: 1
..    :hidden:

..    Credentials
..    Views
..    Upload
..    S3Storage
..    reticulate


.. .. toctree::
..    :caption: Reference
..    :maxdepth: 1
..    :hidden:

..    Client
..    Entity
..    Evaluation
..    Activity
..    Annotations
..    Wiki
..    Utilities
..    Versions
..    CommandLineClient
..    Table
..    sftp
..    Team
..    synapseutils
..    Multipart_upload


.. toctree::
   :caption: News
   :maxdepth: 2
   :hidden:

   news