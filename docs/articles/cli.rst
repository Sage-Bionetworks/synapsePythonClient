*******************
Command Line Client
*******************

The Synapse Python Client can be used from the command line via the **synapse** command.

.. note::
    The command line client is installed along with :doc:`../getting_started/installation` of the Synapse Python client.


Usage
=====

For help, type::

    synapse -h.

For help on specific commands, type::

    synapse [command] -h

.. argparse::
    :module: synapseclient.__main__
    :func: build_parser
    :prog: synapse
    :nodescription:
