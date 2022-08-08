The Synapse Python Client can be used from the command line via the **synapse** command.

Installation
============

The command line client is installed along with :doc:`getting_started/installation` of the Synapse Python client.


Help
====

For help, type::

    synapse -h.

For help on specific commands, type::

    synapse [command] -h

Usage
=====

.. argparse::
    :module: synapseclient.__main__
    :func: build_parser
    :prog: synapse
    :nodescription:
