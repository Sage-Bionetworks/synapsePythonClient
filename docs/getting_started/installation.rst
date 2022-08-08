Installation
============

The `synapseclient <https://pypi.python.org/pypi/synapseclient/>`_ package is available from PyPI. It can be installed
or upgraded with pip. Note that synapseclient requires Python 3, and if you have both Python 2 and Python 3
installations your system, the pip command associated with Python 3 may be named pip3 to distinguish it from a
Python 2 associated command. Prefixing the pip installation with sudo may be necessary if you are installing Python
into a shared system installation of Python.
::

    (sudo) pip3 install (--upgrade) synapseclient[pandas, pysftp]

The dependencies on pandas and pysftp are optional. The Synapse :py:mod:`synapseclient.table` feature integrates with
Pandas. Support for sftp is required for users of SFTP file storage. Both require native libraries to be compiled or
installed separately from prebuilt binaries.

Source code and development versions are `available on Github \
<https://github.com/Sage-Bionetworks/synapsePythonClient>`_.
Installing from source
::

    git clone git://github.com/Sage-Bionetworks/synapsePythonClient.git
    cd synapsePythonClient

You can stay on the master branch to get the latest stable release or check out the develop branch or a tagged
revision
::

    git checkout <branch or tag>

Next, either install the package in the site-packages directory ``python setup.py install`` or
``python setup.py develop`` to make the installation follow the head without having to reinstall
::

    python setup.py <install or develop>
