======================================================
Using the Synapse Python Client with SFTP data storage
======================================================

Installation
------------
Installing the extra libraries that the Python client uses to communication
with SFTP servers may add a few steps to the installation process.

The required libraries are:
 * `pysftp <https://pypi.python.org/pypi/pysftp>`_
 * `paramiko <http://www.paramiko.org/>`_
 * `pycrypto <https://www.dlitz.net/software/pycrypto/>`_
 * `ecdsa <https://pypi.python.org/pypi/ecdsa/>`_

Installing on Unix variants
~~~~~~~~~~~~~~~~~~~~~~~~~~~

Building these libraries on Unix OS's is straight forward, but you need the
Python development headers and libraries. For example, in Debian or Ubuntu
distributions::

    sudo apt-get install python-dev

Once this requirement is met, ``sudo pip install synapseclient`` should be able
to build pycrypto.

Installing on Windows
~~~~~~~~~~~~~~~~~~~~~

`Binary distributions of pycrypto <http://www.voidspace.org.uk/python/modules.shtml#pycrypto>`_ built for Windows is available from Michael Foord at Voidspace. Install this before installing the Python client.

After running the pycrypto installer, ``sudo pip install synapseclient`` should work.

Another option is to build your own binary with either the `free developer tools from Microsoft <http://www.visualstudio.com/en-us/products/visual-studio-community-vs>`_ or the `MinGW compiler <http://www.mingw.org/>`_.


