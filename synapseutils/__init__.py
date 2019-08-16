"""
********
Overview
********

The ``synapseutils`` package provides both higher level functions as well as utilities for interacting with
`Synapse <http://www.synapse.org>`_.  These functionalities include:

- :py:func:`copy.copy`
- :py:func:`copy.copyWiki`
- :py:func:`walk.walk`
- :py:func:`sync.syncFromSynapse`
- :py:func:`sync.syncToSynapse`
- :py:func:`monitor.notifyMe`
"""

from .copy import copyWiki, copyFileHandles, changeFileMetaData, _copy_file_handles_batch, \
    _create_batch_file_handle_copy_request
from .walk import walk
from .sync import syncFromSynapse, syncToSynapse
from .monitor import notifyMe, with_progress_bar
