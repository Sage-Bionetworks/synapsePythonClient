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

from .copy_functions import copy, copyWiki, copyFileHandles, changeFileMetaData
from .walk import walk
from .sync import syncFromSynapse, syncToSynapse
from .monitor import notifyMe, with_progress_bar
