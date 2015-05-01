"""
********
Overview
********

The ``synapseclient`` package provides an interface to
`Synapse <http://www.synapse.org>`_, a collaborative
workspace for reproducible data intensive research projects,
providing support for:

- integrated presentation of data, code and text
- fine grained access control
- provenance_ tracking

The ``synapseclient`` package lets you communicate with the cloud-hosted
Synapse service to access data and create shared data analysis projects from
within Python scripts or at the interactive Python console. Other Synapse clients
exist for `R <https://www.synapse.org/#!Synapse:syn1834618>`_,
`Java <https://github.com/Sage-Bionetworks/Synapse-Repository-Services/tree/develop/client/synapseJavaClient>`_,
and the `web <https://www.synapse.org/>`_. The Python client can also be used from the
`command line <CommandLineClient.html>`_.

If you're just getting started with Synapse,
have a look at the Getting Started guides for `Synapse <https://www.synapse.org/#!Wiki:syn1669771/ENTITY/54546>`_
and `the Python client <https://www.synapse.org/#!Synapse:syn1768504>`_.

Good example projects are:

- `TCGA Pan-cancer (syn300013) <https://www.synapse.org/#!Synapse:syn300013>`_
- `Development of a Prognostic Model for Breast Cancer Survival in an Open Challenge Environment (syn1721874) <https://www.synapse.org/#!Synapse:syn1721874>`_
- `Demo projects (syn1899339) <https://www.synapse.org/#!Synapse:syn1899339>`_

Installation
============

The `synapseclient <https://pypi.python.org/pypi/synapseclient/>`_ package is available from PyPI. It can
be installed or upgraded with pip::

    (sudo) pip install (--upgrade) synapseclient[pandas,pysftp]

The dependencies on pandas and pysftp are optional. The Synapse :py:mod:`synapseclient.table`
feature integrates with Pandas. Support for sftp is required for users of SFTP file storage.
Both require native libraries to be compiled or installed separately from prebuilt binaries.

Source code and development versions are `available on Github <https://github.com/Sage-Bionetworks/synapsePythonClient>`_.
Installing from source::

    git clone git://github.com/Sage-Bionetworks/synapsePythonClient.git
    cd synapsePythonClient

You can stay on the master branch to get the latest stable release or check out the develop branch or a tagged revision::

    get checkout <branch or tag>

Next, either install the package in the site-packages directory ``python setup.py install`` or ``python setup.py develop`` to make the installation follow the head without having to reinstall::

    python setup.py <install or develop>


Connecting to Synapse
=====================

To use Synapse, you'll need to
`register <https://www.synapse.org/#!RegisterAccount:0>`_
for an account. The Synapse website can authenticate using a Google account,
but you'll need to take the extra step of creating a Synapse password
to use the programmatic clients.

Once that's done, you'll be able to load the library, create a :py:class:`Synapse` object and login::

    import synapseclient
    syn = synapseclient.Synapse()

    syn.login('me@nowhere.com', 'secret')

For more information, see:

- :py:class:`Synapse`
- :py:func:`Synapse.login`
- :py:func:`Synapse.logout`

Imports
=======

Several components of the synapseclient can be imported as needed::

    from synapseclient import Activity
    from synapseclient import Entity, Project, Folder, File
    from synapseclient import Evaluation, Submission, SubmissionStatus
    from synapseclient import Wiki

Accessing Data
==============

Synapse identifiers are used to refer to projects and data which are represented by
:py:mod:`synapseclient.entity` objects. For
example, the entity `syn1899498 <https://www.synapse.org/#!Synapse:syn1899498>`_
represents a tab-delimited file containing a 100 by 4 matrix. Getting the
entity retrieves an object that holds metadata describing the matrix,
and also downloads the file to a local cache::

    entity = syn.get('syn1899498')

View the entity's metadata in the Python console::

    print entity

This is one simple way to read in a small matrix::

    rows = []
    with open(entity.path) as f:
        header = f.readline().split('\\t')
        for line in f:
            row = [float(x) for x in line.split('\\t')]
            rows.append(row)

View the entity in the browser::

    syn.onweb('syn1899498')

- :py:class:`synapseclient.entity.Entity`
- :py:func:`synapseclient.Synapse.get`
- :py:func:`synapseclient.Synapse.onweb`


Organizing data in a Project
============================

You can create your own projects and upload your own data sets. Synapse stores
entities in a hierarchical or tree structure. Projects are at the top level and
must be uniquely named::

    import synapseclient
    from synapseclient import Project, Folder, File

    project = Project('My uniquely named project')
    project = syn.store(project)

Creating a folder::

    data_folder = Folder('Data', parent=project)
    data_folder = syn.store(data_folder)

Adding files to the project::

    test_entity = File('/path/to/data/file.xyz', description='Fancy new data', parent=data_folder)
    test_entity = syn.store(test_entity)

In addition to simple data storage, Synapse entities can be `annotated <#annotating-synapse-entities>`_ with
key/value metadata, described in markdown documents (wikis_), and linked
together in provenance_ graphs to create a reproducible record of a data
analysis pipeline.

See also:

- :py:class:`synapseclient.entity.Entity`
- :py:class:`synapseclient.entity.Project`
- :py:class:`synapseclient.entity.Folder`
- :py:class:`synapseclient.entity.File`
- :py:func:`synapseclient.Synapse.store`

Annotating Synapse entities
===========================

Annotations are arbitrary metadata attached to Synapse entities, for example::

    test_entity.genome_assembly = "hg19"

See:

- :py:mod:`synapseclient.annotations`

Provenance
==========

Synapse provides tools for tracking 'provenance', or the transformation of raw data
into processed results, by linking derived data objects to source data and the
code used to perform the transformation.

See:

- :py:class:`synapseclient.activity.Activity`

Tables
======

Tables can be built up by adding sets of rows that follow a user-defined schema
and queried using a SQL-like syntax.

See:

- :py:mod:`synapseclient.table`
- :py:class:`synapseclient.table.Schema`
- :py:class:`synapseclient.table.Column`
- :py:func:`synapseclient.Synapse.getColumns`
- :py:func:`synapseclient.Synapse.getTableColumns`

Wikis
=====

Wiki pages can be attached to an Synapse entity (i.e. project, folder, file, etc).
Text and graphics can be composed in markdown and rendered in the web view of
the object.

See:

- :py:func:`synapseclient.Synapse.getWiki`
- :py:class:`synapseclient.wiki.Wiki`

Evaluations
===========

An evaluation is a Synapse construct useful for building processing pipelines and
for scoring predictive modelling and data analysis challenges.

See:

- :py:mod:`synapseclient.evaluation`
- :py:func:`synapseclient.Synapse.getEvaluation`
- :py:func:`synapseclient.Synapse.submit`
- :py:func:`synapseclient.Synapse.joinEvaluation`
- :py:func:`synapseclient.Synapse.getSubmissions`
- :py:func:`synapseclient.Synapse.getSubmission`
- :py:func:`synapseclient.Synapse.getSubmissionStatus`

Querying
========

Synapse supports a `SQL-like query language <https://sagebionetworks.jira.com/wiki/display/PLFM/Repository+Service+API#RepositoryServiceAPI-QueryAPI>`_::

    results = syn.query('SELECT id, name FROM entity WHERE parentId=="syn1899495"')

    for result in results['results']:
        print result['entity.id'], result['entity.name']

Querying for my projects. Finding projects owned by the current user::

    profile = syn.getUserProfile()
    results = syn.query('SELECT id, name FROM project WHERE project.createdByPrincipalId==%s' % profile['ownerId'])

    for result in results['results']:
        print result['entity.id'], result['entity.name']

See:

- :py:func:`synapseclient.Synapse.query`
- :py:func:`synapseclient.Synapse.chunkedQuery`

Access control
==============

By default, data sets in Synapse are private to your user account, but they can
easily be shared with specific users, groups, or the public.

TODO: finish this once there is a reasonable way to find principalIds.

See:

- :py:func:`Synapse.getPermissions`
- :py:func:`Synapse.setPermissions`

Accessing the API directly
==========================

These methods enable access to the Synapse REST(ish) API taking care of details
like endpoints and authentication. See the
`REST API documentation <http://rest.synapse.org/>`_.

See:

- :py:func:`synapseclient.Synapse.restGET`
- :py:func:`synapseclient.Synapse.restPOST`
- :py:func:`synapseclient.Synapse.restPUT`
- :py:func:`synapseclient.Synapse.restDELETE`

More information
================

For more information see the
`Synapse User Guide <https://www.synapse.org/#!Synapse:syn1669771>`_. These
API docs are browsable online at
`python-docs.synapse.org <http://python-docs.synapse.org/>`_.

Getting updates
===============

To get information about new versions of the client including development versions
see `synapseclient.check_for_updates() <Versions.html#synapseclient.version_check.check_for_updates>`_ and `synapseclient.release_notes() <Versions.html#synapseclient.version_check.release_notes>`_.
"""

import json
import pkg_resources
__version__ = json.loads(pkg_resources.resource_string('synapseclient', 'synapsePythonClient'))['latestVersion']

import requests
USER_AGENT = {'User-Agent':'synapseclient/%s %s' % (__version__, requests.utils.default_user_agent())}

from client import Synapse, login
from activity import Activity
from entity import Entity, Project, Folder, File
from evaluation import Evaluation, Submission, SubmissionStatus
from table import Schema, Column, RowSet, Row, as_table_columns, Table
from wiki import Wiki

from version_check import check_for_updates
from version_check import release_notes

from client import PUBLIC, AUTHENTICATED_USERS
from client import ROOT_ENTITY
