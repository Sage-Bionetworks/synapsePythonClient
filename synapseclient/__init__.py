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
- provenance tracking

If you're just getting started with Synapse, you might want to
have a look at the `Getting Started Guide <https://www.synapse.org/#!Wiki:syn1669771/ENTITY/54546>`_
and `Getting started with the Python client for Synapse <https://www.synapse.org/#!Synapse:syn1768504>`_.

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

- :py:class:`synapseclient.Synapse`
- :py:func:`synapseclient.Synapse.login`

Accessing Data
==============

Synapse identifiers are used to refer to projects and data entities. For
example, the entity `syn1899498 <https://www.synapse.org/#!Synapse:syn1899498>`_ 
represents a tab-delimited file containing a 100 by 4 matrix. Getting the 
entity retrieves an object that holds metadata describing the matrix, 
and also downloads the file to a local cache::

    entity = syn.get('syn1899498')

    with open(entity.path) as f:
        # ... read the matrix ...

To view the entity in the browser::

    syn.onweb('syn1899498')

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
- :py:mod:`synapseclient.entity`

Querying
========

Synapse supports a `SQL-like query language <https://sagebionetworks.jira.com/wiki/display/PLFM/Repository+Service+API#RepositoryServiceAPI-QueryAPI>`_::

    results = syn.query('select id, name from entity where parentId=="syn1899495"')

    for result in results['results']:
        print result['entity.id'], result['entity.name']

See:

- :py:func:`synapseclient.Synapse.query`

Access control
==============

By default, data sets in Synapse are private to your user account, but they can
easily be shared with specific users, groups, or the public.

TODO: finish this once there is a reasonable way to find principleIds.

See:

- :py:func:`Synapse.getPermissions`
- :py:func:`Synapse.setPermissions`

Provenance
==========

Synapse provides tools for tracking 'provenance', or the transformation of raw data
into processed results, by linking derived data objects to source data and the
code used to perform the transformation.

See:

- :py:class:`synapseclient.activity.Activity`

Evaluations
===========

An evaluation is a Synapse construct useful for building processing pipelines.

See:

- :py:class:`synapseclient.evaluation.Evaluation`
- :py:func:`synapseclient.Synapse.getEvaluation`
- :py:func:`synapseclient.Synapse.submit`
- :py:func:`synapseclient.Synapse.joinEvaluation`
- :py:func:`synapseclient.Synapse.getSubmissions`
- :py:func:`synapseclient.Synapse.getSubmission`
- :py:func:`synapseclient.Synapse.getSubmissionStatus`

Wikis
=====

Wiki pages can be attached to an Synapse entity (i.e. project, folder, file, etc).
Text and graphics can be composed in markdown and rendered in the web view of
the object.

See:

- :py:func:`synapseclient.Synapse.getWiki`

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
`Synapse User Guide <https://www.synapse.org/#!Synapse:syn1669771>`_

"""

import json
import pkg_resources

__version__ = json.loads(pkg_resources.resource_string('synapseclient', 'synapsePythonClient'))['latestVersion']


from client import Synapse, login
from activity import Activity
from entity import Entity, Project, Folder, File
from entity import Analysis, Code, Data, Study, Summary
from evaluation import Evaluation, Submission, SubmissionStatus
from wiki import Wiki



