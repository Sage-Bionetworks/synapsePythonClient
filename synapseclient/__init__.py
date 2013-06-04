"""

*************************
Synapse client for Python
*************************

The synapseclient package provides an interface to `Synapse`_, a collaborative
workspace for reproducible data intensive science.

Synapse provides software infrastructure for sharing living research projects,
including:
  * integrated presentation of data, code and text
  * fine grained access control
  * provenance tracking

Connecting to Synapse
=====================

To use Synapse, you'll need to register for an account on synapse.org. In your
Python script, load the client library, create a Synapse object and login.

    import synapseclient
    syn = synapseclient.Synapse()

    syn.login('me@nowhere.com', 'secret')

see:
    Synapse
    Synapse.login

Accessing Data
==============

Synapse identifiers are used to refer to projects and data entities. For
example, the entity `syn1899498`_ represents a tab-delimited file containing a
100 by 4 matrix. Getting the entity retrieves an object that holds metadata
describing the matrix, and also downloads the file to a local cache.

    entity = syn.get('syn1899498')

    with open(entity.path) as f:
        ## ...read the matrix

To view the entity in the browser:

    syn.onweb('syn1899498')

Creating a project
==================

You can create your own projects and upload your own data sets. Synapse stores
entities in a hierarchical or tree structure. Projects are at the top level and
must be uniquely named.

    import synapseclient
    from synapseclient import Project, Folder, File

    project = Project('My uniquely named project')
    project = syn.store(project)

Creating a folder:

    data_folder = Folder('Data', parent=project)
    data_folder = syn.store(data_folder)

Adding files to the project:

    test_entity = File('/path/to/data/file.xyz', description='Fancy new data', parent=data_folder)
    test_entity = syn.store(test_entity)

see:
    entity
    Synapse.store

Querying
========

Synapse supports a SQL-like query language.

    results = syn.query('select id, name from entity where parentId=="syn1899495"')

    for result in results['results']:
        print result['entity.id'], result['entity.name']

see:
    Synapse.query

Access control
==============

By default, data sets in Synapse are private to your user account, but they can
easily be shared with specific users, groups, or the public.

TODO: finish this once there is a reasonable way to find principleIds.

see:
    Synapse.getPermission
    Synapse.setPermission

Provenance
==========

Synapse provides tools for tracking provenance - the transformation of raw data
into processed results - by linking derived data objects to source data and the
code used to perform the transformation.

see:
    Synapse.Activity

Evaluations
===========

An evaluation is a Synapse construct useful for building processing pipelines.

see:
    evaluation,
    Synapse.getEvaluation,
    Synapse.submit,
    Synapse.addEvaluationParticipant,
    Synapse.getSubmissions,
    Synapse.getSubmission,
    Synapse.getSubmissionStatus

Wikis
=====

Wiki pages can be attached to an Synapse entity - project, folder, file, etc.
Text and graphics can be composed in markdown and rendered in the web view of
the object.

see:
    Synapse.getWiki

Accessing the API directly
==========================

see:
    Synapse.restGET
    Synapse.restPOST
    Synapse.restPUT
    Synapse.restDELETE

More information
================

For more information see the `Synapse User Guide`_

.. _Synapse: http://www.synapse.org
.. _syn1899498: https://www.synapse.org/#!Synapse:syn1899498
.. _Synapse User Guide: https://www.synapse.org/#!Synapse:syn1669771

"""

from client import Synapse, __version__
from activity import Activity
from entity import Entity, Project, Folder, File
from entity import Analysis, Code, Data, Study, Summary
from evaluation import Evaluation, Submission, SubmissionStatus
from wiki import Wiki
