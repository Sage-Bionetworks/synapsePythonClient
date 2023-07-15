********
Tutorial
********

Authentication
==============

Most operations in Synapse require you to be logged in.  Please follow instructions in
:doc:`credentials` to configure your client.::

    import synapseclient
    syn = synapseclient.Synapse()
    syn.login()
    # If you aren't logged in, this following command will
    # show that you are an "anonymous" user.
    syn.getUserProfile()

Accessing Data
==============

Synapse identifiers are used to refer to projects and data which are represented by :py:mod:`synapseclient.entity`
objects. For example, the entity `syn1899498 <https://www.synapse.org/#!Synapse:syn1899498>`_ represents a tab-delimited
file containing a 100 by 4 matrix. Getting the entity retrieves an object that holds metadata describing the matrix,
and also downloads the file to a local cache::

    import synapseclient
    # This is a shortcut to login
    syn = synapseclient.login()
    entity = syn.get('syn1899498')

View the entity's metadata in the Python console::

    print(entity)

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


Managing Data in a Project
==========================

You can create your own projects and upload your own data sets. Synapse stores entities in a hierarchical or tree
structure. Projects are at the top level and must be uniquely named::

    import synapseclient
    from synapseclient import Project, Folder, File, Link

    syn = synapseclient.login()
    project = Project('My uniquely named project')
    project = syn.store(project)

Creating a folder::

    data_folder = Folder('Data', parent=project)
    data_folder = syn.store(data_folder)

Adding files to the project::

    test_entity = File('/path/to/data/file.xyz', description='Fancy new data', parent=data_folder)
    test_entity = syn.store(test_entity)

In addition to simple data storage, Synapse entities can be `annotated <#annotating-synapse-entities>`_ with key/value
metadata, described in markdown documents (wikis_), and linked together in provenance_ graphs to create a reproducible
record of a data analysis pipeline.

See also:

- :py:class:`synapseclient.entity.Entity`
- :py:class:`synapseclient.entity.Project`
- :py:class:`synapseclient.entity.Folder`
- :py:class:`synapseclient.entity.File`
- :py:class:`synapseclient.entity.Link`
- :py:func:`synapseclient.Synapse.store`

Annotating Synapse Entities
===========================

Annotations are arbitrary metadata attached to Synapse entities, for example::

    test_entity.genome_assembly = "hg19"

See:

- :py:mod:`synapseclient.annotations`

Provenance
==========

Synapse provides tools for tracking 'provenance', or the transformation of raw data into processed results, by linking
derived data objects to source data and the code used to perform the transformation.

See:

- :py:class:`synapseclient.activity.Activity`

Tables
======

Tables can be built up by adding sets of rows that follow a user-defined schema and queried using a SQL-like syntax.

See:

- :py:mod:`synapseclient.table`
- :py:class:`synapseclient.table.Schema`
- :py:class:`synapseclient.table.Column`
- :py:func:`synapseclient.Synapse.getColumns`
- :py:func:`synapseclient.Synapse.getTableColumns`

File Views
==========

A view is a type of table. Views display rows and columns of information, and they can be
shared and queried just like a table. Unlike tables, views are essentially queries of
other data already in Synapse. They allow you to see groups of files, tables, projects, or submissions and any associated annotations about those items.

Annotations are an essential component to building a view. Annotations are labels
that you apply to your data, stored as key-value pairs in Synapse.

See:

- :py:class:`synapseclient.table.EntityViewSchema`


Wikis
=====

Wiki pages can be attached to an Synapse entity (i.e. project, folder, file, etc). Text and graphics can be composed in
markdown and rendered in the web view of the object.

See:

- :py:func:`synapseclient.Synapse.getWiki`
- :py:class:`synapseclient.wiki.Wiki`


Access Control
==============

By default, data sets in Synapse are private to your user account, but they can easily be shared with specific users,
groups, or the public.

See:

- :py:func:`synapseclient.Synapse.getPermissions`
- :py:func:`synapseclient.Synapse.setPermissions`


More Information
================

For more information see the `Synapse Getting Started <https://help.synapse.org/docs/Getting-Started.2055471150.html>`_.

Getting Updates
===============

To get information about new versions of the client, see:
`synapseclient.check_for_updates() <Versions.html#synapseclient.version_check.check_for_updates>`_.
