=====
Views
=====


A view is a view of all entities (File, Folder, Project, Table, Docker Repository, View) within one or more Projects or Folders. Views can:

* Provide a way of isolating or linking data based on similarities
* Provide the ability to link entities together by their annotations
* Allow view/editing entities attributes in bulk
* Allow entities to be easily searched and queried

Let's go over some examples to demonstrate how view works. First, create a new project and add some files::

    import synapseclient
    from synapseclient import Project, File, Column, Table, EntityViewSchema, EntityViewType
    syn = synapseclient.Synapse()
    syn.login()

    # Create a new project

    project = syn.store(Project("test view"))

    # Create some files

    file1 = syn.store(File(path="path/to/file1.txt", parent=project))
    file2 = syn.store(File(path="path/to/file2.txt", parent=project))

    # add some annotations

    syn.setAnnotations(file1, {"contributor":"Sage", "class":"V"})
    syn.setAnnotations(file2, {"contributor":"UW", "rank":"X"})

Creating a View
===============

To create a view, defines its name, columns, parent, scope, and the type of the view::

    view = EntityViewSchema(name="my first file view",
                            columns=[
                                Column(name="contributor", columnType="STRING"),
                                Column(name="class", columnType="STRING"),
                                Column(name="rank", columnType="STRING")),
                            parent=project['id'],
                            scopes=project['id'],
                            includeEntityTypes=[EntityViewType.FILE, EntityViewType.FOLDER],
                            add_default_columns=True)
    view = syn.store(view)


We support the following entity type in a View::

    EntityViewType.FILE
    EntityViewType.PROJECT
    EntityViewType.TABLE
    EntityViewType.FOLDER
    EntityViewType.VIEW
    EntityViewType.DOCKER


To see the content of your newly created View, use ``syn.tableQuery()``::

    query_results = syn.tableQuery("select * from %s" % view['id'])
    data = query_results.asDataFrame()

Updating Annotations using View
===============================

To update ``class`` annotation for ``file2``, simply update the view::

    # Retrieve the view data using table query
    query_results = syn.tableQuery("select * from %s" % view['id'])
    data = query_results.asDataFrame()

    # Modify the annotations by modifying the view data and store it
    data["class"] = ["V", "VI"]
    syn.store(Table(view['id'], data))

The change in annotations reflect in synGetAnnotations()::

    syn.getAnnotations(file2['id'])

A View is a Table. Please visit `Tables <https://python-docs.synapse.org/build/html/Table.html#module-synapseclient.table>`_ to see how to change schema, update content, and other operations that can be done on View.