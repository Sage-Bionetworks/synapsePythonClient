=============================================
Using synapseclient with R through reticulate
=============================================

This article describes using the Python synapseclient with R through the
`reticulate <https://rstudio.github.io/reticulate/>`__ package, which provides an interface between R and Python
libraries.

While the separate `synapser <https://github.com/Sage-Bionetworks/synapser>`__ R package exists and can be installed
directly in an R environment without the need for reticulate, it is not currently compatible with an
R environment that already includes reticulate. In such cases using the Python synapseclient is an alternative.


Installation
============

Installing reticulate
+++++++++++++++++++++

This article assumes that reticulate is installed and available in your R environment. If not it can be installed
as follows:

  .. code-block::

    install.packages("reticulate")

Installing synapseclient
++++++++++++++++++++++++

The Python synapseclient can be installed either directly into the Python installation you intend to use with
reticulate or from within R using the reticulate library.

synapseclient has the same requirements and dependencies when installed for use with reticulate as it does in other
usage. In particular note that synapseclient requires a Python version of 3.6 or greater.

Installing into Python
----------------------

The Python synapseclient is available on the `PyPi package repository <https://pypi.org/project/synapseclient/>`__
and can be installed through Python tools that interface with the repository, such as **pip**. To install synapseclient for
use with reticulate directly into a Python environment, first ensure that the current Python interpreter is the one you
intend to use with reticulate. This may be a particular installation of Python, or a loaded
`virtual environment <https://docs.python.org/3/tutorial/venv.html>`__. See reticulate's
`Python version configuration documentation <https://rstudio.github.io/reticulate/articles/versions.html>`__ for more
information on how reticulate can be configured to use particular Python environments.

Once you have ensured you are interacting with your intended Python interpreter, follow the standard synapseclient
`installation instructions <index.html#installation>`__ to install synapseclient.

Installing from R/Reticulate
----------------------------

To install synapseclient from within R, first ensure that the reticulate library is loaded.

  .. code-block::

    library(reticulate)

Once loaded, ensure that reticulate will use the Python installation you intend. You may need to provide reticulate
a hint or otherwise `point it at the proper Python installation
<https://rstudio.github.io/reticulate/articles/versions.html>`__.

Next install the synapseclient using reticulate's `py_install
<https://rstudio.github.io/reticulate/reference/py_install.html>`__ command, e.g.

  .. code-block::

    py_install("synapseclient")

You may also want to install some of synapseclient's optional dependencies, such as `Pandas
<https://pandas.pydata.org/>`__ for table support.

  .. code-block::

    py_install("pandas")

See synapseclient's `installation instructions <index.html#installation>`__ for more information on optional
dependencies.

Usage
=====

Once synapseclient is installed it can be used once it is imported through R's `import
<https://rstudio.github.io/reticulate/reference/imporl.html>`__ command:

  .. code-block::

    synapseclient <- import("synapseclient")

If you are familiar with the **synapser** R package, many of the commands will be similar, but unlike in synapser
where package functions and classes are made available in the global namespace through the search path,
when using synapseclient through reticulate, classes are accessed through the imported synapseclient module and
functionality is provided through an instantiated Synapse instance.

For example classes that were globally available are now available through the imported synapseclient module.

  .. code-block::

    # File from synapser
    synapseclient$File

    # Table from synapser
    synapseclient$Table

And various syn functions are now methods on the Synapse object:

  .. code-block::

    # using synapseclient with reticulate we must instantiate a Synapse instance
    syn <- synapseclient$Synapse()

    # synLogin from synapser
    syn$login()

    # synGet from synapser
    syn$get(identifier)

    # synStore from syanpser
    syn$store(entity)

Each synapse object has its own state, such as configuration and login credentials.


Credentials
===========

synapseclient accessed through reticulate supports the same authentication options as it does when accessed directly
from Python, for example:

  .. code-block::

    syn <- synapseclient$synapse()

    # one time login
    syn$login('<username', '<password>')

    # login and store credentials for future use
    syn$login('<username', '<password>', rememberMe=TRUE)

See `Managing Synapse Credentials <Credentials.html#manage-synapse-credentials>`__ for complete documentation on how
synapseclient handles credentials and authentication.



Accessing Data
==============

The following illustrates some examples of storing and retrieving data in Synapse using
synapseclient through reticulate.

See `here <index.html#accessing-data>`__ for more details on available data access APIs.

Create a project with a unique name

  .. code-block::

    # use hex_digits to generate random string and use it to name a project
    hex_digits <- c(as.character(0:9), letters[1:6])
    projectName <- sprintf("My unique project %s", paste0(sample(hex_digits, 32, replace = TRUE), collapse = ""))

    project <- synapseclient$Project(projectName)
    project <- syn$store(project)

Create, store, and retrieve a file

  .. code-block::

    filePath <- tempfile()
    connection <- file(filePath)
    writeChar("a \t b \t c \n d \t e \t f \n", connection, eos = NULL)
    close(connection)

    file <- synapseclient$File(path = filePath, parent = project)
    file <- syn$store(file)
    synId <- file$properties$id

    # download the file using its identifier to specific path
    fileEntity <- syn$get(synId, downloadLocation="/path/to/folder")

    # view the file meta data in the console
    print(fileEntity)

    # view the file on the web
    syn$onweb(synId)

Create folder and add files to the folder:

  .. code-block::

    dataFolder <- synapseclient$Folder("Data", parent = project)
    dataFolder <- syn$store(dataFolder)

    filePath <- tempfile()
    connection <- file(filePath)
    writeChar("this is the content of the file", connection, eos = NULL)
    close(connection)
    file <- synapseclient$File(path = filePath, parent = dataFolder)
    file <- syn$store(file)


Annotating Synapse Entities
===========================

This illustrates adding annotations to a Synapse entity.

  .. code-block::

    # first retrieve the existing annotations object
    annotations = syn$get_annotations(project)

    annotations$foo <- "bar"
    annotations$fooList <- list("bar", "baz")

    syn$set_annotations(annotations)

See `here <index.html#annotating-synapse-entities>`__ for more information on annotations.

Provenance
==========

This example illustrates creating an entity with associated provenance.

See `here <index.html#provenance>`__ for more information on Provenance related APIs.

  .. code-block::

    act <- synapseclient$Activity(
      name = "clustering",
      description = "whizzy clustering",
      used = c("syn1234", "syn1235"),
      executed = "syn4567")

  .. code-block::

    filePath <- tempfile()
    connection <- file(filePath)
    writeChar("some test", connection, eos = NULL)
    close(connection)

    file = synapseclient$File(filePath, name="provenance_file.txt", parent=project)
    file <- syn$store(, activity = act)


Tables
======

These examples illustrate manipulating Synapse Tables.
Note that you must have installed the Pandas dependency into the Python environment as described
above in order to use this feature.

See `here <index.html#tables>`__ for more information on tables.

The following illustrates building a table from an R data frame. The schema will be generated
from the data types of the values within the data frame.

  .. code-block::

    # start with an R data frame
    genes <- data.frame(
      Name = c("foo", "arg", "zap", "bah", "bnk", "xyz"),
      Chromosome = c(1, 2, 2, 1, 1, 1),
      Start = c(12345, 20001, 30033, 40444, 51234, 61234),
      End = c(126000, 20200, 30999, 41444, 54567, 68686),
      Strand = c("+", "+", "-", "-", "+", "+"),
      TranscriptionFactor = c(F, F, F, F, T, F))

    # build a Synapse table from the data frame.
    # a schema is automatically generated
    # note that reticulate will automatically convert from an R data frame to Pandas
    table <- synapseclient$build_table("My Favorite Genes", project, genes)

    table <- syn$store(table)

Alternately the schema can be specified. At this time when using date values it is necessary
to use string or millisecond values and explicitly specify the type in the schema due to how
dates are translated to the Python client.

  .. code-block::

    prez_birthdays <- data.frame(
      Name = c("George Washington", "Thomas Jefferson", "Abraham Lincoln"),
      Time = c("1732-02-22 11:23:11.024", "1743-04-13 00:00:00.000", "1809-02-12 01:02:03.456"))

    cols <- list(
        synapseclient$Column(name = "Name", columnType = "STRING", maximumSize = 20),
        synapseclient$Column(name = "Time", columnType = "DATE"))

    schema <- synapseclient$Schema(name = "President Birthdays", columns = cols, parent = project)
    table <- synapseclient$Table(schema, prez_birthdays)

    # store the table in Synapse
    table <- syn$store(table)

We can query a table as in the following:

  .. code-block::

    tableId <- table$tableId

    results <- syn$tableQuery(sprintf("select * from %s where Name='George Washington'", tableId))
    results$asDataFrame()

Wikis
=====

This example illustrates creating a wiki.

See `here <index.html#wikis>`__ for more information on wiki APIs.

  .. code-block::

    content <- "
    # My Wiki Page
    Here is a description of my **fantastic** project!
    "

    # attachment
    filePath <- tempfile()
    connection <- file(filePath)
    writeChar("this is the content of the file", connection, eos = NULL)
    close(connection)
    wiki <- synapseclient$Wiki(
                owner = project,
                title = "My Wiki Page",
                markdown = content,
                attachments = list(filePath)
    )
    wiki <- syn$store(wiki)

An existing wiki can be updated as follows.

  .. code-block::

    wiki <- syn$getWiki(project)
    wiki$markdown <- "
    # My Wiki Page
    Here is a description of my **fantastic** project! Let's
    *emphasize* the important stuff.
    "
    wiki <- syn$store(wiki)


Evaluations
===========

An Evaluation is a Synapse construct useful for building processing pipelines and
for scoring predictive modeling and data analysis challenges.

See `here <index.html#evaluations>`__ for more information on Evaluations.

Creating an Evaluation:

  .. code-block::

    eval <- synapseclient$Evaluation(
      name = sprintf("My unique evaluation created on %s", format(Sys.time(), "%a %b %d %H%M%OS4 %Y")),
      description = "testing",
      contentSource = project,
      submissionReceiptMessage = "Thank you for your submission!",
      submissionInstructionsMessage = "This evaluation only accepts files.")

    eval <- syn$store(eval)

    eval <- syn$getEvaluation(eval$id)

Submitting a file to an existing Evaluation:

  .. code-block::

    # first create a file to submit
    filePath <- tempfile()
    connection <- file(filePath)
    writeChar("this is my first submission", connection, eos = NULL)
    close(connection)
    file <- synapseclient$File(path = filePath, parent = project)
    file <- syn$store(file)
    # submit the created file
    submission <- syn$submit(eval, file)

List submissions:

  .. code-block::

    submissions <- syn$getSubmissionBundles(eval)

    # submissions are returned as a generator
    list(iterate(submissions))

Retrieving submission by id:

  .. code-block::

    # Not evaluating this section because of SYNPY-235
    submission <- syn$getSubmission(submission$id)

Retrieving the submission status:

  .. code-block::

    submissionStatus <- syn$getSubmissionStatus(submission)
    submissionStatus

Query an evaluation:

  .. code-block::

    queryString <- sprintf("query=select * from evaluation_%s LIMIT %s OFFSET %s'", eval$id, 10, 0)
    syn$restGET(paste("/evaluation/submission/query?", URLencode(queryString), sep = ""))


Sharing Access to Content
=========================

The following illustrates sharing access to a Synapse Entity.

See `here <index.html#access-control>`__ for more information on Access Control including all available permissions.

  .. code-block::

    # get permissions on an entity
    # to get permissions for a user/group pass a principalId identifier,
    # otherwise the assumed permission will apply to the public

    # make the project publicly accessible
    acl <- syn$setPermissions(project, accessType = list("READ"))

    perms = syn$getPermissions(project)


Views
=====

A view is a view of all entities (File, Folder, Project, Table, Docker Repository, View) within one or more Projects or Folders. Views can:
The following examples illustrate some view operations.

See `here <index.html#views>`__ for more information on Views. A view is implemented as a Table,
see `here <index.html#tables>`__ for more information on Tables.

First create some files we can use in a view:

  .. code-block::

    filePath1 <- tempfile()
    connection <- file(filePath1)
    writeChar("this is the content of the first file", connection, eos = NULL)
    close(connection)
    file1 <- synapseclient$File(path = filePath1, parent = project)
    file1 <- syn$store(file1)
    filePath2 <- tempfile()
    connection2 <- file(filePath2)
    writeChar("this is the content of the second file", connection, eos = NULL)
    close(connection2)
    file2 <- synapseclient$File(path = filePath2, parent = project)
    file2 <- syn$store(file2)

    # add some annotations
    fileAnnotations1 <- syn$get_annotations(file1)
    fileAnnotations2 <- syn$get_annotations(file2)

    fileAnnotations1$contributor <- "Sage"
    fileAnnotations1$class <- "V"
    syn$set_annotations(fileAnnotations1)

    fileAnnotations2$contributor = "UW"
    fileAnnotations2$rank = "X"
    syn$set_annotations(fileAnnotations2)

Now create a view:

  .. code-block::

    columns = c(
      synapseclient$Column(name = "contributor", columnType = "STRING"),
      synapseclient$Column(name = "class", columnType = "STRING"),
      synapseclient$Column(name = "rank", columnType = "STRING")
    )

    view <- synapseclient$EntityViewSchema(
        name = "my first file view",
        columns = columns,
        parent = project,
        scopes = project,
        includeEntityTypes = c(synapseclient$EntityViewType$FILE, synapseclient$EntityViewType$FOLDER),
        add_default_columns = TRUE
    )

    view <- syn$store(view)

We can now see content of our view (note that views are not created synchronously it may take a few seconds
for the view table to be queryable).

  .. code-block::

    queryResults <- syn$tableQuery(sprintf("select * from %s", view$properties$id))
    data <- queryResults$asDataFrame()
    data

We can update annotations using a view as follows:

  .. code-block::

    data["class"] <- c("V", "VI")
    syn$store(synapseclient$Table(view$properties$id, data))

    # the change in annotations is reflected in synGetAnnotations():
    syn$get_annotations(file2$properties$id)

Update View's Content

  .. code-block::

    A view can contain different types of entity. To change the types of entity that will show up in a view:
    view <- syn$get(view$properties$id)
    view$set_entity_types(list(synapseclient$EntityViewType$FILE))



