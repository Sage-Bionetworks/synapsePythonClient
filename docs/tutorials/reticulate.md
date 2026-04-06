# Using synapseclient with R through reticulate

This article describes using the Python synapseclient with R through the [reticulate](https://rstudio.github.io/reticulate/) package, which provides an interface between R and Python libraries.

While the separate [synapser](https://github.com/Sage-Bionetworks/synapser) R package exists and can be installed directly in an R environment without the need for reticulate, it is not currently compatible with an R environment that already includes reticulate. In such cases using the Python synapseclient is an alternative.

## Installation

### Setting up a fresh RStudio environment with Python

The following Docker command starts an RStudio instance with Python dependencies pre-installed, which you can use to follow this guide:

```bash
docker run --rm -it -p 8787:8787 \
  -e PASSWORD=rstudio \
  rocker/rstudio:latest \
  bash -lc "
    apt-get update &&
    apt-get install -y python3 python3-venv python3-pip python3-dev build-essential libcurl4-openssl-dev libssl-dev libxml2-dev &&
    /init
  "
```

Then open `http://localhost:8787` in your browser (username: `rstudio`, password: `rstudio`).

### Installing reticulate

This article assumes that reticulate is installed and available in your R environment. If not it can be installed as follows:

```r
install.packages("reticulate")
```

### Installing synapseclient

The Python synapseclient can be installed either directly into the Python installation you intend to use with reticulate or from within R using the reticulate library.

synapseclient has the same requirements and dependencies when installed for use with reticulate as it does in other usage. In particular note that synapseclient requires a Python version of 3.10 or greater.

#### Installing into Python

The Python synapseclient is available on the [PyPi package repository](https://pypi.org/project/synapseclient/) and can be installed through Python tools that interface with the repository, such as **pip**. To install synapseclient for use with reticulate directly into a Python environment, first ensure that the current Python interpreter is the one you intend to use with reticulate. This may be a particular installation of Python, or a loaded [virtual environment](https://docs.python.org/3/tutorial/venv.html). See reticulate's [Python version configuration documentation](https://rstudio.github.io/reticulate/articles/versions.html) for more information on how reticulate can be configured to use particular Python environments.

Once you have ensured you are interacting with your intended Python interpreter, follow the standard synapseclient [installation instructions](./installation.md) to install synapseclient.

#### Installing from R/Reticulate

To install synapseclient from within R, first ensure that the reticulate library is loaded.

```r
library(reticulate)
```

Once loaded, ensure that reticulate will use the Python installation you intend. You may need to provide reticulate a hint or otherwise [point it at the proper Python installation](https://rstudio.github.io/reticulate/articles/versions.html).

Next install the synapseclient using reticulate's [py_install](https://rstudio.github.io/reticulate/reference/py_install.html) command. We recommend installing with the `pandas` and `curator` optional dependencies:

```r
py_install("synapseclient[pandas,curator]")
```

See synapseclient's [installation instructions](./installation.md) for more information on optional dependencies.

## Usage

Once synapseclient is installed, import the top-level module and the `models` and `operations` submodules through R's [import](https://rstudio.github.io/reticulate/reference/import.html) command:

```r
synapseclient <- import("synapseclient")
models <- import("synapseclient.models")
operations <- import("synapseclient.operations")
```

The `models` module contains dataclass entity types (e.g. `File`, `Project`, `Folder`). Each instance exposes methods like `store()`, `get()`, and `delete()` directly. The `operations` module provides top-level functions — most usefully `operations$get()` for retrieving an entity by Synapse ID when the entity type is not known in advance.

## Credentials

synapseclient accessed through reticulate supports the same authentication options as it does when accessed directly from Python. Log in once per session using the `Synapse` class and your auth token:

```r
syn <- synapseclient$Synapse()
syn$login()
```

See [Managing Synapse Credentials](./authentication.md) for complete documentation on how synapseclient handles credentials and authentication.

## Accessing Data

The following illustrates storing and retrieving data in Synapse using the new OOP models and operations API.

See [here](./python_client.md#accessing-data) for more details on available data access APIs.

### Create a project

```r
# use hex_digits to generate a random string for the project name
hex_digits <- c(as.character(0:9), letters[1:6])
projectName <- sprintf("My unique project %s", paste0(sample(hex_digits, 32, replace = TRUE), collapse = ""))

project <- models$Project(name = projectName)
project <- project$store()
```

### Create, store, and retrieve a file

```r
filePath <- tempfile()
connection <- file(filePath)
writeChar("a \t b \t c \n d \t e \t f \n", connection, eos = NULL)
close(connection)

# store a file inside the project
fileEntity <- models$File(path = filePath, parent_id = project$id)
fileEntity <- fileEntity$store()
synId <- fileEntity$id

# retrieve the file by its Synapse ID
# use operations$get when you don't know the entity type in advance
fileEntity <- operations$get(synId)

# view the file metadata in the console
print(fileEntity)

# open the file on the web
operations$onweb(synId)
```

### Create a folder and add files to it

```r
dataFolder <- models$Folder(name = "Data", parent_id = project$id)
dataFolder <- dataFolder$store()

filePath <- tempfile()
connection <- file(filePath)
writeChar("this is the content of the file", connection, eos = NULL)
close(connection)

fileEntity <- models$File(path = filePath, parent_id = dataFolder$id)
fileEntity <- fileEntity$store()
```

## Annotating Synapse Entities

Annotations can be stored directly on model objects via the `annotations` attribute and then stored to Synapse:

```r
project$annotations <- list(foo = "bar", fooList = list("bar", "baz"))
project <- project$store()
```

Alternatively, retrieve and update annotations directly:

```r
annotations <- syn$get_annotations(project$id)

annotations$foo <- "bar"
annotations$fooList <- list("bar", "baz")

syn$set_annotations(annotations)
```

See [here][synapseclient.annotations] for more information on annotations.

## Activity/Provenance

This example illustrates creating an entity with associated provenance.

See [here][synapseclient.activity] for more information on Activity/Provenance related APIs.

```r
act <- models$Activity(
  name = "clustering",
  description = "whizzy clustering",
  used = list(
    models$UsedEntity(target_id = "syn1234"),
    models$UsedEntity(target_id = "syn1235")
  ),
  executed = list(
    models$UsedURL(url = "https://github.com/my-org/my-repo")
  )
)

filePath <- tempfile()
connection <- file(filePath)
writeChar("some test", connection, eos = NULL)
close(connection)

fileEntity <- models$File(path = filePath, name = "provenance_file.txt", parent_id = project$id)
fileEntity$activity <- act
fileEntity <- fileEntity$store()
```

## Tables

These examples illustrate manipulating Synapse Tables.
Note that you must have installed the Pandas dependency into the Python environment as described
above in order to use this feature.

See [here][synapseclient.models.Table] for more information on tables.

The following illustrates building a table from an R data frame with the schema automatically
inferred from the data types of the columns.

```r
# start with an R data frame
genes <- data.frame(
  Name = c("foo", "arg", "zap", "bah", "bnk", "xyz"),
  Chromosome = c(1, 2, 2, 1, 1, 1),
  Start = c(12345, 20001, 30033, 40444, 51234, 61234),
  End = c(126000, 20200, 30999, 41444, 54567, 68686),
  Strand = c("+", "+", "-", "-", "+", "+"),
  TranscriptionFactor = c(F, F, F, F, T, F))

# create the table schema in Synapse
table <- models$Table(name = "My Favorite Genes", parent_id = project$id)
table <- table$store()

# upload rows — reticulate auto-converts the R data frame to a pandas DataFrame.
# INFER_FROM_DATA automatically creates columns from the data frame's schema.
table$store_rows(
  values = genes,
  schema_storage_strategy = models$SchemaStorageStrategy$INFER_FROM_DATA
)
```

Alternately the schema can be specified explicitly using `Column` objects. When using date
values it is necessary to use a date string formatted in "YYYY-MM-dd HH:mm:ss.mmm" format
or an integer unix epoch millisecond value and explicitly specify the column type.

```r
prez_birthdays <- data.frame(
  Name = c("George Washington", "Thomas Jefferson", "Abraham Lincoln"),
  Time = c("1732-02-22 11:23:11.024", "1743-04-13 00:00:00.000", "1809-02-12 01:02:03.456"))

cols <- list(
  models$Column(name = "Name", column_type = models$ColumnType$STRING, maximum_size = 20L),
  models$Column(name = "Time", column_type = models$ColumnType$DATE)
)

table <- models$Table(name = "President Birthdays", parent_id = project$id, columns = cols)
table <- table$store()
table$store_rows(values = prez_birthdays)
```

We can query a table using `models$query`, which returns a pandas DataFrame that reticulate
automatically converts to an R data frame:

```r
results <- models$query(
  sprintf("select * from %s where Name='George Washington'", table$id)
)
results
```

## Views

An EntityView provides a table-like interface over entities (Files, Folders, Projects, Tables, etc.) spread across one or more Projects or Folders. You can query a view with SQL, making it easy to filter, sort, and inspect entity metadata at scale.
The following examples illustrate some view operations.

See [here](../guides/views.md) for more information on Views.

First create some files we can use in a view:

```r
filePath1 <- tempfile()
connection <- file(filePath1)
writeChar("this is the content of the first file", connection, eos = NULL)
close(connection)
fileEntity1 <- models$File(path = filePath1, parent_id = project$id)
fileEntity1 <- fileEntity1$store()

filePath2 <- tempfile()
connection2 <- file(filePath2)
writeChar("this is the content of the second file", connection2, eos = NULL)
close(connection2)
fileEntity2 <- models$File(path = filePath2, parent_id = project$id)
fileEntity2 <- fileEntity2$store()

# add some annotations and re-store
fileEntity1$annotations <- list(contributor = "Sage", class = "V")
fileEntity1 <- fileEntity1$store()

fileEntity2$annotations <- list(contributor = "UW", rank = "X")
fileEntity2 <- fileEntity2$store()
```

Now create an `EntityView` scoped to the project. Use `bitwOr` to combine `ViewTypeMask`
values for multiple entity types:

```r
view <- models$EntityView(
  name = "my first file view",
  parent_id = project$id,
  scope_ids = list(project$id),
  view_type_mask = bitwOr(
    as.integer(models$ViewTypeMask$FILE),
    as.integer(models$ViewTypeMask$FOLDER)
  )
)

view <- view$store()
```

We can now query the view (note that views are not created synchronously; it may take a few seconds for the view table to be queryable):

```r
results <- models$query(sprintf("select * from %s", view$id))
results
```

## Using with a Shiny App

Reticulate and the Python synapseclient can be used to workaround an issue that exists when using synapser with a Shiny App. Since synapser shares a Synapse client instance within the R process, multiple users of a synapser integrated Shiny App may end up sharing a login if precautions aren't taken. When using reticulate with synapseclient, session scoped Synapse client objects can be created that avoid this issue.

See [SynapseShinyApp](https://github.com/Sage-Bionetworks/SynapseShinyApp) for a sample application and a discussion of the issue, and the [reticulate](https://github.com/Sage-Bionetworks/SynapseShinyApp/tree/reticulate) branch for an alternative implementation using reticulate with synapseclient.

## Developing an R Package with synapseclient and reticulate

If you are building an R package that wraps synapseclient, wrap all imports in `.onLoad` and use the `delay_load` option. This lets users configure their Python environment before any import occurs, regardless of when they load your package:

```r
synapseclient <- NULL
models <- NULL
operations <- NULL

.onLoad <- function(libname, pkgname) {
  synapseclient <<- reticulate::import("synapseclient", delay_load = TRUE)
  models        <<- reticulate::import("synapseclient.models", delay_load = TRUE)
  operations    <<- reticulate::import("synapseclient.operations", delay_load = TRUE)
}
```

More information on this technique can be found [here](https://rstudio.github.io/reticulate/articles/package.html).
