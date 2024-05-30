# Working with the Python client

Welcome to the Synapse Python client! In the following tutorials you'll create a
new project and learn about things you can do with that project.

These examples are themed around a biomedical researcher working with Alzheimer's
Disease wanting to share their findings with their colleagues, and the world.

## Prerequisites

To get started with these tutorials make sure that you have completed the
[Installation](./installation.md) and [Authentication](./authentication.md) setup.


## End goal
By the end of these tutorials you'll have:

- A [Project](./python/project.md) created in Synapse
- [Folders](./python/folder.md) and [Files](./python/file.md) added to your project
- [Annotations](./python/annotation.md) added to your Project, Folders, and Files
- A File with multiple [Versions](./python/versions.md)
- A File that has an [Activity/Provenance](./python/activity.md) added to it
- A [File View](./python/file_view.md) created for your Project
- A [Table](./python/table.md) created for your Project
- [Create, Read, Update, Delete operations](./python/table_crud.md) for your table
- A [Dataset](./python/dataset.md) created for your project
- A File with updated [sharing settings](./python/sharing_settings.md)
- A [Wiki](./python/wiki.md) for your Project
- A [Team](./python/team.md) created with one or more members
- Methods to [upload data in bulk](./python/upload_data_in_bulk.md)
- Methods to [download data in bulk](./python/download_data_in_bulk.md)
- Methods to [move files and folders](./python/move_files_and_folders.md)
- Methods to [migrate data to other storage locations](./python/migrate_data_to_other_storage_locations.md)


## Current python client documentation being split into individual tutorials
----
## Authentication

Most operations in Synapse require you to be logged in. Please follow instructions in [authentication](authentication.md) to configure your client:

```python
import synapseclient
syn = synapseclient.Synapse()
syn.login()
# If you aren't logged in, this following command will
# show that you are an "anonymous" user.
syn.getUserProfile()
```

## Accessing Data

Synapse identifiers are used to refer to projects and data which are represented by `synapseclient.entity` objects. For example, the entity [syn1899498](https://www.synapse.org/#!Synapse:syn1899498) represents a tab-delimited file containing a 100 by 4 matrix. Getting the entity retrieves an object that holds metadata describing the matrix, and also downloads the file to a local cache:

```python
import synapseclient
# This is a shortcut to login
syn = synapseclient.login()
entity = syn.get('syn1899498')
```

View the entity's metadata in the Python console:

```python
print(entity)
```

This is one simple way to read in a small matrix:

```python
rows = []
with open(entity.path) as f:
    header = f.readline().split('\t')
    for line in f:
        row = [float(x) for x in line.split('\t')]
        rows.append(row)
```

View the entity in the browser:

```python
syn.onweb('syn1899498')
```

- [synapseclient.entity.Entity][]
- [synapseclient.Synapse.get][]
- [synapseclient.Synapse.onweb][]

## Managing Data in a Project

You can create your own projects and upload your own data sets. Synapse stores entities
in a hierarchical or tree structure. Projects are at the top level and must be uniquely named:

```python
import synapseclient
from synapseclient import Project, Folder, File

syn = synapseclient.login()
# Project names must be globally unique
project = Project('My uniquely named project')
project = syn.store(project)
```

Creating a folder:

```python
data_folder = Folder('Data', parent=project)
data_folder = syn.store(data_folder)
```

Adding files to the project. You will get an error if you try to store an empty file in Synapse. Here we create temporary files, but you can specify your own file path:

```python
import tempfile

temp = tempfile.NamedTemporaryFile(prefix='your_file', suffix='.txt')
with open(temp.name, "w") as temp_f:
    temp_f.write("Example text")
filepath = temp.name
test_entity = File(filepath, description='Fancy new data', parent=data_folder)
test_entity = syn.store(test_entity)
print(test_entity)
```

You may notice that there is "downloadAs" name and "entity name". By default, the client will use the file's name as the entity name, but you can configure the file to display a different name on Synapse:

```python
test_second_entity = File(filepath, name="second file", parent=data_folder)
test_second_entity = syn.store(test_second_entity)
print(test_second_entity)
```

In addition to simple data storage, Synapse entities can be [annotated](#annotating-synapse-entities) with key/value metadata, described in markdown documents ([Wiki](../reference/wiki.md)), and linked together in provenance graphs to create a reproducible record of a data analysis pipeline.

See also:

- [synapseclient.entity.Entity][]
- [synapseclient.entity.Project][]
- [synapseclient.entity.Folder][]
- [synapseclient.entity.File][]
- [synapseclient.Synapse.store][]

## Annotating Synapse Entities

Annotations are arbitrary metadata attached to Synapse entities. There are different ways to creating annotations. Using the entity created from the previous step in the tutorial, for example:

```python
# First method
test_ent = syn.get(test_entity.id)
test_ent.foo = "foo"
test_ent.bar = "bar"
syn.store(test_ent)

# Second method
test_ent = syn.get(test_entity.id)
annotations = {"foo": "foo", "bar": "bar"}
test_ent.annotations = annotations
syn.store(test_ent)
```

See:

- [synapseclient.annotations][]

## Versioning

Synapse supports versioning of many entity types. This tutorial will focus on File versions. Using the project/folder created earlier in this tutorial

Uploading a new version. Synapse leverages the entity name to version entities:

```python
import tempfile

temp = tempfile.NamedTemporaryFile(prefix='second', suffix='.txt')
with open(temp.name, "w") as temp_f:
    temp_f.write("First text")

version_entity = File(temp.name, parent=data_folder)
version_entity = syn.store(version_entity)
print(version_entity.versionNumber)

with open(temp.name, "w") as temp_f:
    temp_f.write("Second text")
version_entity = File(temp.name, parent=data_folder)
version_entity = syn.store(version_entity)
print(version_entity.versionNumber)
```

Downloading a specific version. By default, Synapse downloads the latest version unless a version is specified:
```python
version_1 = syn.get(version_entity, version=1)
```


## Provenance

Synapse provides tools for tracking 'provenance', or the transformation of raw data into processed results, by linking derived data objects to source data and the code used to perform the transformation:

```python
# pass the provenance to the store function
provenance_ent = syn.store(
    version_entity,
    used=[version_1.id],
    executed=["https://github.com/Sage-Bionetworks/synapsePythonClient/tree/v2.7.2"]
)
```

See:

- [synapseclient.activity.Activity][]

## File Views

Views display rows and columns of information, and they can be shared and queried with SQL. Views are queries of other data already in Synapse. They allow you to see groups of files, tables, projects, or submissions and any associated annotations about those items.

Annotations are an essential component to building a view. Annotations are labels that you apply to your data, stored as key-value pairs in Synapse.

We will create a file view from the project above:

```python
import synapseclient
syn = synapseclient.login()
# Here we are using project.id from the earlier sections from this tutorial
project_id = project.id
fileview = EntityViewSchema(
    name='MyTable',
    parent=project_id,
    scopes=[project_id]
)
fileview_ent = syn.store(fileview)
```

You can now query it to see all the files within the project. Note: it is highly recommended to install `pandas`:

```python
query = syn.tableQuery(f"select * from {fileview_ent.id}")
query_results = query.asDataFrame()
print(query_results)
```

See:

- [synapseclient.table.EntityViewSchema][]
- [Using Entity Views](../guides/views.md)

## More Information

For more information see the [Synapse Getting Started](https://help.synapse.org/docs/Getting-Started.2055471150.html).
