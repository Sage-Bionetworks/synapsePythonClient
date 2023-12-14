# Tables

Tables can be built up by adding sets of rows that follow a user-defined schema and queried using a SQL-like syntax.

## Creating a table and loading it with data
### Initial setup:

```python
import synapseclient
from synapseclient import Project, File, Folder
from synapseclient import Schema, Column, Table, Row, RowSet, as_table_columns, build_table

syn = synapseclient.Synapse()
syn.login()

project = syn.get('syn123')
```

### Example data

First, let's load some data. Let's say we had a file, genes.csv:

```csv
Name,Chromosome,Start,End,Strand,TranscriptionFactor
foo,1,12345,12600,+,False
arg,2,20001,20200,+,False
zap,2,30033,30999,-,False
bah,1,40444,41444,-,False
bnk,1,51234,54567,+,True
xyz,1,61234,68686,+,False
```

### Creating a table with columns

```python
table = build_table('My Favorite Genes', project, "/path/to/genes.csv")
syn.store(table)
```

[build_table][synapseclient.table.build_table] will set the Table [Schema][synapseclient.table.Schema] which defines the columns of the table.
To create a table with a custom [Schema][synapseclient.table.Schema], first create the [Schema][synapseclient.table.Schema]:

```python
cols = [
    Column(name='Name', columnType='STRING', maximumSize=20),
    Column(name='Chromosome', columnType='STRING', maximumSize=20),
    Column(name='Start', columnType='INTEGER'),
    Column(name='End', columnType='INTEGER'),
    Column(name='Strand', columnType='STRING', enumValues=['+', '-'], maximumSize=1),
    Column(name='TranscriptionFactor', columnType='BOOLEAN')]

schema = Schema(name='My Favorite Genes', columns=cols, parent=project)
```

### Storing the table in Synapse

```python
table = Table(schema, "/path/to/genes.csv")
table = syn.store(table)
```

The `Table` function takes two arguments, a schema object and data in some form, which can be:

  * a path to a CSV file
  * a [Pandas](http://pandas.pydata.org/) [DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe)
  * a `RowSet` object
  * a list of lists where each of the inner lists is a row

### Querying for data

With a bit of luck, we now have a table populated with data. Let's try to query:

```python
results = syn.tableQuery("select * from %s where Chromosome='1' and Start < 41000 and End > 20000"
                         % table.schema.id)
for row in results:
    print(row)
```

## Using Pandas to accomplish setup and querying

[Pandas](http://pandas.pydata.org/) is a popular library for working with tabular data. If you have Pandas installed, the goal is that Synapse Tables will play nice with it.

Create a Synapse Table from a [DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe):

```python
import pandas as pd

df = pd.read_csv("/path/to/genes.csv", index_col=False)
table = build_table('My Favorite Genes', project, df)
table = syn.store(table)
```

`build_table` uses pandas DataFrame dtype to set the Table `Schema`.
To create a table with a custom `Schema`, first create the `Schema`:

```python
schema = Schema(name='My Favorite Genes', columns=as_table_columns(df), parent=project)
table = syn.store(Table(schema, df))
```

Get query results as a [DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe):

```python
results = syn.tableQuery("select * from %s where Chromosome='2'" % table.schema.id)
df = results.asDataFrame()
```

## Changing Data

Once the schema is settled, changes come in two flavors: appending new rows and updating existing ones.

**Appending** new rows is fairly straightforward. To continue the previous example, we might add some new genes from another file:

```python
table = syn.store(Table(table.schema.id, "/path/to/more_genes.csv"))
```

To quickly add a few rows, use a list of row data:

```python
new_rows = [["Qux1", "4", 201001, 202001, "+", False],
            ["Qux2", "4", 203001, 204001, "+", False]]
table = syn.store(Table(schema, new_rows))
```

**Updating** rows requires an etag, which identifies the most recent change set plus row IDs and version numbers for each row to be modified. We get those by querying before updating. Minimizing changesets to contain only rows that actually change will make processing faster.

For example, let's update the names of some of our favorite genes:

```python
results = syn.tableQuery("select * from %s where Chromosome='1'" % table.schema.id)
df = results.asDataFrame()
df['Name'] = ['rzing', 'zing1', 'zing2', 'zing3']
```

Note that we're propagating the etag from the query results. Without it, we'd get an error saying something about an "Invalid etag":

```python
table = syn.store(Table(schema, df, etag=results.etag))
```

The etag is used by the server to prevent concurrent users from making conflicting changes, a technique called optimistic concurrency. In case of a conflict, your update may be rejected. You then have to do another query and try your update again.

## Changing Table Structure

Adding columns can be done using the methods `Schema.addColumn` or `addColumns` on the `Schema` object:

```python
schema = syn.get("syn000000")
bday_column = syn.store(Column(name='birthday', columnType='DATE'))
schema.addColumn(bday_column)
schema = syn.store(schema)
```

Renaming or otherwise modifying a column involves removing the column and adding a new column:

```python
cols = syn.getTableColumns(schema)
for col in cols:
    if col.name == "birthday":
        schema.removeColumn(col)
bday_column2 = syn.store(Column(name='birthday2', columnType='DATE'))
schema.addColumn(bday_column2)
schema = syn.store(schema)
```

## Table attached files

Synapse tables support a special column type called 'File' which contain a file handle, an identifier of a file stored in Synapse. Here's an example of how to upload files into Synapse, associate them with a table and read them back later:

```python
# your synapse project
import tempfile
project = syn.get(...)

# Create temporary files to store
temp = tempfile.NamedTemporaryFile()
with open(temp.name, "w+") as temp_d:
    temp_d.write("this is a test")

temp2 = tempfile.NamedTemporaryFile()
with open(temp2.name, "w+") as temp_d:
    temp_d.write("this is a test 2")

# store the table's schema
cols = [
    Column(name='artist', columnType='STRING', maximumSize=50),
    Column(name='album', columnType='STRING', maximumSize=50),
    Column(name='year', columnType='INTEGER'),
    Column(name='catalog', columnType='STRING', maximumSize=50),
    Column(name='cover', columnType='FILEHANDLEID')]
schema = syn.store(Schema(name='Jazz Albums', columns=cols, parent=project))

# the actual data
data = [["John Coltrane",  "Blue Train",   1957, "BLP 1577", temp.name],
        ["Sonny Rollins",  "Vol. 2",       1957, "BLP 1558", temp.name],
        ["Sonny Rollins",  "Newk's Time",  1958, "BLP 4001", temp2.name],
        ["Kenny Burrel",   "Kenny Burrel", 1956, "BLP 1543", temp2.name]]

# upload album covers
for row in data:
    file_handle = syn.uploadFileHandle(row[4], parent=project)
    row[4] = file_handle['id']

# store the table data
row_reference_set = syn.store(RowSet(schema=schema, rows=[Row(r) for r in data]))

# Later, we'll want to query the table and download our album covers
results = syn.tableQuery(f"select artist, album, cover from {schema.id} where artist = 'Sonny Rollins'")
test_files = syn.downloadTableColumns(results, ['cover'])
```

## Deleting rows

Query for the rows you want to delete and call syn.delete on the results:

```python
results = syn.tableQuery("select * from %s where Chromosome='2'" % table.schema.id)
a = syn.delete(results)
```

## Deleting the whole table

Deleting the schema deletes the whole table and all rows:

```python
syn.delete(schema)
```

## Queries

The query language is quite similar to SQL select statements, except that joins are not supported. The documentation for the Synapse API has lots of [query examples](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html).



See:

<!-- Schema
Column
Row
RowSet
.. autoclass:: synapseclient.table.TableAbstractBaseClass
   :members:
.. autoclass:: synapseclient.table.RowSetTable
   :members:
.. autoclass:: synapseclient.table.TableQueryResult
   :members:
.. autoclass:: synapseclient.table.CsvFileTable
   :members:
   Dataset
See also:
 - :py:meth:`synapseclient.Synapse.getColumns`
 - :py:meth:`synapseclient.Synapse.getTableColumns`
 - :py:meth:`synapseclient.Synapse.tableQuery`
 - :py:meth:`synapseclient.Synapse.get`
 - :py:meth:`synapseclient.Synapse.store`
 - :py:meth:`synapseclient.Synapse.delete` -->
- [synapseclient.table][]
- [synapseclient.table.Schema][]
- [synapseclient.table.Column][]
- [synapseclient.Synapse.getColumns][]
- [synapseclient.Synapse.getTableColumns][]
