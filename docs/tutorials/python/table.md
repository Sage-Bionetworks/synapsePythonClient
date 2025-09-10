# Tables

Tables in Synapse allow you to upload a CSV and/or TSV into a querable interface that follow a user-defined schema and queried using a SQL-like syntax.


## Tutorial Purpose
In this tutorial you will:

1. Create a Table and load it with data
1. Query your data
1. Update your table
1. Change table structure
1. Delete rows and tables


## Prerequisites
* Make sure that you have completed the [Project](./project.md) tutorial.
* The tutorial assumes you have data in a dataframe and/or CSV you want to query in a SQL like interface in Synapse

## 1. Creating a table and loading it with data

### Initial setup:

```python
import synapseclient
from synapseclient.models import Column, Project, query, SchemaStorageStrategy, Table

syn = synapseclient.Synapse()
syn.login()

project = Project(name="My uniquely named project about Alzheimer's Disease").get()
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

### Creating a table without specifying the columns

```python
table = Table(
    name="My Favorite Genes",
    parent_id=project.id,
)
table = table.store()
table.store_rows(values="/path/to/genes.csv", schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA)
```

### Creating a table with specified columns

```python
columns = [
    Column(name='Name', column_type='STRING', maximum_size=20),
    Column(name='Chromosome', column_type='STRING', maximum_size=20),
    Column(name='Start', column_type='INTEGER'),
    Column(name='End', column_type='INTEGER'),
    Column(name='Strand', column_type='STRING', enum_values=['+', '-'], maximum_size=1),
    Column(name='TranscriptionFactor', column_type='BOOLEAN')
]
table = Table(
    name="My Favorite Genes",
    columns=columns,
    parent_id=project.id,
)
table = table.store()
table.store_rows(values="/path/to/genes.csv")
```

### Storing the table in Synapse using pandas


[Pandas](http://pandas.pydata.org/) is a popular library for working with tabular data. If you have Pandas installed, the goal is that Synapse Tables will play nice with it. Create a Synapse Table from a [DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe):

```python
import pandas as pd

df = pd.read_csv("/path/to/genes.csv", index_col=False)
table = Table(
    name="My Favorite Genes",
    columns=columns,
    parent_id=project.id,
)
table = table.store()
table.store_rows(values=df)
```

## 2. Querying for data

The query language is quite similar to SQL select statements, except that joins are not supported. The documentation for the Synapse API has lots of [query examples](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html). With a bit of luck, we now have a table populated with data. Let's try to query:

```python
results = query(
    f"SELECT * FROM {table.id} WHERE Chromosome = '1' AND Start < 41000 AND \"End\" > 20000"
)
for _, row_info in results.iterrows():
    print(row_info)
```


## 3. Changing Data

Once the schema is settled, changes come in two flavors: appending new rows and updating existing ones.

### Appending new rows

**Appending** new rows is fairly straightforward. To continue the previous example, we might add some new genes from another file.
Lets take this new data in `more_genes.csv`

```csv
Name,Chromosome,Start,End,Strand,TranscriptionFactor
Qux1,4,201001,202001,+,False
Qux2,4,203001,204001,+,False
```

To add it via a filepath

```python
table = Table(
    name="My Favorite Genes",
    parent_id=project.id,
).get()
table = table.store_rows(values="/path/to/more_genes.csv")
```

Using pandas:

```python
new_rows_df = pd.DataFrame({
    "Name": ["Qux3", "Qux4"],
    "Chromosome": ["4", "4"],
    "Start": [201001, 203001],
    "End": [202001, 204001],
    "Strand": ["+", "+"],
    "TranscriptionFactor": [False, False]
})
table.store_rows(values=new_rows_df)
```

### Updating existing rows

**Updating** rows requires an etag, which identifies the most recent change set plus row IDs and version numbers for each row to be modified. We get those by querying before updating. Minimizing changesets to contain only rows that actually change will make processing faster.

For example, let's update the names of some of our favorite genes:

```python
results_df = query(f"select * from {table.id} where Chromosome='1'")
results_df['Name'] = ['rzing', 'zing1', 'zing2', 'zing3']
table.store_rows(values=results_df)
```

### Upserting values

If your table has a primary key, you can use the upsert functionality to update existing rows or insert new rows based on whether the primary key value already exists in the table. Here is an example.

```python
to_upsert_df = pd.DataFrame({
    "Name": ["Qux3", "Qux5"],
    "Chromosome": ["4", "4"],
    "Start": [201001, 203001],
    "End": [202001, 204001],
    "Strand": ["-", "+"],
    "TranscriptionFactor": [True, False]
})
# Qux3 will be updated, Qux5 will be inserted
table.upsert_rows(values=to_upsert_df,  primary_keys=['Name'])
```

## 4. Changing Table Structure

Adding columns can be done using the methods `table.add_column`.

```python
table.add_column(
    Column(name="Expression", column_type="STRING")
)
table.store()
```

Renaming or modifying a column:

```python
table.columns['Expression'].name = 'Expression2'
table.columns['Expression'].column_type = 'INTEGER'
table.store()
```

Removing a column

```python
table.delete_column(name="Expression2")
table.store()
```

## 5. Deleting rows

Query for the rows you want to delete and call syn.delete on the results:

```python
table.delete_rows(query=f"SELECT * FROM {table.id} WHERE Strand = '+'")
```

## 6. Deleting the whole table
Deleting the schema deletes the whole table and all rows:

```python
table.delete()
```

## References used in this tutorial

- [Project][project-reference-sync]
- [syn.login][synapseclient.Synapse.login]
- [query][synapseclient.query]
