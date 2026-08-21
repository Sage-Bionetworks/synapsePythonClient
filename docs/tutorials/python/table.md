# Table

Tables in Synapse allow you to upload a CSV and/or TSV into a querable interface that follow a user-defined schema and queried using a SQL-like syntax.


## Tutorial Purpose
In this tutorial you will:

1. Create a Table and load it with data
1. Query your Table
1. Update your Table
1. Change Table structure
1. Deleting Table rows and Tables
1. Work with dates and datetimes


## Prerequisites
* Make sure that you have completed the [Project](./project.md) tutorial.
* The tutorial assumes you have data in a dataframe and/or CSV you want to query in a SQL like interface in Synapse

## 1. Creating a table

### Initial setup

```python
import synapseclient
from synapseclient.models import Column, Project, query, SchemaStorageStrategy, Table
from datetime import date, datetime, timezone
import pandas as pd

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

### Loading Data

* Creating a table from a CSV without specifying initial columns

    ```python
    table = Table(
        name="My Favorite Genes",
        parent_id=project.id,
    )
    table = table.store()
    table.store_rows(values="/path/to/genes.csv", schema_storage_strategy=SchemaStorageStrategy.INFER_FROM_DATA)
    ```

* Creating a table from a CSV with specified columns

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

* Creating a table from a Pandas [DataFrame](http://pandas.pydata.org/pandas-docs/stable/api.html#dataframe) - [Pandas](http://pandas.pydata.org/) is a popular library for working with tabular data.

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

The query language is quite similar to SQL select statements, except that joins are not supported. The documentation for the Synapse API has lots of [query examples](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html). Let's try to query:

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

Appending new rows is fairly straightforward. To continue the previous example, we might add some new genes from another file.
Lets take this new data in `more_genes.csv`

```csv
Name,Chromosome,Start,End,Strand,TranscriptionFactor
Qux1,4,201001,202001,+,False
Qux2,4,203001,204001,+,False
```

* Using a CSV

    ```python
    table = Table(
        name="My Favorite Genes",
        parent_id=project.id,
    ).get()
    table = table.store_rows(values="/path/to/more_genes.csv")
    ```

* Using Pandas

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

* Updating the existing table - query the existing table and update the data. Minimizing changesets to contain only rows that actually change will make processing faster.

    ```python
    results_df = query(f"select * from {table.id} where Chromosome='1'")
    results_df['Name'] = ['rzing', 'zing1', 'zing2', 'zing3']
    table.store_rows(values=results_df)
    ```

* Upserting rows (update or insert) - If your data has a primary key, you can use the upsert functionality to update existing rows or insert new rows based on this primary key.  This way you won't have to query for the existing data to update your table. This function does not do deletions.

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

* Adding columns

    ```python
    table.add_column(
        Column(name="Expression", column_type="STRING")
    )
    table.store()
    ```

* Renaming or modifying a column:

    ```python
    table.columns['Expression'].name = 'Expression2'
    table.columns['Expression'].column_type = 'INTEGER'
    table.store()
    ```

* Removing a column

    ```python
    table.delete_column(name="Expression2")
    table.store()
    ```

## 5. Deleting Table rows & Tables

* Deleting specific rows - Query for the rows you want to delete and call delete_rows on the results

    ```python
    table.delete_rows(query=f"SELECT * FROM {table.id} WHERE Strand = '+'")
    ```

* Or deleting rows based on a dataframe, where the ROW_ID and ROW_VERSION columns specify the rows to be deleted from the table. In this example, rows 2 and 3 are deleted. See this document that describes the expected columns of the dataframe: <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/table/Row.html>. Note: The ROW_VERSION begins at 1 upon row creation and increases by one with every subsequent update.

    ```python
    table.delete_rows(df = pd.DataFrame({"ROW_ID": [2, 3], "ROW_VERSION": [1, 1]}))
    ```

* Deleting the whole table will deletes the whole table and all rows

    ```python
    table.delete()
    ```

## 6. Working with dates and datetimes

Synapse `DATE` columns store an exact moment in time, represented as the number
of milliseconds since `1970-01-01 00:00:00` UTC. Before upload, the client
converts the datetime values in your data to that representation, and how a
value is converted depends on whether it carries timezone information. The
examples below create a small table with a `DATE` column and walk through the
different ways datetime data can be stored and queried.

```python
table = Table(
    name="Sample Collection Dates",
    parent_id=project.id,
    columns=[
        Column(name="sample_id", column_type="STRING", maximum_size=20),
        Column(name="collected_on", column_type="DATE"),
    ],
)
table = table.store()
```
### Storing timezone-aware datetimes (recommended)

A timezone-aware datetime is converted to UTC exactly — the stored value does
not depend on the timezone settings of the machine performing the upload or on
the date the upload is run. Localizing with a zone name like
`"America/Los_Angeles"` interprets each value using the UTC offset in effect on
that value's own date, so values on either side of a daylight saving switch are
both stored correctly.

```python
df = pd.DataFrame(
{
    "sample_id": ["S1", "S2"],
    "collected_on": [
        datetime(2017, 2, 14, 11, 23),  # winter in Los Angeles (PST, UTC-8)
        datetime(2018, 10, 1, 9, 30),  # summer in Los Angeles (PDT, UTC-7)
    ],
}
)

# Localize the values so each one carries its own timezone information.
# tz_localize looks up the UTC offset in effect on each value's own date:
# UTC-8 for the February value, UTC-7 for the October value.
df["collected_on"] = df["collected_on"].dt.tz_localize("America/Los_Angeles")

table.store_rows(values=df)

```
<details class="example">
  <summary>The collected_on column will contain timezone-aware datetimes in UTC:</summary>
```
  sample_id               collected_on
0        S1  2017-02-14 19:23:00+00:00
1        S2  2018-10-01 16:30:00+00:00
```

</details>

### Storing naive datetimes
A naive datetime (one without `tzinfo`) is assumed to be in the local timezone
of the machine **at the time of upload**, and that single UTC offset is applied
to every value in the upload.

```python
naive_df = pd.DataFrame(
    {
        "sample_id": ["S3"],
        "collected_on": [datetime(2017, 2, 14, 11, 23)],  # no timezone info; for this example, the local timezone is UTC-7

    }
)

table.store_rows(values=naive_df)
```

<details class="example">
  <summary>The collected_on column will contain timezone-aware datetimes in UTC:</summary>
```
  sample_id               collected_on
0        S1  2017-02-14 19:23:00+00:00
1        S2  2018-10-01 16:30:00+00:00
2        S3  2017-02-14 18:23:00+00:00
```

</details>


This is convenient when your data and your machine share a timezone and daylight
saving period, but any value whose date falls in a different daylight saving
period than the upload date is stored shifted by one hour. Here, `2017-02-14
11:23` is a winter (PST, UTC-8) wall-clock time; uploading it from a machine
currently on summer time (PDT, UTC-7) stores `18:23` UTC instead of the correct
`19:23` UTC. Midnight values deserve extra care: the same one-hour shift can
move a naive midnight to 11 PM of the previous day, changing the calendar date
the value displays as. To avoid all of this, localize your data first as shown
in the previous example.

### Storing dates without a time of day

Plain `datetime.date` objects (as opposed to datetimes) are treated as
`midnight` of that date and converted the same way naive datetimes are: using
the local timezone of the machine at the time of upload, with the same
daylight-saving-shift risk described above.

```python
date_df = pd.DataFrame(
    {
        "sample_id": ["S4"],
        "collected_on": [date(2017, 2, 14)],  # a date, not a datetime
    }
)

table.store_rows(values=date_df)

query_results = query(
f"SELECT * FROM {table.id} ORDER BY collected_on",
include_row_id_and_row_version=False,
convert_to_datetime=True,
)
print(query_results)
```
<details class="example">
  <summary>The collected_on column will contain timezone-aware datetimes in UTC:</summary>
```
  sample_id               collected_on
0        S1  2017-02-14 19:23:00+00:00
1        S2  2018-10-01 16:30:00+00:00
2        S4  2017-02-14 07:00:00+00:00
3        S3  2017-02-14 18:23:00+00:00
```

</details>

### Loading date columns from a CSV

When storing rows from a CSV file, use `date_columns` and `date_format` to tell
the client which columns hold formatted date strings and how to parse them.
Including the UTC offset in the strings (parsed by `%z`) keeps the values
timezone-aware, with the same benefits as localizing a DataFrame column.

```python
csv_content = (
    "sample_id,collected_on\n"
    "S5,2017-02-14 11:23 -0800\n"
    "S6,2018-10-01 09:30 -0700\n"
)
with open("collection_dates.csv", "w") as f:
    f.write(csv_content)

table.store_rows(
    values="collection_dates.csv",
    date_columns=["collected_on"],
    date_format="%Y-%m-%d %H:%M %z",
)
```

### Querying datetime data back

Pass `convert_to_datetime=True` to `query` to get `DATE` columns back as
timezone-aware datetimes in UTC instead of raw epoch-millisecond integers.

```python
results = query(
    f"SELECT * FROM {table.id} ORDER BY collected_on",
    include_row_id_and_row_version=False,
    convert_to_datetime=True,
)
print(results)
```

<details class="example">
  <summary>The collected_on column will contain timezone-aware datetimes in UTC:</summary>
```
  sample_id               collected_on
0        S1  2017-02-14 19:23:00+00:00
1        S2  2018-10-01 16:30:00+00:00
2        S3  2017-02-14 18:23:00+00:00
3        S4  2017-02-14 07:00:00+00:00
4        S5  2017-02-14 19:23:00+00:00
5        S6  2018-10-01 16:30:00+00:00
```
</details>

```python
# DATE columns are returned as timezone-aware datetimes in UTC. Use tz_convert to view them on another wall clock:
results["collected_on"] = results["collected_on"].dt.tz_convert("America/Los_Angeles")
print(results)
```

<details class="example">
  <summary>The collected_on column now displays timezone-aware datetimes converted to America/Los_Angeles time:</summary>
```
  sample_id               collected_on
0        S1  2017-02-14 19:23:00+00:00
1        S2  2018-10-01 16:30:00+00:00
2        S3  2017-02-14 18:23:00+00:00
3        S4  2017-02-14 07:00:00+00:00
4        S5  2017-02-14 19:23:00+00:00
5        S6  2018-10-01 16:30:00+00:00
```

Note how the timezone-aware rows (`S1`, `S2`, `S5`, `S6`) round-trip back to the
exact wall-clock times they were created with, while the naive datetime row `S3`
and date row `S4` were converted using that same upload-time PDT offset. For
`S4`, that offset is applied to midnight because dates have no time of day.
</details>

To filter on a `DATE` column in a query, compare it against epoch milliseconds:

```python
# In a WHERE clause, compare a DATE column against epoch milliseconds:
# Example: filter for rows where 'collected_on' is on or after January 1, 2018
cutoff = int(datetime(2018, 1, 1, tzinfo=timezone.utc).timestamp() * 1000)
recent = query(
    f"SELECT sample_id FROM {table.id} WHERE collected_on >= {cutoff}",
    include_row_id_and_row_version=False,
)
print(recent)
```

<details class="example">
  <summary>The printed results will look like:</summary>
```
  sample_id
0        S2
1        S6
```
</details>

For the full details of how datetime values are interpreted during upload, see
the [store_rows][synapseclient.models.Table.store_rows] reference documentation.

## References used in this tutorial

- [Project][project-reference-sync]
- [syn.login][synapseclient.Synapse.login]
- [Table][table-reference-sync]
- [Column][column-reference-sync]
