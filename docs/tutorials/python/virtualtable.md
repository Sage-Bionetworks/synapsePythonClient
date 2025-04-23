# Virtual Tables

Virtual Tables in Synapse allow you to create queryable views based on existing Synapse tables. 
Unlike Materialized Views, Virtual Tables don't store the results but instead provide a 
view that is evaluated at query time. They're useful for providing different perspectives
of your data without duplicating the storage.

This tutorial will walk you through the basics of working with Virtual Tables
using the Synapse Python client.

## Tutorial Purpose
In this tutorial, you will:

1. Log in, get your project, and create tables with data
2. Create and query a basic Virtual Table
3. Create a Virtual Table with column selection
4. Create a Virtual Table with filtering
5. Create a Virtual Table with ordering
6. Create a Virtual Table with aggregation

## Prerequisites
* This tutorial assumes that you have a Synapse project.
* Pandas must also be installed as shown in the [installation documentation](../installation.md).

## 1. Log in, get your project, and create tables with data

Before creating Virtual Tables, we need to log in to Synapse, retrieve your project,
and create the tables with data that will be used.

You will want to replace `"My uniquely named project about Alzheimer's Disease"` with
the name of your project.

```python
{!docs/tutorials/python/tutorial_scripts/virtualtable.py!lines=3-72}
```

**Note**: Virtual Tables do not support JOIN or UNION operations in the defining SQL query.
If you need to combine data from multiple tables, consider using a
[Materialized View](materializedview.md) instead.

## 2. Create and query a basic Virtual Table

First, we will create a simple Virtual Table that selects all rows from a table and
then query it to retrieve the results.

```python
{!docs/tutorials/python/tutorial_scripts/virtualtable.py!lines=75-97}
```

<details class="example">
  <summary>The result of querying your Virtual Table should look like:</summary>
```
Results from the basic virtual table:
  sample_id patient_id  age    diagnosis
0        S1         P1   70  Alzheimer's
1        S2         P2   65      Healthy
2        S3         P3   72  Alzheimer's
3        S4         P4   68      Healthy
4        S5         P5   75  Alzheimer's
5        S6         P6   80      Healthy
```
</details>

## 3. Create a Virtual Table with column selection

Next, we'll create a Virtual Table that selects only specific columns from the source table.

```python
{!docs/tutorials/python/tutorial_scripts/virtualtable.py!lines=100-129}
```

<details class="example">
  <summary>The result of querying your Virtual Table with column selection should look like:</summary>
```
Results from the virtual table with column selection:
  patient_id  age
0         P1   70
1         P2   65
2         P3   72
3         P4   68
4         P5   75
5         P6   80
```
</details>

## 4. Create a Virtual Table with filtering

We can create a Virtual Table that filters rows from the source table using a WHERE clause.

```python
{!docs/tutorials/python/tutorial_scripts/virtualtable.py!lines=132-161}
```

<details class="example">
  <summary>The result of querying your Virtual Table with filtering should look like:</summary>
```
Results from the virtual table with filtering:
  sample_id patient_id  age    diagnosis
0        S1         P1   70  Alzheimer's
1        S3         P3   72  Alzheimer's
2        S5         P5   75  Alzheimer's
```
</details>

## 5. Create a Virtual Table with ordering

You can also create a Virtual Table that orders the rows from the source table using an ORDER BY clause.

```python
{!docs/tutorials/python/tutorial_scripts/virtualtable.py!lines=164-193}
```

<details class="example">
  <summary>The result of querying your Virtual Table with ordering should look like:</summary>
```
Results from the virtual table with ordering:
  sample_id patient_id  age    diagnosis
0        S6         P6   80      Healthy
1        S5         P5   75  Alzheimer's
2        S3         P3   72  Alzheimer's
3        S1         P1   70  Alzheimer's
4        S4         P4   68      Healthy
5        S2         P2   65      Healthy
```
</details>

## 6. Create a Virtual Table with aggregation

Finally, we can create a Virtual Table that aggregates data using functions like COUNT, along with GROUP BY.

```python
{!docs/tutorials/python/tutorial_scripts/virtualtable.py!lines=196-225}
```

<details class="example">
  <summary>The result of querying your Virtual Table with aggregation should look like:</summary>
```
Results from the virtual table with aggregation:
     diagnosis  patient_count
0  Alzheimer's              3
1      Healthy              3
```
</details>

## Source Code for this Tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/virtualtable.py!}
```
</details>

## References
- [VirtualTable][synapseclient.models.VirtualTable]
- [Column][synapseclient.models.Column]
- [Project][synapseclient.models.Project]
- [syn.login][synapseclient.Synapse.login]
- [query examples](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html)
