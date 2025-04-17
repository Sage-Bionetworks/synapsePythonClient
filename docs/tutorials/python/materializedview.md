# Materialized Views

Materialized Views in Synapse allow you to create queryable views that store the
results of a Synapse SQL statement. These views are useful for combining, filtering, or
transforming data from multiple tables into a single, queryable entity.

This tutorial will walk you through the basics of working with Materialized Views
using the Synapse Python client.

## Tutorial Purpose
In this tutorial, you will:

1. Log in, get your project, and create tables with data
2. Create and query a Materialized View
3. Create and query a Materialized View with a JOIN clause
4. Create and query a Materialized View with a LEFT JOIN clause
5. Create and query a Materialized View with a RIGHT JOIN clause
6. Create and query a Materialized View with a UNION clause

## Prerequisites
* This tutorial assumes that you have a Synapse project.
* Pandas must also be installed as shown in the [installation documentation](../installation.md).

## 1. Log in, get your project, and create tables with data

Before creating Materialized Views, we need to log in to Synapse, retrieve your project,
and create the tables with data that will be used in the views.

You will want to replace `"My uniquely named project about Alzheimer's Disease"` with
the name of your project.

```python
{!docs/tutorials/python/tutorial_scripts/materializedview.py!lines=3-72}
```

## 2. Create and query a Materialized View

First, we will create a simple Materialized View that selects all rows from a table and
then query it to retrieve the results.

```python
{!docs/tutorials/python/tutorial_scripts/materializedview.py!lines=75-97}
```

<details class="example">
  <summary>The result of querying your Materialized View should look like:</summary>
```
Results from the materialized view:
  sample_id patient_id  age    diagnosis
0        S1         P1   70  Alzheimer's
1        S2         P2   65      Healthy
2        S3         P3   72  Alzheimer's
3        S4         P4   68      Healthy
4        S5         P5   75  Alzheimer's
5        S6         P6   80      Healthy
```
</details>

## 3. Create and query a Materialized View with a JOIN clause

Next, we will create a Materialized View that combines data from two tables using a JOIN
clause and then query it to retrieve the results.

```python
{!docs/tutorials/python/tutorial_scripts/materializedview.py!lines=100-130}
```

<details class="example">
  <summary>The result of querying your Materialized View with a JOIN clause should look
  like:</summary>
```
Results from the materialized view with JOIN:
  sample_id patient_id  age    diagnosis   gene  expression_level
0        S1         P1   70  Alzheimer's   APOE               2.5
1        S2         P2   65      Healthy    APP               1.8
2        S3         P3   72  Alzheimer's  PSEN1               3.2
3        S4         P4   68      Healthy   MAPT               2.1
4        S5         P5   75  Alzheimer's    APP               3.5
```
</details>

## 4. Create and query a Materialized View with a LEFT JOIN clause

We can also create a Materialized View that includes all rows from one table and matches
rows from another table using a LEFT JOIN clause and then query it to retrieve the
results.

```python
{!docs/tutorials/python/tutorial_scripts/materializedview.py!lines=133-163}
```

<details class="example">
  <summary>The result of querying your Materialized View with a LEFT JOIN clause should
  look like:</summary>
```
Results from the materialized view with LEFT JOIN:
  sample_id patient_id  age    diagnosis   gene  expression_level
0        S1         P1   70  Alzheimer's   APOE               2.5
1        S2         P2   65      Healthy    APP               1.8
2        S3         P3   72  Alzheimer's  PSEN1               3.2
3        S4         P4   68      Healthy   MAPT               2.1
4        S5         P5   75  Alzheimer's    APP               3.5
5        S6         P6   80      Healthy    NaN               NaN
```
</details>

## 5. Create and query a Materialized View with a RIGHT JOIN clause

Similarly, we can create a Materialized View that includes all rows from one table and
matches rows from another table using a RIGHT JOIN clause and then query it to retrieve
the results.

```python
{!docs/tutorials/python/tutorial_scripts/materializedview.py!lines=166-196}
```

<details class="example">
  <summary>The result of querying your Materialized View with a RIGHT JOIN clause should
  look like:</summary>
```
Results from the materialized view with RIGHT JOIN:
  sample_id patient_id   age    diagnosis   gene  expression_level
0        S1         P1  70.0  Alzheimer's   APOE               2.5
1        S2         P2  65.0      Healthy    APP               1.8
2        S3         P3  72.0  Alzheimer's  PSEN1               3.2
3        S4         P4  68.0      Healthy   MAPT               2.1
4        S5         P5  75.0  Alzheimer's    APP               3.5
5        S7        NaN   NaN          NaN  PSEN2               1.9
```
</details>

## 6. Create and query a Materialized View with a UNION clause

Finally, we can create a Materialized View that combines rows from two tables using a
UNION clause and then query it to retrieve the results.

```python
{!docs/tutorials/python/tutorial_scripts/materializedview.py!lines=199-229}
```

<details class="example">
  <summary>The result of querying your Materialized View with a UNION clause should look
  like:</summary>
```
Results from the materialized view with UNION:
  sample_id
0        S1
1        S2
2        S3
3        S4
4        S5
5        S6
6        S7
```
</details>

## Source Code for this Tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/materializedview.py!}
```
</details>

## References
- [MaterializedView][synapseclient.models.MaterializedView]
- [Column][synapseclient.models.Column]
- [Project][synapseclient.models.Project]
- [syn.login][synapseclient.Synapse.login]
- [query examples](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html)
