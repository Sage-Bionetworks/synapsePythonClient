JSON Schema is a tool used to validate data. In Synapse, JSON Schemas can be used to validate the metadata applied to an entity such as project, file, folder, table, or view, including the [annotations](https://help.synapse.org/docs/Annotating-Data-With-Metadata.2667708522.html) applied to it. To learn more about JSON Schemas, check out [JSON-Schema.org](https://json-schema.org/).

Synapse supports a subset of features from [json-schema-draft-07](https://json-schema.org/draft-07). To see the list of features currently supported, see the [JSON Schema object definition](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchema.html) from Synapse's REST API Documentation.

In this tutorial, you will learn how to create these JSON Schema using an existing data model.

## Tutorial Purpose
You will create a JSON schema using your data model.

## Prerequisites
* You have a working [installation](../installation.md) of the Synapse Python Client.
* You have a data model, see this [example data model](https://github.com/Sage-Bionetworks/schematic/blob/develop/tests/data/example.model.column_type_component.csv).

## 1. Imports

```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=1-2}
```

## 2. Set up your variables

```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=4-10}
```

To create a JSON Schema you need a data model, and the data types you want to create.
The data model must be in either CSV or JSON-LD form. The data model may be a local path or a URL.
[Example data model](https://github.com/Sage-Bionetworks/schematic/blob/develop/tests/data/example.model.column_type_component.csv).

The data types must exist in your data model. This can be a list of data types, or `None` to create all data types in the data model.

## 3. Log into Synapse
```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=12-13}
```


## 4. Create the JSON Schema
Create the JSON Schema(s)
```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=15-23}
```
You should see the first JSON Schema for the datatype(s) you selected printed.
It will look like [this schema](https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/dpetest-test.schematic.Patient).


## Source Code for this Tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!}
```
</details>


## Reference
- [JSON Schema Object Definition](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchema.html)
- [JSON Schema Draft 7](https://json-schema.org/draft-07)
- [JSON-Schema.org](https://json-schema.org/)
