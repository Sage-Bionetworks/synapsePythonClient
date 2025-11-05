JSON Schema is a tool used to validate data. In Synapse, JSON Schemas can be used to validate the metadata applied to an entity such as project, file, folder, table, or view, including the [annotations](https://help.synapse.org/docs/Annotating-Data-With-Metadata.2667708522.html) applied to it. To learn more about JSON Schemas, check out [JSON-Schema.org](https://json-schema.org/).

Synapse supports a subset of features from [json-schema-draft-07](https://json-schema.org/draft-07). To see the list of features currently supported, see the [JSON Schema object definition](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchema.html) from Synapse's REST API Documentation.

In this tutorial, you will learn how to create these JSON Schema using an existing data-model.

## Tutorial Purpose
You will create a JSON schema using your data model.

## Prerequisites
* You have a working [installation](../installation.md) of the Synapse Python Client.


## 1. Set up your variables

```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=10-14}
```

To create a JSON Schema you need a data-model, and the data-type you want to create. The data-model must be in either CSV or JSON-LD form. See [here](https://sagebionetworks.jira.com/wiki/spaces/SCHEM/pages/2473623559/The+Data+Model+Schema) for instructions on how to crate a data-model.The data-type must exist in your data-model.

## 2. Create the JSON Schema
Try creating the JSON Schema
```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=16-24}
```
You should see the JSON Schema for the datatype you selected printed.


## Source Code for this Tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!}
```
</details>


## Reference
- [Data models](https://sagebionetworks.jira.com/wiki/spaces/SCHEM/pages/2473623559/The+Data+Model+Schema)
- [JSON Schema Object Definition](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchema.html)
- [JSON Schema Draft 7](https://json-schema.org/draft-07)
- [JSON-Schema.org](https://json-schema.org./)
