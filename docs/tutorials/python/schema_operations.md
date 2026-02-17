JSON Schema is a tool used to validate data. In Synapse, JSON Schemas can be used to validate the metadata applied to an entity such as project, file, folder, table, or view, including the [annotations](https://help.synapse.org/docs/Annotating-Data-With-Metadata.2667708522.html) applied to it. To learn more about JSON Schemas, check out [JSON-Schema.org](https://json-schema.org/).

Synapse supports a subset of features from [json-schema-draft-07](https://json-schema.org/draft-07). To see the list of features currently supported, see the [JSON Schema object definition](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchema.html) from Synapse's REST API Documentation.

In this tutorial, you will learn how to create, register, and bind JSON Schemas using an existing data model.

## Tutorial Purpose

You will learn the complete JSON Schema workflow:
1. **Generate** JSON schemas from your data model
2. **Register** schemas to a Synapse organization
3. **Bind** schemas to Synapse entities for metadata validation

This tutorial uses the Python client as a library. To use the CLI tool, see the [command line documentation](../command_line_client.md).

## Prerequisites

* You have a working [installation](../installation.md) of the Synapse Python Client.
* You have a data model, see this [data model_documentation](../../explanations/curator_data_model.md).

## 1. Initial set up

```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=1-18}
```

To create a JSON Schema you need a data model, and the data types you want to create.
The data model must be in either CSV or JSON-LD form. The data model may be a local path or a URL.
[Data model_documentation](../../explanations/curator_data_model.md).

The data types must exist in your data model. This can be a list of data types, or `None` to create all data types in the data model.

## 2. Create a JSON Schema

Create a JSON Schema

```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=20-27}
```

You should see the first JSON Schema for the datatype you selected printed.
It will look like [this schema](https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/dpetest-test.schematic.Patient).
By setting the `output` parameter as path to a "temp" directory, the file will be created as "temp/Patient.json".

## 3. Create multiple JSON Schema

Create multiple JSON Schema

```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=30-36}
```

The `data_types` parameter is a list and can have multiple data types.

## 4. Create every JSON Schema

Create every JSON Schema

```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=38-43}
```

If you don't set a `data_types` parameter a JSON Schema will be created for every data type in the data model.

## 5. Create a JSON Schema with a certain path

Create a JSON Schema

```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=45-51}
```

If you have only one data type and set the `output` parameter to a file path(ending in.json), the JSON Schema file will have that path.

## 6. Create a JSON Schema in the current working directory

Create a JSON Schema

```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=53-58}
```

If you don't set `output` parameter the JSON Schema file will be created in the current working directory.

## 7. Create a JSON Schema using display names

Create a JSON Schema

```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=60-66}
```

You can have Curator format the property names and valid values in the JSON Schema. This will remove whitespace and special characters.

## 8. Register a JSON Schema to Synapse

Once you've created a JSON Schema file, you can register it to a Synapse organization.

```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=68-76}
```

The `register_jsonschema` function:
- Takes a path to your generated JSON Schema file
- Registers it with the specified organization in Synapse
- Returns the schema URI and a success message
- You can optionally specify a version (e.g., "0.0.1") or let it auto-generate

## 9. Bind a JSON Schema to a Synapse Entity

After registering a schema, you can bind it to Synapse entities (files, folders, etc.) for metadata validation.

```python
{!docs/tutorials/python/tutorial_scripts/schema_operations.py!lines=78-85}
```

The `bind_jsonschema` function:
- Takes a Synapse entity ID (e.g., "syn12345678")
- Binds the registered schema URI to that entity
- Optionally enables derived annotations to auto-populate metadata
- Returns binding details

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
