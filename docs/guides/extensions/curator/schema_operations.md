# How to Generate JSONschemas from Curator CSV data models

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

* You have a working [installation](../../tutorials/installation.md) of the Synapse Python Client. You must install the Curator extensions package.
* You have a data model, see this [data model_documentation](../../explanations/curator_data_model.md).

## 1. Initial set up

```python
from synapseclient import Synapse
from synapseclient.extensions.curator import (
    bind_jsonschema,
    generate_jsonschema,
    register_jsonschema,
)

# Path or URL to your data model (CSV or JSONLD format)
# Example: "path/to/my_data_model.csv" or "https://raw.githubusercontent.com/example.csv"
DATA_MODEL_SOURCE = "tests/unit/synapseclient/extensions/schema_files/example.model.csv"
# List of component names/data types to create schemas for, or None for all components/data types
# Example: ["Patient", "Biospecimen"] or None
DATA_TYPE = ["Patient"]
# Directory where JSON Schema files will be saved
OUTPUT_DIRECTORY = "temp"
# Path to a generated JSON Schema file for registration
SCHEMA_PATH = "temp/Patient.json"
# Your Synapse organization name for schema registration
ORGANIZATION_NAME = "my.organization"
# Name for the schema
SCHEMA_NAME = "patient.schema"
# Version number for the schema
SCHEMA_VERSION = "0.0.1"
# Synapse entity ID to bind the schema to (file, folder, etc.)
ENTITY_ID = "syn12345678"
```

To create a JSON Schema you need a data model, and the data types you want to create.
The data model must be in either CSV or JSON-LD form. The data model may be a local path or a URL.
[Data model_documentation](../../explanations/curator_data_model.md).

The data types must exist in your data model. This can be a list of data types, or `None` to create all data types in the data model.

## 2. Create JSON Schemas

Create multiple JSON Schema

```python
# Create JSON Schemas for multiple data types
schemas, file_paths = generate_jsonschema(
    data_model_source=DATA_MODEL_SOURCE,
    output=OUTPUT_DIRECTORY,
    data_types=DATA_TYPE,
    synapse_client=syn,
)
```

The JSONschema looks like [this](https://repo-prod.prod.sagebase.org/repo/v1/schema/type/registered/dpetest-test.schematic.Patient).

By setting the `output` parameter as path to a "temp" directory, the file will be created as "temp/Patient.json".

The `data_types` parameter is a list and can have multiple data types.

## 3. Create every JSON Schema

Create every JSON Schema

```python
# Create JSON Schemas for all data types
schemas, file_paths = generate_jsonschema(
    data_model_source=DATA_MODEL_SOURCE,
    output=OUTPUT_DIRECTORY,
    synapse_client=syn,
)
```

If you don't set a `data_types` parameter a JSON Schema will be created for every data type in the data model.

## 4. Create a JSON Schema with a certain path

Create a JSON Schema

```python
# Specify path for JSON Schema
schemas, file_paths = generate_jsonschema(
    data_model_source=DATA_MODEL_SOURCE,
    data_types=DATA_TYPE,
    output="test.json",
    synapse_client=syn,
)
```

If you have only one data type and set the `output` parameter to a file path(ending in.json), the JSON Schema file will have that path.

## 5. Create a JSON Schema in the current working directory

Create a JSON Schema

```python
# Create JSON Schema in cwd
schemas, file_paths = generate_jsonschema(
    data_model_source=DATA_MODEL_SOURCE,
    data_types=DATA_TYPE,
    synapse_client=syn,
)
```

If you don't set `output` parameter the JSON Schema file will be created in the current working directory.

## 6. Create a JSON Schema using display names

Create a JSON Schema

```python
# Create JSON Schema using display names for both properties names and valid values
schemas, file_paths = generate_jsonschema(
    data_model_source=DATA_MODEL_SOURCE,
    data_types=DATA_TYPE,
    data_model_labels="display_label",
    synapse_client=syn,
)
```

You can have Curator format the property names and valid values in the JSON Schema. This will remove whitespace and special characters.

## 7. Register a JSON Schema to Synapse

Once you've created a JSON Schema file, you can register it to a Synapse organization.

```python
# Register a JSON Schema to Synapse
json_schema = register_jsonschema(
    schema_path=SCHEMA_PATH,
    organization_name=ORGANIZATION_NAME,
    schema_name=SCHEMA_NAME,
    schema_version=SCHEMA_VERSION,
    synapse_client=syn,
)
print(f"Registered schema URI: {json_schema.uri}")
```

The `register_jsonschema` function:
- Takes a path to your generated JSON Schema file
- Registers it with the specified organization in Synapse
- Returns the schema URI and a success message
- You can optionally specify a version (e.g., "0.0.1") or let it auto-generate

## 8. Bind a JSON Schema to a Synapse Entity

After registering a schema, you can bind it to Synapse entities (files, folders, etc.) for metadata validation.

```python
# Bind a JSON Schema to a Synapse entity
result = bind_jsonschema(
    entity_id=ENTITY_ID,
    json_schema_uri=json_schema.uri,
    enable_derived_annotations=True,
    synapse_client=syn,
)
print(f"Successfully bound schema to entity: {result}")
```

The `bind_jsonschema` function:
- Takes a Synapse entity ID (e.g., "syn12345678")
- Binds the registered schema URI to that entity
- Optionally enables derived annotations to auto-populate metadata
- Returns binding details


## Reference
- [JSON Schema Object Definition](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchema.html)
- [JSON Schema Draft 7](https://json-schema.org/draft-07)
- [JSON-Schema.org](https://json-schema.org/)
