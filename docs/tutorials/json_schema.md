In this tutorial, you will learn how to bind a JSON Schema to a folder. You can also bind a JSON Schema to a project, a file, a table, or a view. This tutorial will walk you through each step, from schema creation to validation.

## Tutorial Purpose
By the end of this tutorial, you will:

1. Log in and create a project and folder
2. Create a JSON Schema and an organization
3. Bind the JSON Schema to a folder
4. Add annotations and validate against the schema
5. Attach and validate a file
6. View schema validation statistics and results

## Prerequisites
* You have a working installation of the Synapse Python Client
* Make sure that you have completed the [Project](./project.md) tutorial.
* You are familiar with Synapse concepts: Project, Folder, File


## Step 1: Set up Synapse Python client and retrieve project
```python
{!docs/tutorials/python/tutorial_scripts.json_schema.py!lines=9-20}
```

## Step 2: Take a look at the constants and structure of the JSON schema

For the purpose of this tutorial, an organization named "myUniqueAlzheimersResearchOrgTutorial" has been created, and within it, a schema named clinicalObservations has been registered.

```python
{!docs/tutorials/python/tutorial_scripts.json_schema.py!lines=21-49}
```

Synapse supports a subset of features from json-schema-draft-07. As you can see here, you could use derived annotations to prescribe default annotation values. Please read more about derived annotations [here](https://help.synapse.org/docs/JSON-Schemas.3107291536.html#JSONSchemas-DerivedAnnotations).


## Step 3: Retrieve test organization
Next, create an organization to store the schema:
```python
{!docs/tutorials/python/tutorial_scripts.json_schema.py!lines=50-54}
```

## Step 4: Bind the JSON schema to the folder
After retrieving the organization, you can now bind your json schema to a test folder:
```python
{!docs/tutorials/python/tutorial_scripts.json_schema.py!lines=55-63}
```

## Step 5: Retrieve the Bound Schema
Next, we can bind the JSON Schema to a folder.
```python
{!docs/tutorials/python/tutorial_scripts.json_schema.py!lines=64-67}
```

## Step 6: Add Invalid Annotations to the Folder and Store
Try adding invalid annotations to your folder:
```python
{!docs/tutorials/python/tutorial_scripts.json_schema.py!lines=69-76}
```

## Step 7: Validate Folder Against the Schema
```python
{!docs/tutorials/python/tutorial_scripts.json_schema.py!lines=78-81}
```

## Step 8: Create a File with Invalid Annotations and Upload It
```python
{!docs/tutorials/python/tutorial_scripts.json_schema.py!lines=83-111}
```

## Step 9: View Validation Statistics
```python
{!docs/tutorials/python/tutorial_scripts.json_schema.py!lines=112-115}
```

## Step 10: View Invalid Validation Results
```python
{!docs/tutorials/python/tutorial_scripts.json_schema.py!lines=117-122}
```

## Source Code for this Tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/json_schema.py!}
```
</details>


## Reference
- [syn.findEntityId][synapseclient.Synapse.findEntityId]
- [syn.store][synapseclient.Synapse.store]
- [syn.login][synapseclient.Synapse.login]
- [File][file-reference-sync]
- [Folder][folder-reference-sync]
- [JSONSChema Mixins][json-schema-mixin]
