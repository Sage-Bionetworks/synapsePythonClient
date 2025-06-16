JSON Schema is a tool used to validate data. In Synapse, JSON Schemas can be used to validate the metadata applied to a project, file, folder, table, or view, including the [annotations](https://help.synapse.org/docs/Annotating-Data-With-Metadata.2667708522.html) applied to it. To learn more about JSON Schemas, check out [JSON-Schema.org](https://json-schema.org./).

Synapse supports a subset of features from [json-schema-draft-07](https://json-schema.org/draft-07). To see the list of features currently supported, see the [JSON Schema object definition](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchema.html) from Synapse's REST API Documentation

In this tutorial, you will learn how to bind a JSON Schema to a folder, which allows you to enforce data standards on the folder and all its children. You can also bind a JSON Schema to a project, file, table, or view. This tutorial will walk you through each step, from binding a schema to validation.

## Tutorial Purpose
By the end of this tutorial, you will:

1. Log in and create a project and folder
2. Retrieve the JSON Schema and the organization that have already been created for the purpose of this tutorial
3. Bind the JSON Schema to a folder
4. Add annotations and validate against the schema
5. Attach and validate a file
6. View schema validation statistics and results

## Prerequisites
* You have a working [installation](../installation.md)of the Synapse Python Client.
* Make sure that you have completed the [Project](./project.md) tutorial, which covers creating and managing projects in Synapse. This is a prerequisite because you need a project to organize and store the folder used in this tutorial.
* You are familiar with Synapse concepts: [Project](./project.md), [Folder](./folder.md), [File](./file.md).
* You are familiar with [adding annotations](./annotation.md) to synapse entity.


## Step 1: Set up Synapse Python client and retrieve project

```python
{!docs/tutorials/python/tutorial_scripts/json_schema.py!lines=1-20}
```

## Step 2: Take a look at the constants and structure of the JSON schema

For the purpose of this tutorial, an organization named "myUniqueAlzheimersResearchOrgTutorial" has been created, and within it, a schema named clinicalObservations has been registered.

```python
{!docs/tutorials/python/tutorial_scripts/json_schema.py!lines=23-49}
```

Derived annotations allow you to define default values for annotations based on schema rules, ensuring consistency and reducing manual input errors. As you can see here, you could use derived annotations to prescribe default annotation values. Please read more about derived annotations [here](https://help.synapse.org/docs/JSON-Schemas.3107291536.html#JSONSchemas-DerivedAnnotations).


## Step 3: Retrieve test organization
Next, retrieve an organization:
```python
{!docs/tutorials/python/tutorial_scripts/json_schema.py!lines=51-54}
```

## Step 4: Bind the JSON schema to the folder
After retrieving the organization, you can now bind your json schema to a test folder. When you bind a JSON Schema to a project or folder, then all items inside of the project or folder will inherit the schema binding, unless the item has a schema bound to itself.

When you bind the schema, you may also include the boolean property `enable_derived_annos` to have Synapse automatically calculate derived annotations based on the schema:

```python
{!docs/tutorials/python/tutorial_scripts/json_schema.py!lines=56-63}
```

## Step 5: Retrieve the Bound Schema
Next, we can retrieve the bound schema:
```python
{!docs/tutorials/python/tutorial_scripts/json_schema.py!lines=65-67}
```

## Step 6: Add Invalid Annotations to the Folder and Store
Try adding invalid annotations to your folder: This step and the step below demonstrate how the system handles invalid annotations and how the schema validation process works.
```python
{!docs/tutorials/python/tutorial_scripts/json_schema.py!lines=70-76}
```

In the synapse web UI, you could also see your invalid annotations being marked by a yellow label similar to this:

![json_schema](./tutorial_screenshots/jsonschema_folder.png)


## Step 7: Validate Folder Against the Schema
Try validating the folder. You should be able to see messages related to invalid annotations.
```python
{!docs/tutorials/python/tutorial_scripts/json_schema.py!lines=79-81}
```

## Step 8: Create a File with Invalid Annotations and Upload It
Try creating a test file locally and store the file in the folder that we created earlier. Then, try adding invalid annotations to a file. This step demonstrates how the files inside a folder also inherit the schema from the parent entity.
```python
{!docs/tutorials/python/tutorial_scripts/json_schema.py!lines=84-111}
```

In the synapse web UI, you could also see your invalid annotations being marked by a yellow label similar to this:

![jsonschema](./tutorial_screenshots/jsonschema_file.png)

## Step 9: View Validation Statistics
This step is only relevant for container entities, such as a folder or a project. Using this function could provide you with information such as the number of children with invalid annotations inside a container.
```python
{!docs/tutorials/python/tutorial_scripts/json_schema.py!lines=113-115}
```

## Step 10: View Invalid Validation Results
This step is also only relevant for container entities, such as a folder or a project. Using this function allows you to see more detailed results of all the children inside a container, which includes all validation messages and validation exception details.
```python
{!docs/tutorials/python/tutorial_scripts/json_schema.py!lines=118-122}
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
- [Annotations][synapseclient.Annotations]
- [JSONSChema Mixins][json-schema-mixin]
- [Annotating Data With Metadata](https://help.synapse.org/docs/Annotating-Data-With-Metadata.2667708522.html)
- [JSON Schema Object Definition](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/schema/JsonSchema.html)
- [JSON Schema Draft 7](https://json-schema.org/draft-07)
- [JSON-Schema.org](https://json-schema.org./)
