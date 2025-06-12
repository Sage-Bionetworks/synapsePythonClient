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
* You have a project on Synapse ready for testing
* You are familiar with Synapse concepts: Project, Folder, File
* You are authenticated `(syn = synapseclient.Synapse(); syn.login())`


##  Step 1: Define a JSON Schema
```python
title = "OOP Test Schema"
schema = {
    "$schema": "http://json-schema.org/draft-07/schema#",
    "$id": "https://example.com/schema/ooptest.json",
    "title": title,
    "type": "object",
    "properties": {
        "test_string": {"type": "string"},
        "test_int": {"type": "integer"},
        "test_derived_annos": {
            "description": "Derived annotation property",
            "type": "string",
            "const": "default value",
        },
    },
}
```
Synapse supports a subset of features from json-schema-draft-07. As you can see here, you could use derived annotations to prescribe default annotation values. Please read more about derived annotations [here](https://help.synapse.org/docs/JSON-Schemas.3107291536.html#JSONSchemas-DerivedAnnotations).


##  Step 2: Create JSON Schema Organizations
Next, create an organization to store the schema:

```python
ORG_NAME = "<your organization name>"
js = syn.service("json_schema")

created_org = js.create_organization(ORG_NAME)
print(f"Organization created: {created_org}")
```
If you are interested, you could also see all the organizations that you have access to:
```python
all_org = js.list_organizations()
for org in all_org:
    print(f"Found organization: {org['name']} with id: {org['id']}")
```

## Step 3: Create and Register the JSON Schema
After creating the organization, you can now register your schema under the organization:
```python
SCHEMA_NAME = "test"
VERSION = "0.0.1"
test_org = js.JsonSchemaOrganization(ORG_NAME)
created_schema = test_org.create_json_schema(schema, SCHEMA_NAME, VERSION)
print(created_schema)
```

## Step 4: Bind the JSON schema to the folder
Next, we can bind the JSON Schema to a folder. We can then add a folder to our test project:
```python
PROJECT_ID="<your test project id>"
test_folder = Folder(name="test_folder", parent_id=PROJECT_ID).store()
```

Then, bind the JSON schema to the folder:
```python
bound_schema = test_folder.bind_json_schema_to_entity(
    json_schema_uri=created_schema.uri, enable_derived_annos=True
)
print("JSON schema was bound successfully:")
pprint(vars(bound_schema.json_schema_version_info))
```

Step 5: Retrieve the Bound Schema
You can retrieve your bound schema:
```python
schema = test_folder.get_json_schema_from_entity()
print("Retrieved bound schema:")
pprint(vars(schema))
```

Step 7: Add Invalid Annotations to the Folder and Store
Try adding invalid annotations to your folder:
```python
test_folder.annotations = {
    "test_string": "example_value",
    "test_int": "invalid str",
}
test_folder.store()
time.sleep(2)
```

Step 8: Validate Folder Against the Schema
```python
validation_results = test_folder.validate_entity_with_json_schema()
print("Validation Results:")
pprint(vars(validation_results))
```

Step 9: Create a File with Invalid Annotations and Upload It
```python
os.makedirs(os.path.expanduser("~/temp/testJSONSchemaFiles"), exist_ok=True)
path_to_file = os.path.join(os.path.expanduser("~/temp/testJSONSchemaFiles"), "test_file.txt")
create_random_file(path_to_file)

annotations = {"test_string": "child_value", "test_int": "invalid child str"}
child_file = File(path=path_to_file, parent_id=test_folder.id, annotations=annotations).store()
time.sleep(2) # ensure the job can be finished
```


Step 10: View Validation Statistics
```python
validation_statistics = test_folder.get_json_schema_validation_statistics()
print("Validation Statistics:")
pprint(vars(validation_statistics))
```

Step 11: View Invalid Validation Results
```python
invalid_validation = test_folder.get_invalid_json_schema_validation()
for child in invalid_validation:
    print("Invalid child validation:")
    pprint(vars(child))
```

## Source Code for this Tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/json_schema.py!}
```
</details>


## Reference
- [File][file-reference-sync]
- [Folder][folder-reference-sync]
- [syn.login][synapseclient.Synapse.login]
- [JSONSChema][json-schema-reference-sync]
