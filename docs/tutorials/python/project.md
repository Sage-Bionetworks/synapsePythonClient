# Projects in Synapse
Projects in Synapse are “containers” that group relevant content and people together. All data must be uploaded into a project. Projects can be private so only you can see the contents, they can be shared with your collaborators, or they can be made public so anyone on the web can view your research.

[Read more about Projects](../../explanations/domain_models_of_synapse.md#projects)


[Recommendations for Structuring Your Synapse Project](../../explanations/structuring_your_project.md)

## Tutorial Purpose
In this tutorial you will:

1. Create a new project
1. Print stored attributes about your project
1. Get an existing project

## Prerequisites
* Make sure that you have completed the
[Installation](../installation.md) and [Authentication](../authentication.md) setup.

## 1. Create a new project

```python
import synapseclient
from synapseclient.models import Project

syn = synapseclient.login()

# Project names must be globally unique
project = Project(name="My uniquely named project about Alzheimer's Disease")
project.store()
```

Now that you have created your project you are able to inspect the project in the [synapse web UI](https://www.synapse.org/#!Profile:v/projects/created_by_me).



## 2. Print stored attributes about your project
```python
my_synapse_project_id = project.id
print(f"My project ID is: {my_synapse_project_id}")

project_creation_date = project.created_on
print(f"I created my project on: {project_creation_date}")

user_id_who_created_project = project.created_by
print(f"The ID of the user that created my project is: {user_id_who_created_project}")

project_modified_on_date = project.modified_on
print(f"My project was last modified on: {project_modified_on_date}")
```
<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
My project ID is: syn12345678
I created my project on: 2000-01-01T12:00:00.001Z
The ID of the user that created my project is: 1234567
My project was last modified on: 2000-01-01T12:00:00.001Z
```
</details>


Find all of the available attributes about your project in the
[API Reference section of the documentation][synapseclient.entity.Project].


## 3. Get an existing project
Each Project only needs to be created once. Since you've already created it you can
access it again by retrieving the synapse ID of the project and retrieving
the existing project object.
```python
from synapseclient.models import Project

my_project_object = Project(id=my_project_id).get()
print(f"I just got my project: {my_project_object.name}, id: {my_project_id}")
```

<details class="example">
  <summary>The result will look like:</summary>

```
I just got my project: My uniquely named project about Alzheimer's Disease, id: syn12345678
```
</details>

## Source code for this tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/project.py!}
```
</details>

## References used in this tutorial

- [Project][project-reference-sync]
- [syn.login][synapseclient.Synapse.login]
- [syn.store][synapseclient.Synapse.store]
- [syn.get][synapseclient.Synapse.get]
- [syn.findEntityId][synapseclient.Synapse.findEntityId]
