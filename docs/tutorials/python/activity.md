# Activity/Provenance

Provenance is a concept describing the origin of something. In Synapse, it is used to describe the connections between the workflow steps used to create a particular file or set of results. Data analysis often involves multiple steps to go from a raw data file to a finished analysis. Synapse's provenance tools allow users to keep track of each step involved in an analysis and share those steps with other users.

The model Synapse uses for provenance is based on the [W3C provenance spec](https://www.w3.org/TR/prov-n/) where items are derived from an activity which has components that were **used** and components that were **executed**. Think of the **used** items as input files and **executed** items as software or code. Both **used** and **executed** items can reside in Synapse or in URLs such as a link to a GitHub commit or a link to a specific version of a software tool.


## Tutorial Purpose
In this tutorial you will:

1. Add a new Activity to your File
1. Add a new Activity to a specific version of your File
1. Print stored activities on your File
1. Delete an activity

## Prerequisites
- In order to follow this tutorial you will need to have a [Project](./project.md) created with a Folder named `biospecimen_experiment_1` containing at least one [File](./file.md). You will also need the Synapse ID of that file (e.g. `synNNNNN`).

## 1. Add a new Activity to your File

#### First retrieve the project, folder, and file we want to track provenance for

```python
--8<-- "docs/tutorials/python/tutorial_scripts/activity.py:retrieve_project_folder_file"
```

#### Create an Activity and attach it to the file

An `Activity` captures what was **used** (input data and reference URLs) and **executed** (code and software) to produce a file. Here we record a QC pipeline run on the biospecimen data:

```python
--8<-- "docs/tutorials/python/tutorial_scripts/activity.py:create_activity"
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
Stored file: fileA.txt (version 1) with activity: Quality Control Analysis
```
</details>


## 2. Add a new Activity to a specific version of your File

Each time you store an updated file, Synapse creates a new version. You can associate a distinct activity with each version to capture the full history of how the data evolved. Here we record a downstream analysis step that used the QC-passed data from version 1:

```python
--8<-- "docs/tutorials/python/tutorial_scripts/activity.py:add_activity_to_version"
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
Stored activity 'Downstream Analysis' on file fileA.txt (version 2)
```
</details>


## 3. Print stored activities on your File

Use `Activity.from_parent()` to retrieve the provenance for any version of a file. Pass a `parent_version_number` to retrieve the activity for a specific older version:

```python
--8<-- "docs/tutorials/python/tutorial_scripts/activity.py:print_activities"
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
Activity on latest version (v1):
  Name: Downstream Analysis
  Description: Downstream analysis of QC-passed biospecimen samples.
  Used: UsedURL(name='Seurat v5.0.0', url='https://github.com/satijalab/seurat/releases/tag/v5.0.0')
  Used: UsedEntity(target_id='syn12345678', target_version_number=1)
  Executed: UsedURL(name='Downstream Analysis Script', url='https://github.com/Sage-Bionetworks/analysis-scripts/blob/v1.0/downstream_analysis.py')

Activity on version 1:
  Name: Quality Control Analysis
  Description: Initial QC analysis of biospecimen data using the FastQC pipeline.
```
</details>


## 4. Delete an activity

Deleting an activity is a two-step process: first call `disassociate_from_entity()` to remove the link between the activity and the file version, then call `delete()` to remove the activity record from Synapse entirely:

```python
--8<-- "docs/tutorials/python/tutorial_scripts/activity.py:delete_activity"
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
Deleted activity from: fileA.txt (version 2)
Activity after deletion: None
```
</details>


## Source code for this tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
--8<-- "docs/tutorials/python/tutorial_scripts/activity.py"
```
</details>

## References used in this tutorial

- [Activity][synapseclient.models.Activity]
- [UsedEntity][synapseclient.models.UsedEntity]
- [UsedURL][synapseclient.models.UsedURL]
- [File][file-reference-sync]
- [syn.login][synapseclient.Synapse.login]
