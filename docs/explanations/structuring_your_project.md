# Structuring Your Synapse Project

Based on the experience on managing data coordination projects for 10+ years at Sage, the below are recommendations on how best to manage your Synapse project for data sharing and management.

> Note: This page is a work in progress and will contain code examples at a later date.

## Permissions Management

We recommend creating Synapse Teams for permission management on your Synapse project so that you manage users that are in these teams instead of granting individual users access to your project. These are the recommended teams to create:

* **<project> Admin** - This team should have "Administrator" access to the project and is used to used to manage the project and grant access to other teams.
* **<project> Users** - This team is optional, but can be used to grant a curated set of users download access to the project, by grant "Can Download" permission to the team. If you want to grant all registered users on Synapse download access to your project, click the "Make Public" button in project's sharing settings instead of creating and managing this team.

Below are some key permission criteria to consider when setting up your project:

* Create a Synapse Team and Project per data contributor if there are (1) multiple data contributors and (2) the data contributors should not have access to each other's raw data. You would then create a central "public" project that will contain the harmonized data. You can technically leverage [local share settings](https://help.synapse.org/docs/Sharing-Settings,-Permissions,-and-Conditions-for-Use.2024276030.html#SharingSettings,Permissions,andConditionsforUse-EditSharingSettingsonFiles,Folders,andTables) by creating private folders in one project but managing local share settings is more complicated and not recommended.
* Do not mix data that requires different permission models within a folder. For example, if you have a project that contains both public and private data, you should create two folders, one for public data and one for private data. You can then grant the appropriate permissions to each folder. You can use local share settings to manage each file's permission, but this is not recommended!


## Project Structure

When organizing your data for upload we have a preferred organization (flattened data layout) and an alternate option (hierarchy data layout) if your project requires that. Synapse files are automatically versioned when you create a file with the same filename, so be sure to account for that when organizing your folders.

> NOTE: If you and your contributing site decide to use a hierarchical file structure within your cloud storage location, please remember that each top-level folder and all of its subfolders must contain data of the same type (see details below).


### Top Level Folder Names

Top level folders correspond to the datasets being submitted. See the examples below. You can name your datasets in a way that is descriptive for your contributing site.

You can use either the Hierarchy or Flattened data layout according to the examples below.

#### Flattened Data Layout Example

This is the preferred dataset organization option. Each dataset folder contains the same datatype, and there aren’t nested folders containing datasets.

```
.
├── biospecimen_experiment_1
    ├── manifest1.tsv
├── biospecimen_experiment_2
    ├── manifestA.tsv
├── single_cell_RNAseq_batch_1
    ├── manifestX.tsv
    ├── fileA.txt
    ├── fileB.txt
    ├── fileC.txt
    └── fileD.txt
└── single_cell_RNAseq_batch_2
    ├── manifestY.tsv
    └── file1.txt
```

#### Hierarchy Data Layout Example

In this option, subfolders should be of the same data type and level as the root folder they are contained in. For example, you should not put a biospecimen and a clinical demographics subfolder within the same folder. Your files should be reasonably descriptive in stating the assay type and level and be consistently prefixed with the assay type.

* A dataset folder can’t be inside another dataset folder. For example, a clinical demographics folder can't be inside the biospecimen folder.
* The dataset folders must have unique names.
* Folder hierarchy may contain non-dataset folders (e.g. storing reports or other kinds of entities).

```
.
├── clinical_diagnosis
├── clinical_demographics
├── biospecimen
    ├── experiment_1
        ├── manifest1.tsv
    └── experiment_2
        ├── manifestA.tsv
└── single_cell
    ├── batch_1
        ├── manifestX.tsv
        ├── fileA.txt
        ├── fileB.txt
        ├── fileC.txt
        └── fileD.txt
    └── batch_2
        ├── manifestY.tsv
        └── file1.txt
```

### File Views

A File View allows you to see groups of files, tables, projects, or submissions and any associated annotations about those items. Annotations are an essential component to building a view. Annotations are labels that you apply to your data, stored as key-value pairs in Synapse. You can use annotations to select specific subsets of your data across many projects or folders and group things together in one view.

You can use a view to:

- Search and query many files, tables, projects, and submissions at once
- View and edit file or table annotations in bulk
- Group or link files, tables, projects, or submissions together by their annotations


#### Creating the File View

* Create a File View with the project set to the scope of the File View
* Give every Team Download level access to this File View.
* Note: creating this File View will not be possible if files/folders don’t yet exist in the data contributing site specific projects; Synapse will not allow you to create a File View with an empty scope.
* Make sure to add both file and folder entities to the scope of the File View.
* Make sure you leverage Synapse annotations per file and folder to allow for your files to be more easily discoverable via a File View.

For more information, visit [File Views](https://help.synapse.org/docs/Views.2011070739.html).

#### Uploading annotations with manifests

Manifests are crucial for the organization of your data in Synapse. In the **hierarchical case**, you would fill in one manifest and include all files in experiment/batches; in the **flattened case**, you would fill in one manifest for each top level folder. The manifest would contain Synapse annotations which can be used to query the data when a File View is created. Please read [manifest_tsv](manifest_tsv.md) for more information.


### An example: ELITE portal

Synapse Project: https://www.synapse.org/Synapse:syn27229419/wiki/623145

This project powers the elite portal: https://eliteportal.synapse.org/. More information about the studies and the files can be found in this portal.
