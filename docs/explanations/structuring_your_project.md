# Structuring Your Synapse Project

Based on the experience on managing data coordination projects for 10+ years at Sage, the below are recommendations on how best to manage your Synapse project for data sharing and management.

> Note: This page is a work in progress and will contain code examples at a later date.

## Permissions Management

We recommend creating Synapse Teams for permission management on your Synapse project.  These are the recommended teams to create:

* **<project> Admin** - This team should have "Administrator" access to the project.  This team should be used to manage the project and grant access to other teams.

* **<project> Users** - Optional: If you want to grant all registered users on Synapse download access to your project, you can click the "Make Public" button in project's sharing settings.  This team should have "Can Download" access to the project.  This team should be used to grant download access to the project.

Below are some key permission criterias to consider when setting up your project:

* Create a Synapse Team and Project per data contributor if there are (1) multiple data contributors and (2) the data contributors should not have access to each other's raw data. You would then create a central "public" project that will contain the harmonized data. You can technically leverage local share settings by creating private folders in one project but managing local share settings is more complicated and not recommended.
* Do not mix data that requires different permission models within a folder. For example, if you have a folder that contains both public and private data, you should create two folders, one for public data and one for private data.  You can then grant the appropriate permissions to each folder. You can use local share settings to manage each file's permission, but this is not recommended!


## Project Structure

This document will help you understand how to organize your data in Synapse to make it easier for the community to find and use your data.

When organizing your data for upload we have a preferred organization (flattened data layout) and an alternate option (hierarchy data layout) if your project requires that.

> NOTE: If you and your contributing site decide to use a hierarchical file structure within your cloud storage location, please remember that each top-level folder and all of its subfolders must contain data of the same type (see details below).


### Top Level Folder Names

Top level folders correspond to the datasets being submitted. See the examples below. You can name your datasets in a way that is descriptive for your contributing site.

You can use either the Hierarchy or Flattened data layout according to the examples below. In the hierarchical case, you would fill in one manifest and include all files in experiment/batches; in the flattened case, you would fill in one manifest for each top level folder.  The manifest would be Synapse annotations which can be used to query the data when a File View is created.

### Flattened Data Layout Example

This is the preferred dataset organization option.  Each dataset folder contains the same datatype, and there aren’t nested folders containing datasets.

```
.
├── biospecimen_experiment_1
    ├── manifest1.csv
├── biospecimen_experiment_2
    ├── manifestA.csv
├── single_cell_RNAseq_batch_1
    ├── manifestX.csv
    ├── fileA.txt
    ├── fileB.txt
    ├── fileC.txt
    └── fileD.txt
└── single_cell_RNAseq_batch_2
    ├── manifestY.csv
    └── file1.txt
```

### Hierarchy Data Layout Example

In this option, subfolders should be of the same data type and level as the root folder they are contained in. For example, you should not put a biospecimen and a clinical demographics subfolder within the same folder.  Your files should be reasonably descriptive in stating the assay type and level and be consistently prefixed with the assay type.

* each dataset folder must have Synapse annotation contentType:dataset
* a dataset folder can’t be inside another dataset folder
* dataset folders must have unique names
* folder hierarchy may contain non-dataset folders (e.g. storing reports or other kinds of entities)

```
.
├── clinical_diagnosis
├── clinical_demographics
├── biospecimen
   ├── experiment_1
        ├── manifest1.csv
    └── experiment_2
        ├── manifestA.csv
└── single_cell
    ├── batch_1
        ├── manifestX.csv
        ├── fileA.txt
        ├── fileB.txt
        ├── fileC.txt
        └── fileD.txt
    └── batch_2
        ├── manifestY.csv
        └── file1.txt
```


### To Create a Project Fileview with scope set to the project:

* Add column contentType to the Fileview schema (default parameters for the column schema will work).
* Give every Team Download level access to this fileview.
* Note: creating this file view will not be possible if files/folders don’t yet exist in the center-specific projects; Synapse will not allow you to create a file view with an empty scope.
* Make sure to add both file and folder entities to the scope of the Fileview.

### An example: ELITE portal

Synapse Project: https://www.synapse.org/#!Synapse:syn27229419/wiki/623145

This project powers the elite portal: https://eliteportal.synapse.org/.  More information about the studies and the files can be found in this portal.
