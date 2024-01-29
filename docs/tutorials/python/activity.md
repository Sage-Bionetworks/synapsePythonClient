# Activity/Provenance
[See the current available tutorial](../python_client.md#provenance)

![Under Construction](../../assets/under_construction.png)

Provenance is a concept describing the origin of something. In Synapse, it is used to describe the connections between the workflow steps used to create a particular file or set of results. Data analysis often involves multiple steps to go from a raw data file to a finished analysis. Synapse’s provenance tools allow users to keep track of each step involved in an analysis and share those steps with other users.

The model Synapse uses for provenance is based on the [W3C provenance spec](https://www.w3.org/TR/prov-n/) where items are derived from an activity which has components that were **used** and components that were **executed**. Think of the **used** items as input files and **executed** items as software or code. Both **used** and **executed** items can reside in Synapse or in URLs such as a link to a GitHub commit or a link to a specific version of a software tool.

[Dive into Activity/Provenance further here](../../explanations/domain_models_of_synapse.md#activityprovenance)

## Tutorial Purpose
In this tutorial you will:

1. Add a new Activity to your File
1. Add a new Activity to a specific version of your File
1. Print stored activities on your File
1. Delete an activity

## Prerequisites
- In order to follow this tutorial you will need to have a [Project](./project.md) created with at least one [File](./file.md) with multiple [Versions](./versions.md).
