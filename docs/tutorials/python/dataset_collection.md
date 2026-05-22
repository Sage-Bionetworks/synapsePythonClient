# Dataset Collections
Dataset Collections are a way to organize, annotate, and publish sets of datasets for others to use. Dataset Collections behave similarly to Tables and EntityViews, but provide some default behavior that makes it easy to put a group of datasets together.

This tutorial will walk through basics of working with Dataset Collections using the Synapse Python Client.

# Tutorial Purpose
In this tutorial, you will:

- Create a Dataset Collection
- Add datasets to the collection
- Add a custom column to the collection
- Update the collection with new annotations
- Query the collection
- Save a snapshot of the collection

# Prerequisites
* This tutorial assumes that you have a project in Synapse and have already created datasets that you would like to add to a Dataset Collection.
* If you need help creating datasets, you can refer to the [dataset tutorial](./dataset.md).
* Pandas must be installed as shown in the [installation documentation](../installation.md)

## 1. Get the ID of your Synapse project

Let's get started by authenticating with Synapse and retrieving the ID of your project.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/dataset_collection.py:setup"
```

## 2. Create your Dataset Collection

Next, we will create the Dataset Collection using the project ID to tell Synapse where we want the Dataset Collection to be created. After this step, we will have a Dataset Collection object with all of the necessary information to start building the collection.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/dataset_collection.py:create_collection"
```

Because we haven't added any datasets to the collection yet, it will be empty, but if you view the Dataset Collection's schema in the UI, you will notice that Dataset Collections come with default columns.

![Dataset Collection Default Schema](./tutorial_screenshots/dataset_collection_default_schema.png)

## 3. Add Datasets to the Dataset Collection

Now, let's add some datasets to the collection. We will loop through our dataset ids and add each dataset to the collection using the `add_item` method.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/dataset_collection.py:add_datasets"
```

Whenever we make changes to the Dataset Collection, we need to call the `store()` method to save the changes to Synapse.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/dataset_collection.py:store_collection"
```

And now we are able to see our Dataset Collection with all of the datasets that we added to it.

![Dataset Collection with Datasets](./tutorial_screenshots/dataset_collection_with_datasets.png)

## 4. Retrieve the Dataset Collection

Now that our Dataset Collection has been created and we have added some Datasets to it, we can retrieve the Dataset Collection from Synapse the next time we need to use it.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/dataset_collection.py:retrieve_collection"
```

## 5. Add a custom column to the Dataset Collection

In addition to the default columns, you may want to annotate items in your DatasetCollection using custom columns.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/dataset_collection.py:add_custom_column"
```

Our custom column isn't all that useful empty, so let's update the Dataset Collection with some values.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/dataset_collection.py:update_custom_column_values"
```

## 6. Query the Dataset Collection

If you want to query your DatasetCollection for items that match certain criteria, you can do so using the `query` method.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/dataset_collection.py:query_collection"
```

## 7. Save a snapshot of the Dataset Collection

Finally, let's save a snapshot of the Dataset Collection. This creates a read-only version of the Dataset Collection that captures the current state of the Dataset Collection and can be referenced later.

```python
--8<-- "docs/tutorials/python/tutorial_scripts/dataset_collection.py:snapshot_collection"
```

## Source Code for this Tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
--8<-- "docs/tutorials/python/tutorial_scripts/dataset_collection.py"
```
</details>

## References
- [DatasetCollection][dataset-collection-reference-sync]
- [Dataset][dataset-reference-sync]
- [Project][project-reference-sync]
- [Column][column-reference-sync]
- [syn.login][synapseclient.Synapse.login]
