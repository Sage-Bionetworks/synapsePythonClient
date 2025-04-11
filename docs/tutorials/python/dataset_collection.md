# Dataset Collections
Dataset Collections are a way to organize, annotate, and publish sets of datasets for others to use. Dataset Collections behave similarly to Tables and EntityViews, but provide some default behavior that makes it easy to put a group of datasets together.

This tutorial will walk through basics of working with Dataset Collections using the Synapse Python Client.

# Tutorial Purpose
In this tutorial, you will:

1. Create a Dataset Collection
2. Add datasets to the collection
3. Add a custom column to the collection
4. Update the collection with new annotations
5. Query the collection
6. Save a snapshot of the collection

# Prerequisites
* This tutorial assumes that you have a project in Synapse and have already created datasets that you would like to add to a Dataset Collection.
* If you need help creating datasets, you can refer to the [dataset tutorial](./dataset.md).
* Pandas must be installed as shown in the [installation documentation](../installation.md)

## 1. Get the ID of your Synapse project

Let's get started by authenticating with Synapse and retrieving the ID of your project.

```python
{!docs/tutorials/python/tutorial_scripts/dataset_collection.py!lines=9-14}
```

## 2. Create your Dataset Collection

Next, we will create the Dataset Collection. We will use the project ID to tell Synapse where we want the Dataset Collection to be created. After this step, we will have a Dataset Collection object with all of the needed information to start building the collection.

```python
{!docs/tutorials/python/tutorial_scripts/dataset_collection.py!lines=23-25}
```

Because we haven't added any datasets to the collection yet, it will be empty, but if you view the Dataset Collection's schema in the UI, you will notice that Dataset Collections come with default columns.

![Dataset Collection Default Schema](./tutorial_screenshots/dataset_collection_default_schema.png)

## 3. Add Datasets to the Dataset Collection

Now, let's add some datasets to the collection. We will loop through our dataset ids and add each dataset to the collection using the `add_item` method.

```python
{!docs/tutorials/python/tutorial_scripts/dataset_collection.py!lines=29-31}
```

Whenever we make changes to the Dataset Collection, we need to call the `store()` method to save the changes to Synapse.

```python
{!docs/tutorials/python/tutorial_scripts/dataset_collection.py!lines=32}
```

And now we are able to see our Dataset Collection with all of the datasets that we added to it.

![Dataset Collection with Datasets](./tutorial_screenshots/dataset_collection_with_datasets.png)

## 4. Retrieve the Dataset Collection

Now that our Dataset Collection has been created and we have added some Datasets to it, we can retrieve the Dataset Collection from Synapse the next time we need to use it.

```python
{!docs/tutorials/python/tutorial_scripts/dataset_collection.py!lines=36-38}
```

## 5. Add a custom column to the Dataset Collection

In addition to the default columns, you may want to annotate items in your DatasetCollection using custom columns.

```python
{!docs/tutorials/python/tutorial_scripts/dataset_collection.py!lines=42-48}
```

Our custom column isn't all that useful empty, so let's update the Dataset Collection with some values.

```python
{!docs/tutorials/python/tutorial_scripts/dataset_collection.py!lines=51-54}
```

## 6. Query the Dataset Collection

If you want to query your DatasetCollection for items that match certain criteria, you can do so using the `query` method.

```python
{!docs/tutorials/python/tutorial_scripts/dataset_collection.py!lines=58-61}
```

## 7. Save a snapshot of the Dataset Collection

Finally, let's save a snapshot of the Dataset Collection. This creates a read-only version of the Dataset Collection that captures the current state of the Dataset Collection and can be referenced later.

```python
{!docs/tutorials/python/tutorial_scripts/dataset_collection.py!lines=64}
```

## Source Code for this Tutorial

<details class="quote">
  <summary>Click to show me</summary>

```python
{!docs/tutorials/python/tutorial_scripts/dataset_collection.py!}
```
</details>

## References
- [DatasetCollection][../../reference/experimental/sync/dataset_collection.md]
- [Dataset][../../reference/experimental/sync/dataset.md]
- [Column][synapseclient.models.Column]
- [Project][../../reference/experimental/sync/project.md]
- [syn.login][synapseclient.Synapse.login]
