"""Here is where you'll find the code for the DatasetCollection tutorial."""

# --8<-- [start:setup]
import pandas as pd

from synapseclient import Synapse
from synapseclient.models import Column, ColumnType, Dataset, DatasetCollection, Project

# First, let's get the project that we want to create the DatasetCollection in
syn = Synapse()
syn.login()

project = Project(
    name="My uniquely named project about Alzheimer's Disease"
).get()  # Replace with your project name
project_id = project.id
print(f"My project ID is {project_id}")
# --8<-- [end:setup]

# This tutorial assumes that you have already created datasets that you would like to add to a DatasetCollection.
# If you need help creating datasets, you can refer to the dataset tutorial.

# For this example, we will be using datasets already created in the project.
# Let's create the DatasetCollection. We'll use the project id as the parent id.
# At first, the DatasetCollection will be empty, but if you view the DatasetCollection's schema in the UI,
# you will notice that DatasetCollections come with default columns.
# --8<-- [start:create_collection]
DATASET_IDS = [
    "syn65987017",
    "syn65987019",
    "syn65987020",
]  # Replace with your dataset IDs
test_dataset_collection = DatasetCollection(
    parent_id=project_id, name="test_dataset_collection"
).store()
print(f"My DatasetCollection's ID is {test_dataset_collection.id}")
# --8<-- [end:create_collection]

# Now, let's add some datasets to the collection. We will loop through our dataset ids and add each dataset to the
# collection using the `add_item` method.
# --8<-- [start:add_datasets]
for dataset_id in DATASET_IDS:
    test_dataset_collection.add_item(Dataset(id=dataset_id).get())
# --8<-- [end:add_datasets]
# Our changes won't be persisted to Synapse until we call the `store` method on our DatasetCollection.
# --8<-- [start:store_collection]
test_dataset_collection.store()
# --8<-- [end:store_collection]

# Now that our DatasetCollection with all of our datasets has been created, the next time we want to use it,
# we can retrieve it from Synapse.
# --8<-- [start:retrieve_collection]
my_retrieved_dataset_collection = DatasetCollection(id=test_dataset_collection.id).get()
print(f"My DatasetCollection's ID is still {my_retrieved_dataset_collection.id}")
print(f"My DatasetCollection has {len(my_retrieved_dataset_collection.items)} items")
# --8<-- [end:retrieve_collection]

# In addition to the default columns, you may want to annotate items in your DatasetCollection using
# custom columns.
# --8<-- [start:add_custom_column]
my_retrieved_dataset_collection.add_column(
    column=Column(
        name="my_annotation",
        column_type=ColumnType.STRING,
    )
)
my_retrieved_dataset_collection.store()
# --8<-- [end:add_custom_column]

# Now that our custom column has been added, we can update the DatasetCollection with new annotations.
# --8<-- [start:update_custom_column_values]
modified_data = pd.DataFrame(
    {
        "id": DATASET_IDS,
        "my_annotation": ["good dataset"] * len(DATASET_IDS),
    }
)
my_retrieved_dataset_collection.update_rows(
    values=modified_data, primary_keys=["id"], dry_run=False
)
# --8<-- [end:update_custom_column_values]

# If you want to query your DatasetCollection for items that match certain criteria, you can do so
# using the `query` method.
# --8<-- [start:query_collection]
rows = my_retrieved_dataset_collection.query(
    query=f"SELECT id, my_annotation FROM {my_retrieved_dataset_collection.id} WHERE my_annotation = 'good dataset'"
)
print(rows)
# --8<-- [end:query_collection]

# Create a snapshot of the DatasetCollection
# --8<-- [start:snapshot_collection]
my_retrieved_dataset_collection.snapshot(comment="test snapshot")
# --8<-- [end:snapshot_collection]
