"""Here is where you'll find the code for the dataset tutorial."""

# --8<-- [start:setup]
import pandas as pd

from synapseclient import Synapse
from synapseclient.models import (
    Column,
    ColumnType,
    Dataset,
    EntityRef,
    File,
    Folder,
    Project,
)

# First, let's get the project that we want to create the dataset in
syn = Synapse()
syn.login()

project = Project(
    name="My uniquely named project about Alzheimer's Disease"
).get()  # Replace with your project name
project_id = project.id
print(f"My project ID is {project_id}")
# --8<-- [end:setup]

# --8<-- [start:create_dataset]
# Next, let's create the dataset. We'll use the project id as the parent id.
# To begin, the dataset will be empty, but if you view the dataset's schema in the UI,
# you will notice that datasets come with default columns.
my_new_dataset = Dataset(parent_id=project_id, name="My New Dataset").store()
print(f"My Dataset's ID is {my_new_dataset.id}")
# --8<-- [end:create_dataset]

# Now, let's add some files to the dataset. There are three ways to add files to a dataset:
# 1. Add an Entity Reference to a file with its ID and version
# --8<-- [start:add_entity_ref]
my_new_dataset.add_item(
    EntityRef(id="syn51790029", version=1)
)  # Replace with the ID of the file you want to add
# --8<-- [end:add_entity_ref]
# 2. Add a File with its ID and version
# --8<-- [start:add_file]
my_new_dataset.add_item(
    File(id="syn51790028", version_number=1)
)  # Replace with the ID of the file you want to add
# --8<-- [end:add_file]
# 3. Add a Folder. In this case, all child files of the folder are added to the dataset recursively.
# --8<-- [start:add_folder]
my_new_dataset.add_item(
    Folder(id="syn64893446")
)  # Replace with the ID of the folder you want to add
# --8<-- [end:add_folder]
# Our changes won't be persisted to Synapse until we call the store() method.
# --8<-- [start:store_dataset]
my_new_dataset.store()
# --8<-- [end:store_dataset]

# Now that our Dataset with all of our files has been created, the next time
# we want to use it, we can retrieve it from Synapse.
# --8<-- [start:retrieve_dataset]
my_retrieved_dataset = Dataset(id=my_new_dataset.id).get()
print(f"My Dataset's ID is {my_retrieved_dataset.id}")
print(len(my_retrieved_dataset.items))
# --8<-- [end:retrieve_dataset]

# If you want to query your dataset for files that match certain criteria, you can do so
# using the query method.
# --8<-- [start:query_dataset]
rows = Dataset.query(
    query=f"SELECT * FROM {my_retrieved_dataset.id} WHERE name like '%test%'"
)
print(rows)
# --8<-- [end:query_dataset]

# In addition to the default columns, you may want to annotate items in your dataset using
# custom columns.
# --8<-- [start:add_custom_column]
my_retrieved_dataset.add_column(
    column=Column(
        name="my_annotation",
        column_type=ColumnType.STRING,
    )
)
my_retrieved_dataset.store()
# --8<-- [end:add_custom_column]

# Now that our custom column has been added, we can update the dataset with new values.
# --8<-- [start:update_custom_column_values]
modified_data = pd.DataFrame(
    {
        "id": "syn51790028",  # The ID of one of our Files
        "my_annotation": ["excellent data"],
    }
)
my_retrieved_dataset.update_rows(
    values=modified_data, primary_keys=["id"], dry_run=False
)
# --8<-- [end:update_custom_column_values]


# Finally, let's save a snapshot of the dataset.
# --8<-- [start:snapshot_dataset]
snapshot_info = my_retrieved_dataset.snapshot(
    comment="My first snapshot",
    label="My first snapshot",
)
print(snapshot_info)
# --8<-- [end:snapshot_dataset]
