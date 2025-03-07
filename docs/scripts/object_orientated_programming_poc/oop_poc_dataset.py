import uuid

import pandas as pd

from synapseclient import Synapse
from synapseclient.models import Column, ColumnType, File, Folder
from synapseclient.models.dataset import Dataset, EntityRef

syn = Synapse()
syn.login()

PROJECT = "syn41746002"  # replace with your project id
FILE = File(
    id="syn51790028", version_label=1
)  # replace with a Synapse ID for a file which you have access to
ENTITY_REF = EntityRef(
    id="syn51790029", version=1
)  # replace with a Synapse ID for another file which you have access to
FOLDER = Folder(
    id="syn64893446"
)  # replace with a Synapse ID for a folder that contains files which you have access to


def store_dataset():
    # Create a new dataset
    my_initialized_dataset = Dataset(parent_id=PROJECT, name="my-new-dataset")

    # Add items to the dataset

    # Add an EntityRef directly
    my_initialized_dataset.add_item(ENTITY_REF)

    # Add a File
    my_initialized_dataset.add_item(FILE)

    # Add a Folder (all children are added recursively
    my_initialized_dataset.add_item(FOLDER)

    # Store a dataset
    my_initialized_dataset.store()

    # Retrieve a dataset
    my_retrieved_dataset = Dataset(id=my_initialized_dataset.id).get()

    # Query a dataset
    row = Dataset.query(
        query=f"SELECT * FROM {my_retrieved_dataset.id} WHERE id = '{FILE.id}'"
    )
    print(row)

    # Add a custom column
    my_retrieved_dataset.add_column(
        column=Column(
            name="my_annotation",
            column_type=ColumnType.STRING,
        )
    )
    my_retrieved_dataset.store()

    # Update dataset rows - does not work for default columns
    modified_data = pd.DataFrame(
        {
            "id": [FILE.id],
            "my_annotation": ["excellent data"],
        }
    )
    my_retrieved_dataset.update_rows(
        values=modified_data, primary_keys=["id"], dry_run=False
    )

    # Save a Snapshot of the dataset
    my_retrieved_dataset.snapshot(
        comment=str(uuid.uuid4()),
        label=str(uuid.uuid4()),
    )


if __name__ == "__main__":
    store_dataset()
