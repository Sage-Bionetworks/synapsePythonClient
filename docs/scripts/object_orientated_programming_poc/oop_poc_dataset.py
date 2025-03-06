import asyncio
import uuid

import pandas as pd

from synapseclient import Synapse
from synapseclient.models.dataset import Dataset, EntityRef
from synapseclient.models import File, Folder, Column, ColumnType

from synapseclient.api import get_default_columns, ViewTypeMask

#
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


async def main():
    # # Create a new dataset
    my_initialized_dataset = Dataset(parent_id=PROJECT, name="my-new-new-dataset")
    # Add items to the dataset
    # Add EntityRef directly
    await my_initialized_dataset.add_item_async(ENTITY_REF)
    # Add File
    await my_initialized_dataset.add_item_async(FILE)
    # Add Folder (all children are added)
    await my_initialized_dataset.add_item_async(FOLDER)
    # Store the dataset
    await my_initialized_dataset.store_async()

    # Retrieve the dataset
    my_retrieved_dataset = await Dataset(id=my_initialized_dataset.id).get_async()

    # Query Data
    row = await my_retrieved_dataset.query_async(
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
    await my_retrieved_dataset.store_async()

    # Upsert data - only works for updating, not inserting
    modified_data = pd.DataFrame(
        {
            "id": ["syn51790028"],
            "my_annotation": [str(uuid.uuid4())],
        }
    )
    await my_retrieved_dataset.upsert_rows_async(
        values=modified_data, primary_keys=["id"], dry_run=False
    )

    await my_retrieved_dataset.delete_async()

    # update the custom annotation column - only works for custom columns
    # dataset_df = await my_retrieved_dataset.query_async(
    #     query=f"SELECT * FROM {my_retrieved_dataset.id}"
    # )
    # dataset_df["my_annotation"] = "good data"
    # await my_retrieved_dataset.store_rows_async(values=dataset_df)

    # Delete Data - this is not working
    # my_retrieved_dataset.delete_rows(
    #     query=f"SELECT * FROM {my_retrieved_dataset.id} WHERE id = '{FILE.id}'"
    # )
    # my_retrieved_dataset.store()


if __name__ == "__main__":
    asyncio.run(main())
