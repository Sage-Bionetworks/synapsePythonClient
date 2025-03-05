import asyncio

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
    # my_initialized_dataset = Dataset(parent_id=PROJECT, name="my-new-dataset")
    # # Add items to the dataset
    # # Add EntityRef directly
    # await my_initialized_dataset.add_item_async(ENTITY_REF)
    # # Add File
    # await my_initialized_dataset.add_item_async(FILE)
    # # Add Folder (all children are added)
    # await my_initialized_dataset.add_item_async(FOLDER)
    # # Store the dataset
    # my_initialized_dataset.store()
    # # Retrieve the dataset
    # my_retrieved_dataset = Dataset(id=my_initialized_dataset.id).get()

    # # Query Data
    # row = my_retrieved_dataset.query(
    #     query=f"SELECT * FROM {my_retrieved_dataset.id} WHERE id = '{FILE.id}'"
    # )
    # print(row)

    # # Add a custom column
    # my_retrieved_dataset.add_column(
    #     column=Column(
    #         name="my_annotation",
    #         column_type=ColumnType.STRING,
    #     )
    # )
    # my_retrieved_dataset.store()

    # # update the custom annotation column - only works for custom columns
    # dataset_df = my_retrieved_dataset.query(
    #     query=f"SELECT * FROM {my_retrieved_dataset.id}"
    # )
    # dataset_df["my_annotation"] = "good data"
    # my_retrieved_dataset.store_rows(values=dataset_df)

    # Delete Data - this is not working
    # my_retrieved_dataset.delete_rows(
    #     query=f"SELECT * FROM {my_retrieved_dataset.id} WHERE id = '{FILE.id}'"
    # )
    # my_retrieved_dataset.store()

    # Upsert data - only works for updating, not inserting
    my_retrieved_dataset = Dataset(id="syn64956609").get()
    modified_data = pd.DataFrame(
        {
            "dataFileMD5Hex": ["not_a_md5"],
            "my_annotation": ["also good data"],
        }
    )
    my_retrieved_dataset.upsert_rows(
        values=modified_data, primary_keys=["dataFileMD5Hex"], dry_run=False
    )
    # my_retrieved_dataset.store()


if __name__ == "__main__":
    asyncio.run(main())


# # Getting a dataset
# my_dataset = Dataset(id=REFERENCE_DATASET).get()

# # Store with no changes
# my_dataset.store()


# # Upserting data
# modified_data = pd.DataFrame(
#     {
#         "dataFileMD5Hex": ["ff034dc4449631db217e639d48a45ab1"],
#     }
# )

# my_dataset.upsert_rows(
#     values=modified_data, primary_keys=["dataFileMD5Hex"], dry_run=False
# )
# my_dataset.store()
# breakpoint()
# # Deleting data out of the dataset
# my_dataset.delete_rows(
#     query=f"SELECT * FROM {REFERENCE_DATASET} WHERE etag = '5d637c89-4577-467a-bb8e-1203dee78f48'"
# )
# breakpoint()
# # Querying for data
# query_for_row = my_dataset.query(
#     query=f"SELECT * FROM {REFERENCE_DATASET} WHERE dataFileMD5Hex = 'ff034dc4449631db217e639d48a45ab1'"
# )
# print(query_for_row)

# # Modifying data
# query_for_row.loc[
#     query_for_row["dataFileMD5Hex"] == "ff034dc4449631db217e639d48a45ab1",
#     "manually_defined_column",
# ] = "ccc"
# print(query_for_row)
# my_dataset.store_rows(values=query_for_row)
