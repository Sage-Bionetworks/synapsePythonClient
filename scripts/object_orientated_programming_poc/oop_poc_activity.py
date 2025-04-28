"""The purpose of this script is to demonstrate how to use the OOP interface for Activity.
The following actions are shown in this script:
1. Creating a file with an Activity
2. Retrieve an activity by parent File
3. Creating a second file with the same activity
4. Modifying the activity attached to both Files
5. Creating a table with an Activity
"""

import os

import synapseclient
from synapseclient.models import (
    Activity,
    Column,
    ColumnType,
    File,
    Table,
    UsedEntity,
    UsedURL,
)

PROJECT_ID = "syn52948289"

syn = synapseclient.Synapse(debug=True)
syn.login()


def create_random_file(
    path: str,
) -> None:
    """Create a random file with random data.

    Arguments:
        path: The path to create the file at.
    """
    with open(path, "wb") as f:
        f.write(os.urandom(1))


def store_activity_on_file():
    name_of_file = "my_file_with_random_data.txt"
    path_to_file = os.path.join(os.path.expanduser("~/temp"), name_of_file)
    create_random_file(path_to_file)

    name_of_second_file = "my_second_file_with_random_data.txt"
    path_to_second_file = os.path.join(
        os.path.expanduser("~/temp"), name_of_second_file
    )
    create_random_file(path_to_second_file)

    # Create an activity =================================================================
    activity = Activity(
        name="My Activity",
        description="This is an activity.",
        used=[
            UsedURL(name="Used URL", url="https://www.synapse.org/"),
            UsedEntity(target_id=PROJECT_ID),
        ],
        executed=[
            UsedURL(name="Used URL", url="https://www.synapse.org/"),
            UsedEntity(target_id=PROJECT_ID),
        ],
    )

    # Creating and uploading a file to a project =========================================
    file = File(
        path=path_to_file,
        name=name_of_file,
        parent_id=PROJECT_ID,
        description="This is a file with random data.",
        activity=activity,
    )

    file = file.store()

    print("File created with activity:")
    print(activity)

    activity_copy = Activity.from_parent(parent=file)

    # Storing a second file to a project and re-use the activity =========================
    second_file = File(
        path=path_to_second_file,
        name=name_of_second_file,
        parent_id=PROJECT_ID,
        description="This is a file with random data.",
        activity=activity_copy,
    )

    second_file.store()

    print("Second file created with activity:")
    print(second_file.activity)

    # # Update the already created activity, which updates the activity on both files ====
    new_activity_instance = Activity(
        # In order to update an existing activity you must provide the id and etag.
        id=second_file.activity.id,
        etag=second_file.activity.etag if second_file.activity else None,
        name="My Activity - MODIFIED",
        used=[
            UsedURL(name="Used URL", url="https://www.synapse.org/"),
            UsedEntity(target_id=PROJECT_ID),
        ],
        executed=[
            UsedURL(name="Used URL", url="https://www.synapse.org/"),
            UsedEntity(target_id=PROJECT_ID),
        ],
    )
    # These 3 will be the same activity
    print("Modified activity showing name is updated for all files:")
    print(new_activity_instance.store().name)
    print(Activity.from_parent(parent=file).name)
    print(Activity.from_parent(parent=second_file).name)


def store_activity_on_table():
    # Create an activity =================================================================
    activity = Activity(
        name="My Activity",
        description="This is an activity.",
        used=[
            UsedURL(name="Used URL", url="https://www.synapse.org/"),
            UsedEntity(target_id=PROJECT_ID),
        ],
        executed=[
            UsedURL(name="Used URL", url="https://www.synapse.org/"),
            UsedEntity(target_id=PROJECT_ID),
        ],
    )

    # Creating columns for my table ======================================================
    columns = [
        Column(id=None, name="my_string_column", column_type=ColumnType.STRING),
    ]

    # Creating a table ===============================================================
    table = Table(
        name="my_first_test_table",
        columns=columns,
        parent_id=PROJECT_ID,
        activity=activity,
    )

    table = table.store_schema()

    print("Table created with activity:")
    print(table.activity)


store_activity_on_file()
store_activity_on_table()
