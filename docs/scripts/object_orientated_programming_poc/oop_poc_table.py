"""The purpose of this script is to demonstrate how to use the new OOP interface for tables.
The following actions are shown in this script:
1. Creating a table
2. Storing a table
3. Getting a table
4. Storing rows in a table
5. Querying for data from a table
6. Deleting a row from a table
7. Deleting a table
"""

import csv
import os
import random
import string
from datetime import date, datetime, timedelta, timezone

import synapseclient
from synapseclient.models import Column, ColumnType, CsvResultFormat, Row, Table

PROJECT_ID = "syn52948289"

syn = synapseclient.Synapse(debug=True)
syn.login()


def write_random_csv_with_data(path: str):
    randomized_data_columns = {
        "my_string_column": str,
        "my_integer_column": int,
        "my_double_column": float,
        "my_boolean_column": bool,
    }

    # Generate randomized data
    data = {}
    for name, type in randomized_data_columns.items():
        if type == int:
            data[name] = [random.randint(0, 100) for _ in range(10)]
        elif type == float:
            data[name] = [random.uniform(0, 100) for _ in range(10)]
        elif type == bool:
            data[name] = [bool(random.getrandbits(1)) for _ in range(10)]
        elif type == str:
            data[name] = [
                "".join(random.choices(string.ascii_uppercase + string.digits, k=5))
                for _ in range(10)
            ]

    with open(path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile)

        # Write column names
        writer.writerow(data.keys())

        # Write data
        for i in range(10):
            writer.writerow([values[i] for values in data.values()])


def store_table():
    # Creating annotations for my table ==================================================
    annotations_for_my_table = {
        "my_single_key_string": "a",
        "my_key_string": ["b", "a", "c"],
        "my_key_bool": [False, False, False],
        "my_key_double": [1.2, 3.4, 5.6],
        "my_key_long": [1, 2, 3],
        "my_key_date": [date.today(), date.today() - timedelta(days=1)],
        "my_key_datetime": [
            datetime.today(),
            datetime.today() - timedelta(days=1),
            datetime.now(tz=timezone(timedelta(hours=-5))),
            datetime(2023, 12, 7, 13, 0, 0, tzinfo=timezone(timedelta(hours=0))),
            datetime(2023, 12, 7, 13, 0, 0, tzinfo=timezone(timedelta(hours=-7))),
        ],
    }

    # Creating columns for my table ======================================================
    columns = [
        Column(id=None, name="my_string_column", column_type=ColumnType.STRING),
        Column(id=None, name="my_integer_column", column_type=ColumnType.INTEGER),
        Column(id=None, name="my_double_column", column_type=ColumnType.DOUBLE),
        Column(id=None, name="my_boolean_column", column_type=ColumnType.BOOLEAN),
    ]

    # Creating a table ===============================================================
    table = Table(
        name="my_first_test_table",
        columns=columns,
        parent_id=PROJECT_ID,
        annotations=annotations_for_my_table,
    )

    table = table.store_schema()

    print("Table created:")
    print(table)

    # Getting a table =================================================================
    copy_of_table = Table(id=table.id)

    copy_of_table = copy_of_table.get()

    print("Table retrieved:")
    print(copy_of_table)

    # Updating annotations on my table ===============================================
    copy_of_table.annotations["my_key_string"] = ["new", "values", "here"]
    stored_table = copy_of_table.store_schema()
    print("Table updated:")
    print(stored_table)

    # Storing data to a table =========================================================
    name_of_csv = "my_csv_file_with_random_data"
    path_to_csv = os.path.join(os.path.expanduser("~/temp"), f"{name_of_csv}.csv")
    write_random_csv_with_data(path_to_csv)

    csv_path = copy_of_table.store_rows_from_csv(csv_path=path_to_csv)

    print("Stored data to table from CSV:")
    print(csv_path)

    # Querying for data from a table =================================================
    destination_csv_location = os.path.expanduser("~/temp/my_query_results")

    table_id_to_query = copy_of_table.id
    Table.query(
        query=f"SELECT * FROM {table_id_to_query}",
        result_format=CsvResultFormat(download_location=destination_csv_location),
    )

    print(f"Created results at: {destination_csv_location}")

    # Deleting rows from a table =====================================================
    copy_of_table.delete_rows(rows=[Row(row_id=1)])

    # Deleting a table ===============================================================
    table_to_delete = Table(
        name="my_test_table_I_want_to_delete",
        columns=columns,
        parent_id=PROJECT_ID,
    ).store_schema()

    table_to_delete.delete()


store_table()
