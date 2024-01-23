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
import asyncio
import os
import csv
import random
import string
from synapseclient.models import (
    Table,
    Column,
    ColumnType,
    Row,
    CsvResultFormat,
)
import synapseclient
from datetime import date, datetime, timedelta, timezone

from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.sdk.resources import SERVICE_NAME, Resource
from opentelemetry.exporter.otlp.proto.http.trace_exporter import OTLPSpanExporter

trace.set_tracer_provider(
    TracerProvider(resource=Resource(attributes={SERVICE_NAME: "oop_table"}))
)
trace.get_tracer_provider().add_span_processor(BatchSpanProcessor(OTLPSpanExporter()))
tracer = trace.get_tracer("my_tracer")


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


@tracer.start_as_current_span("Table")
async def store_table():
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

    with tracer.start_as_current_span("Creating a table"):
        # Creating a table ===============================================================
        table = Table(
            name="my_first_test_table",
            columns=columns,
            parent_id=PROJECT_ID,
            annotations=annotations_for_my_table,
        )

        table = await table.store_schema()

        print(table)

    with tracer.start_as_current_span("Getting a table"):
        # Getting a table =================================================================
        copy_of_table = Table(id=table.id)

        copy_of_table = await copy_of_table.get()

        print(copy_of_table)

    with tracer.start_as_current_span("Updating annotations on my table"):
        # Updating annotations on my table ===============================================
        copy_of_table.annotations["my_key_string"] = ["new", "values", "here"]
        stored_table = await copy_of_table.store_schema()
        print(stored_table)

    with tracer.start_as_current_span("Storing data to a table"):
        # Storing data to a table =========================================================
        name_of_csv = "my_csv_file_with_random_data"
        path_to_csv = os.path.join(os.path.expanduser("~/temp"), f"{name_of_csv}.csv")
        write_random_csv_with_data(path_to_csv)

        csv_path = await copy_of_table.store_rows_from_csv(csv_path=path_to_csv)

        print(csv_path)

    with tracer.start_as_current_span("Querying for data from a table"):
        # Querying for data from a table =================================================
        destination_csv_location = os.path.expanduser("~/temp/my_query_results")

        table_id_to_query = copy_of_table.id
        await Table.query(
            query=f"SELECT * FROM {table_id_to_query}",
            result_format=CsvResultFormat(download_location=destination_csv_location),
        )

        print(f"Created results at: {destination_csv_location}")

    with tracer.start_as_current_span("Deleting rows from a table"):
        # Deleting rows from a table =====================================================
        await copy_of_table.delete_rows(rows=[Row(row_id=1)])

    with tracer.start_as_current_span("Deleting a table"):
        # Deleting a table ===============================================================
        table_to_delete = await Table(
            name="my_test_table_I_want_to_delete",
            columns=columns,
            parent_id=PROJECT_ID,
        ).store_schema()

        await table_to_delete.delete()


asyncio.run(store_table())
