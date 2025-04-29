"""Here is where you'll find the code for the VirtualTable tutorial."""

import pandas as pd

from synapseclient import Synapse
from synapseclient.models import Column, ColumnType, Project, Table, VirtualTable

# Initialize Synapse client
syn = Synapse()
syn.login()

# Get the project where we want to create the virtual table
project = Project(name="My uniquely named project about Alzheimer's Disease").get()
project_id = project.id
print(f"Got project with ID: {project_id}")

# Create the first table with some columns and rows
table1_columns = [
    Column(name="sample_id", column_type=ColumnType.STRING),
    Column(name="patient_id", column_type=ColumnType.STRING),
    Column(name="age", column_type=ColumnType.INTEGER),
    Column(name="diagnosis", column_type=ColumnType.STRING),
]

table1 = Table(
    name="Patient Demographics",
    parent_id=project_id,
    columns=table1_columns,
)
table1 = table1.store()
print(f"Created table 1 with ID: {table1.id}")

# Add rows to the first table
data1 = pd.DataFrame(
    [
        {"sample_id": "S1", "patient_id": "P1", "age": 70, "diagnosis": "Alzheimer's"},
        {"sample_id": "S2", "patient_id": "P2", "age": 65, "diagnosis": "Healthy"},
        {"sample_id": "S3", "patient_id": "P3", "age": 72, "diagnosis": "Alzheimer's"},
        {"sample_id": "S4", "patient_id": "P4", "age": 68, "diagnosis": "Healthy"},
        {"sample_id": "S5", "patient_id": "P5", "age": 75, "diagnosis": "Alzheimer's"},
        {"sample_id": "S6", "patient_id": "P6", "age": 80, "diagnosis": "Healthy"},
    ]
)
table1.upsert_rows(values=data1, primary_keys=["sample_id"])

# Create the second table with some columns and rows
table2_columns = [
    Column(name="sample_id", column_type=ColumnType.STRING),
    Column(name="gene", column_type=ColumnType.STRING),
    Column(name="expression_level", column_type=ColumnType.DOUBLE),
]

table2 = Table(
    name="Gene Expression Data",
    parent_id=project_id,
    columns=table2_columns,
)
table2 = table2.store()
print(f"Created table 2 with ID: {table2.id}")

# Add rows to the second table
data2 = pd.DataFrame(
    [
        {"sample_id": "S1", "gene": "APOE", "expression_level": 2.5},
        {"sample_id": "S2", "gene": "APP", "expression_level": 1.8},
        {"sample_id": "S3", "gene": "PSEN1", "expression_level": 3.2},
        {"sample_id": "S4", "gene": "MAPT", "expression_level": 2.1},
        {"sample_id": "S5", "gene": "APP", "expression_level": 3.5},
        {"sample_id": "S7", "gene": "PSEN2", "expression_level": 1.9},
    ]
)
table2.upsert_rows(values=data2, primary_keys=["sample_id"])
# Note: VirtualTables do not support JOIN or UNION operations in the defining_sql.
# If you need to combine data from multiple tables, consider using a MaterializedView instead.


def create_basic_virtual_table():
    """
    Example: Create a basic virtual table with a simple SELECT query.
    """
    virtual_table = VirtualTable(
        name="Patient Data View",
        description="A virtual table showing patient demographics",
        parent_id=project_id,
        defining_sql=f"SELECT * FROM {table1.id}",
    )
    virtual_table = virtual_table.store()
    print(f"Created Virtual Table with ID: {virtual_table.id}")

    virtual_table_id = virtual_table.id

    query = f"SELECT * FROM {virtual_table_id}"
    query_result: pd.DataFrame = virtual_table.query(
        query=query, include_row_id_and_row_version=False
    )

    # Print the results to the console
    print("Results from the basic virtual table:")
    print(query_result)


def create_virtual_table_with_column_selection():
    """
    Example: Create a virtual table that selects only specific columns.
    """
    virtual_table = VirtualTable(
        name="Patient Age View",
        description="A virtual table showing only patient IDs and ages",
        parent_id=project_id,
        defining_sql=f"SELECT patient_id, age FROM {table1.id}",
    )
    virtual_table = virtual_table.store()
    print(f"Created Virtual Table with ID: {virtual_table.id}")

    virtual_table_id = virtual_table.id

    query = f"SELECT * FROM {virtual_table_id}"
    query_result: pd.DataFrame = virtual_table.query(
        query=query, include_row_id_and_row_version=False
    )

    # Print the results to the console
    print("Results from the virtual table with column selection:")
    print(query_result)


def create_virtual_table_with_filtering():
    """
    Example: Create a virtual table with a WHERE clause for filtering.
    """
    virtual_table = VirtualTable(
        name="Alzheimer's Patients",
        description="A virtual table showing only patients with Alzheimer's",
        parent_id=project_id,
        defining_sql=f"SELECT * FROM {table1.id} WHERE diagnosis = 'Alzheimer''s'",
    )
    virtual_table = virtual_table.store()
    print(f"Created Virtual Table with ID: {virtual_table.id}")

    virtual_table_id = virtual_table.id

    query = f"SELECT * FROM {virtual_table_id}"
    query_result: pd.DataFrame = virtual_table.query(
        query=query, include_row_id_and_row_version=False
    )

    # Print the results to the console
    print("Results from the virtual table with filtering:")
    print(query_result)


def create_virtual_table_with_ordering():
    """
    Example: Create a virtual table with an ORDER BY clause.
    """
    virtual_table = VirtualTable(
        name="Patients by Age",
        description="A virtual table showing patients ordered by age",
        parent_id=project_id,
        defining_sql=f"SELECT * FROM {table1.id} ORDER BY age DESC",
    )
    virtual_table = virtual_table.store()
    print(f"Created Virtual Table with ID: {virtual_table.id}")

    virtual_table_id = virtual_table.id

    query = f"SELECT * FROM {virtual_table_id}"
    query_result: pd.DataFrame = virtual_table.query(
        query=query, include_row_id_and_row_version=False
    )

    # Print the results to the console
    print("Results from the virtual table with ordering:")
    print(query_result)


def create_virtual_table_with_aggregation():
    """
    Example: Create a virtual table with an aggregate function.
    """
    virtual_table = VirtualTable(
        name="Diagnosis Count",
        description="A virtual table showing the count of patients by diagnosis",
        parent_id=project_id,
        defining_sql=f"SELECT diagnosis, COUNT(*) AS patient_count FROM {table1.id} GROUP BY diagnosis",
    )
    virtual_table = virtual_table.store()
    print(f"Created Virtual Table with ID: {virtual_table.id}")

    virtual_table_id = virtual_table.id

    query = f"SELECT * FROM {virtual_table_id}"
    query_result: pd.DataFrame = virtual_table.query(
        query=query, include_row_id_and_row_version=False
    )

    # Print the results to the console
    print("Results from the virtual table with aggregation:")
    print(query_result)


def main():
    create_basic_virtual_table()
    create_virtual_table_with_column_selection()
    create_virtual_table_with_filtering()
    create_virtual_table_with_ordering()
    create_virtual_table_with_aggregation()


if __name__ == "__main__":
    main()
