"""Here is where you'll find the code for the MaterializedView tutorial."""

import pandas as pd

from synapseclient import Synapse
from synapseclient.models import Column, ColumnType, MaterializedView, Project, Table

# Initialize Synapse client
syn = Synapse()
syn.login()

# Get the project where we want to create the materialized view
project = Project(name="My uniquely named project about Alzheimer's Disease").get()
project_id = project.id
print(f"Created project with ID: {project_id}")

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


def create_materialized_view():
    """
    Example: Create a new materialized view with a defining SQL query.
    """
    materialized_view = MaterializedView(
        name="Patient Data View",
        description="A view combining patient demographics and gene expression data",
        parent_id=project_id,
        defining_sql=f"SELECT * FROM {table1.id}",
    )
    materialized_view = materialized_view.store()
    print(f"Created Materialized View with ID: {materialized_view.id}")

    materialized_view_id = materialized_view.id

    query = f"SELECT * FROM {materialized_view_id}"
    query_result: pd.DataFrame = materialized_view.query(
        query=query, include_row_id_and_row_version=False
    )

    # Print the results to the console
    print("Results from the materialized view:")
    print(query_result)


def create_materialized_view_with_join():
    """
    Example: Create a materialized view with a JOIN clause.
    """
    defining_sql = f"""
    SELECT t1.sample_id AS sample_id, t1.patient_id AS patient_id, t1.age AS age, t1.diagnosis AS diagnosis,
           t2.gene AS gene, t2.expression_level AS expression_level
    FROM {table1.id} t1
    JOIN {table2.id} t2
    ON t1.sample_id = t2.sample_id
    """

    materialized_view = MaterializedView(
        name="Joined Patient Data View",
        description="A materialized view joining patient demographics with gene expression data",
        parent_id=project_id,
        defining_sql=defining_sql,
    )
    materialized_view = materialized_view.store()
    print(f"Created Materialized View with ID: {materialized_view.id}")

    materialized_view_id = materialized_view.id

    query = f"SELECT * FROM {materialized_view_id}"
    query_result: pd.DataFrame = materialized_view.query(
        query=query, include_row_id_and_row_version=False
    )

    # Print the results to the console
    print("Results from the materialized view with JOIN:")
    print(query_result)


def create_materialized_view_with_left_join():
    """
    Example: Create a materialized view with a LEFT JOIN clause.
    """
    defining_sql = f"""
    SELECT t1.sample_id AS sample_id, t1.patient_id AS patient_id, t1.age AS age, t1.diagnosis AS diagnosis,
           t2.gene AS gene, t2.expression_level AS expression_level
    FROM {table1.id} t1
    LEFT JOIN {table2.id} t2
    ON t1.sample_id = t2.sample_id
    """

    materialized_view = MaterializedView(
        name="Left Joined Patient Data View",
        description="A materialized view with a LEFT JOIN clause, including all patients even if they lack gene expression data",
        parent_id=project_id,
        defining_sql=defining_sql,
    )
    materialized_view = materialized_view.store()
    print(f"Created Materialized View with ID: {materialized_view.id}")

    materialized_view_id = materialized_view.id

    query = f"SELECT * FROM {materialized_view_id}"
    query_result: pd.DataFrame = materialized_view.query(
        query=query, include_row_id_and_row_version=False
    )

    # Print the results to the console
    print("Results from the materialized view with LEFT JOIN:")
    print(query_result)


def create_materialized_view_with_right_join():
    """
    Example: Create a materialized view with a RIGHT JOIN clause.
    """
    defining_sql = f"""
    SELECT t1.sample_id AS sample_id, t1.patient_id AS patient_id, t1.age AS age, t1.diagnosis AS diagnosis,
           t2.gene AS gene, t2.expression_level AS expression_level
    FROM {table1.id} t1
    RIGHT JOIN {table2.id} t2
    ON t1.sample_id = t2.sample_id
    """

    materialized_view = MaterializedView(
        name="Right Joined Patient Data View",
        description="A materialized view with a RIGHT JOIN clause, including all gene expression data even if no patient matches",
        parent_id=project_id,
        defining_sql=defining_sql,
    )
    materialized_view = materialized_view.store()
    print(f"Created Materialized View with ID: {materialized_view.id}")

    materialized_view_id = materialized_view.id

    query = f"SELECT * FROM {materialized_view_id}"
    query_result: pd.DataFrame = materialized_view.query(
        query=query, include_row_id_and_row_version=False
    )

    # Print the results to the console
    print("Results from the materialized view with RIGHT JOIN:")
    print(query_result)


def create_materialized_view_with_union():
    """
    Example: Create a materialized view with a UNION clause.
    """
    defining_sql = f"""
    SELECT t1.sample_id AS sample_id
    FROM {table1.id} t1
    UNION
    SELECT t2.sample_id AS sample_id
    FROM {table2.id} t2
    """

    materialized_view = MaterializedView(
        name="Union Patient Data View",
        description="A materialized view with a UNION clause",
        parent_id=project_id,
        defining_sql=defining_sql,
    )
    materialized_view = materialized_view.store()
    print(f"Created Materialized View with ID: {materialized_view.id}")

    materialized_view_id = materialized_view.id

    query = f"SELECT * FROM {materialized_view_id}"
    query_result: pd.DataFrame = materialized_view.query(
        query=query, include_row_id_and_row_version=False
    )

    # Print the results to the console
    print("Results from the materialized view with UNION:")
    print(query_result)


def main():
    create_materialized_view()
    create_materialized_view_with_join()
    create_materialized_view_with_left_join()
    create_materialized_view_with_right_join()
    create_materialized_view_with_union()


if __name__ == "__main__":
    main()
