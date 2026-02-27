"""
Script: Working with Grid sessions.
Covers creating sessions, importing CSV data, downloading data,
synchronizing changes, and listing/deleting sessions.
"""

from synapseclient import Synapse
from synapseclient.models import Grid, Query

syn = Synapse()
syn.login()

# Create a Grid session from a RecordSet
grid = Grid(record_set_id="syn987654321")
grid = grid.create()
print(f"Grid session: {grid.session_id}")

# Or create a Grid session from an EntityView query
grid_from_query = Grid(initial_query=Query(sql="SELECT * FROM syn123456789"))
grid_from_query = grid_from_query.create()

# Import a CSV from a local file path.
# Column names are read from the CSV header and types are resolved
# from the JSON schema bound to the grid session automatically.
grid = grid.import_csv(path="path/to/metadata.csv")

print(f"Imported {grid.csv_import_total_count} rows")
print(f"  Created: {grid.csv_import_created_count}")
print(f"  Updated: {grid.csv_import_updated_count}")

# Or import directly from a pandas DataFrame
import pandas as pd

df = pd.DataFrame(
    {
        "individualID": ["ANIMAL001", "ANIMAL002"],
        "species": ["Mouse", "Mouse"],
        "sex": ["female", "male"],
        "genotype": ["5XFAD", "APOE4KI"],
    }
)

grid = grid.import_csv(dataframe=df)

# You can also provide an explicit schema to override auto-derivation:
from synapseclient.models import Column, ColumnType

schema = [
    Column(name="individualID", column_type=ColumnType.STRING),
    Column(name="species", column_type=ColumnType.STRING),
    Column(name="sex", column_type=ColumnType.STRING),
    Column(name="genotype", column_type=ColumnType.STRING),
]

grid = grid.import_csv(path="path/to/metadata.csv", schema=schema)

# Download grid data as a local CSV file
file_path = grid.download_csv(download_location="/tmp")
print(f"Downloaded grid data to: {file_path}")

# Synchronize grid changes with the data source
grid = grid.synchronize()

if grid.synchronize_error_messages:
    print("Synchronization errors:")
    for msg in grid.synchronize_error_messages:
        print(f"  - {msg}")
else:
    print("Synchronization successful")

# List all active grid sessions
for session in Grid.list():
    print(f"Session: {session.session_id}, Source: {session.source_entity_id}")

# List sessions for a specific source
for session in Grid.list(source_id="syn987654321"):
    print(f"Session: {session.session_id}")

# Delete a grid session
grid.delete()
