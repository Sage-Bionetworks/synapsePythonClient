"""
Here is where you'll find the code for the FileView tutorial.
"""

import pandas as pd

from synapseclient import Synapse
from synapseclient.models import (
    Column,
    ColumnType,
    FileView,
    Project,
    ViewTypeMask,
    query,
)

syn = Synapse()
syn.login()

# First let's get the project we want to create the File View in
my_project = Project(name="My uniquely named project about Alzheimer's Disease").get()
project_id = my_project.id

# Next let's add some columns to the File View, the data in these columns will end up
# being stored as annotations on the files
columns = [
    Column(name="species", column_type=ColumnType.STRING),
    Column(name="dataType", column_type=ColumnType.STRING),
    Column(name="assay", column_type=ColumnType.STRING),
    Column(name="fileFormat", column_type=ColumnType.STRING),
]

# Then we will create a FileView that is scoped to the project, and will contain a row
# for each file in the project
fileview = FileView(
    name="My File View",
    parent_id=project_id,
    scope_ids=[project_id],
    view_type_mask=ViewTypeMask.FILE,
    columns=columns,
).store()

print(f"My FileView ID is: {fileview.id}")

# When the columns are printed you'll notice that it contains a number of columns that
# are automatically added by Synapse in addition to the ones we added
print(fileview.columns.keys())

# Query the File View
results_as_dataframe: pd.DataFrame = query(
    query=f"SELECT id, name, species, dataType, assay, fileFormat, path FROM {fileview.id} WHERE path like '%single_cell_RNAseq_batch_1%'",
    include_row_id_and_row_version=False,
)
print(results_as_dataframe)

# Finally let's update the annotations on the files in the project
results_as_dataframe["species"] = ["Homo sapiens"] * len(results_as_dataframe)
results_as_dataframe["dataType"] = ["geneExpression"] * len(results_as_dataframe)
results_as_dataframe["assay"] = ["SCRNA-seq"] * len(results_as_dataframe)
results_as_dataframe["fileFormat"] = ["fastq"] * len(results_as_dataframe)

fileview.update_rows(
    values=results_as_dataframe,
    primary_keys=["id"],
    wait_for_eventually_consistent_view=True,
)


# Over time you may have a need to add or remove scopes from the FileView, you may
# use `add` or `remove` along with the Synapse ID of the scope you wish to add/remove
fileview.scope_ids.add("syn1234")
# fileview.scope_ids.remove("syn1234")
fileview.store()

# You may also need to add or remove the types of Entities that may show up in your view
# You will be able to specify multiple types using the bitwise OR operator, or a single value
fileview.view_type_mask = ViewTypeMask.FILE | ViewTypeMask.FOLDER
# fileview.view_type_mask = ViewTypeMask.FILE
fileview.store()
