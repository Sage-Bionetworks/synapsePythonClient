"""
Here is where you'll find the code for the uploading data in bulk tutorial.
"""

import os

import synapseclient
from synapseclient.models import Project

syn = synapseclient.Synapse()
syn.login()

# Create some constants to store the paths to the data
DIRECTORY_FOR_MY_PROJECT = os.path.expanduser(os.path.join("~", "my_ad_project"))
PATH_TO_MANIFEST_FILE = os.path.expanduser(os.path.join("~", "manifest-for-upload.csv"))

# Step 1: Let's find the synapse ID of our project:
my_project_id = syn.findEntityId(
    name="My uniquely named project about Alzheimer's Disease"
)

# Step 2: Create a manifest CSV file to upload data in bulk
# Walk the local directory tree and build a manifest with the required "path" and
# "parentId" columns.  Folders that do not yet exist in Synapse are created
# automatically by sync_to_synapse, so we set parentId to the project for every file.
# TODO: https://sagebionetworks.jira.com/browse/SYNPY-1804
# In a future release, Project.sync_from_synapse will support writing a manifest CSV directly, removing the need to build one manually.
import pandas as pd

rows = []
for dirpath, _dirnames, filenames in os.walk(DIRECTORY_FOR_MY_PROJECT):
    for filename in filenames:
        rows.append(
            {
                "path": os.path.join(dirpath, filename),
                "parentId": my_project_id,
            }
        )

df = pd.DataFrame(rows)
df.to_csv(PATH_TO_MANIFEST_FILE, index=False)

# Step 3: After generating the manifest file, we can upload the data in bulk
project = Project(id=my_project_id)
project.sync_to_synapse(manifest_path=PATH_TO_MANIFEST_FILE)

# Step 4: Let's add an annotation to our manifest file
# Pandas is a powerful data manipulation library in Python, although it is not required
# for this tutorial, it is used here to demonstrate how you can manipulate the manifest
# file before uploading it to Synapse.

# Read CSV file into a pandas DataFrame
df = pd.read_csv(PATH_TO_MANIFEST_FILE)

# Add a new column to the DataFrame
df["species"] = "Homo sapiens"

# Write the DataFrame back to the manifest file
df.to_csv(PATH_TO_MANIFEST_FILE, index=False)

project.sync_to_synapse(manifest_path=PATH_TO_MANIFEST_FILE)

# Step 5: Let's create an Activity/Provenance
# First let's find the row in the CSV we want to update. This code finds the row number
# that we would like to update.
row_index = df[
    df["path"] == f"{DIRECTORY_FOR_MY_PROJECT}/biospecimen_experiment_1/fileA.txt"
].index


# After finding the row we want to update let's go ahead and add a relationship to
# another file in our manifest. This allows us to say "We used 'this' file in some way".
df.loc[row_index, "used"] = (
    f"{DIRECTORY_FOR_MY_PROJECT}/single_cell_RNAseq_batch_1/SRR12345678_R1.fastq.gz"
)

# Let's also link to the pipeline that we ran in order to produce these results. In a
# real scenario you may want to link to a specific run of the tool where the results
# were produced.
df.loc[row_index, "executed"] = "https://nf-co.re/rnaseq/3.14.0"

# Let's also add a description for this Activity/Provenance
df.loc[row_index, "activityDescription"] = (
    "Experiment results created as a result of the linked data while running the pipeline."
)

# Write the DataFrame back to the manifest file
df.to_csv(PATH_TO_MANIFEST_FILE, index=False)

project.sync_to_synapse(manifest_path=PATH_TO_MANIFEST_FILE)
