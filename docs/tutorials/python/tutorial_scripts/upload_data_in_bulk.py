"""
Here is where you'll find the code for the uploading data in bulk tutorial.
"""

import pandas as pd

import synapseclient
from synapseclient.models import Project

syn = synapseclient.Synapse()
syn.login()

# Step 1: Create some constants to store the paths to the data
DIRECTORY_FOR_MY_PROJECT = "test_folder"  # This should exist with your files in it
PATH_TO_MANIFEST_FILE = "test_manifest.csv"  # This doesn't need to exist yet
SYNAPSE_PROJECT_ID = "syn74396054"

# TODO switch to using new version of synapseutils/sync.py.generate_sync_manifest
# https://sagebionetworks.jira.com/browse/SYNPY-1809

# Step 2: Create a manifest CSV file with the paths to the files and their parent folders
# old function generates a TSV
from synapseutils import generate_sync_manifest

generate_sync_manifest(
    syn=syn,
    directory_path=DIRECTORY_FOR_MY_PROJECT,
    parent_id=SYNAPSE_PROJECT_ID,
    manifest_path=PATH_TO_MANIFEST_FILE,
)
# reformat the manifest file to work with sync_to_synapse
manifest_df = pd.read_csv(PATH_TO_MANIFEST_FILE, sep="\t")
manifest_df.rename(columns={"parent": "parentId"}, inplace=True)
manifest_df.to_csv(PATH_TO_MANIFEST_FILE, index=False)

# Step 3: After generating the manifest file, we can upload the data in bulk
project = Project(id=SYNAPSE_PROJECT_ID)
project.sync_to_synapse(manifest_path=PATH_TO_MANIFEST_FILE, send_messages=False)

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

project.sync_to_synapse(manifest_path=PATH_TO_MANIFEST_FILE, send_messages=False)

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

project.sync_to_synapse(manifest_path=PATH_TO_MANIFEST_FILE, send_messages=False)
