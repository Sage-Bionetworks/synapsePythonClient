"""
Here is where you'll find the code for the uploading data in bulk tutorial.
"""

import os

import synapseclient
import synapseutils

syn = synapseclient.Synapse()
syn.login()

# Create some constants to store the paths to the data
DIRECTORY_FOR_MY_PROJECT = os.path.expanduser(os.path.join("~", "my_ad_project"))
PATH_TO_MANIFEST_FILE = os.path.expanduser(os.path.join("~", "manifest-for-upload.tsv"))

# Step 1: Let's find the synapse ID of our project:
my_project_id = syn.findEntityId(
    name="My uniquely named project about Alzheimer's Disease"
)

# Step 2: Create a manifest TSV file to upload data in bulk
# Note: When this command is run it will re-create your directory structure within
# Synapse. Be aware of this before running this command.
# If folders with the exact names already exists in Synapse, those folders will be used.
synapseutils.generate_sync_manifest(
    syn=syn,
    directory_path=DIRECTORY_FOR_MY_PROJECT,
    parent_id=my_project_id,
    manifest_path=PATH_TO_MANIFEST_FILE,
)

# Step 3: After generating the manifest file, we can upload the data in bulk
synapseutils.syncToSynapse(
    syn=syn, manifestFile=PATH_TO_MANIFEST_FILE, sendMessages=False
)

# Step 4: Let's add an annotation to our manifest file
# Pandas is a powerful data manipulation library in Python, although it is not required
# for this tutorial, it is used here to demonstrate how you can manipulate the manifest
# file before uploading it to Synapse.
import pandas as pd

# Read TSV file into a pandas DataFrame
df = pd.read_csv(PATH_TO_MANIFEST_FILE, sep="\t")

# Add a new column to the DataFrame
df["species"] = "Homo sapiens"

# Write the DataFrame back to the manifest file
df.to_csv(PATH_TO_MANIFEST_FILE, sep="\t", index=False)

synapseutils.syncToSynapse(
    syn=syn,
    manifestFile=PATH_TO_MANIFEST_FILE,
    sendMessages=False,
)

# Step 5: Let's create an Activity/Provenance
# First let's find the row in the TSV we want to update. This code finds the row number
# that we would like to update.
row_index = df[
    df["path"] == f"{DIRECTORY_FOR_MY_PROJECT}/biospecimen_experiment_1/fileA.txt"
].index


# After finding the row we want to update let's go ahead and add a relationship to
# another file in our manifest. This allows us to say "We used 'this' file in some way".
df.loc[
    row_index, "used"
] = f"{DIRECTORY_FOR_MY_PROJECT}/single_cell_RNAseq_batch_1/SRR12345678_R1.fastq.gz"

# Let's also link to the pipeline that we ran in order to produce these results. In a
# real scenario you may want to link to a specific run of the tool where the results
# were produced.
df.loc[row_index, "executed"] = "https://nf-co.re/rnaseq/3.14.0"

# Let's also add a description for this Activity/Provenance
df.loc[
    row_index, "activityDescription"
] = "Experiment results created as a result of the linked data while running the pipeline."

# Write the DataFrame back to the manifest file
df.to_csv(PATH_TO_MANIFEST_FILE, sep="\t", index=False)

synapseutils.syncToSynapse(
    syn=syn,
    manifestFile=PATH_TO_MANIFEST_FILE,
    sendMessages=False,
)
