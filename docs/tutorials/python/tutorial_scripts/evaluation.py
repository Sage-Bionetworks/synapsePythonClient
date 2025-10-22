"""
Here is where you'll find the code for the Evaluation tutorial.
"""

from synapseclient import Synapse
from synapseclient.models import Evaluation, Project

syn = Synapse()
syn.login()

# REQUIRED: Set this to the Synapse user ID or team ID you want to grant permissions to
# Do NOT leave this as None - the script will not work properly
PRINCIPAL_ID = None  # Replace with actual user/team ID

# Retrieve the Project where your Evaluation will be stored
project = Project(name="My uniquely named project about Alzheimer's Disease").get()
project_id = project.id

print(f"Working within Project: {project_id}")

# Create a new Evaluation object
evaluation = Evaluation(
    name="My Challenge Evaluation - Round 1",
    description="Evaluation for my data challenge",
    content_source=project_id,
    submission_instructions_message="Submit CSV files only",
    submission_receipt_message="Thank you for your submission!",
)

# Create the Evaluation on Synapse
evaluation.store()

print("Evaluation has been created with the following name and description:")
print(evaluation.name)
print(evaluation.description)

# Update the Evaluation object's name and description
evaluation.name = "My Challenge Evaluation - Round 2"
evaluation.description = "Updated description for my evaluation"

# Update the Evaluation on Synapse
evaluation.store()

print("Evaluation has been updated with the following name and description:")
print(evaluation.name)
print(evaluation.description)

# Confirm what's in Synapse matches the evaluation stored
from_synapse = Evaluation(id=evaluation.id).get()

print("The following evaluation has been retrieved from Synapse:")
print(from_synapse)

# Update the Evaluation's ACL on Synapse by adding a new user
assert (
    PRINCIPAL_ID is not None
), "PRINCIPAL_ID must be set to the Synapse user ID or team ID you want to grant permissions to."

evaluation.update_acl(principal_id=PRINCIPAL_ID, access_type=["READ", "SUBMIT"])

# Get the Evaluation's ACL to confirm the update
acl = evaluation.get_acl()
print("The following ACL has been retrieved from Synapse:")
print(acl)

# Now let's remove the user we just added from the Evaluation's ACL
evaluation.update_acl(principal_id=PRINCIPAL_ID, access_type=[])

# Finally let's retrieve all Evaluations stored within this project, including the one we just created
evaluations_list = Evaluation.get_evaluations_by_project(project_id)

# Let's delete the evaluation we created for this tutorial, and any other evaluations in this project (uncomment below to enable deletion)
# for evaluation_to_delete in evaluations_list:
#     print(f"Deleting evaluation: {evaluation_to_delete.name}")
#     evaluation_to_delete.delete()
