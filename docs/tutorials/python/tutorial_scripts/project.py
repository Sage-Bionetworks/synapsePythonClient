"""
Here is where you'll find the code for the project tutorial.
"""

# Step 1: Create a new project
import synapseclient
from synapseclient import Project

syn = synapseclient.login()

# Project names must be globally unique
project = Project(name="My uniquely named project about Alzheimer's Disease")
project = syn.store(obj=project)

# Step 2: Print stored attributes about your project
my_synapse_project_id = project.id
print(f"My project ID is: {my_synapse_project_id}")

project_creation_date = project.createdOn
print(f"I created my project on: {project.createdOn}")

user_id_who_created_project = project.createdBy
print(f"The ID of the user that created my project is: {user_id_who_created_project}")

project_modified_on_date = project.modifiedOn
print(f"My project was last modified on: {project_modified_on_date}")

# Step 3: Get an existing project
my_project_id = syn.findEntityId(
    name="My uniquely named project about Alzheimer's Disease"
)
my_project_object = syn.get(entity=my_project_id)
print(f"I just got my project: {my_project_object.name}, id: {my_project_id}")
