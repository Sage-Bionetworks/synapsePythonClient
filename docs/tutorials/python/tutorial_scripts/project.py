"""
Here is where you'll find the code for the Project tutorial.
"""

# Step 1: Create a new project
import synapseclient
from synapseclient.models import Project

syn = synapseclient.login()

# Project names must be globally unique
project = Project(name="My uniquely named project about Alzheimer's Disease")
project.store()

# Step 2: Print stored attributes about your project
print(f"My project ID is: {project.id}")

print(f"I created my project on: {project.created_on}")

print(f"The ID of the user that created my project is: {project.created_by}")

print(f"My project was last modified on: {project.modified_on}")

# Step 3: Get an existing project
my_project_object = Project(
    name="My uniquely named project about Alzheimer's Disease"
).get()
print(f"I just got my project: {my_project_object.name}, id: {my_project_object.id}")
