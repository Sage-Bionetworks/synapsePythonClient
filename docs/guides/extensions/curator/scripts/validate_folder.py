"""
Script: Validating folder annotations.
For file-based workflows, validates annotations on files
within a schema-bound folder.
"""

from synapseclient import Synapse
from synapseclient.models import Folder

syn = Synapse()
syn.login()

folder = Folder(id="syn987654321").get()

# Get summary statistics
stats = folder.get_schema_validation_statistics()
print(f"Valid: {stats.number_of_valid_children}")
print(f"Invalid: {stats.number_of_invalid_children}")

# Get details for invalid files
for result in folder.get_invalid_validation():
    print(f"Entity {result.object_id}: {result.validation_error_message}")
