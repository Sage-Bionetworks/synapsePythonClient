"""
Here is where you'll find the code for the Sharing Settings tutorial.

This tutorial demonstrates how to manage ACL (Access Control List) permissions
for entities in Synapse, including folders and their contents.

IMPORTANT: Before running this script, you MUST set the PRINCIPAL_ID variable
below to a valid Synapse user ID or team ID. You can find user IDs by looking
at their profile page in Synapse, or by using syn.getUserProfile().

Examples of principal IDs:
- A specific user ID (e.g., 123456)
- Authenticated users: 273948
- Public access: 273949
"""

import synapseclient
from synapseclient.models import Folder, Project

# REQUIRED: Set this to the Synapse user ID or team ID you want to grant permissions to
# Do NOT leave this as None - the script will not work properly
PRINCIPAL_ID = None  # Replace with actual user/team ID

if PRINCIPAL_ID is None:
    raise ValueError(
        "You must set PRINCIPAL_ID to a valid Synapse user ID or team ID before running this script. "
        "Examples: ###### (user ID), 273948 (authenticated users), 273949 (public)"
    )

syn = synapseclient.login()

# Step 1: Get or create a project for this tutorial
print("=== Step 1: Getting project ===")
project = Project(name="My uniquely named project about Alzheimer's Disease").get()

# Step 2: Create a main folder to set custom sharing settings
print("\n=== Step 2: Creating main to set custom permissions ===")
main_folder = Folder(
    name="Research Data",
    description="Main folder for research data with custom sharing settings",
    parent_id=project.id,
)
main_folder = main_folder.store()
print(f"Created main folder: {main_folder.name} (ID: {main_folder.id})")

# Step 3: Demonstrate get_permissions() - Get current user's permissions
print("\n=== Step 3: Getting current user's permissions ===")
permissions = main_folder.get_permissions()
print(f"Current user permissions on main folder: {permissions.access_types}")

# Step 4: Demonstrate get_acl() - Get ACL for specific principal
print("\n=== Step 4: Getting ACL for specific principal ===")
# First check what permissions the principal currently has (likely inherited from project)
current_acl = main_folder.get_acl(principal_id=PRINCIPAL_ID, check_benefactor=True)
print(
    f"Principal {PRINCIPAL_ID} current permissions (including inherited): {current_acl}"
)

# Check ACL only on the folder itself (not inherited)
folder_only_acl = main_folder.get_acl(principal_id=PRINCIPAL_ID, check_benefactor=False)
print(f"Principal {PRINCIPAL_ID} permissions on folder only: {folder_only_acl}")

# Step 5: Demonstrate set_permissions() - Set specific permissions for the main folder
print("\n=== Step 5: Setting permissions on main folder ===")
main_folder_permissions = ["READ", "DOWNLOAD"]
main_folder.set_permissions(
    principal_id=PRINCIPAL_ID,
    access_type=main_folder_permissions,
    modify_benefactor=False,  # Create local ACL for this folder
    overwrite=True,
)
print(
    f"Set permissions for principal {PRINCIPAL_ID} on main folder: {main_folder_permissions}"
)

# Verify the permissions were set
new_acl = main_folder.get_acl(principal_id=PRINCIPAL_ID, check_benefactor=False)
print(f"Verified new permissions: {new_acl}")

# Step 6: Create a sub-folder with different sharing settings
print("\n=== Step 6: Creating sub-folder with different permissions ===")
sub_folder = Folder(
    name="Sensitive Analysis",
    description="Sub-folder with more restrictive permissions",
    parent_id=main_folder.id,
)
sub_folder = sub_folder.store()
print(f"Created sub-folder: {sub_folder.name} (ID: {sub_folder.id})")

# Set more restrictive permissions on the sub-folder
sub_folder_permissions = ["READ"]  # More restrictive - only read access
sub_folder.set_permissions(
    principal_id=PRINCIPAL_ID,
    access_type=sub_folder_permissions,
    modify_benefactor=False,
    overwrite=True,
)
print(
    f"Set more restrictive permissions for principal {PRINCIPAL_ID} on sub-folder: {sub_folder_permissions}"
)

# Step 7: Demonstrate list_acl() - List all ACLs
print("\n=== Step 7: Listing ACLs ===")

# List ACLs recursively for main folder and all contents
print("Folder ACL:")
recursive_acl_result = main_folder.list_acl(
    recursive=True,
    include_container_content=True,
    log_tree=True,  # This will also log the tree structure
)
print(f"Folder ({main_folder.id}) ACL entries:")
for entity_acl in recursive_acl_result.all_entity_acls:
    print(f"\nEntity {entity_acl.entity_id} ACL:")
    for acl_entry in entity_acl.acl_entries:
        print(f"  Principal {acl_entry.principal_id}: {acl_entry.permissions}")

# Step 8: Demonstrate additional set_permissions() options
print("\n=== Step 8: Demonstrating additional permission options ===")

# Add permissions non-destructively (overwrite=False)
print("Adding additional permissions non-destructively...")
main_folder.set_permissions(
    principal_id=PRINCIPAL_ID,
    access_type=["UPDATE"],  # Add UPDATE permission
    overwrite=False,  # Don't overwrite existing permissions
)

# Verify the permissions were added
updated_acl = main_folder.get_acl(principal_id=PRINCIPAL_ID)
print(f"Updated permissions after adding UPDATE: {updated_acl}")

# Step 9: Demonstrate permission removal using set_permissions with empty list
print("\n=== Step 9: Removing specific permissions ===")
print(
    f"Before removal - Principal {PRINCIPAL_ID} permissions: {main_folder.get_acl(principal_id=PRINCIPAL_ID)}"
)

# Remove permissions for specific principal by setting empty access_type
main_folder.set_permissions(
    principal_id=PRINCIPAL_ID,
    access_type=[],  # Empty list removes all permissions for this principal
    overwrite=True,
)

removed_acl = main_folder.get_acl(principal_id=PRINCIPAL_ID)
print(f"After removal - Principal {PRINCIPAL_ID} permissions: {removed_acl}")

# Step 10: Demonstrate delete_permissions() with dry run
print("\n=== Step 10: Demonstrating delete_permissions() ===")

# First, let's set some permissions back so we have something to delete
sub_folder.set_permissions(principal_id=PRINCIPAL_ID, access_type=["READ", "UPDATE"])
print("Re-added permissions to sub-folder for demonstration")

# Dry run - see what would be deleted without actually deleting
print("\nDry run - showing what would be deleted:")
sub_folder.delete_permissions(dry_run=True, show_acl_details=True)

# Actually delete the permissions (this will make the sub-folder inherit from parent)
print("\nActually deleting ACL from sub-folder:")
sub_folder.delete_permissions(include_self=True)

# Verify inheritance
inherited_acl = sub_folder.get_acl(principal_id=PRINCIPAL_ID)
print(f"Sub-folder now inherits permissions: {inherited_acl}")

# Step 11: Final ACL overview
print("\n=== Step 11: Final ACL overview ===")
final_overview = main_folder.list_acl(
    recursive=True, include_container_content=True, log_tree=True
)
