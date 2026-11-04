"""Tutorial code for Index and migrate files to the new storage location"""

# --8<-- [start:setup]
import synapseclient
from synapseclient.models import Folder, Project

syn = synapseclient.login()
my_project = Project(name="My uniquely named project about Alzheimer's Disease").get()
# --8<-- [end:setup]
MY_S3_STORAGE_LOCATION_ID = "1234567890"
# --8<-- [start:index_and_migrate_files]
# WARNING: This will actually migrate files associated with the project/folder.
# Run against a test project first and review the index (MigrationResult) before
# migrating production data.
my_migration_folder = Folder(
    name="my-data-migration-folder", parent_id=my_project.id
).get()
index_result = my_migration_folder.index_files_for_migration(
    dest_storage_location_id=MY_S3_STORAGE_LOCATION_ID,
    db_path="/path/to/your/migration.db",
    include_table_files=False,  # Set True if you also want table-attached files
)
index_result.as_csv("/path/to/your/index_results.csv")
print(f"Migration index database: {index_result.db_path}")
print(f"Indexed counts by status: {index_result.counts_by_status}")

migrate_result = my_migration_folder.migrate_indexed_files(
    db_path="/path/to/your/migration.db",
    continue_on_error=True,
    force=True,  # Skip interactive confirmation for tutorial purposes
)
migrate_result.as_csv("/path/to/your/migrate_results.csv")
if migrate_result is not None:
    print(f"Migrated counts by status: {migrate_result.counts_by_status}")
else:
    print("Migration was aborted (confirmation declined).")
# --8<-- [end:migrate_indexed_files]
