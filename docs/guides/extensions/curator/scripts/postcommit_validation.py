"""
Script: Post-commit validation via RecordSet export.
Exports grid session to RecordSet (commits changes) and retrieves
detailed per-row validation results.
"""

from synapseclient import Synapse
from synapseclient.models import Grid, RecordSet

syn = Synapse()
syn.login()

grid = Grid(record_set_id="syn987654321")
grid = grid.create()

# Export to RecordSet (commits changes + generates validation)
grid = grid.export_to_record_set()

if grid.validation_summary_statistics:
    stats = grid.validation_summary_statistics
    print(f"Valid: {stats.number_of_valid_children}")
    print(f"Invalid: {stats.number_of_invalid_children}")

# Clean up the grid session
grid.delete()

# Get detailed per-row validation from the RecordSet
record_set = RecordSet(id="syn987654321").get()
validation_df = record_set.get_detailed_validation_results()

if validation_df is not None:
    invalid = validation_df[validation_df["is_valid"] == False]  # noqa: E712
    for _, row in invalid.iterrows():
        print(f"Row {row['row_index']}: {row['validation_error_message']}")
