"""
Script: Pre-commit validation via WebSocket snapshot.
Gets per-row validation results from an active grid session
WITHOUT committing changes.
"""

from synapseclient import Synapse
from synapseclient.models import Grid

syn = Synapse()
syn.login()

grid = Grid(record_set_id="syn987654321")
grid = grid.create()

# (Import data into the grid first — see grid_session_operations.py)

# Get validation results without committing
snapshot = grid.get_snapshot()

print(f"Validation summary: {snapshot.validation_summary}")
# Example output: {'total': 100, 'valid': 85, 'invalid': 12, 'pending': 3}

# Inspect individual row validation
for row in snapshot.rows:
    if row.validation and not row.validation.is_valid:
        print(f"Row {row.row_id}: {row.validation.validation_error_message}")
        for msg in row.validation.all_validation_messages or []:
            print(f"  - {msg}")
