"""
Data models for grid session snapshots and per-row validation results.

These models represent the read-only view of a grid session's current state,
including row data and validation results extracted via WebSocket connection.
"""

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class GridRowValidation:
    """Per-row validation results from an active grid session.

    Attributes:
        is_valid: True if the row passes schema validation, False if invalid,
            None if validation has not been computed yet.
        validation_error_message: Summary error message if invalid.
        all_validation_messages: Detailed list of all validation errors
            (one per sub-schema violation).
        validation_status: Computed status: 'valid', 'invalid', or 'pending'
            (when data has been modified after the last validation).
    """

    is_valid: Optional[bool] = None
    """True if valid, False if invalid, None if not yet validated."""

    validation_error_message: Optional[str] = None
    """Summary error message if the row is invalid."""

    all_validation_messages: Optional[List[str]] = None
    """Detailed list of all validation errors."""

    validation_status: Optional[str] = None
    """Computed status: 'valid', 'invalid', or 'pending'."""


@dataclass
class GridRow:
    """A single row from a grid session with data and validation.

    Attributes:
        row_id: The logical row identifier in format 'replicaId.sequenceNumber'.
        data: The row's cell values as a dict mapping column name to value.
        validation: Per-row validation results, if available.
    """

    row_id: Optional[str] = None
    """The logical row identifier."""

    data: Optional[Dict[str, Any]] = None
    """The row's cell values as {column_name: value}."""

    validation: Optional[GridRowValidation] = None
    """Per-row validation results."""


@dataclass
class GridSnapshot:
    """Read-only snapshot of a grid session's current state.

    Contains the column names, row data, and per-row validation results
    extracted from the grid session via WebSocket connection.

    Attributes:
        column_names: Ordered list of column names in the grid.
        rows: List of GridRow objects with data and validation.
    """

    column_names: List[str] = field(default_factory=list)
    """Ordered list of column names in the grid."""

    rows: List[GridRow] = field(default_factory=list)
    """List of rows with data and validation results."""

    @property
    def total_rows(self) -> int:
        """Total number of rows in the grid."""
        return len(self.rows)

    @property
    def valid_rows(self) -> int:
        """Number of rows that pass validation."""
        return sum(
            1
            for row in self.rows
            if row.validation and row.validation.is_valid is True
        )

    @property
    def invalid_rows(self) -> int:
        """Number of rows that fail validation."""
        return sum(
            1
            for row in self.rows
            if row.validation and row.validation.is_valid is False
        )

    @property
    def pending_rows(self) -> int:
        """Number of rows where validation is pending or not yet computed."""
        return sum(
            1
            for row in self.rows
            if not row.validation
            or row.validation.is_valid is None
            or row.validation.validation_status == "pending"
        )

    @property
    def validation_summary(self) -> Dict[str, int]:
        """Returns a summary of validation counts.

        Returns:
            Dict with keys: total, valid, invalid, pending.
        """
        return {
            "total": self.total_rows,
            "valid": self.valid_rows,
            "invalid": self.invalid_rows,
            "pending": self.pending_rows,
        }
