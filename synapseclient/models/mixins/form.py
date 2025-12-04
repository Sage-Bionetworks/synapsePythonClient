from dataclasses import dataclass
from typing import Any, Optional

from synapseclient.core.async_utils import async_to_sync


@dataclass
@async_to_sync
class FormGroup:
    group_id: Optional[str] = None
    """Unique identifier provided by the system."""

    name: Optional[str] = None
    """Unique name for the group provided by the caller."""

    created_by: Optional[str] = None
    """Id of the user that created this group"""

    created_on: Optional[str] = None
    """The date this object was originally created."""

    def fill_from_dict(self, synapse_response: dict[str, Any]) -> "FormGroup":
        """Converts a response from the REST API into this dataclass."""
        self.group_id = synapse_response.get("groupId", None)
        self.name = synapse_response.get("name", None)
        self.created_by = synapse_response.get("createdBy", None)
        self.created_on = synapse_response.get("createdOn", None)

        return self
