from dataclasses import dataclass
from typing import Optional

from synapseclient.team import UserGroupHeader as Synapse_UserGroupHeader


@dataclass
class UserGroupHeader:
    """
    Select metadata about a Synapse principal.
    In practice the constructor is not called directly by the client.

    Attributes:
        owner_id: A foreign key to the ID of the 'principal' object for the user.
        first_name: First Name
        last_name: Last Name
        user_name: A name chosen by the user that uniquely identifies them.
        email: User's current email address
        is_individual: True if this is a user, false if it is a group
    """

    owner_id: Optional[int] = None
    """A foreign key to the ID of the 'principal' object for the user."""

    first_name: Optional[str] = None
    """First Name"""

    last_name: Optional[str] = None
    """Last Name"""

    user_name: Optional[str] = None
    """A name chosen by the user that uniquely identifies them."""

    is_individual: Optional[bool] = None
    """True if this is a user, false if it is a group"""

    email: Optional[str] = None
    """User's current email address"""

    def fill_from_dict(
        self, synapse_user_group_header: Synapse_UserGroupHeader
    ) -> "UserGroupHeader":
        self.owner_id = synapse_user_group_header.get("ownerId", None)
        self.first_name = synapse_user_group_header.get("firstName", None)
        self.last_name = synapse_user_group_header.get("lastName", None)
        self.user_name = synapse_user_group_header.get("userName", None)
        self.email = synapse_user_group_header.get("email", None)
        self.is_individual = synapse_user_group_header.get("isIndividual", None)
        return self
