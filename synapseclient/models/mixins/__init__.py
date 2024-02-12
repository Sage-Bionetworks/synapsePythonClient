"""References to the mixins that are used in the Synapse models."""

from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.mixins.storable_container import StorableContainer

__all__ = [
    "AccessControllable",
    "StorableContainer",
]
