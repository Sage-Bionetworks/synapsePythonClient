"""
Services package for Synapse Desktop Client backend.

This package contains service classes that handle business logic
and external service interactions.
"""

from .config_service import ConfigManager
from .synapse_service import SynapseClientManager

__all__ = [
    "ConfigManager",
    "SynapseClientManager",
]
