"""
Utility functions for Synapse telemetry.
"""

import re
from typing import Optional

# TODO: Is this function being used in the correct spot?


def detect_storage_provider(url_or_path: str) -> Optional[str]:
    """
    Detect the storage provider based on a URL or path.

    Args:
        url_or_path: The URL or path to analyze

    Returns:
        The detected storage provider name or None if unknown
    """
    if not url_or_path:
        return None

    # Check for known URL patterns
    if url_or_path.startswith("s3://") or ".s3." in url_or_path or ".amazonaws.com" in url_or_path:
        return "S3"
    elif url_or_path.startswith("sftp://") or "@" in url_or_path and ":" in url_or_path:
        return "SFTP"
    elif re.search(r"\.blob\.core\.windows\.net", url_or_path) or url_or_path.startswith("azure://"):
        return "Azure"
    elif ".storage.googleapis.com" in url_or_path or url_or_path.startswith("gs://"):
        return "GCS"
    elif url_or_path.startswith("file://") or "/" in url_or_path and not re.search(r"^[a-zA-Z]+://", url_or_path):
        return "Local"
    elif "synapse.org" in url_or_path or url_or_path.startswith("syn"):
        return "Synapse"

    return None
