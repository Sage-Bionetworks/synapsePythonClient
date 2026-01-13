"""Typing utilities for optional dependencies.

This module provides type aliases for optional dependencies like pandas and numpy,
allowing proper type checking without requiring these packages to be installed.
"""

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    try:
        from pandas import DataFrame, Series
    except ImportError:
        DataFrame = Any  # type: ignore[misc, assignment]
        Series = Any  # type: ignore[misc, assignment]

    try:
        import numpy as np
    except ImportError:
        np = Any  # type: ignore[misc, assignment]

    try:
        import networkx as nx
    except ImportError:
        nx = Any  # type: ignore[misc, assignment]
else:
    # At runtime, use object as a placeholder
    DataFrame = object
    Series = object
    np = object  # type: ignore[misc, assignment]
    nx = object  # type: ignore[misc, assignment]

__all__ = ["DataFrame", "Series", "np", "nx"]
