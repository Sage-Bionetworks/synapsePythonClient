from synapseclient import Synapse
from synapseclient.models import Project
from synapseclient.operations import get

"""This number represents a safeguard against infinite loops when traversing the folder hierarchy to find the project ID."""
MAX_HIERARCHY_DEPTH = 1000


def project_id_from_entity_id(entity_id: str, synapse_client: Synapse) -> str:
    """
    Retrieves the project ID from a given entity ID by traversing up the folder hierarchy

    Args:
        entity_id: The Synapse ID of the entity (e.g., folder, file) to start from.
        synapse_client: Authenticated Synapse client instance

    Returns:
        The Synapse ID of the project that the entity belongs to.

    Raises:
        ValueError: If the project ID cannot be found within 1000 iterations.
    """

    # Get the project ID from the folder ID
    current_obj = get(entity_id, synapse_client=synapse_client)
    iterations = 0
    while not isinstance(current_obj, Project):
        current_obj = get(current_obj.parent_id, synapse_client=synapse_client)
        iterations += 1
        if iterations > MAX_HIERARCHY_DEPTH:
            raise ValueError("Could not find project ID in folder hierarchy")
    return current_obj.id


def validate_column_order_list(column_order_list: list[str] | None) -> list[str]:
    """
    Validate the shape of a caller supplied column order value.

    This checks everything that can be checked without knowing which columns are
    actually available: that the value is a list, that every entry is a non-empty
    string, and that no entry is repeated. Use this to fail fast before any entities
    are created in Synapse. The check that every requested column actually exists is
    performed later by resolve_column_order_list.

    Arguments:
        column_order_list: The caller supplied column order, or None.

    Returns:
        The column order as a list. An empty list is returned when
        column_order_list is None.

    Raises:
        ValueError: If column_order_list is not a list, contains a non-string or
            empty value, or contains duplicate values.
    """
    if column_order_list is None:
        return []

    if not isinstance(column_order_list, list):
        raise ValueError(
            "column_order must be a list of column names, but received "
            f"{type(column_order_list).__name__}."
        )

    invalid_values = [
        value for value in column_order_list if not isinstance(value, str) or not value
    ]
    if invalid_values:
        raise ValueError(
            "column_order must contain only non-empty strings. The following values "
            f"are not valid column names: {invalid_values}."
        )

    seen = set()
    duplicates = []
    for value in column_order_list:
        if value in seen and value not in duplicates:
            duplicates.append(value)
        seen.add(value)
    if duplicates:
        raise ValueError(
            f"column_order contains duplicate values: {duplicates}. Each column may "
            "only be listed once."
        )

    return list(column_order_list)


def resolve_column_order_list(
    available_columns: list[str],
    pinned_columns: list[str],
    requested_columns: list[str] | None = None,
) -> list[str]:
    """
    Return pinned, requested, and remaining columns in final display order.

    The resulting order is:

    1. The pinned columns, in the order given.
    2. The requested columns that were not already pinned, in the order given.
    3. The remaining available columns, in their existing relative order.

    A column never appears more than once, so including a pinned column in
    requested_columns leaves it in its pinned position.

    Callers are responsible for making sure every pinned column is present among the
    available columns.

    Arguments:
        available_columns: Every column that may appear in the final order.
        pinned_columns: Columns that must lead the final order, in the desired order.
        requested_columns: Optional caller supplied ordering applied after the pinned
            columns.

    Returns:
        The full list of column names in their final display order.

    Raises:
        ValueError: If requested_columns fails validate_column_order_list, or if
            it names a column that is not among available_columns.
    """
    requested_columns = validate_column_order_list(requested_columns)

    available_set = set(available_columns)
    unknown_columns = [
        column for column in requested_columns if column not in available_set
    ]
    if unknown_columns:
        raise ValueError(
            "The following column_order values were not found among the available "
            f"columns: {unknown_columns}."
        )

    ordered_columns: list[str] = []
    placed = set()
    for column in list(pinned_columns) + requested_columns + list(available_columns):
        if column not in placed:
            ordered_columns.append(column)
            placed.add(column)

    return ordered_columns
