"""
Query the Synapse schema registry table to retrieve Schema URIs based on configurable filters.

This module provides library functions for querying schema registry tables to find matching
schemas based on any number of configurable filter parameters. Results are sorted by version
and the URIs are returned as a list of strings. The module supports flexible column mappings
and filtering criteria.

Default Schema Registry Table Structure:
    The default schema registry table (syn69735275) contains the following columns:
    - dcc: STRING - Data Coordination Center identifier (e.g., 'ad', 'amp', 'mc2', 'veo')
    - datatype: STRING - Data type name from the schema (e.g., 'Analysis', 'Biospecimen')
    - version: STRING or DOUBLE - Schema version for sorting (e.g., '0.0.0', '12.0.0')
    - uri: STRING - JSON schema URI identifier (e.g., 'sage.schemas.v2571-ad.Analysis.schema-0.0.0')

    Additional columns may be present in the table and can be used for filtering.

    Sample data from the schema registry table:
    ```
    dcc | datatype    | version | uri
    ad  | Analysis    | 0.0.0   | sage.schemas.v2571-ad.Analysis.schema-0.0.0
    amp | Biospecimen | 0.0.1   | sage.schemas.v2571-amp.Biospecimen.schema-0.0.1
    mc2 | Biospecimen | 9.0.0   | sage.schemas.v2571-mc2.Biospecimen.schema-9.0.0
    veo | Biospecimen | 0.3.0   | sage.schemas.v2571-veo.Biospecimen.schema-0.3.0
    MC2 | Biospecimen | 12.0.0  | MultiConsortiaCoordinatingCenter-Biospecimen-12.0.0

    Example URIs returned by queries:
    - "sage.schemas.v2571-ad.Analysis.schema-0.0.0"
    - "sage.schemas.v2571-amp.Biospecimen.schema-0.0.1"
    - "MultiConsortiaCoordinatingCenter-Biospecimen-12.0.0"
    ```

Custom Schema Registry Tables:
    If using a custom schema registry table, it should contain at minimum:
    - A version column for sorting results (configurable name, default: 'version')
    - A URI column containing schema URIs (configurable name, default: 'uri')
    - Any number of filterable columns (configurable names and mappings)

    Column names and filterable parameters can be customized using the
    SchemaRegistryColumnConfig class.

Flexible Filtering System:
    The query functions use a flexible filtering system that allows you to search for
    schemas based on any combination of criteria. Here's how it works:

    Filter Parameters:
        - Any column name in the schema registry table can be used as a filter parameter
        - Simply pass the column name as a keyword argument to the query functions
        - Common filters include: dcc, datatype, version, uri
        - Additional columns in your table can also be used for filtering

    Filter Values:
        - Exact Match: Use a simple string value (e.g., dcc="ad")
        - Pattern Match: Use SQL LIKE patterns with % and _ wildcards:
          * % matches any sequence of characters (e.g., datatype="%spec%" matches "Biospecimen")

    Filter Logic:
        - Multiple filters are combined with AND logic
        - All specified filters must match for a schema to be included in results
        - At least one filter parameter must be provided

    Available Filters (based on table columns):
        - dcc: Data Coordination Center identifier (exact values: 'ad', 'amp', 'mc2', etc.)
        - datatype: Schema data type name (e.g., 'Analysis', 'Biospecimen')
        - version: Schema version (e.g., '0.0.0', '12.0.0')
        - uri: Schema URI identifier
        - Any other column present in your schema registry table
"""

from dataclasses import dataclass
from typing import List, Optional, Union

from synapseclient import Synapse
from synapseclient.models import Table

# The Synapse ID of the schema registry table
SCHEMA_REGISTRY_TABLE_ID = "syn69735275"


@dataclass
class SchemaRegistryColumnConfig:
    """
    Configuration for schema registry table column names.

    This class allows you to customize the column names used for version sorting
    and URI extraction when working with different schema registry table structures.

    Attributes:
        version_column: Name of the column containing schema version information.
                       Used for sorting results (newest versions first).
                       Default: 'version'

        uri_column: Name of the column containing the JSON schema URI.
                   This is what gets returned in query results.
                   Default: 'uri'

    Usage:
        # Default configuration (works with standard schema registry table)
        config = SchemaRegistryColumnConfig()
        # Uses 'version' column for sorting, 'uri' column for results

        # Custom table with different column names
        config = SchemaRegistryColumnConfig(
            version_column="schema_version",    # Table has 'schema_version' not 'version'
            uri_column="schema_uri"            # Table has 'schema_uri' not 'uri'
        )

    Filtering:
        Any column in the schema registry table can be used for filtering by passing it
        as a keyword argument to query functions. No configuration is needed - just use
        the actual column name from your table:

        # Examples (assuming these columns exist in your table):
        query_schema_registry(dcc="ad", datatype="Analysis")
        query_schema_registry(status="approved", author="john.doe")
        query_schema_registry(org="sage.schemas.v2571", version="0.0.0")
    """

    version_column: str = "version"
    uri_column: str = "uri"


def query_schema_registry(
    synapse_client: Optional[Synapse] = None,
    schema_registry_table_id: Optional[str] = None,
    column_config: Optional[SchemaRegistryColumnConfig] = None,
    return_latest_only: bool = True,
    **filters,
) -> Union[str, List[str], None]:
    """
    Query the schema registry table to find schemas matching the provided filters.

    This function searches the Synapse schema registry table for schemas that match
    the provided filter parameters. Results are sorted by version in descending order
    (newest first). The function supports any number of filter parameters as long as
    they are configured in the column_config.

    Arguments:
        synapse_client: Optional authenticated Synapse client instance
        schema_registry_table_id: Optional Synapse ID of the schema registry table.
                                  If None, uses the default table ID.
        column_config: Optional configuration for custom column names.
                      If None, uses default configuration ('version' and 'uri' columns).
        return_latest_only: If True (default), returns only the latest URI as a string.
                           If False, returns all matching URIs as a list of strings.
        **filters: Filter parameters to search for matching schemas. These work as follows:

                  Column-Based Filtering:
                  - Any column name in the schema registry table can be used as a filter
                  - Pass column names directly as keyword arguments
                  - Common filters: dcc, datatype, version, uri
                  - Any additional columns in your table can be used

                  Filter Values:
                  - Exact matching: Use plain strings (e.g., dcc="ad")
                  - Pattern matching: Use SQL LIKE patterns with wildcards:
                    * % = any sequence of characters
                  - Examples:
                    * dcc="ad" → matches exactly "ad"
                    * datatype="%spec%" → matches any datatype containing "spec"

                  Filter Logic:
                  - Multiple filters are combined with AND (all must match)
                  - At least one filter must be provided

    Returns:
        If return_latest_only is True: Single URI string of the latest version, or None if not found
        If return_latest_only is False: List of URI strings sorted by version (highest version first)

    Raises:
        ValueError: If no filter parameters are provided

    Expected Table Structure:
        The schema registry table should contain columns for:

        - Schema version for sorting (default: 'version')
        - JSON schema URI (default: 'uri')
        - Any filterable columns as configured in column_config

        Additional columns may be present and will be included in results.

    Example: Comprehensive filter usage demonstrations
        This includes several examples of how to use the filtering system.

        Basic Filtering (using default filters):
        ```python
        from synapseclient import Synapse
        from synapseclient.extensions.curator import query_schema_registry

        syn = Synapse()
        syn.login()

        # 1. Get latest schema URI for a specific DCC and datatype
        latest_uri = query_schema_registry(
            synapse_client=syn,
            dcc="ad",  # Exact match for Alzheimer's Disease DCC
            datatype="Analysis"  # Exact datatype match
        )
        # Returns: "sage.schemas.v2571-ad.Analysis.schema-0.0.0"

        # 2. Get all versions of matching schemas (not just latest)
        all_versions = query_schema_registry(
            synapse_client=syn,
            dcc="mc2",
            datatype="Biospecimen",
            return_latest_only=False
        )
        # Returns: ["MultiConsortiaCoordinatingCenter-Biospecimen-12.0.0",
        #           "sage.schemas.v2571-mc2.Biospecimen.schema-9.0.0"]

        # 3. Pattern matching with wildcards
        # Find all "Biospecimen" schemas across all DCCs
        biospecimen_schemas = query_schema_registry(
            synapse_client=syn,
            datatype="Biospecimen",  # Exact match for Biospecimen
            return_latest_only=False
        )
        # Returns: ["MultiConsortiaCoordinatingCenter-Biospecimen-12.0.0",
        #           "sage.schemas.v2571-mc2.Biospecimen.schema-9.0.0",
        #           "sage.schemas.v2571-veo.Biospecimen.schema-0.3.0",
        #           "sage.schemas.v2571-amp.Biospecimen.schema-0.0.1"]

        # 4. Pattern matching for DCC variations
        mc2_schemas = query_schema_registry(
            synapse_client=syn,
            dcc="%C2",  # Matches 'mc2' and 'MC2'
            return_latest_only=False
        )
        # Returns schemas from both 'mc2' and 'MC2' DCCs

        # 5. Using additional columns for filtering (if they exist in your table)
        specific_schemas = query_schema_registry(
            synapse_client=syn,
            dcc="amp",  # Must be AMP DCC
            org="sage.schemas.v2571",  # Must match organization
            return_latest_only=False
        )
        # Returns schemas that match BOTH conditions
        ```

        Direct Column Filtering (simplified approach):
        ```python
        # Any column in the schema registry table can be used for filtering
        # Just use the column name directly as a keyword argument

        # Basic filters using standard columns
        query_schema_registry(dcc="ad", datatype="Analysis")
        query_schema_registry(version="0.0.0")
        query_schema_registry(uri="sage.schemas.v2571-ad.Analysis.schema-0.0.0")

        # Additional columns (if they exist in your table)
        query_schema_registry(org="sage.schemas.v2571")
        query_schema_registry(name="ad.Analysis.schema")

        # Multiple column filters (all must match)
        query_schema_registry(
            dcc="mc2",
            datatype="Biospecimen",
            org="MultiConsortiaCoordinatingCenter"
        )
        ```

        Filter Value Examples with Real Data:
        ```python
        # Exact matching
        query_schema_registry(dcc="ad")                   # Returns schemas with dcc="ad"
        query_schema_registry(datatype="Biospecimen")     # Returns schemas with datatype="Biospecimen"
        query_schema_registry(dcc="MC2")                  # Returns schemas with dcc="MC2" (case sensitive)

        # Pattern matching with wildcards
        query_schema_registry(dcc="%C2")                   # Matches "mc2", "MC2"
        query_schema_registry(datatype="%spec%")           # Matches "Biospecimen"

        # Examples with expected results:
        query_schema_registry(dcc="ad", datatype="Analysis")
        # Returns: "sage.schemas.v2571-ad.Analysis.schema-0.0.0"

        query_schema_registry(datatype="Biospecimen", return_latest_only=False)
        # Returns: ["MultiConsortiaCoordinatingCenter-Biospecimen-12.0.0",
        #           "sage.schemas.v2571-mc2.Biospecimen.schema-9.0.0", ...]

        # Multiple conditions (all must be true)
        query_schema_registry(
            dcc="amp",             # AND
            datatype="Biospecimen", # AND
            org="sage.schemas.v2571"  # AND (if org column exists)
        )
        # Returns: ["sage.schemas.v2571-amp.Biospecimen.schema-0.0.1"]
        ```
    """
    syn = Synapse.get_client(synapse_client=synapse_client)
    logger = syn.logger

    # Use provided table ID or default
    table_id = (
        schema_registry_table_id
        if schema_registry_table_id
        else SCHEMA_REGISTRY_TABLE_ID
    )

    # Use provided column config or default
    if column_config is None:
        column_config = SchemaRegistryColumnConfig()

    # Validate that we have at least one filter
    if not filters:
        raise ValueError("At least one filter parameter must be provided")

    # Build WHERE clause from filters using column names directly
    where_conditions = []
    for column_name, filter_value in filters.items():
        # Check if the value contains SQL wildcards (% or _)
        if isinstance(filter_value, str) and (
            "%" in filter_value or "_" in filter_value
        ):
            # Use LIKE for pattern matching
            where_conditions.append(f"{column_name} LIKE '{filter_value}'")
        else:
            # Use exact match
            where_conditions.append(f"{column_name} = '{filter_value}'")

    where_clause = " AND ".join(where_conditions)

    # Construct SQL query using configurable column names
    # Results are sorted by version in descending order (newest first)
    query = f"""
    SELECT * FROM {table_id}
    WHERE {where_clause}
    ORDER BY {column_config.version_column} DESC
    """

    # Create a readable filter summary for logging
    filter_summary = ", ".join([f"{k}='{v}'" for k, v in filters.items()])

    logger.info(f"Querying schema registry with filters: {filter_summary}")
    logger.info(f"Using table: {table_id}")
    logger.info(f"SQL Query: {query}")

    # Query the table and get results as a pandas DataFrame
    table = Table(id=table_id)
    results_df = table.query(query=query, synapse_client=syn)

    if results_df.empty:
        logger.info(f"No schemas found matching filters: {filter_summary}")
        return None if return_latest_only else []

    # Extract URIs from the results and return as a list of strings
    uri_list = results_df[column_config.uri_column].tolist()

    logger.info(f"Found {len(uri_list)} matching schema(s):")
    for i, uri in enumerate(uri_list, 1):
        logger.info(f"  {i}. URI: {uri}")

    if return_latest_only:
        return uri_list[0] if uri_list else None
    else:
        return uri_list


def get_latest_schema_uri(
    synapse_client: Optional[Synapse] = None,
    schema_registry_table_id: Optional[str] = None,
    column_config: Optional[SchemaRegistryColumnConfig] = None,
    **filters,
) -> Optional[str]:
    """
    Get the URI of the latest schema version for the given filter criteria.

    This function queries the schema registry and returns the URI of the most recent
    version of the schema that matches the provided filter parameters. This is equivalent
    to calling query_schema_registry with return_latest_only=True.

    Arguments:
        synapse_client: Optional authenticated Synapse client instance
        schema_registry_table_id: Optional Synapse ID of the schema registry table.
                                  If None, uses the default table ID.
        column_config: Optional configuration for custom column names.
                      If None, uses default configuration ('version' and 'uri' columns).
        **filters: Filter parameters to match against the table. Column names are used directly
                  as filter parameters.

    Returns:
        URI string of the latest schema version, or None if not found

    Example:
        Get the latest schema URI using direct column filtering:

        ```python
        import synapseclient
        from synapseclient.extensions.curator import get_latest_schema_uri

        syn = synapseclient.login()

        latest_uri = get_latest_schema_uri(
            synapse_client=syn,
            dcc="ad",
            datatype="Analysis"
        )

        if latest_uri:
            print(f"Latest schema URI: {latest_uri}")
        else:
            print("No schema found for the specified criteria")
        ```
    """
    syn = Synapse.get_client(synapse_client=synapse_client)
    logger = syn.logger

    # Use query_schema_registry with return_latest_only=True (default)
    result = query_schema_registry(
        synapse_client=synapse_client,
        schema_registry_table_id=schema_registry_table_id,
        column_config=column_config,
        return_latest_only=True,
        **filters,
    )

    if result:
        logger.info(f"Latest schema URI: {result}")
        return result
    else:
        # Create a readable filter summary for logging
        filter_summary = ", ".join([f"{k}='{v}'" for k, v in filters.items()])
        logger.info(f"No schema found matching filters: {filter_summary}")
        return None
