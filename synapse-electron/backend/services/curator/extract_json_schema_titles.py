"""
Library functions to extract properties from a JSON schema file and create a DataFrame
with title fields as columns.

Usage:

# This will read a JSON schema from a file and create a DataFrame with the titles of the properties as columns.
extract_schema_properties_from_file("/path/to/json/file.json")
"""

import json
from typing import Any, Dict, List

import pandas as pd


def extract_property_titles(schema_data: Dict[str, Any]) -> List[str]:
    """
    Extract title fields from all properties in a JSON schema.

    Args:
        schema_data: The parsed JSON schema data

    Returns:
        List of title values from the properties
    """
    titles = []

    # Check if 'properties' exists in the schema
    if "properties" not in schema_data:
        return titles

    properties = schema_data["properties"]

    for property_name, property_data in properties.items():
        if isinstance(property_data, dict):
            if "title" in property_data:
                titles.append(property_data["title"])
            else:
                titles.append(property_name)

    return titles


def create_dataframe_from_titles(titles: List[str]) -> pd.DataFrame:
    """
    Create an empty DataFrame with the extracted titles as column names.

    Args:
        titles: List of title strings to use as column names

    Returns:
        Empty DataFrame with titles as columns
    """
    if not titles:
        return pd.DataFrame()

    df = pd.DataFrame(columns=titles)
    return df


def extract_schema_properties_from_dict(schema_data: Dict[str, Any]) -> pd.DataFrame:
    """
    Process a JSON schema dictionary and return a DataFrame with property titles as columns.

    Args:
        schema_data: The parsed JSON schema data as a dictionary

    Returns:
        DataFrame with property titles as columns
    """
    titles = extract_property_titles(schema_data)

    df = create_dataframe_from_titles(titles)

    return df


def extract_schema_properties_from_file(json_file_path: str) -> pd.DataFrame:
    """
    Process a JSON schema file and return a DataFrame with property titles as columns.

    Args:
        json_file_path: Path to the JSON schema file

    Returns:
        DataFrame with property titles as columns

    Raises:
        FileNotFoundError: If the JSON file doesn't exist
        json.JSONDecodeError: If the JSON file is malformed
        ValueError: If the file doesn't contain a valid schema structure
    """
    try:
        with open(json_file_path, "r", encoding="utf-8") as file:
            schema_data = json.load(file)

        return extract_schema_properties_from_dict(schema_data)

    except FileNotFoundError as e:
        raise FileNotFoundError(f"JSON schema file not found: {json_file_path}") from e
    except json.JSONDecodeError as e:
        raise json.JSONDecodeError(
            f"Invalid JSON in file '{json_file_path}': {e}", e.doc, e.pos
        )
