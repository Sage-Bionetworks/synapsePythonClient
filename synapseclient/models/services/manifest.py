"""Functions for generating manifest CSV files from File entities."""

import csv
import datetime
import io
import os
from typing import TYPE_CHECKING, Any, Union

from synapseclient.core import utils

if TYPE_CHECKING:
    from synapseclient.models import File

MANIFEST_CSV_FILENAME = "manifest.csv"
DEFAULT_GENERATED_MANIFEST_CSV_KEYS = [
    "path",
    "parentId",
    "name",
    "ID",
    "synapseStore",
    "contentType",
    "used",
    "executed",
    "activityName",
    "activityDescription",
]


def _manifest_csv_filename(path: str) -> str:
    return os.path.join(os.path.expanduser(path), MANIFEST_CSV_FILENAME)


def _get_entity_provenance_dict_for_manifest(entity: File) -> dict[str, str]:
    """
    Gets the provenance metadata for the entity.

    Arguments:
        entity: A File entity object

    Returns:
        dict[str, str]: a dictionary with a subset of the provenance metadata for the entity.
              An empty dictionary is returned if the metadata does not have a provenance record.
    """
    if not entity.activity:
        return {}
    used = [a.format_for_manifest() for a in entity.activity.used]
    executed = [a.format_for_manifest() for a in entity.activity.executed]
    return {
        "used": ";".join(used),
        "executed": ";".join(executed),
        "activityName": entity.activity.name or "",
        "activityDescription": entity.activity.description or "",
    }


def _convert_manifest_data_items_to_string_list(
    items: list[Union[str, datetime.datetime, bool, int, float]],
) -> Union[str, list[str]]:
    """
    Handle coverting an individual key that contains a possible list of data into a
    list of strings or objects that can be written to the manifest file.

    This has specific logic around how to handle datetime fields.

    When working with datetime fields we are printing the ISO 8601 UTC representation of
    the datetime.

    When working with non strings we are printing the non-quoted version of the object.

    Example: Examples
        Several examples of how this function works.

            >>> _convert_manifest_data_items_to_string_list(["a", "b", "c"])
            '[a,b,c]'
            >>> _convert_manifest_data_items_to_string_list(["string,with,commas", "string without commas"])
            '["string,with,commas",string without commas]'
            >>> _convert_manifest_data_items_to_string_list(["string,with,commas"])
            'string,with,commas'
            >>> _convert_manifest_data_items_to_string_list
                ([datetime.datetime(2020, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)])
            '2020-01-01T00:00:00Z'
            >>> _convert_manifest_data_items_to_string_list([True])
            'True'
            >>> _convert_manifest_data_items_to_string_list([1])
            '1'
            >>> _convert_manifest_data_items_to_string_list([1.0])
            '1.0'
            >>> _convert_manifest_data_items_to_string_list
                ([datetime.datetime(2020, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc),
                datetime.datetime(2021, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)])
            '[2020-01-01T00:00:00Z,2021-01-01T00:00:00Z]'


    Args:
        items: The list of items to convert.

    Returns:
        The list of items converted to strings.
    """
    items_to_write = []
    for item in items:
        if isinstance(item, datetime.datetime):
            items_to_write.append(
                utils.datetime_to_iso(dt=item, include_milliseconds_if_zero=False)
            )
        else:
            # If a string based annotation has a comma in it
            # this will wrap the string in quotes so it won't be parsed
            # as multiple values. For example this is an annotation with 2 values:
            # [my first annotation, "my, second, annotation"]
            # This is an annotation with 4 value:
            # [my first annotation, my, second, annotation]
            if isinstance(item, str):
                if len(items) > 1 and "," in item:
                    items_to_write.append(f'"{item}"')
                else:
                    items_to_write.append(item)
            else:
                items_to_write.append(repr(item))

    if len(items_to_write) > 1:
        return f'[{",".join(items_to_write)}]'
    elif len(items_to_write) == 1:
        return items_to_write[0]
    else:
        return ""


def _extract_entity_metadata_for_manifest_csv(
    all_files: list[File],
) -> tuple[list[str], list[dict[str, Any]]]:
    """Extracts metadata from a list of File entities into a form usable by csv.DictWriter.

    Builds the column header list starting from DEFAULT_GENERATED_MANIFEST_CSV_KEYS, then
    appends any annotation keys discovered across all files. Each row dict contains the
    standard fields plus annotation values (serialized via
    _convert_manifest_data_items_to_string_list) and provenance fields from
    _get_entity_provenance_dict_for_manifest.

    Arguments:
        all_files: A list of File model objects to extract metadata from.

    Returns:
        A tuple of (keys, data) where keys is the ordered list of column headers and
        data is a list of row dicts, one per file.
    """
    keys = list(DEFAULT_GENERATED_MANIFEST_CSV_KEYS)
    annotation_keys: set = set()
    data = []
    for entity in all_files:
        row: dict = {
            "path": entity.path,
            "parentId": entity.parent_id,
            "name": entity.name,
            "ID": entity.id,
            "synapseStore": entity.synapse_store,
            "contentType": entity.content_type,
        }
        if entity.annotations:
            for key, val in entity.annotations.items():
                annotation_keys.add(key)
                row[key] = (
                    _convert_manifest_data_items_to_string_list(val)
                    if isinstance(val, list)
                    else val
                )
        row.update(_get_entity_provenance_dict_for_manifest(entity=entity))
        data.append(row)
    keys.extend(annotation_keys)
    return keys, data


def _convert_manifest_data_row_to_dict(row: dict, keys: list[str]) -> dict:
    """
    Convert a row of data to a dict that can be written to a manifest file.

    Args:
        row: The row of data to convert.
        keys: The keys of the manifest. Used to select the rows of data.

    Returns:
        The dict representation of the row.
    """
    data_to_write = {}
    for key in keys:
        data_for_key = row.get(key, "")
        if isinstance(data_for_key, list):
            items_to_write = _convert_manifest_data_items_to_string_list(data_for_key)
            data_to_write[key] = items_to_write
        else:
            data_to_write[key] = data_for_key
    return data_to_write


def _write_manifest_data_csv(path: str, keys: list[str], data: list[dict]) -> None:
    """Writes manifest data to a CSV file using csv.DictWriter with QUOTE_MINIMAL that automatically quotes any cell containing a comma, newline, or the quote character.

    Each row dict is normalized via _convert_manifest_data_row_to_dict so that
    list-valued annotation fields are serialized to strings before writing. Missing
    fields default to an empty string; extra keys not in fieldnames are silently ignored.

    Arguments:
        path: Absolute path of the CSV file to create or overwrite.
        keys: Ordered list of column headers used as DictWriter fieldnames.
        data: List of row dicts, one per file. Keys absent from a row are written as
            empty strings; keys not in fieldnames are ignored.
    """
    with io.open(path, "w", encoding="utf8", newline="") as fp:
        writer = csv.DictWriter(
            fp,
            fieldnames=keys,
            restval="",
            extrasaction="ignore",
            quoting=csv.QUOTE_MINIMAL,
        )
        writer.writeheader()
        for row in data:
            writer.writerow(_convert_manifest_data_row_to_dict(row, keys))


def generate_manifest_csv(all_files: list["File"], path: str) -> None:
    """Generates a manifest.csv file based on a list of File entities.

    The generated file uses CSV format with comma delimiter and is interoperable
    with the Synapse UI download cart. Column names follow the new convention:
    `parentId` (instead of `parent`) and `ID` (instead of `id`).

    Arguments:
        all_files: A list of File model objects.
        path: The directory path where manifest.csv will be written.
    """
    if path and all_files:
        filename = _manifest_csv_filename(path=path)
        keys, data = _extract_entity_metadata_for_manifest_csv(all_files=all_files)
        _write_manifest_data_csv(filename, keys, data)
