"""Functions for generating manifest CSV files from File entities."""

import csv
import io
import os
from typing import TYPE_CHECKING, Dict, List, Tuple

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


def _get_entity_provenance_dict_for_manifest(entity: "File") -> Dict[str, str]:
    """
    Get the provenance metadata for the entity.
    Arguments:
        entity: Entity object

    Returns:
        Dict[str, str]: a dictionary with a subset of the provenance metadata for the entity.
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


def _extract_entity_metadata_for_manifest_csv(
    all_files: List["File"],
) -> Tuple[List[str], List[Dict]]:
    """
    Extracts metadata from the list of File Entities and returns them in a form
    usable by csv.DictWriter

    Arguments:
        all_files: an iterable that provides File entities

    Returns:
        keys: a list column headers
        data: a list of dicts containing data from each row
    """
    from synapseutils.sync import _convert_manifest_data_items_to_string_list

    keys = list(DEFAULT_GENERATED_MANIFEST_CSV_KEYS)
    annotation_keys: set = set()
    data = []
    for entity in all_files:
        row: Dict = {
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


def _write_manifest_data_csv(filename: str, keys: List[str], data: List[Dict]) -> None:
    """
    Write the manifest data to a CSV file.
    Arguments:
        filename: The name of the file to write to.
        keys: The keys of the manifest.
        data: The data to write to the manifest. This should be a list of dicts where
            each dict represents a row of data.
    Returns:
        None
    """
    from synapseutils.sync import _convert_manifest_data_row_to_dict

    with io.open(filename, "w", encoding="utf8", newline="") as fp:
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


def generate_manifest_csv(all_files: List["File"], path: str) -> None:
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
