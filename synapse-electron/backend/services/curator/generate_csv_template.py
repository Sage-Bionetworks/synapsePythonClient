"""
generate_csv_template.py - Generate and upload CSV templates for record-based metadata

Notes:
    Requires conflicting versions of schematicpy and synapseclient.
    Install schematicpy dependencies first, then uninstall synapseclient and reinstall with:
    pip install git+https://github.com/Sage-Bionetworks/synapsePythonClient.git@synpy-1653-metadata-tasks-and-recordsets

Configuration:
    You can either:
    1. Set default values in the constants at the top of this file
    2. Pass arguments via command line (will override constants)
    3. Use a combination of both

Usage:
    # Using constants set in file (no args needed if all constants are set):
    python generate_csv_template.py

    # Override specific constants with command-line args:
    python generate_csv_template.py --folder-id syn12345678 --dcc AD

    # Use all command-line args (if constants are not set):
    python generate_csv_template.py --folder-id syn12345678 --dcc AD \\
        --datatype BiospecimenMetadataTemplate \\
        --data_model_jsonld /path/to/model.json --schema_uri schema_uri
"""

import argparse
import tempfile
from pprint import pprint

from services.curator.extract_json_schema_titles import (
    extract_schema_properties_from_file,
)

import synapseclient
from synapseclient.models import (
    CurationTask,
    Grid,
    RecordBasedMetadataTaskProperties,
    RecordSet,
)

# Default constants - can be overridden by command-line arguments
DEFAULT_FOLDER_ID = None  # Set to your default folder ID, e.g., "syn12345678"
DEFAULT_DCC = None  # Set to your default DCC, e.g., "AD"
DEFAULT_DATATYPE = (
    None  # Set to your default datatype, e.g., "BiospecimenMetadataTemplate"
)
DEFAULT_DATA_MODEL_JSONLD = (
    None  # Set to your default path, e.g., "/path/to/model.json"
)
DEFAULT_SCHEMA_URI = None  # Set to your default schema URI


def main():
    """Main function for command-line usage."""
    parser = argparse.ArgumentParser(
        description="Generate and upload CSV templates for record-based metadata"
    )

    parser.add_argument(
        "--folder_id",
        type=str,
        default=DEFAULT_FOLDER_ID,
        help="Synapse folder ID for upload",
    )
    parser.add_argument(
        "--dcc", type=str, default=DEFAULT_DCC, help="Data Coordination Center"
    )
    parser.add_argument(
        "--datatype", type=str, default=DEFAULT_DATATYPE, help="Data type name"
    )
    parser.add_argument(
        "--data_model_jsonld",
        type=str,
        default=DEFAULT_DATA_MODEL_JSONLD,
        help="Path to data model JSONLD file",
    )
    parser.add_argument(
        "--schema_uri", type=str, default=DEFAULT_SCHEMA_URI, help="JSON schema URI"
    )

    args = parser.parse_args()

    # Validate that all required arguments are provided (either as defaults or via command line)
    required_args = {
        "folder_id": args.folder_id,
        "dcc": args.dcc,
        "datatype": args.datatype,
        "data_model_jsonld": args.data_model_jsonld,
        "schema_uri": args.schema_uri,
    }

    missing_args = [
        arg_name for arg_name, value in required_args.items() if value is None
    ]
    if missing_args:
        error_msg = (
            f"The following arguments are required (either as constants "
            f"or command-line args): {', '.join(missing_args)}"
        )
        parser.error(error_msg)

    syn = synapseclient.Synapse()
    syn.login()

    manifest_df = extract_schema_properties_from_file(args.data_model_jsonld)

    tmp = tempfile.NamedTemporaryFile(delete=False, suffix=".csv")
    with open(tmp.name, "w", encoding="utf-8") as f:
        manifest_df.to_csv(f, index=False)

    with open(tmp.name, "r", encoding="utf-8") as f:
        recordset_with_data = RecordSet(
            name=f"{args.dcc}_{args.datatype}_RecordSet",
            parent_id=args.folder_id,
            description=f"RecordSet for {args.dcc} {args.datatype}",
            path=f.name,
            upsert_keys=["specimenID"],
        ).store(synapse_client=syn)
        pprint(recordset_with_data)
        recordset_id = recordset_with_data.id

    curation_task = CurationTask(
        data_type=args.datatype,
        project_id=args.folder_id,
        instructions="Test instructions",
        task_properties=RecordBasedMetadataTaskProperties(
            record_set_id=recordset_id,
        ),
    ).store(synapse_client=syn)
    pprint(curation_task)

    curation_grid: Grid = Grid(
        record_set_id=recordset_id,
    )
    curation_grid.create(synapse_client=syn)
    curation_grid = curation_grid.export_to_record_set(synapse_client=syn)
    pprint(curation_grid)


if __name__ == "__main__":
    main()
