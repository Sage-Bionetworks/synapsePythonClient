"""Mixin for objects that can generate and read manifest TSV files."""

import csv
import datetime
import io
import os
import re
import sys
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Tuple, Union

from synapseclient import Synapse
from synapseclient.core import utils
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.utils import is_synapse_id_str, is_url, topolgical_sort
from synapseclient.models.protocols.manifest_protocol import (
    ManifestGeneratableSynchronousProtocol,
)

if TYPE_CHECKING:
    from synapseclient.models import File

# When new fields are added to the manifest they will also need to be added to
# file.py#_determine_fields_to_ignore_in_merge
REQUIRED_FIELDS = ["path", "parent"]
FILE_CONSTRUCTOR_FIELDS = ["name", "id", "synapseStore", "contentType"]
STORE_FUNCTION_FIELDS = ["activityName", "activityDescription", "forceVersion"]
PROVENANCE_FIELDS = ["used", "executed"]
MANIFEST_FILENAME = "SYNAPSE_METADATA_MANIFEST.tsv"
DEFAULT_GENERATED_MANIFEST_KEYS = [
    "path",
    "parent",
    "name",
    "id",
    "synapseStore",
    "contentType",
    "used",
    "executed",
    "activityName",
    "activityDescription",
]
ARRAY_BRACKET_PATTERN = re.compile(r"^\[.*\]$")
SINGLE_OPEN_BRACKET_PATTERN = re.compile(r"^\[")
SINGLE_CLOSING_BRACKET_PATTERN = re.compile(r"\]$")
# https://stackoverflow.com/questions/18893390/splitting-on-comma-outside-quotes
COMMAS_OUTSIDE_DOUBLE_QUOTES_PATTERN = re.compile(r",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")


def _manifest_filename(path: str) -> str:
    """Get the full path to the manifest file.

    Arguments:
        path: The directory where the manifest file will be created.

    Returns:
        The full path to the manifest file.
    """
    return os.path.join(path, MANIFEST_FILENAME)


def _convert_manifest_data_items_to_string_list(
    items: List[Union[str, datetime.datetime, bool, int, float]],
) -> str:
    """
    Handle converting an individual key that contains a possible list of data into a
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
            >>> _convert_manifest_data_items_to_string_list(
                [datetime.datetime(2020, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc)])
            '2020-01-01T00:00:00Z'
            >>> _convert_manifest_data_items_to_string_list([True])
            'True'
            >>> _convert_manifest_data_items_to_string_list([1])
            '1'
            >>> _convert_manifest_data_items_to_string_list([1.0])
            '1.0'
            >>> _convert_manifest_data_items_to_string_list(
                [datetime.datetime(2020, 1, 1, 0, 0, 0, 0, tzinfo=datetime.timezone.utc),
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


def _convert_manifest_data_row_to_dict(row: dict, keys: List[str]) -> dict:
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


def _write_manifest_data(filename: str, keys: List[str], data: List[dict]) -> None:
    """
    Write a number of keys and a list of data to a manifest file. This will write
    the data out as a tab separated file.

    For the data we are writing to the TSV file we are not quoting the content with any
    characters. This is because the syncToSynapse function does not require strings to
    be quoted. When quote characters were included extra double quotes were being added
    to the strings when they were written to the manifest file. This was not causing
    errors, however, it was changing the content of the manifest file when changes
    were not required.

    Args:
        filename: The name of the file to write to.
        keys: The keys of the manifest.
        data: The data to write to the manifest. This should be a list of dicts where
            each dict represents a row of data.
    """
    with io.open(filename, "w", encoding="utf8") if filename else sys.stdout as fp:
        csv_writer = csv.DictWriter(
            fp,
            keys,
            restval="",
            extrasaction="ignore",
            delimiter="\t",
            quotechar=None,
            quoting=csv.QUOTE_NONE,
        )
        csv_writer.writeheader()
        for row in data:
            csv_writer.writerow(rowdict=_convert_manifest_data_row_to_dict(row, keys))


def _extract_entity_metadata_for_file(
    all_files: List["File"],
) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Extracts metadata from the list of File Entities and returns them in a form
    usable by csv.DictWriter

    Arguments:
        all_files: an iterable that provides File entities

    Returns:
        keys: a list column headers
        data: a list of dicts containing data from each row
    """
    keys = list(DEFAULT_GENERATED_MANIFEST_KEYS)
    annotation_keys = set()
    data = []
    for entity in all_files:
        row = {
            "parent": entity.parent_id,
            "path": entity.path,
            "name": entity.name,
            "id": entity.id,
            "synapseStore": entity.synapse_store,
            "contentType": entity.content_type,
        }

        if entity.annotations:
            annotation_keys.update(set(entity.annotations.keys()))
            row.update(
                {
                    key: (val if len(val) > 0 else "")
                    for key, val in entity.annotations.items()
                }
            )

        row_provenance = _get_entity_provenance_dict_for_file(entity=entity)
        row.update(row_provenance)

        data.append(row)
    keys.extend(annotation_keys)
    return keys, data


def _get_entity_provenance_dict_for_file(entity: "File") -> Dict[str, str]:
    """
    Arguments:
        entity: File entity object

    Returns:
        dict: a dict with a subset of the provenance metadata for the entity.
              An empty dict is returned if the metadata does not have a provenance record.
    """
    if not entity.activity:
        return {}

    used_activities = []
    for used_activity in entity.activity.used:
        used_activities.append(used_activity.format_for_manifest())

    executed_activities = []
    for executed_activity in entity.activity.executed:
        executed_activities.append(executed_activity.format_for_manifest())

    return {
        "used": ";".join(used_activities),
        "executed": ";".join(executed_activities),
        "activityName": entity.activity.name or "",
        "activityDescription": entity.activity.description or "",
    }


def _validate_manifest_required_fields(
    manifest_path: str,
) -> Tuple[bool, List[str]]:
    """
    Validate that a manifest file exists and has the required fields.

    Args:
        manifest_path: Path to the manifest file.

    Returns:
        Tuple of (is_valid, list_of_error_messages).
    """
    errors = []

    if not os.path.isfile(manifest_path):
        errors.append(f"Manifest file not found: {manifest_path}")
        return (False, errors)

    try:
        with io.open(manifest_path, "r", encoding="utf8") as fp:
            reader = csv.DictReader(fp, delimiter="\t")
            headers = reader.fieldnames or []

            # Check for required fields
            for field in REQUIRED_FIELDS:
                if field not in headers:
                    errors.append(f"Missing required field: {field}")

            # Validate each row
            row_num = 1
            for row in reader:
                row_num += 1
                path = row.get("path", "")
                parent = row.get("parent", "")

                if not path:
                    errors.append(f"Row {row_num}: 'path' is empty")

                if not parent:
                    errors.append(f"Row {row_num}: 'parent' is empty")
                elif not is_synapse_id_str(parent) and not is_url(parent):
                    errors.append(
                        f"Row {row_num}: 'parent' is not a valid Synapse ID: {parent}"
                    )

                # Check if path exists (skip URLs)
                if path and not is_url(path):
                    expanded_path = os.path.abspath(
                        os.path.expandvars(os.path.expanduser(path))
                    )
                    if not os.path.isfile(expanded_path):
                        errors.append(f"Row {row_num}: File not found: {path}")

    except Exception as e:
        errors.append(f"Error reading manifest file: {str(e)}")

    return (len(errors) == 0, errors)


@async_to_sync
class ManifestGeneratable(ManifestGeneratableSynchronousProtocol):
    """
    Mixin for objects that can generate and read manifest TSV files.

    In order to use this mixin, the class must have the following attributes:

    - `id`
    - `name`
    - `_synced_from_synapse`

    The class must also inherit from `StorableContainer` mixin which provides:

    - `flatten_file_list()`
    - `map_directory_to_all_contained_files()`
    """

    id: Optional[str] = None
    name: Optional[str] = None
    _synced_from_synapse: bool = False

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"{self.__class__.__name__}_generate_manifest: {self.id}"
    )
    async def generate_manifest_async(
        self,
        path: str,
        manifest_scope: str = "all",
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Optional[str]:
        """
        Generate a manifest TSV file for all files in this container.

        This method should be called after `sync_from_synapse()` to generate
        a manifest of all downloaded files with their metadata.

        Arguments:
            path: The directory where the manifest file(s) will be written.
            manifest_scope: Controls manifest file generation:

                - "all": Create a manifest in each directory level
                - "root": Create a single manifest at the root path only
                - "suppress": Do not create any manifest files
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The path to the root manifest file if created, or None if suppressed.

        Raises:
            ValueError: If the container has not been synced from Synapse.
            ValueError: If manifest_scope is not one of 'all', 'root', 'suppress'.

        Example: Generate manifest after sync
            Generate a manifest file after syncing from Synapse:

                from synapseclient.models import Project

                import synapseclient
                synapseclient.login()

                project = Project(id="syn123").sync_from_synapse(
                    path="/path/to/download"
                )
                manifest_path = project.generate_manifest(
                    path="/path/to/download",
                    manifest_scope="root"
                )
                print(f"Manifest created at: {manifest_path}")
        """
        if manifest_scope not in ("all", "root", "suppress"):
            raise ValueError(
                'Value of manifest_scope should be one of ("all", "root", "suppress")'
            )

        if manifest_scope == "suppress":
            return None

        if not self._synced_from_synapse:
            raise ValueError(
                "Container has not been synced from Synapse. "
                "Call sync_from_synapse() before generating a manifest."
            )

        syn = Synapse.get_client(synapse_client=synapse_client)

        # Expand the path
        path = os.path.expanduser(path) if path else None
        if not path:
            raise ValueError("A path must be provided to generate a manifest.")

        # Get all files from this container
        all_files = self.flatten_file_list()

        if not all_files:
            syn.logger.info(
                f"[{self.id}:{self.name}]: No files found in container, "
                "skipping manifest generation."
            )
            return None

        root_manifest_path = None

        if manifest_scope == "root":
            # Generate a single manifest at the root
            keys, data = _extract_entity_metadata_for_file(all_files=all_files)
            manifest_path = _manifest_filename(path)
            _write_manifest_data(manifest_path, keys, data)
            root_manifest_path = manifest_path
            syn.logger.info(
                f"[{self.id}:{self.name}]: Created manifest at {manifest_path}"
            )
        elif manifest_scope == "all":
            # Generate a manifest at each directory level
            directory_map = self.map_directory_to_all_contained_files(root_path=path)

            for directory_path, files_in_directory in directory_map.items():
                if files_in_directory:
                    keys, data = _extract_entity_metadata_for_file(
                        all_files=files_in_directory
                    )
                    manifest_path = _manifest_filename(directory_path)
                    _write_manifest_data(manifest_path, keys, data)

                    # Track the root manifest path
                    if directory_path == path:
                        root_manifest_path = manifest_path

                    syn.logger.info(
                        f"[{self.id}:{self.name}]: Created manifest at {manifest_path}"
                    )

        return root_manifest_path

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"{self.__class__.__name__}_get_manifest_data: {self.id}"
    )
    async def get_manifest_data_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Tuple[List[str], List[Dict[str, str]]]:
        """
        Get manifest data for all files in this container.

        This method extracts metadata from all files that have been synced
        to this container. The data can be used to generate a manifest file
        or for other purposes.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Tuple of (keys, data) where keys is a list of column headers
            and data is a list of dictionaries, one per file, containing
            the file metadata.

        Raises:
            ValueError: If the container has not been synced from Synapse.

        Example: Get manifest data
            Get manifest data for all files in a project:

                from synapseclient.models import Project

                import synapseclient
                synapseclient.login()

                project = Project(id="syn123").sync_from_synapse(
                    path="/path/to/download"
                )
                keys, data = project.get_manifest_data()
                for row in data:
                    print(f"File: {row['name']} at {row['path']}")
        """
        if not self._synced_from_synapse:
            raise ValueError(
                "Container has not been synced from Synapse. "
                "Call sync_from_synapse() before getting manifest data."
            )

        all_files = self.flatten_file_list()
        return _extract_entity_metadata_for_file(all_files=all_files)

    @classmethod
    @otel_trace_method(
        method_to_trace_name=lambda cls, **kwargs: f"{cls.__name__}_from_manifest"
    )
    async def from_manifest_async(
        cls,
        manifest_path: str,
        parent_id: str,
        dry_run: bool = False,
        merge_existing_annotations: bool = True,
        associate_activity_to_new_version: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List["File"]:
        """
        Upload files to Synapse from a manifest TSV file.

        This method reads a manifest TSV file and uploads all files defined in it
        to Synapse. The manifest file must contain at minimum the 'path' and 'parent'
        columns.

        Arguments:
            manifest_path: Path to the manifest TSV file.
            parent_id: The Synapse ID of the parent container (Project or Folder)
                where files will be uploaded if not specified in the manifest.
            dry_run: If True, validate the manifest but do not upload.
            merge_existing_annotations: If True, merge annotations with existing
                annotations on the file. If False, replace existing annotations.
            associate_activity_to_new_version: If True, copy the activity
                (provenance) from the previous version to the new version.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            List of File objects that were uploaded.

        Raises:
            ValueError: If the manifest file does not exist.
            ValueError: If the manifest file is missing required fields.
            IOError: If a file path in the manifest does not exist.

        Example: Upload files from a manifest
            Upload files from a manifest TSV file:

                from synapseclient.models import Project

                import synapseclient
                synapseclient.login()

                files = Project.from_manifest(
                    manifest_path="/path/to/manifest.tsv",
                    parent_id="syn123"
                )
                for file in files:
                    print(f"Uploaded: {file.name} ({file.id})")

        Example: Dry run validation
            Validate a manifest without uploading:

                from synapseclient.models import Project

                import synapseclient
                synapseclient.login()

                files = Project.from_manifest(
                    manifest_path="/path/to/manifest.tsv",
                    parent_id="syn123",
                    dry_run=True
                )
                print("Manifest is valid, ready for upload")
        """
        from synapseclient.models import Activity, File

        syn = Synapse.get_client(synapse_client=synapse_client)

        # Validate the manifest
        is_valid, errors = _validate_manifest_required_fields(manifest_path)
        if not is_valid:
            raise ValueError(
                "Invalid manifest file:\n" + "\n".join(f"  - {e}" for e in errors)
            )

        # Read the manifest
        rows = []
        with io.open(manifest_path, "r", encoding="utf8") as fp:
            reader = csv.DictReader(fp, delimiter="\t")
            for row in reader:
                rows.append(row)

        if dry_run:
            syn.logger.info(
                f"Dry run: {len(rows)} files would be uploaded from manifest"
            )
            return []

        # Build dependency graph for provenance ordering
        path_to_row = {}
        upload_order = {}

        for row in rows:
            path = row.get("path", "")
            if path and not is_url(path):
                path = os.path.abspath(os.path.expandvars(os.path.expanduser(path)))
            path_to_row[path] = row

            # Collect provenance references
            all_refs = []
            used = row.get("used", "")
            if used and used.strip():
                for item in used.split(";"):
                    item = item.strip()
                    if item:
                        if os.path.isfile(
                            os.path.abspath(
                                os.path.expandvars(os.path.expanduser(item))
                            )
                        ):
                            all_refs.append(
                                os.path.abspath(
                                    os.path.expandvars(os.path.expanduser(item))
                                )
                            )

            executed = row.get("executed", "")
            if executed and executed.strip():
                for item in executed.split(";"):
                    item = item.strip()
                    if item:
                        if os.path.isfile(
                            os.path.abspath(
                                os.path.expandvars(os.path.expanduser(item))
                            )
                        ):
                            all_refs.append(
                                os.path.abspath(
                                    os.path.expandvars(os.path.expanduser(item))
                                )
                            )

            upload_order[path] = all_refs

        # Topologically sort based on provenance dependencies
        sorted_paths = topolgical_sort(upload_order)
        sorted_paths = [p[0] for p in sorted_paths]

        # Track uploaded files for provenance resolution
        path_to_synapse_id: Dict[str, str] = {}
        uploaded_files: List["File"] = []

        for path in sorted_paths:
            row = path_to_row[path]

            # Get parent - use manifest value or fall back to provided parent_id
            file_parent = row.get("parent", "").strip() or parent_id

            # Build the File object
            file = File(
                path=path,
                parent_id=file_parent,
                name=row.get("name", "").strip() or None,
                id=row.get("id", "").strip() or None,
                synapse_store=(
                    row.get("synapseStore", "").strip().lower() != "false"
                    if row.get("synapseStore", "").strip()
                    else True
                ),
                content_type=row.get("contentType", "").strip() or None,
                merge_existing_annotations=merge_existing_annotations,
                associate_activity_to_new_version=associate_activity_to_new_version,
            )

            # Build annotations from extra columns
            annotations = {}
            skip_keys = set(
                REQUIRED_FIELDS
                + FILE_CONSTRUCTOR_FIELDS
                + STORE_FUNCTION_FIELDS
                + PROVENANCE_FIELDS
            )
            for key, value in row.items():
                if key not in skip_keys and value and value.strip():
                    annotations[key] = _parse_manifest_value(value.strip())
            if annotations:
                file.annotations = annotations

            # Build provenance/activity
            used_items = []
            executed_items = []

            used_str = row.get("used", "")
            if used_str and used_str.strip():
                for item in used_str.split(";"):
                    item = item.strip()
                    if item:
                        used_items.append(
                            _resolve_provenance_item(item, path_to_synapse_id)
                        )

            executed_str = row.get("executed", "")
            if executed_str and executed_str.strip():
                for item in executed_str.split(";"):
                    item = item.strip()
                    if item:
                        executed_items.append(
                            _resolve_provenance_item(item, path_to_synapse_id)
                        )

            if used_items or executed_items:
                activity = Activity(
                    name=row.get("activityName", "").strip() or None,
                    description=row.get("activityDescription", "").strip() or None,
                    used=used_items,
                    executed=executed_items,
                )
                file.activity = activity

            # Upload the file
            file = await file.store_async(synapse_client=syn)

            # Track for provenance resolution
            path_to_synapse_id[path] = file.id
            uploaded_files.append(file)

            syn.logger.info(f"Uploaded: {file.name} ({file.id})")

        return uploaded_files

    @staticmethod
    @otel_trace_method(method_to_trace_name=lambda **kwargs: "validate_manifest")
    async def validate_manifest_async(
        manifest_path: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Tuple[bool, List[str]]:
        """
        Validate a manifest TSV file without uploading.

        This method validates a manifest file to ensure it is properly formatted
        and all paths exist.

        Arguments:
            manifest_path: Path to the manifest TSV file.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Tuple of (is_valid, list_of_error_messages). If the manifest is valid,
            is_valid will be True and the list will be empty.

        Example: Validate a manifest file
            Validate a manifest file before uploading:

                from synapseclient.models import Project

                is_valid, errors = Project.validate_manifest(
                    manifest_path="/path/to/manifest.tsv"
                )
                if is_valid:
                    print("Manifest is valid")
                else:
                    for error in errors:
                        print(f"Error: {error}")
        """
        return _validate_manifest_required_fields(manifest_path)

    @staticmethod
    async def generate_download_list_manifest_async(
        download_path: str,
        csv_separator: str = ",",
        include_header: bool = True,
        timeout: int = 120,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> str:
        """
        Generate a manifest file from the current user's download list using the
        Synapse REST API.

        This method creates a CSV manifest containing metadata about all files in
        the user's download list. The manifest is generated server-side by Synapse
        and then downloaded to the specified path.

        This is interoperable with the Synapse download list feature and provides
        a way to export the download list as a manifest file that can be used for
        bulk operations.

        Arguments:
            download_path: The local directory path where the manifest will be saved.
            csv_separator: The delimiter character for the CSV file.
                Defaults to "," for comma-separated values. Use "\t" for tab-separated.
            include_header: Whether to include column headers in the first row.
                Defaults to True.
            timeout: The number of seconds to wait for the job to complete.
                Defaults to 120 seconds.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The full path to the downloaded manifest file.

        Example: Generate manifest from download list
            Generate a manifest from your Synapse download list:

                from synapseclient.models import Project

                import synapseclient
                synapseclient.login()

                # Generate manifest from download list
                manifest_path = Project.generate_download_list_manifest(
                    download_path="/path/to/download"
                )
                print(f"Manifest downloaded to: {manifest_path}")

        Example: Generate tab-separated manifest
            Generate a TSV manifest from your download list:

                from synapseclient.models import Project

                import synapseclient
                synapseclient.login()

                manifest_path = Project.generate_download_list_manifest(
                    download_path="/path/to/download",
                    csv_separator="\t"
                )

        See Also:
            - `DownloadListManifestRequest`: The underlying request class for more
              fine-grained control over the manifest generation process.
        """
        from synapseclient.models.download_list import DownloadListManifestRequest
        from synapseclient.models.table_components import CsvTableDescriptor

        # Create the request with CSV formatting options
        request = DownloadListManifestRequest(
            csv_table_descriptor=CsvTableDescriptor(
                separator=csv_separator,
                is_first_line_header=include_header,
            )
        )

        # Send the job and wait for completion
        await request.send_job_and_wait_async(
            timeout=timeout,
            synapse_client=synapse_client,
        )

        # Download the manifest
        manifest_file_path = await request.download_manifest_async(
            download_path=download_path,
            synapse_client=synapse_client,
        )

        return manifest_file_path


def _resolve_provenance_item(
    item: str,
    path_to_synapse_id: Dict[str, str],
) -> Any:
    """
    Resolve a provenance item to a UsedEntity or UsedURL.

    Args:
        item: The provenance item string (could be a path, Synapse ID, or URL).
        path_to_synapse_id: Mapping of local file paths to their Synapse IDs.

    Returns:
        UsedEntity or UsedURL object.
    """
    from synapseclient.models import UsedEntity, UsedURL

    # Check if it's a local file path that was uploaded
    expanded_path = os.path.abspath(os.path.expandvars(os.path.expanduser(item)))
    if expanded_path in path_to_synapse_id:
        return UsedEntity(target_id=path_to_synapse_id[expanded_path])

    # Check if it's a URL
    if is_url(item):
        return UsedURL(url=item)

    # Check if it's a Synapse ID
    if is_synapse_id_str(item):
        return UsedEntity(target_id=item)

    # Assume it's a Synapse ID
    return UsedEntity(target_id=item)


def _parse_manifest_value(value: str) -> Any:
    """
    Parse a manifest cell value into an appropriate Python type.

    Handles:
    - List syntax: [a,b,c] -> ['a', 'b', 'c']
    - Boolean strings: 'true', 'false' -> True, False
    - Numeric strings: '123' -> 123, '1.5' -> 1.5
    - Everything else: returned as string

    Args:
        value: The string value from the manifest.

    Returns:
        The parsed value.
    """
    # Check for list syntax
    if ARRAY_BRACKET_PATTERN.match(value):
        # Remove brackets
        inner = value[1:-1]
        # Split on commas outside quotes
        items = COMMAS_OUTSIDE_DOUBLE_QUOTES_PATTERN.split(inner)
        result = []
        for item in items:
            item = item.strip()
            # Remove surrounding quotes if present
            if item.startswith('"') and item.endswith('"'):
                item = item[1:-1]
            result.append(item)
        return result

    # Check for boolean
    if value.lower() == "true":
        return True
    if value.lower() == "false":
        return False

    # Check for integer
    try:
        return int(value)
    except ValueError:
        pass

    # Check for float
    try:
        return float(value)
    except ValueError:
        pass

    # Return as string
    return value
