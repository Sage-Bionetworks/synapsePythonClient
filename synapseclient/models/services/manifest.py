"""Services for reading and generating Synapse manifest CSV files used to
drive bulk upload via Project.sync_to_synapse / Folder.sync_to_synapse."""

from __future__ import annotations

import ast
import asyncio
import csv
import datetime
import functools
import os
import re
from dataclasses import dataclass
from typing import TYPE_CHECKING, Iterable, NamedTuple

from synapseclient import Synapse
from synapseclient.core.exceptions import (
    SynapseFileNotFoundError,
    SynapseHTTPError,
    SynapseProvenanceError,
)
from synapseclient.core.utils import (
    bool_or_none,
    datetime_or_none,
    get_synid_and_version,
    is_synapse_id_str,
    is_url,
    test_import_pandas,
    topolgical_sort,
)
from synapseclient.operations.factory_operations import FileOptions
from synapseclient.operations.factory_operations import get_async as factory_get_async

if TYPE_CHECKING:
    from pandas import DataFrame, Series

    from synapseclient.models import UsedEntity, UsedURL
    from synapseclient.models.file import File
    from synapseclient.models.folder import Folder

#: Scalar types that Synapse supports as annotation values.
SynapseAnnotationType = datetime.datetime | float | int | bool | str

# Columns that are NOT annotations — stripped before building File.annotations.
# Covers the standard manifest columns plus the extra metadata columns produced
# by the Synapse UI download cart and synapse get-download-list CLI.
NON_ANNOTATION_COLUMNS = frozenset(
    [
        # Standard manifest columns used directly during upload
        "path",
        "parentId",
        "ID",
        "name",
        "synapseStore",
        "contentType",
        "activityName",
        "activityDescription",
        "forceVersion",
        "used",
        "executed",
        # Download-list / Synapse UI informational columns — ignore for upload
        "error",
        "versionNumber",
        "dataFileSizeBytes",
        "createdBy",
        "createdOn",
        "modifiedBy",
        "modifiedOn",
        "synapseURL",
        "dataFileMD5Hex",
    ]
)

# Regex patterns used when parsing annotation cell values.
# Matches a cell that is a bracket-delimited list, e.g. "[a, b, c]".
# Disallows ']' inside to avoid matching adjacent lists like "[a][b]".
_ARRAY_BRACKET_PATTERN = re.compile(r"^\[[^\]]*\]$")
# https://stackoverflow.com/questions/18893390/splitting-on-comma-outside-quotes
_COMMAS_OUTSIDE_DOUBLE_QUOTES_PATTERN = re.compile(r",(?=(?:[^\"]*\"[^\"]*\")*[^\"]*$)")
# Valid Synapse file name characters (1–256 chars).
_FILE_NAME_PATTERN = re.compile(r"^[`\w \-\+\.\(\)]{1,256}$")


class UploadSyncFile(NamedTuple):
    """Represents a single file being uploaded.

    Attributes:
        entity: The file that is going through the sync process.
        used: Resolved provenance references — absolute local paths for files in
            the upload batch, or File objects for files already in Synapse.
        executed: Same as used but for executed provenance references.
        activity_name: The name of the activity that is being performed.
        activity_description: The description of the activity that is being performed.
    """

    entity: File
    used: list[str | File]
    executed: list[str | File]
    activity_name: str | None
    activity_description: str | None


async def read_manifest_for_upload(
    manifest_path: str,
    syn: Synapse,
    merge_existing_annotations: bool,
    associate_activity_to_new_version: bool,
) -> tuple[list[UploadSyncFile], int]:
    """Read and validate a manifest CSV file, returning items ready for upload.

    Accepts manifests produced by StorableContainer.sync_from_synapse,
    the synapse get-download-list CLI, or the Synapse UI download cart.
    Rows with a non-empty error column (added by get-download-list) are
    silently skipped.

    Arguments:
        manifest_path: Path to the CSV manifest file.
        syn: Authenticated Synapse client.
        merge_existing_annotations: If True, merge manifest annotations with
            existing annotations on Synapse. If False, overwrite them.
        associate_activity_to_new_version: If True and a version update occurs,
            the existing Synapse activity will be associated with the new version.

    Returns:
        A tuple of (items, total_bytes) where items is a list of
        UploadSyncFile objects ready for upload and total_bytes is the
        combined size of all local files.

    Raises:
        ValueError: If required columns are missing, paths are not unique,
            files are empty, file names are invalid, or a parentId is not a
            Folder or Project.
        OSError: If a non-URL path does not exist on disk.
        SynapseProvenanceError: If a provenance item is neither a local file
            path, a URL, nor a valid Synapse ID.
        SynapseHTTPError: If a parentId does not exist in Synapse.
    """
    syn.logger.info(f"Validating manifest: {manifest_path}")
    df = _clean_manifest(manifest_path)

    if df.empty:
        return [], 0

    syn.logger.info("Validating manifest contents...")
    total_size = _validate_manifest(df)

    syn.logger.info("Validating provenance and parent containers...")
    df, _ = await asyncio.gather(
        _sort_and_fix_provenance(syn, df),
        _check_parent_containers_async(df["parentId"].unique(), syn=syn),
    )

    items = _build_upload_files(
        df,
        merge_existing_annotations=merge_existing_annotations,
        associate_activity_to_new_version=associate_activity_to_new_version,
    )

    return items, total_size


def _clean_manifest(manifest_path: str) -> DataFrame:
    """Read a manifest CSV and return a cleaned DataFrame ready for validation.

    Arguments:
        manifest_path: Path to the CSV manifest file.

    Returns:
        A cleaned DataFrame. May be empty if all rows were filtered out.

    Raises:
        ValueError: If required columns (path, parentId) are missing, or if
            file paths are not unique.
        OSError: If a non-URL path does not exist on disk.
    """
    df = _read_and_filter_errors(manifest_path)
    if df.empty:
        return df

    _check_required_columns(df)
    _apply_synapse_store_defaults(df)
    df = df.fillna("")
    df["path"] = df["path"].apply(_check_path_and_normalize)
    _check_unique_paths(df)
    _default_name_column(df)
    return df


def _read_and_filter_errors(manifest_path: str) -> DataFrame:
    """Read a manifest CSV and drop rows with a non-empty error column.

    The error column is added by the Synapse get-download-list CLI and the
    Synapse UI download cart to mark rows that failed to download.

    Arguments:
        manifest_path: Path to the CSV manifest file.

    Returns:
        A DataFrame with error rows removed. May be empty.
    """
    test_import_pandas()
    import pandas as pd

    df = pd.read_csv(manifest_path)
    if "error" in df.columns:
        df = df[df["error"].fillna("") == ""]
    return df


def _check_required_columns(df: DataFrame) -> None:
    """Raise ValueError if the manifest is missing required columns.

    Arguments:
        df: A non-empty manifest DataFrame.

    Raises:
        ValueError: If path or parentId columns are missing.
    """
    for col in ("path", "parentId"):
        if col not in df.columns:
            raise ValueError(f"Manifest must contain a '{col}' column")


def _check_unique_paths(df: DataFrame) -> None:
    """Raise ValueError if any file path appears more than once.

    Arguments:
        df: Manifest DataFrame with a normalized path column.

    Raises:
        ValueError: If duplicate paths are found.
    """
    if df["path"].duplicated().any():
        raise ValueError(
            "All rows in manifest must contain a unique file path to upload"
        )


def _default_name_column(df: DataFrame) -> None:
    """Ensure every row has a name, defaulting to the basename of the path.

    Creates the name column if it does not exist.  For rows where the name
    is blank, fills it from the path column.  Mutates df in place.

    Arguments:
        df: Manifest DataFrame with a path column.
    """
    if "name" not in df.columns:
        df["name"] = df["path"].apply(os.path.basename)
    empty_names = df["name"] == ""
    if empty_names.any():
        df.loc[empty_names, "name"] = df.loc[empty_names, "path"].apply(
            os.path.basename
        )


def _validate_manifest(df: DataFrame) -> int:
    """Run pure validation checks on a cleaned manifest DataFrame.

    Arguments:
        df: A non-empty, cleaned manifest DataFrame as returned by
            _clean_manifest.

    Returns:
        Combined size in bytes of all local (non-URL) files in the manifest.

    Raises:
        ValueError: If any file is empty (0 bytes) or has an invalid name,
            or if (name, parentId) pairs are not unique.
    """
    total_size = _check_size_each_file(df)
    _check_file_names(df)
    return total_size


def _apply_synapse_store_defaults(df: "DataFrame") -> None:
    """Set synapseStore column defaults on the manifest DataFrame in place.

    Steps:
        1. Creates the synapseStore column if the manifest CSV didn't include
           one (defaults to None/NaN).
        2. Sets URL rows to False -- files referenced by URL should not be
           uploaded to Synapse storage.
        3. Sets all remaining nulls to True -- local file paths should be
           uploaded by default.
        4. Casts the column to bool for consistent downstream usage.

    Arguments:
        df: Manifest DataFrame with at least a path column.
    """
    if "synapseStore" not in df.columns:
        df["synapseStore"] = None
    df.loc[df["path"].apply(is_url), "synapseStore"] = False
    df.loc[df["synapseStore"].isnull(), "synapseStore"] = True
    df["synapseStore"] = df["synapseStore"].astype(bool)


def _check_path_and_normalize(f: str) -> str:
    """Return the normalized absolute path for f, or f unchanged if it is a URL.

    Arguments:
        f: A file path or URL as read from the manifest path column.

    Returns:
        The input unchanged if it is a URL, otherwise the resolved absolute
        path after expanding ~ and environment variables.

    Raises:
        OSError: If f is not a URL and the resolved path does not point to
            an existing file on disk.
    """
    if is_url(f):
        return f
    path_normalized = _expand_path(f)
    if not os.path.isfile(path_normalized):
        raise OSError(f"The path {f} is not a file or does not exist")
    return path_normalized


def _expand_path(path: str) -> str:
    """Expand ~ and environment variables, then return the absolute path."""
    return os.path.abspath(os.path.expandvars(os.path.expanduser(path)))


def _check_size_each_file(df: DataFrame) -> int:
    """Raise ValueError if any non-URL file in the manifest is empty (0 bytes).

    Arguments:
        df: Manifest DataFrame containing a path column. Rows whose
            path is a URL are skipped.

    Returns:
        Combined size in bytes of all local (non-URL) files in the manifest.

    Raises:
        ValueError: If any local file referenced by the manifest has a size of
            zero bytes.
    """
    total = 0
    for _, row in df.iterrows():
        file_path = row["path"]
        if not is_url(file_path):
            size = os.stat(file_path).st_size
            if size == 0:
                raise ValueError(
                    f"File {file_path} is empty, empty files cannot be uploaded to Synapse"
                )
            total += size
    return total


def _check_file_names(df: DataFrame) -> None:
    """Validate that each file name is acceptable for Synapse and that all
    (name, parentId) pairs are unique.

    Arguments:
        df: Manifest DataFrame containing name and parentId columns.
            All name cells must already be populated (empty names should be
            defaulted before calling this function).

    Raises:
        ValueError: If any file name contains characters not permitted by
            Synapse, or if two rows share the same name and parentId.
    """
    for _, row in df.iterrows():
        file_name = row["name"]
        if not _FILE_NAME_PATTERN.match(file_name):
            raise ValueError(
                f"File name {file_name} cannot be stored to Synapse. Names may contain"
                " letters, numbers, spaces, underscores, hyphens, periods, plus signs,"
                " backticks, and parentheses"
            )
    if df[["name", "parentId"]].duplicated().any():
        raise ValueError(
            "All rows in manifest must contain a path with a unique file name and"
            " parent to upload. Files uploaded to the same folder/project (parentId)"
            " must have unique file names."
        )


async def _sort_and_fix_provenance(syn: Synapse, df: DataFrame) -> DataFrame:
    """Validate and normalize provenance references, then topologically sort the
    manifest rows so that files are uploaded before any file that depends on them.

    Each used and executed cell is split on ;, and each item is
    resolved to an absolute path (if it is a local file being uploaded), a
    Synapse entity (if the local file already exists in Synapse), or left as-is
    if it is a URL or Synapse ID.

    Arguments:
        syn: Authenticated Synapse client, used to look up local files that are
            not in the upload manifest but may already exist in Synapse.
        df: Manifest DataFrame indexed or containing a path column, with
            optional used and executed columns holding ;-delimited
            provenance strings.

    Returns:
        A new DataFrame with the same rows reordered so that provenance
        dependencies are uploaded before the files that reference them, and with
        used and executed columns replaced by lists of resolved
        references.

    Raises:
        SynapseProvenanceError: If a provenance item is neither a local file
            path, a URL, nor a valid Synapse ID.
    """
    df = df.set_index("path").copy()

    results = await asyncio.gather(
        *[_resolve_row(str(path), row, df, syn) for path, row in df.iterrows()]
    )

    deps: dict[str, list[str]] = {}
    for path, resolved in results:
        # Write resolved provenance back into the DataFrame
        for col, values in resolved.items():
            df.at[path, col] = values

        # Local file paths (str) are upload-order dependencies;
        # File objects (already in Synapse) are not.
        deps[path] = _local_path_refs(resolved)

    sorted_order = [path for path, _deps in topolgical_sort(deps)]
    df = df.reindex(sorted_order)
    return df.reset_index()


def _local_path_refs(
    resolved: dict[str, list[str | File]],
) -> list[str]:
    """Extract local file path references from resolved provenance columns.

    Local paths (str) represent files in the current upload batch that must be
    uploaded first. File objects are already in Synapse and do not create
    upload-order dependencies.

    Arguments:
        resolved: A dict mapping provenance column names (used, executed) to
            their resolved reference lists.

    Returns:
        A flat list of local file path strings found across all columns.
    """
    return [
        ref for values in resolved.values() for ref in values if isinstance(ref, str)
    ]


async def _check_parent_containers_async(parent_ids: list[str], syn: Synapse) -> None:
    """Verify that every parentId in the manifest is a valid Synapse container.

    All parent IDs are validated concurrently.

    Arguments:
        parent_ids: Iterable of Synapse IDs taken from the manifest
            parentId column. Empty strings are silently skipped.
        syn: Authenticated Synapse client used to fetch each entity.

    Raises:
        SynapseHTTPError: If a parentId does not exist in Synapse.
        ValueError: If a parentId exists but is not a Project or Folder.
    """

    async def _check_one(syn_id: str) -> None:
        if not syn_id:
            return
        try:
            container = await factory_get_async(
                synapse_id=syn_id,
                file_options=FileOptions(download_file=False),
                synapse_client=syn,
            )
        except SynapseHTTPError:
            syn.logger.warning(
                f"\n{syn_id} in the parentId column is not a valid Synapse Id\n"
            )
            raise

        from synapseclient.models.folder import Folder
        from synapseclient.models.project import Project

        if not isinstance(container, (Folder, Project)):
            raise ValueError(
                f"{syn_id} in the parentId column is not a Folder or Project"
            )

    await asyncio.gather(*[_check_one(syn_id) for syn_id in parent_ids])


def _build_upload_files(
    df: DataFrame,
    merge_existing_annotations: bool,
    associate_activity_to_new_version: bool,
) -> list[UploadSyncFile]:
    """Convert a validated manifest DataFrame into a list of UploadSyncFile objects,
    one per manifest row.

    All columns not in NON_ANNOTATION_COLUMNS are treated as annotations.

    Arguments:
        df: Validated manifest DataFrame (after provenance sort).
        merge_existing_annotations: If True, manifest annotations are merged with
            existing annotations on each File in Synapse. If False, manifest
            annotations replace them entirely.
        associate_activity_to_new_version: If True and a version update occurs,
            the existing Synapse activity will be associated with the new version.

    Returns:
        List of UploadSyncFile objects ready for upload, one per manifest row.
    """
    from synapseclient.models.file import (
        File,  # lazy import to avoid circular dependency
    )

    items = []
    for _, row in df.iterrows():
        file_entity = File(
            path=row["path"],
            parent_id=row["parentId"],
            name=row.get("name") or None,
            id=row.get("ID") or None,
            synapse_store=row.get("synapseStore", True),
            content_type=row.get("contentType") or None,
            force_version=_parse_force_version(row.get("forceVersion", "")),
            merge_existing_annotations=merge_existing_annotations,
            associate_activity_to_new_version=associate_activity_to_new_version,
            _present_manifest_fields=list(row.index),
        )

        annotation_cols: dict[str, object] = {
            str(k): v for k, v in row.items() if k not in NON_ANNOTATION_COLUMNS
        }
        file_entity.annotations = _build_annotations_for_file(annotation_cols)

        item = UploadSyncFile(
            file_entity,
            used=row.get("used", []) or [],
            executed=row.get("executed", []) or [],
            activity_name=row.get("activityName") or None,
            activity_description=row.get("activityDescription") or None,
        )
        items.append(item)

    return items


def _parse_force_version(raw: object) -> bool:
    """Parse a forceVersion cell into a bool, defaulting to True.

    The input comes from a CSV cell which can arrive in several forms
    depending on whether the column existed, was blank, or had a value.
    The conversion cascade is:

    1. Missing or blank (empty string / None) -- the manifest did not
       include a forceVersion column, or the cell was left empty. Defaults
       to True (force a new version), the safe default for uploads.
    2. Already a bool -- pandas infers the column type as bool when every
       row contains True/False. Used as-is.
    3. Parseable string -- CSV strings like "True"/"False" that pandas
       read as str. bool_or_none handles case-insensitive conversion.
    4. Anything else -- unrecognizable values (e.g. "yes", "1", garbage)
       fall back to True.

    Arguments:
        raw: The raw cell value from the forceVersion manifest column.

    Returns:
        True if a new version should be forced, False otherwise.
    """
    if raw == "" or raw is None:
        return True
    if isinstance(raw, bool):
        return raw
    parsed = bool_or_none(str(raw))
    return parsed if parsed is not None else True


def _build_annotations_for_file(
    manifest_annotations: dict[str, object],
) -> dict[str, list[SynapseAnnotationType]]:
    """Pull annotations out of the manifest format into the client's internal format.

    Annotation values that are empty strings or None are omitted.  All values
    are returned as lists to match the Synapse annotation storage model, where
    every annotation key maps to a list of values.

    Arguments:
        manifest_annotations: A dict mapping annotation key to raw cell value as
            read from the manifest DataFrame row.

    Returns:
        A dict mapping annotation key to a list of converted Python values.
        String values are passed through _parse_annotation_cell.
        Non-string values (e.g. an int or float that pandas inferred from the
        CSV) are wrapped in a single-element list.
    """
    file_annotations = {}
    for annotation_key, annotation_value in manifest_annotations.items():
        if annotation_value is None or annotation_value == "":
            continue
        if isinstance(annotation_value, str):
            file_annotations[annotation_key] = _parse_annotation_cell(
                cell=annotation_value
            )
        else:
            file_annotations[annotation_key] = [annotation_value]
    return file_annotations


def _parse_annotation_cell(
    cell: str,
) -> list[SynapseAnnotationType]:
    """Convert a raw manifest CSV cell string into a typed list of annotation values.

    pandas.read_csv returns every cell as a string, but Synapse annotations
    are typed (int, float, datetime, bool, str). This function parses a single
    cell and returns the correctly-typed Python values so that annotations
    round-trip faithfully through a manifest file.

    Multi-value cells are expressed in the manifest as a bracket-delimited,
    comma-separated string (e.g. "[a, b, c]").  Single-value cells are
    treated as a one-element list.  The return is always a list so callers
    never need to branch on scalar-vs-list.  Empty values (e.g. from
    "[a, , c]") are silently dropped.

    Arguments:
        cell: A single cell string read from a manifest CSV.

    Returns:
        A list of converted values.  A plain scalar cell returns a one-element
        list; a bracket-delimited cell returns one element per non-empty value.
    """
    cell = cell.strip()
    raw_values = (
        _split_csv_cell(cell[1:-1]) if _ARRAY_BRACKET_PATTERN.match(cell) else [cell]
    )
    return [_convert_value(value) for value in raw_values if value.strip()]


def _convert_value(value: str) -> SynapseAnnotationType:
    """Convert a single non-empty string token to its most specific Python type.

    The conversion order matters: each step is tried only if the previous one
    returned no match.

    1. datetime — tried first because date strings like "2024-01-01"
       are also valid ast.literal_eval strings (they parse as subtraction
       expressions), so datetime must win.
    2. bool — tried before ast.literal_eval because "true" and
       "false" (lowercase) are not valid Python literals and would fall
       through to a raw string if literal_eval ran first, giving inconsistent
       results for "true" vs "True".
    3. int / float — parsed via ast.literal_eval.  bool results are
       excluded here because step 2 already handled them (bool is a subclass
       of int, so without the exclusion "True" would come back as 1).
    4. raw string — returned unchanged when no conversion matched.

    Arguments:
        value: A non-empty string token from a manifest cell.

    Returns:
        The token as a datetime.datetime, bool, int, float, or
        the original str if no conversion matched.
    """
    datetime_ = datetime_or_none(value)
    if datetime_ is not None:
        return datetime_

    bool_ = bool_or_none(value)
    if bool_ is not None:
        return bool_

    literal_ = _parse_literal(value)
    if literal_ is not None:
        return literal_

    return value


def _parse_literal(value: str) -> int | float | str | None:
    """Try to parse value as a scalar Python literal via ast.literal_eval.

    Why str is accepted: bracket-array cells like ["foo bar", "baz"]
    are split into tokens such as '"foo bar"' — a string that is itself a
    quoted Python string literal.  ast.literal_eval strips the outer quotes
    and returns "foo bar".  Plain unquoted strings (e.g. "hello") are
    not valid Python literals, so literal_eval raises ValueError and
    the raw string is returned by _convert_value instead.

    Why bool is excluded: bool is a subclass of int, so
    ast.literal_eval("True") returns True and passes the
    isinstance(parsed, int) check.  Without the exclusion, "True" would
    come back as the bool True from this function, but "true" (lowercase)
    would not be a valid literal and would fall through to a raw string — an
    inconsistency.  bool_or_none in _convert_value handles both cases
    uniformly before this function is ever called.

    Why complex types are rejected: tuples, lists, and dicts are valid Python
    literals but are not valid Synapse annotation value types.

    Arguments:
        value: A string token to parse.

    Returns:
        An int, float, or str if the token is a recognized scalar
        literal; None if parsing fails or produces an unsupported type.
    """
    try:
        parsed = ast.literal_eval(value)
        if isinstance(parsed, (int, float, str)) and not isinstance(parsed, bool):
            return parsed
    except (ValueError, SyntaxError):
        pass
    return None


async def upload_sync_files(files: list[UploadSyncFile], syn: Synapse) -> list[File]:
    """Upload files to Synapse concurrently in an order that honours
    interdependent provenance dependencies.

    Arguments:
        files: The list of UploadSyncFile items to upload.
        syn: Authenticated Synapse client.

    Returns:
        List of File entities that were created or updated, in the same
        order as the dependency-graph task execution.

    Raises:
        ValueError: If a provenance reference points to a local file not
            in the upload batch, or if a provenance item is not a valid
            Synapse ID.
        RuntimeError: If prerequisite upload tasks fail to complete.
    """
    plan = _build_upload_plan(items=list(files))
    tasks = _create_upload_tasks(upload_plan=plan, syn=syn)
    results = await asyncio.gather(*tasks)
    return list(results)


@dataclass
class _UploadPlan:
    """Topologically sorted upload plan built from manifest provenance dependencies.

    Attributes:
        path_to_dependencies: Maps each file path to the list of file paths that
            must be uploaded before it (i.e. its provenance dependencies).
        path_to_upload_item: Maps each file path to its UploadSyncFile, ordered
            by the resolved dependency sort.
        path_to_file_check: Cache of os.path.isfile results for provenance
            references encountered during dependency resolution.
    """

    path_to_dependencies: dict[str, list[str]]
    path_to_upload_item: dict[str, UploadSyncFile]
    path_to_file_check: dict[str, bool]


def _build_upload_plan(
    items: list[UploadSyncFile],
) -> _UploadPlan:
    """Determine the order in which files should be uploaded, given that some
    files depend on others via provenance.

    A manifest CSV can declare that file B was derived from file A
    (provenance). If B is uploaded before A, B's provenance record cannot
    reference A's Synapse ID because A does not have one yet. This
    function ensures the upload order respects those constraints.

    Steps:
        1. Build a dependency graph. For each file in the upload batch,
           scan its used and executed provenance references. If a
           reference points to a local file that is also being uploaded,
           that is a dependency edge. If it points to a local file that
           is not in the batch, that is an error.
        2. Topologically sort the graph. This produces an ordering where
           every file comes after its dependencies, guaranteeing that by
           the time B is uploaded, A already has a Synapse ID.
        3. Package the result into an _UploadPlan.

    Arguments:
        items: The list of items to upload.

    Returns:
        An _UploadPlan containing the topologically sorted dependency map
        and the upload items keyed by file path.

    Raises:
        ValueError: If a provenance reference points to a local file
            that is not part of the upload batch.
    """
    items_by_path = {i.entity.path: i for i in items}
    file_check_cache: dict[str, bool] = {}

    graph: dict[str, list[str]] = {}
    for item in items:
        graph[item.entity.path] = _resolve_file_dependencies(
            item, items_by_path, file_check_cache
        )

    graph_sorted = topolgical_sort(graph)
    path_to_dependencies_sorted = {path: deps for path, deps in graph_sorted}
    path_to_upload_items_sorted = {
        path: items_by_path[path] for path in path_to_dependencies_sorted
    }

    return _UploadPlan(
        path_to_dependencies=path_to_dependencies_sorted,
        path_to_upload_item=path_to_upload_items_sorted,
        path_to_file_check=file_check_cache,
    )


def _resolve_file_dependencies(
    item: UploadSyncFile,
    items_by_path: dict[str, UploadSyncFile],
    file_check_cache: dict[str, bool],
) -> list[str]:
    """Return local-file provenance paths that this item depends on.

    Arguments:
        item: The upload item whose provenance references to resolve.
        items_by_path: All items in the upload batch keyed by file path.
        file_check_cache: Mutable cache of os.path.isfile results,
            updated in place for any new paths encountered.

    Returns:
        A list of absolute file paths from the upload batch that this
        item depends on via provenance.

    Raises:
        ValueError: If a provenance reference points to a local file
            that is not part of the upload batch.
    """
    deps: list[str] = []
    for ref in item.used + item.executed:
        # File objects (already in Synapse) are not local-path
        # dependencies — skip them in the dependency graph.
        if not isinstance(ref, str):
            continue
        if ref not in file_check_cache:
            file_check_cache[ref] = os.path.isfile(ref)
        if file_check_cache[ref]:
            if ref not in items_by_path:
                raise ValueError(
                    f"{item.entity.path} depends on"
                    f" {ref} which is not being uploaded"
                )
            deps.append(ref)
    return deps


def _create_upload_tasks(
    upload_plan: _UploadPlan,
    syn: Synapse,
) -> list[asyncio.Task]:
    """Build an asyncio task graph that uploads files concurrently while
    honouring provenance dependencies.

    The manifest may declare that file B was derived from file A
    (provenance). A must be uploaded before B so that B's provenance
    record can reference A's Synapse ID. Files with no dependency
    relationship upload concurrently.

    The function iterates over the upload plan's dependency map (already
    topologically sorted, so dependencies always appear before
    dependents). For each file it:

    1. Collects the already-created asyncio.Task objects for its
       prerequisites -- these are guaranteed to exist because of the
       topological ordering.
    2. Creates a new asyncio.Task wrapping _upload_file_async, passing
       the prerequisite tasks in. That function calls asyncio.wait() on
       them before uploading, so the file will not start uploading until
       its dependencies finish.
    3. Stores the new task so later files can reference it as a
       prerequisite.

    The returned list can be passed directly to asyncio.gather(). The
    concurrency constraints are encoded inside the tasks themselves
    (each one awaits its own prerequisites), so gather fires them all
    off but they naturally serialize where needed.

    Example: if A has no deps, B depends on A, and C has no deps:
        - A and C start uploading immediately (in parallel).
        - B's task starts but immediately awaits A's task.
        - Once A finishes, B proceeds with A's Synapse ID available for
          its provenance record.

    Arguments:
        upload_plan: The topologically sorted upload plan produced by
            _build_upload_plan.
        syn: Authenticated Synapse client.

    Returns:
        A list of asyncio tasks, one per file, that can be passed to
        asyncio.gather for concurrent execution.
    """
    created_tasks_by_path: dict[str, asyncio.Task] = {}

    for file_path, prerequisite_paths in upload_plan.path_to_dependencies.items():
        # Topological sort guarantees every prerequisite was already created.
        prerequisite_tasks = [created_tasks_by_path[p] for p in prerequisite_paths]

        upload_item = upload_plan.path_to_upload_item[file_path]
        file_task = asyncio.create_task(
            _upload_file_async(
                file_entity=upload_item.entity,
                used=upload_item.used,
                executed=upload_item.executed,
                activity_name=upload_item.activity_name,
                activity_description=upload_item.activity_description,
                prerequisite_tasks=prerequisite_tasks,
                syn=syn,
            )
        )
        created_tasks_by_path[file_path] = file_task

    return list(created_tasks_by_path.values())


def _build_activity_linkage(
    used_or_executed: Iterable[str | File],
    resolved_file_ids: dict[str, str],
) -> list[UsedEntity | UsedURL]:
    """Convert raw provenance references into typed Synapse objects (UsedEntity
    or UsedURL) that the Activity model expects.

    Each item in the input list is one of two things:

    1. A File object -- already resolved from a prior provenance pass. Its
       id is extracted and wrapped in a UsedEntity.
    2. A string -- delegated to _resolve_linkage_item, which checks in
       priority order:
       - A local file path that was just uploaded (found in
         resolved_file_ids, mapped to a Synapse ID, returned as UsedEntity).
       - A URL (returned as UsedURL).
       - A Synapse ID like syn123 or syn123.4 (parsed into ID + optional
         version, returned as UsedEntity).
       - If none match, raises ValueError.

    The resolved_file_ids dict is built by _upload_file_async after
    prerequisite uploads finish, mapping each uploaded file's local path to
    the Synapse ID it received. This is how provenance references between
    files in the same manifest batch get wired up: file A uploads first and
    gets syn111, then when file B (which declares used: /path/to/A) uploads,
    resolved_file_ids maps /path/to/A to syn111.

    Arguments:
        used_or_executed: The list of used or executed items. Each item is either
            a File object (already resolved from provenance), or a string that is
            a local file path, URL, or Synapse ID.
        resolved_file_ids: A dictionary that maps the local path of a file to the
            Synapse ID it received after upload. Populated by _upload_file_async
            once prerequisite uploads complete.

    Returns:
        A list of UsedEntity or UsedURL objects.

    Raises:
        ValueError: If a string item is not a resolved file path, a URL, or a
            valid Synapse ID.
    """
    from synapseclient.models import UsedEntity

    return [
        (
            UsedEntity(target_id=item.id)
            if not isinstance(item, str)
            else _resolve_linkage_item(item, resolved_file_ids)
        )
        for item in used_or_executed
    ]


def _resolve_linkage_item(
    item: str,
    resolved_file_ids: dict[str, str],
) -> UsedEntity | UsedURL:
    """Resolve a single string provenance reference to a UsedEntity or UsedURL.

    Arguments:
        item: A string provenance reference — a local file path present in
            resolved_file_ids, a URL, or a Synapse ID.
        resolved_file_ids: Maps local file paths to their Synapse IDs (populated
            after prerequisite uploads complete).

    Returns:
        A UsedEntity if the item resolves to a Synapse ID (either via
        resolved_file_ids or directly), or a UsedURL if the item is a URL.

    Raises:
        ValueError: If the item is not a resolved file path, a URL, or a
            valid Synapse ID.
    """
    from synapseclient.models import UsedEntity, UsedURL

    resolved_file_id = resolved_file_ids.get(item)
    if resolved_file_id:
        return UsedEntity(target_id=resolved_file_id)
    if is_url(item):
        return UsedURL(url=item)
    if not is_synapse_id_str(item):
        raise ValueError(f"{item} is not a valid Synapse id")
    syn_id, version = get_synid_and_version(item)
    return UsedEntity(
        target_id=syn_id,
        target_version_number=int(version) if version else None,
    )


async def _upload_file_async(
    file_entity: File,
    used: Iterable[str | File],
    executed: Iterable[str | File],
    activity_name: str,
    activity_description: str,
    prerequisite_tasks: list[asyncio.Task],
    syn: Synapse,
) -> File:
    """Upload a single file, waiting for any provenance dependencies to finish first.

    This function is invoked as an asyncio.Task by _create_upload_tasks. Many
    instances run concurrently, but each one self-serializes by awaiting only
    its specific prerequisites. Files with no dependencies start uploading
    immediately in parallel.

    The flow:

    1. Wait for prerequisites -- if this file declares provenance on other
       files in the same manifest batch (e.g. "file B was derived from
       file A"), those files must be uploaded first so they have Synapse IDs.
       asyncio.wait blocks until all prerequisite upload tasks finish, then
       a path-to-Synapse-ID mapping is collected from their results.
    2. Build provenance linkages -- converts the raw used and executed
       references (local paths, URLs, Synapse IDs, or File objects) into
       typed UsedEntity/UsedURL objects. Local paths are resolved to Synapse
       IDs using the mapping from step 1.
    3. Attach Activity -- if any provenance references exist, creates an
       Activity with the name, description, and linkages, and attaches it
       to the file.
    4. Store -- calls file_entity.store_async() to perform the actual upload.
    5. Return -- the returned File (now with a Synapse ID) becomes available
       to downstream tasks that depend on it via the resolved mapping.

    Arguments:
        file_entity: The File entity to upload.
        used: Provenance used references (paths, URLs, Synapse IDs, or File
            objects).
        executed: Provenance executed references.
        activity_name: Name for the provenance Activity.
        activity_description: Description for the provenance Activity.
        prerequisite_tasks: Tasks for files that must be uploaded before this one.
        syn: Authenticated Synapse client.

    Returns:
        The stored File entity.

    Raises:
        RuntimeError: If prerequisite tasks have not all completed.
        ValueError: If a provenance item is not a resolved file ID, a URL,
            or a valid Synapse ID.
    """
    from synapseclient.models import Activity

    # Step 1: Wait for prerequisite uploads to finish and collect their
    # Synapse IDs so provenance references can point to them.
    resolved_file_ids: dict[str, str] = {}
    if prerequisite_tasks:
        finished_dependencies, pending = await asyncio.wait(
            prerequisite_tasks, return_when=asyncio.ALL_COMPLETED
        )
        # Defensive check: ALL_COMPLETED guarantees pending is empty, but
        # guard against unexpected asyncio behavior or future refactors.
        if pending:
            raise RuntimeError(
                f"There were {len(pending)} dependencies left when storing {file_entity}"
            )
        for finished_dependency in finished_dependencies:
            result: File = finished_dependency.result()
            resolved_file_ids[result.path] = result.id

    # Step 2: Convert raw provenance references (local paths, URLs, Synapse
    # IDs, File objects) into typed UsedEntity/UsedURL objects. Local paths
    # are resolved to Synapse IDs using the mapping built in step 1.
    used_activity = _build_activity_linkage(
        used_or_executed=used, resolved_file_ids=resolved_file_ids
    )
    executed_activity = _build_activity_linkage(
        used_or_executed=executed, resolved_file_ids=resolved_file_ids
    )

    # Step 3: Attach an Activity to the file if provenance was declared.
    if used_activity or executed_activity:
        file_entity.activity = Activity(
            name=activity_name,
            description=activity_description,
            used=used_activity,
            executed=executed_activity,
        )

    # Step 4: Upload and return the file (now with a Synapse ID).
    await file_entity.store_async(synapse_client=syn)
    return file_entity


def _split_csv_cell(input_string: str) -> list[str]:
    """Split a string on commas that are not inside double quotes.

    Arguments:
        input_string: A string to split apart.

    Returns:
        The list of split items as strings.
    """
    parts = _COMMAS_OUTSIDE_DOUBLE_QUOTES_PATTERN.split(input_string)
    return [item.strip() for item in parts]


async def _resolve_row(
    path: str, row: Series, frame: DataFrame, client: Synapse
) -> tuple[str, dict[str, list[str | File]]]:
    """Resolve provenance columns for a single manifest row.

    Arguments:
        path: The file path for this row (used as its manifest key).
        row: The pandas Series for this row.
        frame: The full manifest DataFrame (path-indexed), passed through to
            _resolve_provenance_column for cross-row lookups.
        client: Authenticated Synapse client.

    Returns:
        A (path, resolved) tuple where resolved maps column names
        (used and/or executed) to their resolved reference lists.

    Raises:
        SynapseProvenanceError: If a provenance item is neither a local file
            path, a URL, nor a valid Synapse ID.
    """
    resolved: dict[str, list[str | File]] = {}
    for col in ("used", "executed"):
        if col in row:
            resolved[col] = await _resolve_provenance_column(
                row[col], path, client, frame
            )
    return path, resolved


async def _resolve_provenance_column(
    cell: str | list[str | File],
    path: str,
    syn: Synapse,
    df: DataFrame,
) -> list[str | File]:
    """Parse and resolve all provenance references in a single manifest cell.

    Handles cells that are already Python lists (converted by
    _parse_annotation_cell) as well as raw
    semicolon-delimited strings from the CSV.  Each item is validated and
    resolved via _check_provenance.

    Arguments:
        cell: The raw cell value from the used or executed column —
            either a semicolon-delimited string or an already-parsed list.
        path: The manifest file path of the row that owns this cell. Used only
            for error messages.
        syn: Authenticated Synapse client.
        df: The manifest DataFrame (path-indexed), used to check whether a
            local file path is part of the current upload batch.

    Returns:
        A list of resolved provenance references.

    Raises:
        SynapseProvenanceError: If a provenance item is neither a local file
            path, a URL, nor a valid Synapse ID.
    """
    items: list[str | File]
    if isinstance(cell, list):
        items = cell
    else:
        items = list(cell.split(";")) if cell.strip() != "" else []

    resolved = await asyncio.gather(
        *[
            _resolve_provenance_item(
                item.strip() if isinstance(item, str) else item,
                owner_path=path,
                syn=syn,
                df=df,
            )
            for item in items
        ]
    )
    return [item for item in resolved if item is not None]


async def _resolve_provenance_item(
    item: str | File | None, owner_path: str, syn: Synapse, df: DataFrame
) -> str | File | None:
    """Resolve a single provenance reference to its normalized form.

    Items that are already resolved (None, non-string File objects, URLs,
    Synapse IDs) are returned as-is. Local file paths are handed off to
    _resolve_local_file_provenance for batch/Synapse lookup.

    Arguments:
        item: A provenance reference — a local file path, a URL, a Synapse
            ID, a File object from a prior resolution pass, or None.
        owner_path: The manifest file path of the file that declares this
            provenance reference. Used only for error messages.
        syn: Authenticated Synapse client.
        df: The manifest DataFrame (path-indexed), used to check whether a
            local file path is part of the current upload batch.

    Returns:
        The resolved reference: a str (absolute local path, URL, or Synapse
        ID), a File (looked up via MD5), or None.

    Raises:
        SynapseProvenanceError: If the item is a local file that is neither
            being uploaded nor found in Synapse.
    """
    if item is None or not isinstance(item, str):
        return item

    if is_url(item) or is_synapse_id_str(item) is not None:
        return item

    return await _resolve_local_file_provenance(item, owner_path, syn, df)


async def _resolve_local_file_provenance(
    raw_path: str, owner_path: str, syn: Synapse, manifest_by_path: DataFrame
) -> str | File:
    """Resolve a local file path to either an in-batch path or a Synapse File.

    Given a manifest row that declares provenance on a local file, this
    function determines where that file is:

    1. If the file does not exist on disk, the provenance reference is
       broken and a SynapseProvenanceError is raised.
    2. If the file is in the current upload batch (present in
       manifest_by_path.index), its absolute path is returned as a string.
       The Synapse ID will be resolved later, after that file is uploaded.
    3. If the file exists on disk but is not being uploaded, it is looked
       up in Synapse by MD5 hash. If found, the File object is returned
       so its Synapse ID can be used for provenance. If not found, a
       SynapseProvenanceError is raised because the reference cannot be
       linked.

    Arguments:
        raw_path: A local file path string from a provenance cell, not yet
            expanded or normalized.
        owner_path: The manifest file path of the file that declares this
            provenance reference. Used only for error messages.
        syn: Authenticated Synapse client.
        manifest_by_path: The manifest DataFrame indexed by absolute file path.

    Returns:
        str — the absolute path if the file is in the upload batch.
        File — a Synapse File model if the file already exists in Synapse.

    Raises:
        SynapseProvenanceError: If the path does not exist on disk, or the
            file is neither in the upload batch nor found in Synapse.
    """
    from synapseclient.models.file import File

    absolute_path = _expand_path(raw_path)

    if not os.path.isfile(absolute_path):
        raise SynapseProvenanceError(
            f"The provenance record for file: {owner_path} is incorrect.\n"
            f"Specifically {raw_path} is not an existing file path, a valid URL, or a Synapse ID."
        )

    if absolute_path in manifest_by_path.index:
        return absolute_path

    try:
        return await File.from_path_async(path=absolute_path, synapse_client=syn)
    except SynapseFileNotFoundError as e:
        raise SynapseProvenanceError(
            f"The provenance record for file: {owner_path} is incorrect.\n"
            f"Specifically {absolute_path} is not being uploaded and is not in Synapse."
        ) from e


GENERATED_MANIFEST_COLUMNS = ["path", "parentId"]


async def _generate_sync_manifest_async(
    directory_path: str,
    parent_id: str,
    manifest_path: str,
    *,
    synapse_client: Synapse | None = None,
) -> None:
    """Implementation backing
    [StorableContainer.generate_sync_manifest_async][synapseclient.models.mixins.StorableContainer.generate_sync_manifest_async].

    See the mixin method docstring for behavior, arguments, and examples.
    """
    client = Synapse.get_client(synapse_client=synapse_client)
    # realpath (not abspath) so that if directory_path is itself a symlink,
    # the manifest records paths under the resolved target. That keeps the
    # manifest valid if the original symlink is later removed or repointed.
    directory_path = os.path.realpath(directory_path)
    if not os.path.isdir(directory_path):
        raise ValueError(f"{directory_path} is not a directory or does not exist")
    await _validate_target_container_async(parent_id, client=client)
    rows: list[dict[str, str]] = []
    # os.walk descends top-down, so a directory's parent is always registered
    # here before the directory itself is visited.
    local_to_synapse_parent: dict[str, str] = {directory_path: parent_id}

    for dirpath, dirnames, filenames in os.walk(
        directory_path, onerror=functools.partial(_log_walk_error, client)
    ):
        # Drop symlinked dirs so we don't create Synapse folders for
        # directories whose contents os.walk (followlinks=False) won't
        # visit. Mutating dirnames in place is the documented os.walk hook
        # for pruning the traversal.
        dirnames[:] = [
            d for d in dirnames if not os.path.islink(os.path.join(dirpath, d))
        ]
        # Sort in place so os.walk also descends in deterministic order.
        dirnames.sort()
        current_parent_id = local_to_synapse_parent[dirpath]

        created = await _create_child_folders_async(
            parent_id=current_parent_id, dirnames=dirnames, client=client
        )
        for dirname, folder in zip(dirnames, created):
            local_to_synapse_parent[os.path.join(dirpath, dirname)] = folder.id

        rows.extend(
            _collect_uploadable_rows(dirpath, filenames, current_parent_id, client)
        )

    if not rows:
        client.logger.warning(
            f"No uploadable files found under {directory_path};"
            " generated manifest contains only the header row."
        )
    _write_manifest_csv(manifest_path, rows)


async def _validate_target_container_async(parent_id: str, client: Synapse) -> None:
    """Verify that parent_id resolves to a Folder or Project in Synapse.

    Dedicated to manifest generation so error messages reference the
    container the user is writing into, not a manifest parentId column.

    Raises:
        SynapseHTTPError: If parent_id does not exist in Synapse.
        ValueError: If parent_id exists but is not a Folder or Project.
    """
    try:
        container = await factory_get_async(
            synapse_id=parent_id,
            file_options=FileOptions(download_file=False),
            synapse_client=client,
        )
    except SynapseHTTPError as err:
        if getattr(err.response, "status_code", None) == 404:
            client.logger.warning(f"{parent_id} is not a valid Synapse Id")
        raise

    from synapseclient.models.folder import Folder
    from synapseclient.models.project import Project

    if not isinstance(container, (Folder, Project)):
        raise ValueError(f"Container {parent_id} is not a Folder or Project")


def _log_walk_error(client: Synapse, err: OSError) -> None:
    """Log and skip an os.walk I/O error during manifest generation."""
    client.logger.warning(
        f"Skipping unreadable path during manifest generation:"
        f" {err.filename} ({err})"
    )


async def _create_child_folders_async(
    parent_id: str, dirnames: list[str], client: Synapse
) -> list[Folder]:
    """Create sibling folders concurrently under a shared Synapse parent.

    Sibling folders have no ordering dependency on each other, so they are
    gathered in a single batch rather than awaited one at a time.
    """
    from synapseclient.models.folder import Folder

    return await asyncio.gather(
        *[
            Folder(name=dirname, parent_id=parent_id).store_async(synapse_client=client)
            for dirname in dirnames
        ]
    )


def _collect_uploadable_rows(
    dirpath: str,
    filenames: Iterable[str],
    parent_id: str,
    client: Synapse,
) -> list[dict[str, str]]:
    """Build manifest rows for the uploadable files in a single directory.

    Files are visited in sorted order so manifest output is deterministic.
    Unreadable and zero-byte files are logged and dropped by
    _is_uploadable_file.
    """
    rows: list[dict[str, str]] = []
    for filename in sorted(filenames):
        filepath = os.path.join(dirpath, filename)
        if _is_uploadable_file(filepath, client):
            rows.append({"path": filepath, "parentId": parent_id})
    return rows


def _write_manifest_csv(manifest_path: str, rows: list[dict[str, str]]) -> None:
    """Write generated manifest rows to a CSV at manifest_path."""
    with open(manifest_path, "w", encoding="utf-8", newline="") as fp:
        writer = csv.DictWriter(fp, fieldnames=GENERATED_MANIFEST_COLUMNS)
        writer.writeheader()
        writer.writerows(rows)


def _is_uploadable_file(filepath: str, client: Synapse) -> bool:
    """Return True if filepath can be included in a generated manifest.

    Logs a warning and returns False for files that cannot be uploaded:
    unreadable files (broken symlinks, permission errors, races) and
    zero-byte files (rejected by Synapse).
    """
    try:
        size = os.stat(filepath).st_size
    except OSError as err:
        client.logger.warning(
            f"Skipping unreadable file during manifest generation:"
            f" {filepath} ({err})"
        )
        return False
    if size == 0:
        client.logger.warning(
            f"Skipping zero-byte file (empty files cannot be"
            f" uploaded to Synapse): {filepath}"
        )
        return False
    return True
