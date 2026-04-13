"""Services for reading a Synapse manifest CSV file and preparing it for upload."""

from __future__ import annotations

import ast
import asyncio
import datetime
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


class SyncUploadItem(NamedTuple):
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


@dataclass
class SyncUploader:
    """Manages the uploads associated with a sync_to_synapse call.

    Files will be uploaded concurrently and in an order that honours any
    interdependent provenance.
    """

    syn: Synapse

    @dataclass
    class DependencyGraph:
        """The graph that represents the dependencies of the files to be uploaded.

        Attributes:
            path_to_dependencies: A dictionary where the key is the path of the file and
                the value is a list of paths that need to be uploaded before the key can
                be uploaded.
            path_to_upload_item: A dictionary where the key is the path of the file and
                the value is the upload item that is associated with the file.
            path_to_file_check: A dictionary where the key is the path of the file and
                the value is a boolean that represents if the file is a file or not.
        """

        path_to_dependencies: dict[str, list[str]]
        path_to_upload_item: dict[str, SyncUploadItem]
        path_to_file_check: dict[str, bool]

    def _build_dependency_graph(
        self, items: Iterable[SyncUploadItem]
    ) -> DependencyGraph:
        """Determine the order in which the files should be uploaded based on their
        dependencies.  This will also verify that the dependencies are valid and that
        there are no cycles in the graph.

        Arguments:
            items: The list of items to upload.

        Return:
            A graph that represents information about how to upload the graph of items
            into Synapse.
        """
        items_by_path = {i.entity.path: i for i in items}
        graph: dict[str, list[str]] = {}
        resolved_file_checks: dict[str, bool] = {}

        for item in items:
            item_file_provenance: list[str] = []
            for provenance_dependency in item.used + item.executed:
                # File objects (already in Synapse) are not local-path
                # dependencies — skip them in the dependency graph.
                if not isinstance(provenance_dependency, str):
                    continue
                if provenance_dependency in resolved_file_checks:
                    is_file = resolved_file_checks[provenance_dependency]
                else:
                    is_file = os.path.isfile(provenance_dependency)
                    resolved_file_checks[provenance_dependency] = is_file
                if is_file:
                    if provenance_dependency not in items_by_path:
                        raise ValueError(
                            f"{item.entity.path} depends on"
                            f" {provenance_dependency} which is not being uploaded"
                        )
                    item_file_provenance.append(provenance_dependency)

            graph[item.entity.path] = item_file_provenance

        graph_sorted = topolgical_sort(graph)
        path_to_dependencies_sorted: dict[str, list[str]] = {}
        path_to_upload_items_sorted: dict[str, SyncUploadItem] = {}
        for path, dependency_paths in graph_sorted:
            path_to_dependencies_sorted[path] = dependency_paths
            path_to_upload_items_sorted[path] = items_by_path.get(path)

        return self.DependencyGraph(
            path_to_dependencies=path_to_dependencies_sorted,
            path_to_upload_item=path_to_upload_items_sorted,
            path_to_file_check=resolved_file_checks,
        )

    def _build_tasks_from_dependency_graph(
        self, dependency_graph: DependencyGraph
    ) -> list[asyncio.Task]:
        """Build the asyncio tasks that will be used to upload the files in the correct
        order based on their dependencies.

        Arguments:
            dependency_graph: The graph that represents the dependencies of the files to
                be uploaded.

        Return:
            A list of asyncio tasks that will upload the files in the correct order.
        """
        created_tasks_by_path: dict[str, asyncio.Task] = {}

        for (
            file_path,
            dependent_file_paths,
        ) in dependency_graph.path_to_dependencies.items():
            dependent_tasks: list[asyncio.Task] = []
            for dependent_file in dependent_file_paths:
                task = created_tasks_by_path.get(dependent_file)
                if task is not None:
                    dependent_tasks.append(task)

            upload_item = dependency_graph.path_to_upload_item.get(file_path)
            file_task = asyncio.create_task(
                self._upload_item_async(
                    item=upload_item.entity,
                    used=upload_item.used,
                    executed=upload_item.executed,
                    activity_name=upload_item.activity_name,
                    activity_description=upload_item.activity_description,
                    dependent_futures=dependent_tasks,
                )
            )
            created_tasks_by_path[file_path] = file_task

        return list(created_tasks_by_path.values())

    async def upload(self, items: Iterable[SyncUploadItem]) -> list[File]:
        """Upload a number of files to Synapse as provided in the manifest file.  This
        will handle ordering the files based on their dependency graph.

        Arguments:
            items: The list of items to upload.

        Returns:
            List of File entities that were created or updated, in the same
            order as the dependency-graph task execution.
        """
        dependency_graph = self._build_dependency_graph(items=list(items))
        tasks = self._build_tasks_from_dependency_graph(
            dependency_graph=dependency_graph
        )
        results = await asyncio.gather(*tasks)
        return list(results)

    def _build_activity_linkage(
        self,
        used_or_executed: Iterable[str | File],
        resolved_file_ids: dict[str, str],
    ) -> list[UsedEntity | UsedURL]:
        """Loop over the incoming list of used or executed items and build the
        appropriate UsedEntity or UsedURL objects.

        Arguments:
            used_or_executed: The list of used or executed items.
            resolved_file_ids: A dictionary that maps the path of a file to its Synapse
                ID.

        Returns:
            A list of UsedEntity or UsedURL objects.
        """
        from synapseclient.models import UsedEntity, UsedURL

        returned_linkage: list[UsedEntity | UsedURL] = []
        for item in used_or_executed:
            if not isinstance(item, str):
                # item is a File object resolved from provenance — use its ID
                returned_linkage.append(UsedEntity(target_id=item.id))
                continue
            resolved_file_id = resolved_file_ids.get(item, None)
            if resolved_file_id:
                returned_linkage.append(UsedEntity(target_id=resolved_file_id))
            elif is_url(item):
                returned_linkage.append(UsedURL(url=item))
            else:
                if not is_synapse_id_str(item):
                    raise ValueError(f"{item} is not a valid Synapse id")
                syn_id, version = get_synid_and_version(item)
                target_version = int(version) if version else None
                returned_linkage.append(
                    UsedEntity(target_id=syn_id, target_version_number=target_version)
                )
        return returned_linkage

    async def _upload_item_async(
        self,
        item: File,
        used: Iterable[str | File],
        executed: Iterable[str | File],
        activity_name: str,
        activity_description: str,
        dependent_futures: list[asyncio.Future],
    ) -> File:
        """Upload a single file, waiting for any provenance dependencies to finish first.

        Arguments:
            item: The File entity to upload.
            used: Provenance used references (paths, URLs, Synapse IDs, or File
                objects).
            executed: Provenance executed references.
            activity_name: Name for the provenance Activity.
            activity_description: Description for the provenance Activity.
            dependent_futures: Futures for files that must be uploaded before this one.

        Returns:
            The stored File entity.
        """
        from synapseclient.models import Activity

        resolved_file_ids: dict[str, str] = {}
        if dependent_futures:
            finished_dependencies, pending = await asyncio.wait(
                dependent_futures, return_when=asyncio.ALL_COMPLETED
            )
            if pending:
                raise RuntimeError(
                    f"There were {len(pending)} dependencies left when storing {item}"
                )
            for finished_dependency in finished_dependencies:
                result = finished_dependency.result()
                resolved_file_ids[result.path] = result.id

        used_activity = self._build_activity_linkage(
            used_or_executed=used, resolved_file_ids=resolved_file_ids
        )
        executed_activity = self._build_activity_linkage(
            used_or_executed=executed, resolved_file_ids=resolved_file_ids
        )

        if used_activity or executed_activity:
            item.activity = Activity(
                name=activity_name,
                description=activity_description,
                used=used_activity,
                executed=executed_activity,
            )
        await item.store_async(synapse_client=self.syn)
        return item


async def read_manifest_for_upload(
    manifest_path: str,
    syn: Synapse,
    merge_existing_annotations: bool,
    associate_activity_to_new_version: bool,
) -> tuple[list[SyncUploadItem], int]:
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
        SyncUploadItem objects ready for upload and total_bytes is the
        combined size of all local files.

    Raises:
        ValueError: If required columns are missing, paths are not unique,
            files are empty, or file names are invalid.
        OSError: If a non-URL path in the manifest does not exist on disk.
    """
    test_import_pandas()
    import pandas as pd

    syn.logger.info(f"Validating manifest: {manifest_path}")
    df = pd.read_csv(manifest_path)

    # Skip rows that failed to download (error column added by get-download-list)
    if "error" in df.columns:
        df = df[df["error"].fillna("") == ""]

    if df.empty:
        return [], 0

    # Validate required columns before attempting any column-dependent operations
    for col in ("path", "parentId"):
        if col not in df.columns:
            raise ValueError(f"Manifest must contain a '{col}' column")

    # synapseStore defaults must be set before fillna("") so that isnull()
    # correctly identifies rows that still need the default (True). After
    # fillna the NaN sentinels would be replaced with "" and isnull() would
    # return False for every row.
    if "synapseStore" not in df.columns:
        df["synapseStore"] = None
    df.loc[df["path"].apply(is_url), "synapseStore"] = False
    df.loc[df["synapseStore"].isnull(), "synapseStore"] = True
    df["synapseStore"] = df["synapseStore"].astype(bool)

    df = df.fillna("")

    syn.logger.info("Validating that all paths exist...")
    df["path"] = df["path"].apply(_check_path_and_normalize)

    syn.logger.info("Validating that all files are unique...")
    if df["path"].duplicated().any():
        raise ValueError(
            "All rows in manifest must contain a unique file path to upload"
        )

    if "name" not in df.columns:
        df["name"] = df["path"].apply(os.path.basename)
    empty_names = df["name"] == ""
    if empty_names.any():
        df.loc[empty_names, "name"] = df.loc[empty_names, "path"].apply(
            os.path.basename
        )

    syn.logger.info("Validating that all the files are not empty...")
    total_size = _check_size_each_file(df)

    syn.logger.info("Validating file names...")
    _check_file_names(df)

    parent_ids = df["parentId"].unique()
    syn.logger.info("Validating provenance and parent containers...")
    df, _ = await asyncio.gather(
        _sort_and_fix_provenance(syn, df),
        _check_parent_containers_async(parent_ids, syn=syn),
    )

    items = _build_upload_items(
        df,
        merge_existing_annotations=merge_existing_annotations,
        associate_activity_to_new_version=associate_activity_to_new_version,
    )

    return items, total_size


def _split_csv_cell(input_string: str) -> list[str]:
    """Split a string on commas that are not inside double quotes.

    Arguments:
        input_string: A string to split apart.

    Returns:
        The list of split items as strings.
    """
    parts = _COMMAS_OUTSIDE_DOUBLE_QUOTES_PATTERN.split(input_string)
    return [item.strip() for item in parts]


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


async def _check_parent_containers_async(
    parent_ids: Iterable[str], syn: Synapse
) -> None:
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
    df = df.set_index("path")

    results = await asyncio.gather(
        *[_resolve_row(str(path), row, df, syn) for path, row in df.iterrows()]
    )

    deps: dict[str, list[str]] = {}
    for path, resolved in results:
        all_refs: list[str] = []
        for col, values in resolved.items():
            df.at[path, col] = values
            all_refs.extend(v for v in values if isinstance(v, str))
        deps[path] = all_refs

    sorted_paths = topolgical_sort(deps)
    df = df.reindex([i[0] for i in sorted_paths])
    return df.reset_index()


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
    """
    items: list[str | File]
    if isinstance(cell, list):
        items = cell
    else:
        # cell is guaranteed to be a str here; .strip() is safe.
        items = list(cell.split(";")) if cell.strip() != "" else []

    return [
        r
        for r in await asyncio.gather(
            *[
                _check_provenance(
                    item.strip() if isinstance(item, str) else item,
                    path,
                    syn,
                    df,
                )
                for item in items
            ]
        )
        if r is not None
    ]


async def _check_provenance(
    item: str | File | None, path: str, syn: Synapse, df: DataFrame
) -> str | File | None:
    """Resolve and validate a single provenance reference.

    Arguments:
        item: A provenance item string — a local file path, a URL, a Synapse
            ID, or None.
        path: The manifest file path of the file that declares this provenance
            reference. Used only for error messages.
        syn: Authenticated Synapse client, used to look up local files that are
            not in the upload manifest but may already exist in Synapse.
        df: The manifest DataFrame (path-indexed), used to determine whether a
            local file path is part of the current upload batch.

    Returns:
        str — an absolute local path (item in the upload batch), a URL,
        or a Synapse ID passed through unchanged.
        File — a Synapse File model resolved via MD5 lookup when the local
        file already exists in Synapse but is not in the current upload batch.
        None — when item is None.

    Raises:
        SynapseProvenanceError: If the item is a local file that is neither
            being uploaded nor found in Synapse, or if the item is not a local
            file path, URL, or valid Synapse ID.
    """
    if item is None:
        return item

    # Non-string items (e.g. File objects from a prior pass) are already resolved
    if not isinstance(item, str):
        return item

    # URLs and Synapse IDs are valid provenance references as-is
    if is_url(item) or (is_synapse_id_str(item) is not None):
        return item

    # Resolve as a local file path
    item_path_normalized = _expand_path(item)

    if not os.path.isfile(item_path_normalized):
        raise SynapseProvenanceError(
            f"The provenance record for file: {path} is incorrect.\n"
            f"Specifically {item} is not an existing file path, a valid URL, or a Synapse ID."
        )

    # Local file in the upload batch
    if item_path_normalized in df.index:
        return item_path_normalized

    # Local file not in the upload batch — check if it already exists in Synapse using MD5 hash.
    # Lazy import to avoid circular dependency with synapseclient.models.
    from synapseclient.models.file import File

    try:
        return await File.from_path_async(path=item_path_normalized, synapse_client=syn)
    except SynapseFileNotFoundError as e:
        raise SynapseProvenanceError(
            f"The provenance record for file: {path} is incorrect.\n"
            f"Specifically {item_path_normalized} is not being uploaded and is not in Synapse."
        ) from e


def _build_upload_items(
    df: DataFrame,
    merge_existing_annotations: bool,
    associate_activity_to_new_version: bool,
) -> list[SyncUploadItem]:
    """Convert a validated manifest DataFrame into a list of upload items.

    All columns not in NON_ANNOTATION_COLUMNS are treated as annotations.

    Arguments:
        df: Validated manifest DataFrame (after provenance sort).
        merge_existing_annotations: Passed through to each File.
        associate_activity_to_new_version: Passed through to each File.

    Returns:
        List of upload items.
    """
    from synapseclient.models.file import (
        File,  # lazy import to avoid circular dependency
    )

    items = []
    for _, row in df.iterrows():
        raw_force_version = row.get("forceVersion", "")
        if raw_force_version == "":
            force_version = True
        elif isinstance(raw_force_version, bool):
            force_version = raw_force_version
        else:
            # CSV strings like "True"/"False" need explicit conversion;
            # fall back to True if the value is unrecognisable.
            parsed = bool_or_none(str(raw_force_version))
            force_version = parsed if parsed is not None else True
        file_entity = File(
            path=row["path"],
            parent_id=row["parentId"],
            name=row.get("name") or None,
            id=row.get("ID") or None,
            synapse_store=row.get("synapseStore", True),
            content_type=row.get("contentType") or None,
            force_version=force_version,
            merge_existing_annotations=merge_existing_annotations,
            associate_activity_to_new_version=associate_activity_to_new_version,
            _present_manifest_fields=list(row.index),
        )

        annotation_cols: dict[str, object] = {
            str(k): v for k, v in row.items() if k not in NON_ANNOTATION_COLUMNS
        }
        file_entity.annotations = _build_annotations_for_file(annotation_cols)

        item = SyncUploadItem(
            file_entity,
            used=row.get("used", []) or [],
            executed=row.get("executed", []) or [],
            activity_name=row.get("activityName") or None,
            activity_description=row.get("activityDescription") or None,
        )
        items.append(item)

    return items


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
