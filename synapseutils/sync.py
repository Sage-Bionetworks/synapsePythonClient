"""This module is responsible for holding sync to/from synapse utility functions."""

import ast
import asyncio
import csv
import datetime
import io
import os
import re
import sys
from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, Iterable, List, NamedTuple, Tuple, Union

from deprecated import deprecated
from tqdm import tqdm

from synapseclient import File as SynapseFile
from synapseclient import Folder as SynapseFolder
from synapseclient import Project as SynapseProject
from synapseclient import Synapse, table
from synapseclient.api import get_entity, get_entity_id_bundle2
from synapseclient.core import utils
from synapseclient.core.async_utils import wrap_async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.exceptions import (
    SynapseError,
    SynapseFileNotFoundError,
    SynapseHTTPError,
    SynapseNotFoundError,
    SynapseProvenanceError,
)
from synapseclient.core.transfer_bar import shared_download_progress_bar
from synapseclient.core.upload.multipart_upload_async import (
    shared_progress_bar as upload_shared_progress_bar,
)
from synapseclient.core.utils import (
    bool_or_none,
    datetime_or_none,
    get_synid_and_version,
    id_of,
    is_synapse_id_str,
    is_url,
)
from synapseclient.entity import is_container
from synapseclient.models import Activity, File, UsedEntity, UsedURL

from .monitor import notify_me_async

if TYPE_CHECKING:
    from synapseclient.models import Folder, Project

# When new fields are added to the manifest they will also need to be added to
# file.py#_determine_fields_to_ignore_in_merge
REQUIRED_FIELDS = ["path", "parent"]
FILE_CONSTRUCTOR_FIELDS = ["name", "id", "synapseStore", "contentType"]
STORE_FUNCTION_FIELDS = ["activityName", "activityDescription", "forceVersion"]
PROVENANCE_FIELDS = ["used", "executed"]
MAX_RETRIES = 4
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


def syncFromSynapse(
    syn: Synapse,
    entity: Union[str, SynapseFile, SynapseProject, SynapseFolder],
    path: str = None,
    ifcollision: str = "overwrite.local",
    allFiles=None,
    followLink: bool = False,
    manifest: str = "all",
    downloadFile: bool = True,
):
    """Synchronizes a File entity, or a Folder entity, meaning all the files in a folder
    (including subfolders) from Synapse, and adds a readme manifest with file metadata.

    There are a few conversions around annotations to call out here.

    ## Conversion of objects from the REST API to Python native objects

    The first annotation conversion is to take the annotations from the REST API and
    convert them into Python native objects. For example the REST API will return a
    milliseconds since epoch timestamp for a datetime annotation, however, we want to
    convert that into a Python datetime object. These conversions take place in the
    [annotations module][synapseclient.annotations].


    ## Conversion of Python native objects into strings

    The second annotation conversion occurs when we are writing to the manifest TSV file.
    In this case we need to convert the Python native objects into strings that can be
    written to the manifest file. In addition we also need to handle the case where the
    annotation value is a list of objects. In this case we are converting the list
    into a single cell of data with a comma `,` delimiter wrapped in brackets `[]`.

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        entity: A Synapse ID, a Synapse Entity object of type file, folder or
                project.
        path: An optional path where the file hierarchy will be reproduced. If not
            specified the files will by default be placed in the synapseCache. A path
            is required in order to create a manifest file. A manifest is TSV file
            that is automatically created that contains metadata (annotations, storage
            location and provenance) of all downloaded files. If no files were
            downloaded, no manifest file will be created.
        ifcollision: Determines how to handle file collisions. Maybe
                     "overwrite.local", "keep.local", or "keep.both".
        allFiles: Deprecated and not to be used. This will be removed in v5.0.0.
        followLink: Determines whether the link returns the target Entity.
        manifest: Determines whether creating manifest file automatically. The
                  optional values here (`all`, `root`, `suppress`).
        downloadFile: Determines whether downloading the files.

    Returns:
        List of [files][synapseclient.File]


    When entity is a Project or Folder, this function will crawl all subfolders
    of the project/folder specified by `entity` and download all files that have
    not already been downloaded. When entity is a File the function will download the
    latest version of the file unless version is denoted in the synid with .version
    notiation (e.g. syn123.1) If there are newer files in Synapse (or a local file
    has been edited outside of the cache) since the last download then local the file
    will be replaced by the new file unless "ifcollision" is changed.

    If the files are being downloaded to a specific location outside of the Synapse
    cache a file (SYNAPSE_METADATA_MANIFEST.tsv) will also be added in the path that
    contains the metadata (annotations, storage location and provenance of all
    downloaded files).

    See also:

    - [synapseutils.sync.syncToSynapse][]

    Example: Using this function
        Download and print the paths of all downloaded files:

            entities = syncFromSynapse(syn, "syn1234")
            for f in entities:
                print(f.path)
    """

    if manifest not in ("all", "root", "suppress"):
        raise ValueError(
            'Value of manifest option should be one of the ("all", "root", "suppress")'
        )

    # we'll have the following threads:
    # 1. the entrant thread to this function walks the folder hierarchy and
    #    schedules files for download,
    #    and then waits for all the file downloads to complete
    # 2. each file download will run in a separate thread in an Executor
    # 3. downloads that support S3 multipart concurrent downloads will be scheduled
    #    by the thread in #2 and have
    #    their parts downloaded in additional threads in the same Executor

    with shared_download_progress_bar(file_size=1, synapse_client=syn):
        root_entity = wrap_async_to_sync(
            coroutine=_sync(
                syn=syn,
                entity=entity,
                path=path,
                if_collision=ifcollision,
                follow_link=followLink,
                download_file=downloadFile,
                manifest=manifest,
            ),
            syn=syn,
        )

    files = []

    from synapseclient.models import Folder, Project

    # Handle the creation of a manifest TSV file. The way that this works is that
    # a manifest is created for each directory level if "all" is specified. If "root"
    # is specified then only the root directory will have a manifest created.
    if isinstance(root_entity, Project) or isinstance(root_entity, Folder):
        files = root_entity.flatten_file_list()
        if manifest == "all" and path:
            for (
                directory_path,
                file_entities,
            ) in root_entity.map_directory_to_all_contained_files(
                root_path=path
            ).items():
                generate_manifest(
                    all_files=file_entities,
                    path=directory_path,
                )
        elif manifest == "root" and path:
            generate_manifest(
                all_files=files,
                path=path,
            )
    elif isinstance(root_entity, File):
        # When the root entity is a file we do not create a manifest file. This is
        # to match the behavior present in v4.x.x of the client.
        files = [root_entity]

    synapse_files = []
    for file in files:
        synapse_files.append(file._convert_into_legacy_file())

    # the allFiles parameter used to be passed in as part of the recursive
    # implementation of this function with the public signature invoking itself. now
    # that this isn't a recursive any longer we don't need allFiles as a parameter
    # (especially on the public signature) but it is retained for now for backwards
    # compatibility with external invokers. To be removed in v5.0.0.
    if allFiles is not None:
        allFiles.extend(synapse_files)
        synapse_files = allFiles

    return synapse_files


async def _sync(
    syn: Synapse,
    entity: Union[str, SynapseFile, SynapseProject, SynapseFolder],
    path: str = None,
    if_collision: str = "overwrite.local",
    follow_link: bool = False,
    download_file: bool = True,
    manifest: str = "all",
) -> Union["File", "Folder", "Project"]:
    """
    Given an entity to sync from synapse handle the download of the entity and
    its children.

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        entity: A Synapse ID, a Synapse Entity object of type file, folder or project.
        path: An optional path where the file hierarchy will be reproduced. If not
            specified the files will by default be placed in the synapseCache. A path
            is required in order to create a manifest file. A manifest is TSV file that
            is automatically created that contains metadata (annotations, storage
            location and provenance) of all downloaded files. If no files were
            downloaded, no manifest file will be created.
        if_collision: Determines how to handle file collisions. May be
            "overwrite.local", "keep.local", or "keep.both".
        follow_link: Determines whether the link returns the target Entity.
        download_file: Determines whether downloading the files.
        manifest: Determines whether creating manifest file automatically. The optional
            values here (all, root, suppress).

    """

    if is_synapse_id_str(entity):
        # ensure that we seed with an actual entity
        synid, version = get_synid_and_version(obj=entity)
        entity = await get_entity(
            entity_id=synid, version_number=version, synapse_client=syn
        )

    if entity is None:
        raise SynapseNotFoundError(f"Entity {entity or synid} not found.")

    entity_type = entity.get("concreteType", None)
    entity_id = id_of(entity)
    entity_version = entity.get("versionNumber", None)
    root_entity = None

    # Path is used to determine `include_activity` here because only when a path
    # is supplied can a manifest possibly be created. If we are not creating a
    # manifest we can skip the activity retrieval process.
    retrieve_activity = path is not None and path != ""
    if entity_type == concrete_types.PROJECT_ENTITY:
        from synapseclient.models import Project

        root_entity = await Project(id=entity_id).sync_from_synapse_async(
            path=path,
            download_file=download_file,
            if_collision=if_collision,
            include_activity=retrieve_activity,
            follow_link=follow_link,
            synapse_client=syn,
            recursive=True,
        )
    elif entity_type == concrete_types.FOLDER_ENTITY:
        from synapseclient.models import Folder

        root_entity = await Folder(id=entity_id).sync_from_synapse_async(
            path=path,
            download_file=download_file,
            if_collision=if_collision,
            include_activity=retrieve_activity,
            follow_link=follow_link,
            synapse_client=syn,
            recursive=True,
        )
    elif entity_type == concrete_types.FILE_ENTITY:
        root_entity = await File(
            id=entity_id,
            version_number=entity_version,
            if_collision=if_collision,
            path=path,
            download_file=download_file,
        ).get_async(
            include_activity=retrieve_activity,
            synapse_client=syn,
        )
    elif follow_link and entity_type == concrete_types.LINK_ENTITY:
        entity_bundle = await get_entity_id_bundle2(
            entity_id=entity_id,
            request={"includeEntity": True},
            synapse_client=syn,
        )

        return await _sync(
            syn=syn,
            entity=entity_bundle["entity"],
            path=path,
            if_collision=if_collision,
            follow_link=follow_link,
            download_file=download_file,
            manifest=manifest,
        )
    else:
        raise ValueError(
            "Cannot initiate a sync from an entity that is not a File, Folder, Project, or Link to a File/Folder."
        )

    return root_entity


class _SyncUploadItem(NamedTuple):
    """Represents a single file being uploaded.

    Attributes:
        entity: The file that is going through the sync process.
        used: Concept from Activity that can be a URL, Synapse ID, or path to a file.
        executed: Concept from Activity that can be a URL, Synapse ID, or path to a
            file.
        activity_name: The name of the activity that is being performed.
        activity_description: The description of the activity that is being performed.
    """

    entity: File
    used: Iterable[str]
    executed: Iterable[str]
    activity_name: str
    activity_description: str


@dataclass
class _SyncUploader:
    """
    Manages the uploads associated associated with a syncToSynapse call.
    Files will be uploaded concurrently and in an order that honors any
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

        path_to_dependencies: Dict[str, List[str]]
        path_to_upload_item: Dict[str, _SyncUploadItem]
        path_to_file_check: Dict[str, bool]

    def _build_dependency_graph(
        self, items: Iterable[_SyncUploadItem]
    ) -> DependencyGraph:
        """
        Determine the order in which the files should be uploaded based on their
        dependencies. This will also verify that the dependencies are valid and that
        there are no cycles in the graph.

        Arguments:
            items: The list of items to upload.

        Return:
            A graph that represents information about how to upload the graph of items
            into Synapse.
        """

        items_by_path = {i.entity.path: i for i in items}
        graph = {}
        resolved_file_checks = {}

        for item in items:
            item_file_provenance = []
            for provenance_dependency in item.used + item.executed:
                is_file = resolved_file_checks.get(
                    provenance_dependency, None
                ) or os.path.isfile(provenance_dependency)
                if provenance_dependency not in resolved_file_checks:
                    resolved_file_checks.update({provenance_dependency: is_file})
                if is_file:
                    if provenance_dependency not in items_by_path:
                        # an upload lists provenance of a file that is not itself
                        # included in the upload
                        raise ValueError(
                            f"{item.entity.path} depends on"
                            f" {provenance_dependency} which is not being uploaded"
                        )

                    item_file_provenance.append(provenance_dependency)

            graph[item.entity.path] = item_file_provenance

        # Used to verify that the graph does not contain any cycles
        graph_sorted = utils.topolgical_sort(graph)
        path_to_dependencies_sorted = {}
        path_to_upload_items_sorted = {}
        for path, dependency_paths in graph_sorted:
            path_to_dependencies_sorted.update({path: dependency_paths})
            path_to_upload_items_sorted.update({path: items_by_path.get(path)})

        return self.DependencyGraph(
            path_to_dependencies=path_to_dependencies_sorted,
            path_to_upload_item=path_to_upload_items_sorted,
            path_to_file_check=resolved_file_checks,
        )

    def _build_tasks_from_dependency_graph(
        self, dependency_graph: DependencyGraph
    ) -> List[asyncio.Task]:
        """
        Build the asyncio tasks that will be used to upload the files in the correct
        order based on their dependencies.

        Arguments:
            dependency_graph: The graph that represents the dependencies of the files to
                be uploaded.

        Return:
            A list of asyncio tasks that will upload the files in the correct order.

        """
        created_tasks_by_path = {}

        # Because the graph is sorted in topological order, we can iterate over the
        # paths in order and create a task for each file. This ensures that before we
        # get to any files that have a dependency the Task to save the dependency has
        # already been created.
        for (
            file_path,
            dependent_file_paths,
        ) in dependency_graph.path_to_dependencies.items():
            dependent_tasks = []
            for dependent_file in dependent_file_paths:
                if dependency_graph.path_to_file_check.get(dependent_file, None):
                    dependent_tasks.append(created_tasks_by_path.get(dependent_file))

            upload_item = dependency_graph.path_to_upload_item.get(file_path)
            file_task = asyncio.create_task(
                coro=self._upload_item_async(
                    item=upload_item.entity,
                    used=upload_item.used,
                    executed=upload_item.executed,
                    activity_name=upload_item.activity_name,
                    activity_description=upload_item.activity_description,
                    dependent_futures=dependent_tasks,
                )
            )
            created_tasks_by_path.update({file_path: file_task})

        return created_tasks_by_path.values()

    async def upload(self, items: Iterable[_SyncUploadItem]) -> None:
        """Upload a number of files to Synapse as provided in the manifest file. This
        will handle ordering the files based on their dependency graph.

        Arguments:
            items: The list of items to upload.

        Returns:
            None
        """
        dependency_graph = self._build_dependency_graph(items=[i for i in items])
        tasks = self._build_tasks_from_dependency_graph(
            dependency_graph=dependency_graph
        )

        await asyncio.gather(*tasks)

    def _build_activity_linkage(
        self, used_or_executed: Iterable[str], resolved_file_ids: Dict[str, str]
    ) -> List[Union[UsedEntity, UsedURL]]:
        """Loop over the incoming list of used or executed items and build the
        appropriate UsedEntity or UsedURL objects.

        Arguments:
            used_or_executed: The list of used or executed items.
            resolved_file_ids: A dictionary that maps the path of a file to its Synapse
                ID.

        Returns:
            A list of UsedEntity or UsedURL objects.
        """
        returned_linkage = []
        for item in used_or_executed:
            resolved_file_id = resolved_file_ids.get(item, None)
            if resolved_file_id:
                returned_linkage.append(UsedEntity(target_id=resolved_file_id))
            elif is_url(item):
                returned_linkage.append(UsedURL(url=item))

            # -- Synapse Entity ID (assuming the string is an ID)
            elif isinstance(item, str):
                if not is_synapse_id_str(item):
                    raise ValueError(f"{item} is not a valid Synapse id")
                synid, version = get_synid_and_version(
                    item
                )  # Handle synapseIds of from syn234.4
                target_version = None
                if version:
                    target_version = int(version)
                returned_linkage.append(
                    UsedEntity(target_id=synid, target_version_number=target_version)
                )
            else:
                raise SynapseError(
                    f"Unexpected parameters in used or executed Activity fields: {item}."
                )
        return returned_linkage

    async def _upload_item_async(
        self,
        item: File,
        used: Iterable[str],
        executed: Iterable[str],
        activity_name: str,
        activity_description: str,
        dependent_futures: List[asyncio.Future],
    ) -> File:
        resolved_file_ids = {}
        if dependent_futures:
            finished_dependencies, pending = await asyncio.wait(dependent_futures)
            if pending:
                raise RuntimeError(
                    f"There were {len(pending)} dependencies left when storing {item}"
                )
            for finished_dependency in finished_dependencies:
                result = finished_dependency.result()
                resolved_file_ids.update({result.path: result.id})

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
        await item.store_async()
        return item


@deprecated(
    version="4.4.0",
    reason="To be removed in 5.0.0. This is being replaced by `generate_manifest`.",
)
def generateManifest(syn, allFiles, filename, provenance_cache=None) -> None:
    """Generates a manifest file based on a list of entities objects.

    [Read more about the manifest file format](../../explanations/manifest_tsv/)

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        allFiles: A list of File Entity objects on Synapse (can't be Synapse IDs)
        filename: file where manifest will be written
        provenance_cache: an optional dict of known provenance dicts keyed by entity
                          ids

    Returns:
        None
    """
    keys, data = _extract_file_entity_metadata(
        syn=syn, allFiles=allFiles, provenance_cache=provenance_cache
    )
    _write_manifest_data(filename, keys, data)


@deprecated(
    version="4.4.0",
    reason="To be removed in 5.0.0. This is being replaced by `_extract_entity_metadata_for_file`.",
)
def _extract_file_entity_metadata(syn, allFiles, *, provenance_cache=None):
    """
    Extracts metadata from the list of File Entities and returns them in a form
    usable by csv.DictWriter

    Arguments:
        syn: instance of the Synapse client
        allFiles: an iterable that provides File entities
        provenance_cache: an optional dict of known provenance dicts keyed by entity
                          ids

    Returns:
        keys: a list column headers
        data: a list of dicts containing data from each row
    """
    keys = list(DEFAULT_GENERATED_MANIFEST_KEYS)
    annotKeys = set()
    data = []
    for entity in allFiles:
        row = {
            "parent": entity["parentId"],
            "path": entity.get("path"),
            "name": entity.name,
            "id": entity.id,
            "synapseStore": entity.synapseStore,
            "contentType": entity["contentType"],
        }
        row.update(
            {
                key: (val if len(val) > 0 else "")
                for key, val in entity.annotations.items()
            }
        )

        entity_id = entity["id"]
        row_provenance = (
            provenance_cache.get(entity_id) if provenance_cache is not None else None
        )
        if row_provenance is None:
            row_provenance = _get_file_entity_provenance_dict(syn, entity)

            if provenance_cache is not None:
                provenance_cache[entity_id] = row_provenance

        row.update(row_provenance)

        annotKeys.update(set(entity.annotations.keys()))

        data.append(row)
    keys.extend(annotKeys)
    return keys, data


@deprecated(
    version="4.4.0",
    reason="To be removed in 5.0.0. This is being replaced by `_get_entity_provenance_dict_for_file`.",
)
def _get_file_entity_provenance_dict(syn, entity):
    """
    Arguments:
        syn: Synapse object
        entity: Entity object

    Returns:
        dict: a dict with a subset of the provenance metadata for the entity.
              An empty dict is returned if the metadata does not have a provenance record.
    """
    try:
        prov = syn.getProvenance(entity)
        return {
            "used": ";".join(prov._getUsedStringList()),
            "executed": ";".join(prov._getExecutedStringList()),
            "activityName": prov.get("name", ""),
            "activityDescription": prov.get("description", ""),
        }
    except SynapseHTTPError as e:
        if e.response.status_code == 404:
            return {}  # No provenance present return empty dict
        else:
            raise  # unexpected error so we re-raise the exception


def generate_manifest(all_files: List[File], path: str) -> None:
    """Generates a manifest file based on a list of entities objects.

    [Read more about the manifest file format](../../explanations/manifest_tsv/)

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        all_files: A list of File objects on Synapse (can't be Synapse IDs)
        path: path where manifest will be written
        provenance_cache: an optional dict of known provenance dicts keyed by entity
                          ids

    Returns:
        None
    """
    if path and all_files:
        filename = _manifest_filename(path=path)
        keys, data = _extract_entity_metadata_for_file(all_files=all_files)
        _write_manifest_data(filename, keys, data)


def _extract_entity_metadata_for_file(
    all_files: List[File],
) -> Tuple[List[str], List[Dict[str, str]]]:
    """
    Extracts metadata from the list of File Entities and returns them in a form
    usable by csv.DictWriter

    Arguments:
        syn: instance of the Synapse client
        allFiles: an iterable that provides File entities
        provenance_cache: an optional dict of known provenance dicts keyed by entity
                          ids

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


def _get_entity_provenance_dict_for_file(entity: File):
    """
    Arguments:
        syn: Synapse object
        entity: Entity object

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


def _convert_manifest_data_items_to_string_list(
    items: List[Union[str, datetime.datetime, bool, int, float]],
) -> str:
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


def _sortAndFixProvenance(syn, df):
    df = df.set_index("path")
    uploadOrder = {}

    def _checkProvenace(item, path):
        """Determines if provenance item is valid"""
        if item is None:
            return item

        item_path_normalized = os.path.abspath(
            os.path.expandvars(os.path.expanduser(item))
        )
        if os.path.isfile(item_path_normalized):
            # Add full path
            item = item_path_normalized
            if item not in df.index:  # If it is a file and it is not being uploaded
                try:
                    bundle = syn._getFromFile(item)
                    return bundle
                except SynapseFileNotFoundError:
                    # TODO absence of a raise here appears to be a bug and yet tests fail if this is raised
                    SynapseProvenanceError(
                        (
                            "The provenance record for file: %s is incorrect.\n"
                            "Specifically %s is not being uploaded and is not in Synapse."
                            % (path, item)
                        )
                    )

        elif not utils.is_url(item) and (utils.is_synapse_id_str(item) is None):
            raise SynapseProvenanceError(
                "The provenance record for file: %s is incorrect.\n"
                "Specifically %s, is neither a valid URL or synapseId." % (path, item)
            )
        return item

    for path, row in df.iterrows():
        allRefs = []
        if "used" in row:
            used = (
                row["used"].split(";") if (row["used"].strip() != "") else []
            )  # Get None or split if string
            df.at[path, "used"] = [_checkProvenace(item.strip(), path) for item in used]
            allRefs.extend(df.loc[path, "used"])
        if "executed" in row:
            # Get None or split if string
            executed = (
                row["executed"].split(";") if (row["executed"].strip() != "") else []
            )
            df.at[path, "executed"] = [
                _checkProvenace(item.strip(), path) for item in executed
            ]
            allRefs.extend(df.loc[path, "executed"])
        uploadOrder[path] = allRefs

    uploadOrder = utils.topolgical_sort(uploadOrder)
    df = df.reindex([i[0] for i in uploadOrder])
    return df.reset_index()


def _check_path_and_normalize(f):
    sys.stdout.write(".")
    if is_url(f):
        return f
    path_normalized = os.path.abspath(os.path.expandvars(os.path.expanduser(f)))
    if not os.path.isfile(path_normalized):
        print(
            f'\nThe specified path "{f}" is either not a file path or does not exist.',
            file=sys.stderr,
        )
        raise IOError("The path %s is not a file or does not exist" % f)
    return path_normalized


def readManifestFile(syn, manifestFile):
    """Verifies a file manifest and returns a reordered dataframe ready for upload.

    [Read more about the manifest file format](../../explanations/manifest_tsv/)

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        manifestFile: A tsv file with file locations and metadata to be pushed to Synapse.

    Returns:
        A pandas dataframe if the manifest is validated.
    """
    table.test_import_pandas()
    import pandas as pd

    if manifestFile is sys.stdin:
        sys.stdout.write("Validation and upload of: <stdin>\n")
    else:
        sys.stdout.write("Validation and upload of: %s\n" % manifestFile)
    # Read manifest file into pandas dataframe
    df = pd.read_csv(manifestFile, sep="\t")
    if "synapseStore" not in df:
        df = df.assign(synapseStore=None)
    df.loc[
        df["path"].apply(is_url), "synapseStore"
    ] = False  # override synapseStore values to False when path is a url
    df.loc[
        df["synapseStore"].isnull(), "synapseStore"
    ] = True  # remaining unset values default to True
    df.synapseStore = df.synapseStore.astype(bool)
    df = df.fillna("")

    sys.stdout.write("Validating columns of manifest...")
    for field in REQUIRED_FIELDS:
        sys.stdout.write(".")
        if field not in df.columns:
            sys.stdout.write("\n")
            raise ValueError("Manifest must contain a column of %s" % field)
    sys.stdout.write("OK\n")

    sys.stdout.write("Validating that all paths exist...")
    df.path = df.path.apply(_check_path_and_normalize)

    sys.stdout.write("OK\n")

    sys.stdout.write("Validating that all files are unique...")
    # Both the path and the combination of entity name and parent must be unique
    if len(df.path) != len(set(df.path)):
        raise ValueError("All rows in manifest must contain a unique file to upload")
    sys.stdout.write("OK\n")

    # Check each size of uploaded file
    sys.stdout.write("Validating that all the files are not empty...")
    _check_size_each_file(df)
    sys.stdout.write("OK\n")

    # check the name of each file should be store on Synapse
    name_column = "name"
    # Create entity name column from basename
    if name_column not in df.columns:
        filenames = [os.path.basename(path) for path in df["path"]]
        df["name"] = filenames

    sys.stdout.write("Validating file names... \n")
    _check_file_name(df)
    sys.stdout.write("OK\n")

    sys.stdout.write("Validating provenance...")
    df = _sortAndFixProvenance(syn, df)
    sys.stdout.write("OK\n")

    sys.stdout.write("Validating that parents exist and are containers...")
    parents = set(df.parent)
    for synId in parents:
        try:
            container = syn.get(synId, downloadFile=False)
        except SynapseHTTPError:
            sys.stdout.write(
                "\n%s in the parent column is not a valid Synapse Id\n" % synId
            )
            raise
        if not is_container(container):
            sys.stdout.write(
                "\n%s in the parent column is is not a Folder or Project\n" % synId
            )
            raise SynapseHTTPError
    sys.stdout.write("OK\n")
    return df


def syncToSynapse(
    syn: Synapse,
    manifestFile,
    dryRun: bool = False,
    sendMessages: bool = True,
    retries: int = MAX_RETRIES,
    merge_existing_annotations: bool = True,
    associate_activity_to_new_version: bool = False,
) -> None:
    """Synchronizes files specified in the manifest file to Synapse.

    Given a file describing all of the uploads, this uploads the content to Synapse and
    optionally notifies you via Synapse messagging (email) at specific intervals, on
    errors and on completion.

    [Read more about the manifest file format](../../explanations/manifest_tsv/)

    There are a few conversions around annotations to call out here.

    ## Conversion of annotations from the TSV file to Python native objects

    The first annotation conversion is from the TSV file into a Python native object. For
    example Pandas will read a TSV file and convert the string "True" into a boolean True,
    however, Pandas will NOT convert our comma delimited and bracket wrapped list of
    annotations into their Python native objects. This means that we need to do that
    conversion here after splitting them apart.

    ## Conversion of Python native objects for the REST API

    The second annotation conversion occurs when we are taking the Python native objects
    and converting them into a string that can be sent to the REST API. For example
    the datetime objects which may have timezone information are converted to milliseconds
    since epoch.

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        manifestFile: A tsv file with file locations and metadata to be pushed to Synapse.
        dryRun: Performs validation without uploading if set to True.
        sendMessages: Sends out messages on completion if set to True.
        retries: Number of retries to attempt if an error occurs.
        merge_existing_annotations: If True, will merge the annotations in the manifest
            file with the existing annotations on Synapse. If False, will overwrite the
            existing annotations on Synapse with the annotations in the manifest file.
        associate_activity_to_new_version: If True, and a version update occurs, the
            existing activity in Synapse will be associated with the new version. The
            exception is if you are specifying new values to be used/executed, it will
            create a new activity for the new version of the entity.

    Returns:
        None
    """
    df = readManifestFile(syn, manifestFile)

    sizes = [
        os.stat(os.path.expandvars(os.path.expanduser(f))).st_size
        for f in df.path
        if not is_url(f)
    ]

    total_upload_size = sum(sizes)

    syn.logger.info(
        f"We are about to upload {len(df)} files with a total size of {total_upload_size}."
    )

    if dryRun:
        syn.logger.info("Returning due to Dry Run.")
        return

    progress_bar = tqdm(
        total=total_upload_size,
        desc=f"Uploading {len(df)} files",
        unit="B",
        unit_scale=True,
        smoothing=0,
    )
    with upload_shared_progress_bar(progress_bar):
        if sendMessages:
            notify_decorator = notify_me_async(
                syn, "Upload of %s" % manifestFile, retries=retries
            )
            upload = notify_decorator(_manifest_upload)
            wrap_async_to_sync(
                upload(
                    syn,
                    df,
                    merge_existing_annotations,
                    associate_activity_to_new_version,
                ),
                syn,
            )
        else:
            wrap_async_to_sync(
                _manifest_upload(
                    syn,
                    df,
                    merge_existing_annotations,
                    associate_activity_to_new_version,
                ),
                syn,
            )
        progress_bar.update(total_upload_size - progress_bar.n)
        progress_bar.close()


def _split_string(input_string: str) -> List[str]:
    """
    Use regex to split a string apart by commas that are not inside of double quotes.


    Args:
        input_string: A string to split apart.

    Returns:
        The list of split items as strings.
    """
    row = COMMAS_OUTSIDE_DOUBLE_QUOTES_PATTERN.split(input_string)

    modified_row = []
    for item in row:
        modified_item = item.strip()
        modified_row.append(modified_item)

    return modified_row


def _convert_cell_in_manifest_to_python_types(
    cell: str,
) -> Union[List, datetime.datetime, float, int, bool, str]:
    """
    Takes a possibly comma delimited cell from the manifest TSV file into a list
    of items to be used as annotations.

    Args:
        cell: The cell item to convert.

    Returns:
        The list of items to be used as annotations. Or a single instance if that is
            all that is present.
    """
    values_to_return = []
    cell = cell.strip()

    if ARRAY_BRACKET_PATTERN.match(string=cell):
        # Replace the first '[' with an empty string
        modified_cell = SINGLE_OPEN_BRACKET_PATTERN.sub(repl="", string=cell)

        # Replace the last ']' with an empty string
        modified_cell = SINGLE_CLOSING_BRACKET_PATTERN.sub(
            repl="", string=modified_cell
        )
        cell_values = _split_string(input_string=modified_cell)
    else:
        cell_values = [cell]

    for annotation_value in cell_values:
        if (possible_datetime := datetime_or_none(annotation_value)) is not None:
            values_to_return.append(possible_datetime)
            # By default `literal_eval` does not convert false or true in different cases
            # to a bool, however, we want to provide that functionality.
        elif (possible_bool := bool_or_none(annotation_value)) is not None:
            values_to_return.append(possible_bool)
        else:
            try:
                value_to_add = ast.literal_eval(node_or_string=annotation_value)
            except (ValueError, SyntaxError):
                value_to_add = annotation_value
            values_to_return.append(value_to_add)
    return values_to_return[0] if len(values_to_return) == 1 else values_to_return


def _build_annotations_for_file(
    manifest_annotations,
) -> Dict[
    str,
    Union[
        List[str],
        List[bool],
        List[float],
        List[int],
        List[datetime.date],
        List[datetime.datetime],
    ],
]:
    """Pull the annotations out of the format defined in the manifest being uploaded
    into a format that is expected internally within the client. For annotations
    that might not contain a value we will assume it to be None and it won't be uploaded
    as a blank annotation.


    Arguments:
        manifest_annotations: The annotations as defined in the manifest file.

    Returns:
        The annoations in a format used in the client.
    """
    # if a item in the manifest upload is an empty string we do not want to upload that
    # as an empty string annotation
    file_annotations = {}

    for annotation_key, annotation_value in manifest_annotations.items():
        if annotation_value is None or annotation_value == "":
            continue
        if isinstance(annotation_value, str):
            file_annotations[
                annotation_key
            ] = _convert_cell_in_manifest_to_python_types(cell=annotation_value)
        else:
            file_annotations[annotation_key] = annotation_value
    return file_annotations


async def _manifest_upload(
    syn: Synapse,
    df,
    merge_existing_annotations: bool = True,
    associate_activity_to_new_version: bool = False,
) -> bool:
    """
    Handles the upload of the manifest file.

    Args:
        syn: The logged in Synapse client.
        df: The dataframe of the manifest file.
        merge_existing_annotations: If True, will merge the annotations in the manifest
            file with the existing annotations on Synapse. If False, will overwrite the
            existing annotations on Synapse with the annotations in the manifest file.
        associate_activity_to_new_version: If True, and a version update occurs, the
            existing activity in Synapse will be associated with the new version. The
            exception is if you are specifying new values to be used/executed, it will
            create a new activity for the new version of the entity.

    Returns:
        If the manifest upload was successful.
    """
    items = []
    for _, row in df.iterrows():
        file = File(
            path=row["path"],
            parent_id=row["parent"],
            name=row["name"] if "name" in row else None,
            id=row["id"] if "id" in row else None,
            synapse_store=row["synapseStore"] if "synapseStore" in row else True,
            content_type=row["contentType"] if "contentType" in row else None,
            force_version=row["forceVersion"] if "forceVersion" in row else True,
            merge_existing_annotations=merge_existing_annotations,
            associate_activity_to_new_version=associate_activity_to_new_version,
            _present_manifest_fields=row.keys().tolist(),
        )

        manifest_style_annotations = dict(
            row.drop(
                FILE_CONSTRUCTOR_FIELDS
                + STORE_FUNCTION_FIELDS
                + REQUIRED_FIELDS
                + PROVENANCE_FIELDS,
                errors="ignore",
            )
        )

        file.annotations = _build_annotations_for_file(manifest_style_annotations)

        item = _SyncUploadItem(
            file,
            row["used"] if "used" in row else [],
            row["executed"] if "executed" in row else [],
            activity_name=row["activityName"] if "activityName" in row else None,
            activity_description=(
                row["activityDescription"] if "activityDescription" in row else None
            ),
        )
        items.append(item)

    uploader = _SyncUploader(syn)
    await uploader.upload(items)

    return True


def _check_file_name(df):
    compiled = re.compile(r"^[`\w \-\+\.\(\)]{1,256}$")
    for idx, row in df.iterrows():
        file_name = row["name"]
        if not file_name:
            directory_name = os.path.basename(row["path"])
            df.loc[df.path == row["path"], "name"] = file_name = directory_name
            sys.stdout.write(
                "No file name assigned to path: %s, defaulting to %s\n"
                % (row["path"], directory_name)
            )
        if not compiled.match(file_name):
            raise ValueError(
                "File name {} cannot be stored to Synapse. Names may contain letters,"
                " numbers, spaces, underscores, hyphens, periods, plus signs,"
                " apostrophes, and parentheses".format(file_name)
            )
    if df[["name", "parent"]].duplicated().any():
        raise ValueError(
            "All rows in manifest must contain a path with a unique file name and"
            " parent to upload. Files uploaded to the same folder/project (parent) must"
            " have unique file names."
        )


def _check_size_each_file(df):
    for idx, row in df.iterrows():
        file_path = row["path"]
        file_name = row["name"] if "name" in row else os.path.basename(row["path"])
        if not is_url(file_path):
            single_file_size = os.stat(
                os.path.expandvars(os.path.expanduser(file_path))
            ).st_size
            if single_file_size == 0:
                raise ValueError(
                    "File {} is empty, empty files cannot be uploaded to Synapse".format(
                        file_name
                    )
                )


def generate_sync_manifest(syn, directory_path, parent_id, manifest_path) -> None:
    """Generate manifest for [syncToSynapse][synapseutils.sync.syncToSynapse] from a local directory.

    [Read more about the manifest file format](../../explanations/manifest_tsv/)

    Arguments:
        syn: A Synapse object with user's login, e.g. syn = synapseclient.login()
        directory_path: Path to local directory to be pushed to Synapse.
        parent_id: Synapse ID of the parent folder/project on Synapse.
        manifest_path: Path to the manifest file to be generated.

    Returns:
        None
    """
    manifest_cols = ["path", "parent"]
    manifest_rows = _walk_directory_tree(syn, directory_path, parent_id)
    _write_manifest_data(manifest_path, manifest_cols, manifest_rows)


def _create_folder(syn, name, parent_id):
    """Create Synapse folder."""
    entity = {
        "name": name,
        "concreteType": "org.sagebionetworks.repo.model.Folder",
        "parentId": parent_id,
    }
    entity = syn.store(entity)
    return entity


def _walk_directory_tree(syn, path, parent_id):
    """Replicate folder structure on Synapse and generate manifest
    rows for files using corresponding Synapse folders as parents.
    """
    rows = list()
    parents = {path: parent_id}
    for dirpath, dirnames, filenames in os.walk(path):
        # Replicate the folders on Synapse
        for dirname in dirnames:
            name = dirname
            folder_path = os.path.join(dirpath, dirname)
            parent_id = parents[dirpath]
            folder = _create_folder(syn, name, parent_id)
            # Store Synapse ID for sub-folders/files
            parents[folder_path] = folder["id"]
        # Generate rows per file for the manifest
        for filename in filenames:
            # Add file to manifest if non-zero size
            filepath = os.path.join(dirpath, filename)
            manifest_row = {
                "path": filepath,
                "parent": parents[dirpath],
            }
            if os.stat(filepath).st_size > 0:
                rows.append(manifest_row)
    return rows


def _manifest_filename(path: str) -> str:
    """
    Create a path to write the manifest file to.

    Arguments:
        path: The directory to write the manifest file to.

    Returns:
        The path to write the manifest file to.
    """
    file_name = MANIFEST_FILENAME
    return os.path.expanduser(
        os.path.normcase(
            os.path.join(
                path, f"{file_name}{'' if file_name.endswith('.tsv') else '.tsv'}"
            )
        )
    )
