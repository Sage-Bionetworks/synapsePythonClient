import ast
import csv
import concurrent.futures
from contextlib import contextmanager
import datetime
import io
import os
import re
import sys
import threading
import typing

from .monitor import notifyMe
from synapseclient.entity import is_container
from synapseclient.core import config
from synapseclient.core.utils import (
    id_of,
    is_url,
    is_synapse_id_str,
    datetime_or_none,
    bool_or_none,
)
from synapseclient import File, table, Synapse
from synapseclient.core.pool_provider import SingleThreadExecutor
from synapseclient.core import utils
from synapseclient.core.cumulative_transfer_progress import CumulativeTransferProgress
from synapseclient.core.exceptions import (
    SynapseFileNotFoundError,
    SynapseHTTPError,
    SynapseProvenanceError,
)
from synapseclient.core.multithread_download.download_threads import (
    shared_executor as download_shared_executor,
)
from synapseclient.core.upload.multipart_upload import (
    shared_executor as upload_shared_executor,
)
from opentelemetry import context, trace
from opentelemetry.context import Context


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


@contextmanager
def _sync_executor(syn):
    """Use this context manager to run some sync code with an executor that will
    be created and then shutdown once the context completes."""
    if syn.max_threads < 2 or config.single_threaded:
        executor = SingleThreadExecutor()
    else:
        executor = concurrent.futures.ThreadPoolExecutor(syn.max_threads)

    try:
        yield executor
    finally:
        executor.shutdown()


def syncFromSynapse(
    syn,
    entity,
    path=None,
    ifcollision="overwrite.local",
    allFiles=None,
    followLink=False,
    manifest="all",
    downloadFile=True,
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
              specified the files will by default be placed in the synapseCache.
        ifcollision: Determines how to handle file collisions. Maybe
                     "overwrite.local", "keep.local", or "keep.both".
        followLink: Determines whether the link returns the target Entity.
        manifest: Determines whether creating manifest file automatically. The
                  optional values here (`all`, `root`, `suppress`).
        downloadFile: Determines whether downloading the files.

    Returns:
        List of entities ([files][synapseclient.File],
            [tables][synapseclient.Table], [links][synapseclient.Link])


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
    # To support multipart downloads in #3 using the same Executor as the download
    # thread #2, we need at least 2 threads always, if those aren't available then
    # we'll run single threaded to avoid a deadlock.
    with _sync_executor(syn) as executor:
        sync_from_synapse = _SyncDownloader(syn, executor)
        files = sync_from_synapse.sync(
            entity, path, ifcollision, followLink, downloadFile, manifest
        )

    # the allFiles parameter used to be passed in as part of the recursive
    # implementation of this function with the public signature invoking itself. now
    # that this isn't a recursive any longer we don't need allFiles as a parameter
    # (especially on the public signature) but it is retained for now for backwards
    # compatibility with external invokers.
    if allFiles is not None:
        allFiles.extend(files)
        files = allFiles

    return files


class _FolderSync:
    """
    A FolderSync tracks the syncFromSynapse activity associated with a
    Folder/container. It has a link to its parent and is kept updated as the
    children of the associated folder are downloaded, and when complete
    it communicates up its chain to the root that it is completed.
    When the root FolderSync is complete the sync is complete.

    It serves as a way to track and store the data related to the sync
    at each folder of the sync so we can generate manifests and notify
    when finished.
    """

    def __init__(self, syn, entity_id, path, child_ids, parent, create_manifest=True):
        self._syn = syn
        self._entity_id = entity_id
        self._path = path
        self._parent = parent
        self._create_manifest = create_manifest

        self._pending_ids = set(child_ids or [])
        self._files = []
        self._provenance = {}
        self._exception = None

        self._lock = threading.Lock()
        self._finished = threading.Condition(lock=self._lock)

    def update(self, finished_id=None, files=None, provenance=None):
        with self._lock:
            if finished_id:
                self._pending_ids.remove(finished_id)
            if files:
                self._files.extend(files)
            if provenance:
                self._provenance.update(provenance)

            if self._is_finished():
                if self._create_manifest:
                    self._generate_folder_manifest()

                if self._parent:
                    self._parent.update(
                        finished_id=self._entity_id,
                        files=self._files,
                        provenance=self._provenance,
                    )

                # in practice only the root folder sync will be waited on/need notifying
                self._finished.notify_all()

    def _manifest_filename(self):
        return os.path.expanduser(
            os.path.normcase(os.path.join(self._path, MANIFEST_FILENAME))
        )

    def _generate_folder_manifest(self):
        # when a folder is complete we write a manifest file if we are downloading
        # to a path outside the Synapse cache and there are actually some files in
        # this folder.
        if self._path and self._files:
            generateManifest(
                self._syn,
                self._files,
                self._manifest_filename(),
                provenance_cache=self._provenance,
            )

    def get_exception(self):
        with self._lock:
            return self._exception

    def set_exception(self, exception):
        with self._lock:
            self._exception = exception

            # an exception that occurred in this container is considered to have also
            # happened in the parent container and up to the root
            if self._parent:
                self._parent.set_exception(exception)

            # an error also results in the folder being finished
            self._finished.notify_all()

    def wait_until_finished(self):
        with self._finished:
            self._finished.wait_for(self._is_finished)
            return self._files

    def _is_finished(self):
        return len(self._pending_ids) == 0 or self._exception


class _SyncDownloader:
    """
    Manages the downloads associated associated with a syncFromSynapse call concurrently.
    """

    def __init__(
        self,
        syn,
        executor: concurrent.futures.Executor,
        max_concurrent_file_downloads=None,
    ):
        """
        Arguments:
            syn: A synapse client
            executor: An ExecutorService in which concurrent file downlaods can be scheduled

        """
        self._syn = syn
        self._executor = executor

        # by default limit the number of concurrent file downloads that can happen at once to some proportion
        # of the available threads. otherwise we could end up downloading a single part from many files at once
        # rather than concentrating our download threads on a few files at a time so those files complete faster.
        max_concurrent_file_downloads = max(
            int(max_concurrent_file_downloads or self._syn.max_threads / 2), 1
        )
        self._file_semaphore = threading.BoundedSemaphore(max_concurrent_file_downloads)

    def sync(
        self, entity, path, ifcollision, followLink, downloadFile=True, manifest="all"
    ):
        progress = CumulativeTransferProgress("Downloaded")

        if is_synapse_id_str(entity):
            # ensure that we seed with an actual entity
            entity = self._syn.get(
                entity,
                downloadLocation=path,
                ifcollision=ifcollision,
                followLink=followLink,
            )

        if is_container(entity):
            root_folder_sync = self._sync_root(
                entity, path, ifcollision, followLink, progress, downloadFile, manifest
            )

            # once the whole folder hierarchy has been traversed this entrant thread
            # waits for all file downloads to complete before returning
            files = root_folder_sync.wait_until_finished()

        elif isinstance(entity, File):
            files = [entity]

        else:
            raise ValueError(
                "Cannot initiate a sync from an entity that is not a File or Folder"
            )

        # since the sub folders could complete out of order from when they were submitted we
        # sort the files by their path (which includes their local folder) to get a
        # predictable ordering.
        # not required but nice for testing etc.
        files.sort(key=lambda f: f.get("path") or "")
        return files

    def _sync_file(
        self,
        entity_id,
        parent_folder_sync,
        path,
        ifcollision,
        followLink,
        progress,
        downloadFile,
    ):
        try:
            # we use syn.get to download the File.
            # these context managers ensure that we are using some shared state
            # when conducting that download (shared progress bar, ExecutorService shared
            # by all multi threaded downloads in this sync)
            with progress.accumulate_progress(), download_shared_executor(
                self._executor
            ):
                entity = self._syn.get(
                    entity_id,
                    downloadLocation=path,
                    ifcollision=ifcollision,
                    followLink=followLink,
                    downloadFile=downloadFile,
                )

            files = []
            provenance = None
            if isinstance(entity, File):
                if path:
                    entity_provenance = _get_file_entity_provenance_dict(
                        self._syn, entity
                    )
                    provenance = {entity_id: entity_provenance}

                files.append(entity)

            # else if the entity is not a File (and wasn't a container)
            # then we ignore it for the purposes of this sync

            parent_folder_sync.update(
                finished_id=entity_id,
                files=files,
                provenance=provenance,
            )

        except Exception as ex:
            # this could be anything raised by any type of download, and so by
            # nature is a broad catch. the purpose here is not to handle it but
            # just to raise it up the folder sync chain such that it will abort
            # the sync and raise the error to the entrant thread. it is not the
            # responsibility here to recover or retry a particular file download,
            # reasonable recovery should be handled within the file download code.
            parent_folder_sync.set_exception(ex)

        finally:
            self._file_semaphore.release()

    def _sync_root(
        self,
        root,
        root_path,
        ifcollision,
        followLink,
        progress,
        downloadFile,
        manifest="all",
    ):
        # stack elements are a 3-tuple of:
        # 1. the folder entity/dict
        # 2. the local path to the folder to download to
        # 3. the FolderSync of the parent to the folder (None at the root)

        create_root_manifest = True if manifest != "suppress" else False
        folder_stack = [(root, root_path, None, create_root_manifest)]
        create_child_manifest = True if manifest == "all" else False

        root_folder_sync = None
        while folder_stack:
            if root_folder_sync:
                # if at any point the sync encounters an exception it will
                # be communicated up to the root at which point we should abort
                exception = root_folder_sync.get_exception()
                if exception:
                    raise ValueError("File download failed during sync") from exception

            (
                folder,
                parent_path,
                parent_folder_sync,
                create_manifest,
            ) = folder_stack.pop()

            entity_id = id_of(folder)
            folder_path = None
            if parent_path is not None:
                folder_path = parent_path
                if root_folder_sync:
                    # syncFromSynapse behavior is that we do NOT create a folder for
                    # the root folder of the sync. we treat the download local path
                    # folder as the root and write the children of the sync
                    # directly into that local folder
                    folder_path = os.path.join(folder_path, folder["name"])
                os.makedirs(folder_path, exist_ok=True)

            child_ids = []
            child_file_ids = []
            child_folders = []
            for child in self._syn.getChildren(entity_id):
                child_id = id_of(child)
                child_ids.append(child_id)
                if is_container(child):
                    child_folders.append(child)
                else:
                    child_file_ids.append(child_id)

            folder_sync = _FolderSync(
                self._syn,
                entity_id,
                folder_path,
                child_ids,
                parent_folder_sync,
                create_manifest=create_manifest,
            )
            if not root_folder_sync:
                root_folder_sync = folder_sync

            if not child_ids:
                # this folder has no children, so it is immediately finished
                folder_sync.update()

            else:
                for child_file_id in child_file_ids:
                    self._file_semaphore.acquire()
                    self._executor.submit(
                        self._sync_file,
                        child_file_id,
                        folder_sync,
                        folder_path,
                        ifcollision,
                        followLink,
                        progress,
                        downloadFile,
                    )

                for child_folder in child_folders:
                    folder_stack.append(
                        (child_folder, folder_path, folder_sync, create_child_manifest)
                    )

        return root_folder_sync


class _PendingProvenance:
    def __init__(self):
        self._pending = set()
        self._pending_count = 0

    def update(self, pending: set):
        """Add pending items"""
        self._pending_count += len(pending.difference(self._pending))
        self._pending.update(pending)

    def finished(self, provenance):
        """Remove the given provenance after it is finished uploading"""
        self._pending.remove(provenance)

    def has_pending(self):
        """Return whether any pending provenance was recorded"""
        return self._pending_count > 0

    def has_finished_provenance(self):
        """Return whether any of the pending provenance has finished"""
        return len(self._pending) < self._pending_count

    def reset_count(self):
        """Reset the pending count to reflect the current pending state"""
        self._pending_count = len(self._pending)


class _SyncUploadItem(typing.NamedTuple):
    """Represents a single file being uploaded"""

    entity: File
    used: typing.Iterable[str]
    executed: typing.Iterable[str]
    store_kwargs: typing.Mapping


class _SyncUploader:
    """
    Manages the uploads associated associated with a syncToSynapse call.
    Files will be uploaded concurrently and in an order that honors any
    interdependent provenance.

    """

    def __init__(
        self,
        syn: Synapse,
        executor: concurrent.futures.Executor,
        max_concurrent_file_transfers: int = None,
    ):
        """
        Arguments:
            syn: A synapse client
            executor: An ExecutorService in which concurrent file downloads
                      can be scheduled
        """
        self._syn = syn

        max_concurrent_file_transfers = max(
            int(max_concurrent_file_transfers or self._syn.max_threads / 2), 1
        )
        self._executor = executor
        self._file_semaphore = threading.BoundedSemaphore(max_concurrent_file_transfers)

    @staticmethod
    def _order_items(items):
        # order items by their interdependent provenance and raise any dependency
        # errors

        items_by_path = {i.entity.path: i for i in items}
        graph = {}

        for item in items:
            item_file_provenance = []
            for provenance_dependency in item.used + item.executed:
                if os.path.isfile(provenance_dependency):
                    if provenance_dependency not in items_by_path:
                        # an upload lists provenance of a file that is not itself
                        # included in the upload
                        raise ValueError(
                            f"{item.entity.path} depends on"
                            f" {provenance_dependency} which is not being uploaded"
                        )

                    item_file_provenance.append(provenance_dependency)

            graph[item.entity.path] = item_file_provenance

        graph_sorted = utils.topolgical_sort(graph)
        return [items_by_path[i[0]] for i in graph_sorted]

    @staticmethod
    def _convert_provenance(provenance, finished_items):
        # convert any string file path provenance to the corresponding entity that
        # has been uploaded

        converted_provenance = []
        pending_provenance = set()
        for p in provenance:
            if os.path.isfile(p):
                converted = finished_items.get(p)
                if converted:
                    converted_provenance.append(converted)
                else:
                    pending_provenance.add(p)
            else:
                converted_provenance.append(p)

        return converted_provenance, pending_provenance

    @staticmethod
    def _abort(futures):
        # abort this sync because of an error

        exception = None
        for future in futures:
            if future.done():
                exception = exception or future.exception()
            else:
                future.cancel()

        # if we are aborted by definition one of the futures should have an
        # exception.
        # if somehow not from None fuctions fine
        raise ValueError("Sync aborted due to upload failure") from exception

    def upload(self, items: typing.Iterable[_SyncUploadItem]):
        progress = CumulativeTransferProgress("Uploaded")

        # flag to set in a child in an upload thread if an error occurs to signal
        # to the entrant thread to stop processing.
        abort_event = threading.Event()

        # used to lock around shared state and to notify when dependencies are
        # resolved so that provenance dependent files can be uploaded
        dependency_condition = threading.Condition()

        pending_provenance = _PendingProvenance()
        finished_items = {}

        ordered_items = self._order_items([i for i in items])

        futures = []
        while ordered_items:
            skipped_items = []
            for item in ordered_items:
                if abort_event.is_set():
                    # if this flag is set, one of the upload threads failed and we
                    # should raise it's error and cancel any remaining futures
                    self._abort(futures)

                with dependency_condition:
                    used, used_pending = self._convert_provenance(
                        item.used, finished_items
                    )
                    executed, executed_pending = self._convert_provenance(
                        item.executed, finished_items
                    )

                    if used_pending or executed_pending:
                        # we can't upload this item yet, it has provenance that
                        # hasn't yet been uploaded
                        skipped_items.append(item)
                        pending_provenance.update(used_pending.union(executed_pending))

                        # skip uploading because dependent provenance hasn't
                        # finished uploading
                        continue

                # else not continued above due to pending provenance
                # all provenance that this item depends on has already been uploaded
                # so we can go ahead and upload this item

                # we acquire the semaphore to ensure that we aren't uploading more
                # than our configured maximum number of files here at once. once we
                # reach the limit we'll block here until one of the existing file uploads completes
                self._file_semaphore.acquire()
                trace.get_current_span().set_attributes(
                    {"thread.id": threading.get_ident()}
                )
                future = self._executor.submit(
                    self._upload_item,
                    item,
                    used,
                    executed,
                    finished_items,
                    pending_provenance,
                    dependency_condition,
                    abort_event,
                    progress,
                    context.get_current(),
                )
                futures.append(future)

            with dependency_condition:
                if pending_provenance.has_pending():
                    # skipped_items contains all the items that we couldn't upload
                    # the previous time through the loop because they depended on
                    # another item for provenance. wait until there at least one
                    # those items finishes before continuing another time through the loop.
                    if not abort_event.is_set():
                        dependency_condition.wait_for(
                            lambda: (
                                pending_provenance.has_finished_provenance()
                                or abort_event.is_set()
                            )
                        )

                pending_provenance.reset_count()

            ordered_items = skipped_items

        # all items have been submitted for upload

        concurrent.futures.wait(futures, return_when=concurrent.futures.FIRST_EXCEPTION)
        if abort_event.is_set():
            # at least one item failed to upload
            self._abort(futures)

    def _upload_item(
        self,
        item,
        used,
        executed,
        finished_items,
        pending_provenance,
        dependency_condition,
        abort_event,
        progress,
        otel_context: typing.Union[Context, None],
    ):
        if otel_context:
            context.attach(otel_context)
        try:
            with upload_shared_executor(self._executor):
                # we configure an upload thread local shared executor so that any
                # multipart uploads that result from this upload will share the
                # executor of this sync rather than creating their own threadpool.

                with progress.accumulate_progress():
                    entity = self._syn.store(
                        item.entity,
                        used=used,
                        executed=executed,
                        **item.store_kwargs,
                        async_file_handle_upload=False,
                    )

                with dependency_condition:
                    finished_items[item.entity.path] = entity
                    try:
                        pending_provenance.finished(item.entity.path)

                        # this item was defined as provenance for another item, now
                        # that it's finished we may be able to upload that depending
                        # item, so wake up he central thread
                        dependency_condition.notify_all()

                    except KeyError:
                        # this item is not used in provenance of another item,
                        # that's fine
                        pass

        except Exception:
            with dependency_condition:
                abort_event.set()
                dependency_condition.notify_all()
            raise

        finally:
            self._file_semaphore.release()


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
        syn, allFiles, provenance_cache=provenance_cache
    )
    _write_manifest_data(filename, keys, data)


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


def _convert_manifest_data_items_to_string_list(
    items: typing.List[typing.Union[str, datetime.datetime, bool, int, float]],
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

    if len(items) > 1:
        return f'[{",".join(items_to_write)}]'
    else:
        return items_to_write[0]


def _convert_manifest_data_row_to_dict(row: dict, keys: typing.List[str]) -> dict:
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


def _write_manifest_data(
    filename: str, keys: typing.List[str], data: typing.List[dict]
) -> None:
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
    syn, manifestFile, dryRun=False, sendMessages=True, retries=MAX_RETRIES
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

    Returns:
        None
    """
    df = readManifestFile(syn, manifestFile)
    # have to check all size of single file
    sizes = [
        os.stat(os.path.expandvars(os.path.expanduser(f))).st_size
        for f in df.path
        if not is_url(f)
    ]
    # Write output on what is getting pushed and estimated times - send out message.
    sys.stdout.write("=" * 50 + "\n")
    sys.stdout.write(
        "We are about to upload %i files with a total size of %s.\n "
        % (len(df), utils.humanizeBytes(sum(sizes)))
    )
    sys.stdout.write("=" * 50 + "\n")

    if dryRun:
        return

    sys.stdout.write("Starting upload...\n")
    if sendMessages:
        notify_decorator = notifyMe(syn, "Upload of %s" % manifestFile, retries=retries)
        upload = notify_decorator(_manifest_upload)
        upload(syn, df)
    else:
        _manifest_upload(syn, df)


def _split_string(input_string: str) -> typing.List[str]:
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
) -> typing.Union[typing.List, datetime.datetime, float, int, bool, str]:
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


def _manifest_upload(syn: Synapse, df) -> bool:
    """
    Handles the upload of the manifest file.

    Args:
        syn: The logged in Synapse client.
        df: The dataframe of the manifest file.

    Returns:
        If the manifest upload was successful.
    """
    items = []
    for _, row in df.iterrows():
        file = File(
            path=row["path"],
            parent=row["parent"],
            **{key: row[key] for key in FILE_CONSTRUCTOR_FIELDS if key in row},
        )

        annotations = dict(
            row.drop(
                FILE_CONSTRUCTOR_FIELDS
                + STORE_FUNCTION_FIELDS
                + REQUIRED_FIELDS
                + PROVENANCE_FIELDS,
                errors="ignore",
            )
        )

        # if a item in the manifest upload is an empty string we do not want to upload that
        # as an empty string annotation
        file_annotations = {}

        for annotation_key, annotation_value in annotations.items():
            if annotation_value is None or annotation_value == "":
                continue
            if isinstance(annotation_value, str):
                file_annotations[
                    annotation_key
                ] = _convert_cell_in_manifest_to_python_types(cell=annotation_value)
            else:
                file_annotations[annotation_key] = annotation_value
        file.annotations = file_annotations

        item = _SyncUploadItem(
            file,
            row["used"] if "used" in row else [],
            row["executed"] if "executed" in row else [],
            {key: row[key] for key in STORE_FUNCTION_FIELDS if key in row},
        )
        items.append(item)

    with _sync_executor(syn) as executor:
        uploader = _SyncUploader(syn, executor)
        uploader.upload(items)

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
