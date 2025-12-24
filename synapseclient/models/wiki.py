"""Script to work with Synapse wiki pages."""

import asyncio
import gzip
import os
import pprint
from dataclasses import dataclass, field
from typing import Any, Dict, List, Literal, Optional, Union

from synapseclient import Synapse
from synapseclient.api import (
    delete_wiki_page,
    get_attachment_handles,
    get_attachment_preview_url,
    get_attachment_url,
    get_markdown_url,
    get_wiki_header_tree,
    get_wiki_history,
    get_wiki_order_hint,
    get_wiki_page,
    post_wiki_page,
    put_wiki_order_hint,
    put_wiki_page,
    put_wiki_version,
)
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.download import (
    PresignedUrlInfo,
    _pre_signed_url_expiration_time,
    download_from_url,
    download_from_url_multi_threaded,
)
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.core.upload.upload_functions_async import upload_file_handle
from synapseclient.core.utils import delete_none_keys, merge_dataclass_entities
from synapseclient.models.protocols.wikipage_protocol import (
    WikiHeaderSynchronousProtocol,
    WikiHistorySnapshotSynchronousProtocol,
    WikiOrderHintSynchronousProtocol,
    WikiPageSynchronousProtocol,
)

# File size threshold for using single-threaded vs multi-threaded download (8 MiB)
SINGLE_THREAD_DOWNLOAD_SIZE_LIMIT = 8 * 1024 * 1024  # 8 MiB in bytes


@dataclass
@async_to_sync
class WikiOrderHint(WikiOrderHintSynchronousProtocol):
    """
    A WikiOrderHint contains the order hint for the root wiki that corresponds to the given owner ID and type.

    Attributes:
        owner_id: The Synapse ID of the owner object (e.g., entity, evaluation, etc.).
        owner_object_type: The type of the owner object.
        id_list: The list of sub wiki ids that in the order that they should be placed relative to their siblings.
        etag: The etag of this object.
    """

    owner_id: Optional[str] = None
    """The Synapse ID of the owner object (e.g., entity, evaluation, etc.)."""

    owner_object_type: Optional[str] = None
    """The type of the owner object."""

    id_list: List[str] = field(default_factory=list)
    """The list of sub wiki ids that in the order that they should be placed relative to their siblings."""

    etag: Optional[str] = field(default=None, compare=False)
    """The etag of this object."""

    def fill_from_dict(
        self,
        wiki_order_hint: Dict[str, Union[str, List[str]]],
    ) -> "WikiOrderHint":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            wiki_order_hint: The response from the REST API.

        Returns:
            The WikiOrderHint object.
        """
        self.owner_id = wiki_order_hint.get("ownerId", None)
        self.owner_object_type = wiki_order_hint.get("ownerObjectType", None)
        self.id_list = wiki_order_hint.get("idList", [])
        self.etag = wiki_order_hint.get("etag", None)
        return self

    def to_synapse_request(self) -> Dict[str, List[str]]:
        """
        Convert the WikiOrderHint object to a request for the REST API.
        """
        result = {
            "ownerId": self.owner_id,
            "ownerObjectType": self.owner_object_type,
            "idList": self.id_list,
            "etag": self.etag,
        }
        delete_none_keys(result)
        return result

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Store_Wiki_Order_Hint: {self.owner_id}"
    )
    async def store_async(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> "WikiOrderHint":
        """
        Store the order hint of a wiki page tree.

        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The updated WikiOrderHint object for the entity.
        Raises:
            ValueError: If owner_id or request is not provided.

        Example: Set the WikiOrderHint for a project
            This example shows how to set a WikiOrderHint for existing wiki pages in a project.
            The WikiOrderHint is not set by default, so you need to set it explicitly.
            from synapseclient import Synapse
            from synapseclient.models import (
                Project,
                WikiOrderHint,
            )
            syn = Synapse()
            syn.login()
            project = await Project(name="My uniquely named project about Alzheimer's Disease").get_async()
            wiki_order_hint = await WikiOrderHint(owner_id=project.id).get_async()

            wiki_order_hint.id_list = [
                root_wiki_page.id,
                wiki_page_1.id,
                wiki_page_3.id,
                wiki_page_2.id,
                wiki_page_4.id,
            ]
            await wiki_order_hint.store_async()
            print(wiki_order_hint)

        Example: Update the WikiOrderHint for a project
            This example shows how to update a WikiOrderHint for existing wiki pages in a project.
            wiki_order_hint.id_list = [
                root_wiki_page.id,
                wiki_page_1.id,
                wiki_page_2.id,
                wiki_page_3.id,
                wiki_page_4.id,
            ]
            await wiki_order_hint.store_async()
            print(wiki_order_hint)
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to store wiki order hint.")

        order_hint_dict = await put_wiki_order_hint(
            owner_id=self.owner_id,
            request=self.to_synapse_request(),
            synapse_client=synapse_client,
        )
        self.fill_from_dict(order_hint_dict)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Wiki_Order_Hint: {self.owner_id}"
    )
    async def get_async(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> "WikiOrderHint":
        """
        Get the order hint of a wiki page tree.

        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            A WikiOrderHint object for the entity.
        Raises:
            ValueError: If owner_id is not provided.

        Example: Get the WikiOrderHint for a project
            This example shows how to get a WikiOrderHint for existing wiki pages in a project.
            from synapseclient import Synapse
            from synapseclient.models import (
                Project,
                WikiOrderHint,
            )
            syn = Synapse()
            syn.login()
            project = await Project(name="My uniquely named project about Alzheimer's Disease").get_async()
            wiki_order_hint = await WikiOrderHint(owner_id=project.id).get_async()
            print(wiki_order_hint)
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get wiki order hint.")

        order_hint_dict = await get_wiki_order_hint(
            owner_id=self.owner_id,
            synapse_client=synapse_client,
        )
        return self.fill_from_dict(order_hint_dict)


@dataclass
@async_to_sync
class WikiHistorySnapshot(WikiHistorySnapshotSynchronousProtocol):
    """
    A WikiHistorySnapshot contains basic information about an update to a WikiPage.

    Attributes:
        version: The version number of the wiki page.
        modified_on: The timestamp when this version was created.
            modified_by: The ID of the user that created this version.
    """

    version: Optional[str] = None
    """The version number of the wiki page."""

    modified_on: Optional[str] = None
    """The timestamp when this version was created."""

    modified_by: Optional[str] = None
    """The ID of the user that created this version."""

    def fill_from_dict(
        self,
        wiki_history: Dict[str, str],
    ) -> "WikiHistorySnapshot":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            wiki_history: The response from the REST API.

        Returns:
            The WikiHistorySnapshot object.
        """
        self.version = wiki_history.get("version", None)
        self.modified_on = wiki_history.get("modifiedOn", None)
        self.modified_by = wiki_history.get("modifiedBy", None)
        return self

    @classmethod
    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Wiki_History for Owner ID {kwargs['owner_id']}, Wiki ID {kwargs['id']}"
    )
    async def get_async(
        cls,
        owner_id: str = None,
        id: str = None,
        *,
        offset: int = 0,
        limit: int = 20,
        synapse_client: Optional["Synapse"] = None,
    ) -> List["WikiHistorySnapshot"]:
        """
        Get the history of a wiki page as a list of WikiHistorySnapshot objects.

        Arguments:
            owner_id: The Synapse ID of the owner entity.
            id: The ID of the wiki page.
            offset: The index of the pagination offset.
            limit: Limits the size of the page returned.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            A list of WikiHistorySnapshot objects for the wiki page.

        Example: Get the history of a wiki page
            history = await WikiHistorySnapshot.get_async(owner_id=project.id, id=wiki_page.id)
            print(f"History: {history}")
        """
        if not owner_id:
            raise ValueError("Must provide owner_id to get wiki history.")
        if not id:
            raise ValueError("Must provide id to get wiki history.")
        snapshots = []
        async for item in get_wiki_history(
            owner_id=owner_id,
            wiki_id=id,  # use id instead of wiki_id to match other classes
            offset=offset,
            limit=limit,
            synapse_client=synapse_client,
        ):
            snapshots.append(cls().fill_from_dict(item))
        return snapshots


@dataclass
@async_to_sync
class WikiHeader(WikiHeaderSynchronousProtocol):
    """
    A WikiHeader contains basic metadata about a WikiPage.

    Attributes:
        id: The unique identifier for this wiki page.
        title: The title of this page.
        parent_id: When set, the WikiPage is a sub-page of the indicated parent WikiPage.
    """

    id: Optional[str] = None
    """The unique identifier for this wiki page."""

    title: Optional[str] = None
    """The title of this page."""

    parent_id: Optional[str] = None
    """When set, the WikiPage is a sub-page of the indicated parent WikiPage."""

    def fill_from_dict(
        self,
        wiki_header: Dict[str, str],
    ) -> "WikiHeader":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            wiki_header: The response from the REST API.

        Returns:
            The WikiHeader object.
        """
        self.id = wiki_header.get("id", None)
        self.title = wiki_header.get("title", None)
        self.parent_id = wiki_header.get("parentId", None)
        return self

    @classmethod
    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Wiki_Header_Tree for Owner ID {kwargs['owner_id']}"
    )
    async def get_async(
        cls,
        owner_id: str = None,
        *,
        offset: int = 0,
        limit: int = 20,
        synapse_client: Optional["Synapse"] = None,
    ) -> List["WikiHeader"]:
        """
        Get the header tree (hierarchy) of wiki pages for an entity.

        Arguments:
            owner_id: The Synapse ID of the owner entity.
            offset: The index of the pagination offset.
            limit: Limits the size of the page returned.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            A list of WikiHeader objects for the entity.

        Example: Get the header tree (hierarchy) of wiki pages for an entity
            headers = await WikiHeader.get_async(owner_id=project.id)
            print(f"Headers: {headers}")
        """
        if not owner_id:
            raise ValueError("Must provide owner_id to get wiki header tree.")
        headers = []
        async for item in get_wiki_header_tree(
            owner_id=owner_id,
            offset=offset,
            limit=limit,
            synapse_client=synapse_client,
        ):
            headers.append(cls().fill_from_dict(item))
        return headers


@dataclass
@async_to_sync
class WikiPage(WikiPageSynchronousProtocol):
    """
    Represents a [Wiki Page](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/v2/wiki/V2WikiPage.html).

    Attributes:
        id: The unique identifier for this wiki page.
        etag: The etag of this object. Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. Since the E-Tag changes every time an entity is updated it is
            used to detect when a client's current representation of an entity is out-of-date.
        title: The title of this page.
        parent_id: When set, the WikiPage is a sub-page of the indicated parent WikiPage.
        markdown: The markdown content of the wiki page.
        attachments: A list of file attachments associated with the wiki page.
        owner_id: The Synapse ID of the owning object (e.g., entity, evaluation, etc.).
        created_on: The timestamp when this page was created.
        created_by: The ID of the user that created this page.
        modified_on: The timestamp when this page was last modified.
        modified_by: The ID of the user that last modified this page.
        wiki_version: The version number of this wiki page.
        markdown_file_handle_id: The ID of the file handle containing the markdown content.
        attachment_file_handle_ids: The list of attachment file handle ids of this page.
    """

    id: Optional[str] = None
    """The unique identifier for this wiki page."""

    etag: Optional[str] = field(default=None, compare=False)
    """The etag of this object. Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent
    updates. Since the E-Tag changes every time an entity is updated it is used to detect
    when a client's current representation of an entity is out-of-date."""

    title: Optional[str] = None
    """The title of this page."""

    parent_id: Optional[str] = None
    """When set, the WikiPage is a sub-page of the indicated parent WikiPage."""

    markdown: Optional[str] = None
    """The markdown content of this page."""

    attachments: List[Dict[str, Any]] = field(default_factory=list)
    """A list of file paths sassociated with this page."""

    owner_id: Optional[str] = None
    """The Synapse ID of the owning object (e.g., entity, evaluation, etc.)."""

    created_on: Optional[str] = field(default=None, compare=False)
    """The timestamp when this page was created."""

    created_by: Optional[str] = field(default=None, compare=False)
    """The ID of the user that created this page."""

    modified_on: Optional[str] = field(default=None, compare=False)
    """The timestamp when this page was last modified."""

    modified_by: Optional[str] = field(default=None, compare=False)
    """The ID of the user that last modified this page."""

    wiki_version: Optional[str] = None
    """The version number of this wiki page."""

    markdown_file_handle_id: Optional[str] = None
    """The ID of the file handle containing the markdown content."""

    attachment_file_handle_ids: List[str] = field(default_factory=list)
    """The list of attachment file handle ids of this page."""

    def fill_from_dict(
        self,
        synapse_wiki: Dict[str, Union[str, List[str], List[Dict[str, Any]]]],
    ) -> "WikiPage":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_wiki: The response from the REST API.

        Returns:
            The WikiPage object.
        """
        self.id = synapse_wiki.get("id", None)
        self.etag = synapse_wiki.get("etag", None)
        self.title = synapse_wiki.get("title", None)
        self.parent_id = synapse_wiki.get("parentWikiId", None)
        self.markdown = self.markdown
        self.attachments = self.attachments
        self.created_on = synapse_wiki.get("createdOn", None)
        self.created_by = synapse_wiki.get("createdBy", None)
        self.modified_on = synapse_wiki.get("modifiedOn", None)
        self.modified_by = synapse_wiki.get("modifiedBy", None)
        self.wiki_version = self.wiki_version
        self.markdown_file_handle_id = synapse_wiki.get("markdownFileHandleId", None)
        self.attachment_file_handle_ids = synapse_wiki.get(
            "attachmentFileHandleIds", []
        )
        return self

    def to_synapse_request(
        self,
    ) -> Dict[str, Union[str, List[str], List[Dict[str, Any]]]]:
        """Convert the wiki page object into a format suitable for the Synapse API."""
        result = {
            "id": self.id,
            "etag": self.etag,
            "title": self.title,
            "parentWikiId": self.parent_id,
            "markdown": self.markdown,
            "attachments": self.attachments,
            "createdOn": self.created_on,
            "createdBy": self.created_by,
            "modifiedOn": self.modified_on,
            "modifiedBy": self.modified_by,
            "wikiVersion": self.wiki_version,
            "markdownFileHandleId": self.markdown_file_handle_id,
            "attachmentFileHandleIds": self.attachment_file_handle_ids,
        }
        delete_none_keys(result)
        return result

    def _to_gzip_file(
        self,
        wiki_content: str,
        synapse_client: Optional[Synapse] = None,
    ) -> str:
        """Convert markdown or attachment to a gzipped file and save it in the synapse cache to get a file handle id later.

        Arguments:
            wiki_content: The markdown or attachment content as plain text, basic HTML, or Markdown, or a file path to such content.
            synapse_client: The Synapse client to use for cache access.

        Returns:
            The path to the gzipped file and the cache directory.
        """
        # check if markdown is a string
        if not isinstance(wiki_content, str):
            raise SyntaxError(f"Expected a string, got {type(wiki_content).__name__}")
        # Get the cache directory path to save the newly created gzipped file
        cache_dir = os.path.join(synapse_client.cache.cache_root_dir, "wiki_content")
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        # Check if markdown looks like a file path and exists
        if os.path.isfile(wiki_content):
            # If it's already a gzipped file, use the file path directly
            if wiki_content.endswith(".gz"):
                file_path = wiki_content
            else:
                # If it's a regular html or markdown file, compress it
                with open(wiki_content, "rb") as f_in:
                    # Open the output gzip file
                    file_path = os.path.join(
                        cache_dir, os.path.basename(wiki_content) + ".gz"
                    )
                    with gzip.open(file_path, "wb") as f_out:
                        f_out.writelines(f_in)

        else:
            # If it's a plain text, write it to a gzipped file and save it in the synapse cache
            file_path = os.path.join(cache_dir, f"wiki_markdown_{self.title}.md.gz")
            with gzip.open(file_path, "wt", encoding="utf-8") as f_out:
                f_out.write(wiki_content)

        return file_path

    @staticmethod
    def unzip_gzipped_file(file_path: str) -> str:
        """Unzip the gzipped file and return the file path to the unzipped file.

        If the file is a markdown file, the content will be printed.
        Arguments:
            file_path: The path to the gzipped file.
        Returns:
            The file path to the unzipped file.
        """
        # Open in binary mode to handle both text and binary files
        with gzip.open(file_path, "rb") as f_in:
            unzipped_content_bytes = f_in.read()

            # Try to decode as UTF-8 to check if it's a text file
            is_text_file = False
            unzipped_content_text = None
            try:
                unzipped_content_text = unzipped_content_bytes.decode("utf-8")
                is_text_file = True
                if file_path.endswith(".md.gz"):
                    pprint.pp(unzipped_content_text)
            except UnicodeDecodeError:
                # It's a binary file, keep as bytes
                pass

            # unzip the file and return the file path
            unzipped_file_path = os.path.join(
                os.path.dirname(file_path),
                os.path.basename(file_path).replace(".gz", ""),
            )
            # Write in text mode for text files, binary mode for binary files
            if is_text_file:
                with open(unzipped_file_path, "wt", encoding="utf-8") as f_out:
                    f_out.write(unzipped_content_text)
            else:
                with open(unzipped_file_path, "wb") as f_out:
                    f_out.write(unzipped_content_bytes)

            return unzipped_file_path

    @staticmethod
    def _get_file_size(filehandle_dict: dict, file_name: str) -> str:
        """Get the file name from the response headers.
        Arguments:
            response: The response from the REST API.
        Returns:
            The file name.
        """
        filehandle_dict = filehandle_dict["list"]
        available_files = [filehandle["fileName"] for filehandle in filehandle_dict]
        # locate the contentSize for given file_name
        for filehandle in filehandle_dict:
            if filehandle["fileName"] == file_name:
                return filehandle["contentSize"]
        raise ValueError(
            f"File {file_name} not found in filehandle_dict. Available files: {available_files}"
        )

    @staticmethod
    def reformat_attachment_file_name(attachment_file_name: str) -> str:
        """Reformat the attachment file name to be a valid attachment path.
        Arguments:
            attachment_file_name: The name of the attachment file.
        Returns:
            The reformatted attachment file name.
        """
        attachment_file_name_reformatted = attachment_file_name.replace(".", "%2E")
        attachment_file_name_reformatted = attachment_file_name_reformatted.replace(
            "_", "%5F"
        )
        return attachment_file_name_reformatted

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get the markdown file handle: {self.owner_id}"
    )
    async def _get_markdown_file_handle(self, synapse_client: Synapse) -> "WikiPage":
        """Get the markdown file handle from the synapse client.
        Arguments:
            synapse_client: The Synapse client to use for cache access.
        Returns:
            A WikiPage with the updated markdown file handle id.
        """
        if not self.markdown:
            return self
        else:
            file_path = self._to_gzip_file(
                wiki_content=self.markdown, synapse_client=synapse_client
            )
            try:
                # Upload the gzipped file to get a file handle
                async with synapse_client._get_parallel_file_transfer_semaphore(
                    asyncio_event_loop=asyncio.get_running_loop()
                ):
                    file_handle = await upload_file_handle(
                        syn=synapse_client,
                        parent_entity_id=self.owner_id,
                        path=file_path,
                    )
                    synapse_client.logger.info(
                        f"Uploaded file handle {file_handle.get('id')} for wiki page markdown."
                    )
                    # Set the markdown file handle ID from the upload response
                    self.markdown_file_handle_id = file_handle.get("id")
            finally:
                # delete the temp directory saving the gzipped file
                if os.path.exists(file_path):
                    os.remove(file_path)
                    synapse_client.logger.debug(f"Deleted temp directory {file_path}")
            return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get the attachment file handles for wiki page: {self.owner_id}"
    )
    async def _get_attachment_file_handles(self, synapse_client: Synapse) -> "WikiPage":
        """Get the attachment file handles from the synapse client.
        Arguments:
            synapse_client: The Synapse client to use for cache access.
        Returns:
            A WikiPage with the updated attachment file handle ids.
        """
        if not self.attachments:
            return self
        else:

            async def task_of_uploading_attachment(attachment: str) -> tuple[str, str]:
                """Process a single attachment and return its file handle ID and cache directory."""
                # if the attachment is not a gzipped file or image file, gzip it
                if (
                    not attachment.endswith(".gz")
                    and not attachment.endswith(".png")
                    and not attachment.endswith(".jpg")
                    and not attachment.endswith(".jpeg")
                ):
                    file_path = self._to_gzip_file(
                        wiki_content=attachment, synapse_client=synapse_client
                    )
                else:
                    file_path = attachment
                try:
                    async with synapse_client._get_parallel_file_transfer_semaphore(
                        asyncio_event_loop=asyncio.get_running_loop()
                    ):
                        file_handle = await upload_file_handle(
                            syn=synapse_client,
                            parent_entity_id=self.owner_id,
                            path=file_path,
                        )
                        synapse_client.logger.info(
                            f"Uploaded file handle {file_handle.get('id')} for wiki page attachment."
                        )
                        return file_handle.get("id")
                finally:
                    if os.path.exists(file_path):
                        os.remove(file_path)
                        synapse_client.logger.debug(
                            f"Deleted temp directory {file_path}"
                        )

            # Process all attachments in parallel
            tasks = [
                asyncio.create_task(task_of_uploading_attachment(attachment))
                for attachment in self.attachments
            ]
            results = await asyncio.gather(*tasks)
            # Set the attachment file handle IDs from the upload response
            self.attachment_file_handle_ids = results
            return self

    async def _determine_wiki_action(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Literal["create_root", "update_root", "create_sub"]:
        """Determine the wiki action to perform.
        Returns:
            The wiki action to perform.
        Raises:
            ValueError: If required fields are missing.
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to modify a wiki page.")

        if self.parent_id:
            return "create_sub_wiki_page"

        try:
            await WikiHeader.get_async(
                owner_id=self.owner_id, synapse_client=synapse_client
            )
        except SynapseHTTPError as e:
            if e.response.status_code == 404:
                return "create_root_wiki_page"
            else:
                raise
        else:
            if not self.id:
                raise ValueError("Must provide id to update existing wiki page.")
            return "update_existing_wiki_page"

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Store the wiki page: {self.owner_id}"
    )
    async def store_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "WikiPage":
        """Store the wiki page. If there is no wiki page, a new wiki page will be created.
        If the wiki page already exists, it will be updated.
        Subwiki pages are created by passing in a parent_id.
        If a parent_id is provided, the wiki page will be created as a subwiki page.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            The created/updated wiki page.

        Raises:
            ValueError: If owner_id is not provided or if required fields are missing.

        Example: Store a wiki page
            This example shows how to store a wiki page.
            from synapseclient import Synapse
            from synapseclient.models import (
                Project,
                WikiPage,
            )
            syn = Synapse()
            syn.login()
            project = await Project(name="My uniquely named project about Alzheimer's Disease").get_async()
            wiki_page = await WikiPage(owner_id=project.id, title="My wiki page").store_async()
            print(wiki_page)
        """
        client = Synapse.get_client(synapse_client=synapse_client)

        wiki_action = await self._determine_wiki_action(synapse_client=client)
        # get the markdown file handle and attachment file handles if the wiki action is valid
        if wiki_action:
            # Update self with the returned WikiPage objects that have file handle IDs set
            self = await self._get_markdown_file_handle(synapse_client=client)
            self = await self._get_attachment_file_handles(synapse_client=client)

        # Handle root wiki page creation if parent_id is not given
        if wiki_action == "create_root_wiki_page":
            client.logger.info(
                "No wiki page exists within the owner. Create a new wiki page."
            )
            # Create the wiki page
            wiki_data = await post_wiki_page(
                owner_id=self.owner_id,
                request=self.to_synapse_request(),
                synapse_client=client,
            )
            client.logger.info(
                f"Created wiki page: {wiki_data.get('title')} with ID: {wiki_data.get('id')}."
            )
        if wiki_action == "update_existing_wiki_page":
            client.logger.info(
                "A wiki page already exists within the owner. Update the existing wiki page."
            )
            # retrieve the wiki page
            existing_wiki_dict = await get_wiki_page(
                owner_id=self.owner_id,
                wiki_id=self.id,
                wiki_version=self.wiki_version,
                synapse_client=client,
            )
            # convert to dataclass
            existing_wiki = WikiPage()
            existing_wiki = existing_wiki.fill_from_dict(
                synapse_wiki=existing_wiki_dict
            )
            # Update existing_wiki with current object's attributes if they are not None
            updated_wiki = merge_dataclass_entities(
                source=existing_wiki,
                destination=self,
                fields_to_ignore=[
                    "modified_on",
                    "modified_by",
                ],
            )
            # update the wiki page
            wiki_data = await put_wiki_page(
                owner_id=self.owner_id,
                wiki_id=self.id,
                request=updated_wiki.to_synapse_request(),
                synapse_client=client,
            )
            client.logger.info(
                f"Updated wiki page: {wiki_data.get('title')} with ID: {wiki_data.get('id')}."
            )

        # Handle sub-wiki page creation if parent_id is given
        if wiki_action == "create_sub_wiki_page":
            client.logger.info(
                f"Creating sub-wiki page under parent ID: {self.parent_id}"
            )
            # Create the sub-wiki page directly
            wiki_data = await post_wiki_page(
                owner_id=self.owner_id,
                request=self.to_synapse_request(),
                synapse_client=client,
            )
            client.logger.info(
                f"Created sub-wiki page: {wiki_data.get('title')} with ID: {wiki_data.get('id')} under parent: {self.parent_id}"
            )
        self.fill_from_dict(wiki_data)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Restore version: {self.wiki_version} for wiki page: {self.id}"
    )
    async def restore_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "WikiPage":
        """Restore a specific version of a wiki page.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            The restored wiki page.

        Example: Restore a specific version of a wiki page
            This example shows how to restore a specific version of a wiki page.
            wiki_page_restored = await WikiPage(owner_id=project.id, id=wiki_page.id, wiki_version="0").restore_async()
            print(wiki_page_restored)
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to restore a wiki page.")
        if not self.id:
            raise ValueError("Must provide id to restore a wiki page.")
        if not self.wiki_version:
            raise ValueError("Must provide wiki_version to restore a wiki page.")

        # restore the wiki page
        wiki_data = await put_wiki_version(
            owner_id=self.owner_id,
            wiki_id=self.id,
            wiki_version=self.wiki_version,
            request=self.to_synapse_request(),
            synapse_client=synapse_client,
        )
        self.fill_from_dict(wiki_data)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Wiki_Page: {self.owner_id}"
    )
    async def get_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "WikiPage":
        """Get a wiki page from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            The wiki page.

        Raises:
            ValueError: If owner_id is not provided.

        Example: Get a wiki page from Synapse
            This example shows how to get a wiki page from Synapse.
            wiki_page = await WikiPage(owner_id=project.id, id=wiki_page.id).get_async()
            print(wiki_page)
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get a wiki page.")
        if not self.id and not self.title:
            raise ValueError("Must provide id or title to get a wiki page.")

        # If we only have a title, find the wiki page with matching title.
        if self.id is None:
            async for result in get_wiki_header_tree(
                owner_id=self.owner_id,
                synapse_client=synapse_client,
            ):
                if result.get("title") == self.title:
                    matching_header = result
                    break
                else:
                    matching_header = None

            if not matching_header:
                raise ValueError(f"No wiki page found with title: {self.title}")
            self.id = matching_header["id"]

        wiki_data = await get_wiki_page(
            owner_id=self.owner_id,
            wiki_id=self.id,
            wiki_version=self.wiki_version,
            synapse_client=synapse_client,
        )
        self.fill_from_dict(wiki_data)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Delete_Wiki_Page: Owner ID {self.owner_id}, Wiki ID {self.id}"
    )
    async def delete_async(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> None:
        """
        Delete this wiki page.

        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Raises:
            ValueError: If required fields are missing.

        Example: Delete a wiki page
            This example shows how to delete a wiki page.
            wiki_page = await WikiPage(owner_id=project.id, id=wiki_page.id).delete_async()
            print(f"Wiki page {wiki_page.title} deleted successfully.")
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to delete a wiki page.")
        if not self.id:
            raise ValueError("Must provide id to delete a wiki page.")

        await delete_wiki_page(
            owner_id=self.owner_id,
            wiki_id=self.id,
            synapse_client=synapse_client,
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Attachment_Handles: Owner ID {self.owner_id}, Wiki ID {self.id}, Wiki Version {self.wiki_version}"
    )
    async def get_attachment_handles_async(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> List[Dict[str, Any]]:
        """
        Get the file handles of all attachments on this wiki page.

        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The list of FileHandles for all file attachments of this WikiPage.
        Raises:
            ValueError: If owner_id or id is not provided.

        Example: Get the file handles of all attachments on a wiki page
            This example shows how to get the file handles of all attachments on a wiki page.
            attachment_handles = await WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment_handles_async()
            print(f"Attachment handles: {attachment_handles['list']}")
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get attachment handles.")
        if not self.id:
            raise ValueError("Must provide id to get attachment handles.")

        return await get_attachment_handles(
            owner_id=self.owner_id,
            wiki_id=self.id,
            wiki_version=self.wiki_version,
            synapse_client=synapse_client,
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Attachment_URL: Owner ID {self.owner_id}, Wiki ID {self.id}, File Name {kwargs['file_name']}"
    )
    async def get_attachment_async(
        self,
        *,
        file_name: str,
        download_file: bool = True,
        download_location: Optional[str] = None,
        synapse_client: Optional["Synapse"] = None,
    ) -> Union[str, None]:
        """
        Download the wiki page attachment to a local file or return the URL.

        Arguments:
            file_name: The name of the file to get. The file name can be in either non-gzipped or gzipped format.
            download_file: Whether associated files should be downloaded. Default is True.
            download_location: The directory to download the file to. Required if download_file is True.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            If download_file is True, the attachment file will be downloaded to the download_location. Otherwise, the URL will be returned.
        Raises:
            ValueError: If owner_id or id is not provided.

        Example: Get the attachment URL for a wiki page
            This example shows how to get the attachment file or URL for a wiki page.
            attachment_file_or_url = await WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment_async(file_name="attachment.txt", download_file=False)
            print(f"Attachment URL: {attachment_file_or_url}")

        Example: Download the attachment file for a wiki page
            This example shows how to download the attachment file for a wiki page.
            attachment_file_path = await WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment_async(file_name="attachment.txt", download_file=True, download_location="~/temp")
            print(f"Attachment file path: {attachment_file_path}")
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get attachment URL.")
        if not self.id:
            raise ValueError("Must provide id to get attachment URL.")
        if not file_name:
            raise ValueError("Must provide file_name to get attachment URL.")

        client = Synapse.get_client(synapse_client=synapse_client)
        # check if the file_name is in gzip format or image format
        if (
            not file_name.endswith(".gz")
            and not file_name.endswith(".png")
            and not file_name.endswith(".jpg")
            and not file_name.endswith(".jpeg")
        ):
            file_name = f"{file_name}.gz"
        attachment_url = await get_attachment_url(
            owner_id=self.owner_id,
            wiki_id=self.id,
            file_name=file_name,
            wiki_version=self.wiki_version,
            synapse_client=client,
        )

        if download_file:
            if not download_location:
                raise ValueError("Must provide download_location to download a file.")

            # construct PresignedUrlInfo for downloading
            presigned_url_info = PresignedUrlInfo(
                url=attachment_url,
                file_name=file_name,
                expiration_utc=_pre_signed_url_expiration_time(attachment_url),
            )
            filehandle_dict = await get_attachment_handles(
                owner_id=self.owner_id,
                wiki_id=self.id,
                wiki_version=self.wiki_version,
                synapse_client=client,
            )
            # check the file_size
            file_size = int(WikiPage._get_file_size(filehandle_dict, file_name))
            # use single thread download if file size < 8 MiB
            if file_size < SINGLE_THREAD_DOWNLOAD_SIZE_LIMIT:
                downloaded_file_path = download_from_url(
                    url=presigned_url_info.url,
                    destination=download_location,
                    url_is_presigned=True,
                    synapse_client=client,
                )
            else:
                # download the file
                downloaded_file_path = await download_from_url_multi_threaded(
                    presigned_url=presigned_url_info,
                    destination=download_location,
                    synapse_client=client,
                )
            # unzip the file if it is a gzipped file
            unzipped_file_path = WikiPage.unzip_gzipped_file(downloaded_file_path)
            client.logger.info(
                f"Downloaded file {presigned_url_info.file_name.replace('.gz', '')} to {unzipped_file_path}."
            )
            os.remove(downloaded_file_path)
            client.logger.debug(f"Removed the gzipped file {downloaded_file_path}.")
            return unzipped_file_path
        else:
            return attachment_url

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Attachment_Preview_URL: Owner ID {self.owner_id}, Wiki ID {self.id}, File Name {kwargs['file_name']}"
    )
    async def get_attachment_preview_async(
        self,
        file_name: str,
        *,
        download_file: bool = True,
        download_location: Optional[str] = None,
        synapse_client: Optional["Synapse"] = None,
    ) -> Union[str, None]:
        """
        Download the wiki page attachment preview to a local file or return the URL.

        Arguments:
            file_name: The name of the file to get. The file name can be in either non-gzipped or gzipped format.
            download_file: Whether associated files should be downloaded. Default is True.
            download_location: The directory to download the file to. Required if download_file is True.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            If download_file is True, the attachment preview file will be downloaded to the download_location. Otherwise, the URL will be returned.
        Raises:
            ValueError: If owner_id or id is not provided.

        Example: Get the attachment preview URL for a wiki page
            This example shows how to get the attachment preview URL for a wiki page.
            Instead of using the file_name from the attachmenthandle response when isPreview=True, you should use the original file name in the get_attachment_preview request.
            The downloaded file will still be named according to the file_name provided in the response when isPreview=True.
            attachment_preview_url = await WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment_preview_async(file_name="attachment.txt.gz", download_file=False)
            print(f"Attachment preview URL: {attachment_preview_url}")

        Example: Download the attachment preview file for a wiki page
            This example shows how to download the attachment preview file for a wiki page.
            attachment_preview_file_path = WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment_preview(file_name="attachment.txt.gz", download_file=True, download_location="~/temp")
            print(f"Attachment preview file path: {attachment_preview_file_path}")
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get attachment preview URL.")
        if not self.id:
            raise ValueError("Must provide id to get attachment preview URL.")
        if not file_name:
            raise ValueError("Must provide file_name to get attachment preview URL.")

        client = Synapse.get_client(synapse_client=synapse_client)
        # check if the file_name is in gzip format or image format
        if not file_name.endswith(".gz"):
            file_name = f"{file_name}.gz"
        attachment_preview_url = await get_attachment_preview_url(
            owner_id=self.owner_id,
            wiki_id=self.id,
            file_name=file_name,
            wiki_version=self.wiki_version,
            synapse_client=client,
        )
        # download the file if download_file is True
        if download_file:
            if not download_location:
                raise ValueError("Must provide download_location to download a file.")

            # construct PresignedUrlInfo for downloading
            presigned_url_info = PresignedUrlInfo(
                url=attachment_preview_url,
                file_name=file_name,
                expiration_utc=_pre_signed_url_expiration_time(attachment_preview_url),
            )

            filehandle_dict = await get_attachment_handles(
                owner_id=self.owner_id,
                wiki_id=self.id,
                wiki_version=self.wiki_version,
                synapse_client=client,
            )
            # check the file_size
            file_size = int(WikiPage._get_file_size(filehandle_dict, file_name))
            # use single thread download if file size < 8 MiB
            if file_size < SINGLE_THREAD_DOWNLOAD_SIZE_LIMIT:
                downloaded_file_path = download_from_url(
                    url=presigned_url_info.url,
                    destination=download_location,
                    url_is_presigned=True,
                    synapse_client=client,
                )
            else:
                # download the file
                downloaded_file_path = await download_from_url_multi_threaded(
                    presigned_url=presigned_url_info,
                    destination=download_location,
                    synapse_client=client,
                )
            client.logger.info(
                f"Downloaded the preview file {presigned_url_info.file_name.replace('.gz', '')} to {downloaded_file_path}."
            )
            return downloaded_file_path
        else:
            return attachment_preview_url

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Markdown_URL: Owner ID {self.owner_id}, Wiki ID {self.id}, Wiki Version {self.wiki_version}"
    )
    async def get_markdown_file_async(
        self,
        *,
        download_file: bool = True,
        download_location: Optional[str] = None,
        synapse_client: Optional["Synapse"] = None,
    ) -> Union[str, None]:
        """
        Get the markdown URL of this wiki page. --> modify this to print the markdown file

        Arguments:
            download_file: Whether associated files should be downloaded. Default is True.
            download_location: The directory to download the file to. Required if download_file is True.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            If download_file is True, the markdown file will be downloaded to the download_location. Otherwise, the URL will be returned.
        Raises:
            ValueError: If owner_id or id is not provided.

        Example: Get the markdown URL for a wiki page
            This example shows how to get the markdown URL for a wiki page.
            markdown_url = await WikiPage(owner_id=project.id, id=wiki_page.id).get_markdown_file_async(download_file=False)
            print(f"Markdown URL: {markdown_url}")

        Example: Download the markdown file for a wiki page
            This example shows how to download the markdown file for a wiki page.
            markdown_file_path = await WikiPage(owner_id=project.id, id=wiki_page.id).get_markdown_file_async(download_file=True, download_location="~/temp")
            print(f"Markdown file path: {markdown_file_path}")
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get markdown URL.")
        if not self.id:
            raise ValueError("Must provide id to get markdown URL.")

        client = Synapse.get_client(synapse_client=synapse_client)
        markdown_url = await get_markdown_url(
            owner_id=self.owner_id,
            wiki_id=self.id,
            wiki_version=self.wiki_version,
            synapse_client=client,
        )
        # download the file if download_file is True
        if download_file:
            if not download_location:
                raise ValueError("Must provide download_location to download a file.")

            # construct PresignedUrlInfo for downloading
            downloaded_file_path = download_from_url(
                url=markdown_url,
                destination=download_location,
                url_is_presigned=True,
                synapse_client=client,
            )
            # unzip the file if it is a gzipped file
            unzipped_file_path = WikiPage.unzip_gzipped_file(downloaded_file_path)
            client.logger.info(
                f"Downloaded and unzipped the markdown file for wiki page {self.id} to {unzipped_file_path}."
            )
            # remove the gzipped file
            os.remove(downloaded_file_path)
            client.logger.debug(f"Removed the gzipped file {downloaded_file_path}.")
            return unzipped_file_path
        else:
            return markdown_url
