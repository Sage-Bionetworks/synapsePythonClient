"""Script to work with Synapse wiki pages."""

import asyncio
import gzip
import os
import pprint
import re
from dataclasses import dataclass, field
from typing import Any, AsyncGenerator, Dict, Generator, List, Literal, Optional, Union

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
    post_file_handles_copy,
    post_wiki_page,
    put_wiki_order_hint,
    put_wiki_page,
    put_wiki_version,
)
from synapseclient.core.async_utils import (
    async_to_sync,
    otel_trace_method,
    skip_async_to_sync,
    wrap_async_generator_to_sync_generator,
)
from synapseclient.core.download import (
    PresignedUrlInfo,
    _pre_signed_url_expiration_time,
    download_from_url,
    download_from_url_multi_threaded,
)
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.core.upload.upload_functions_async import upload_file_handle
from synapseclient.core.utils import (
    delete_none_keys,
    is_synapse_id_str,
    merge_dataclass_entities,
)
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
            ```python
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
            ```
        Example: Update the WikiOrderHint for a project
            This example shows how to update a WikiOrderHint for existing wiki pages in a project.
            ```python
            wiki_order_hint.id_list = [
                root_wiki_page.id,
                wiki_page_1.id,
                wiki_page_2.id,
                wiki_page_3.id,
                wiki_page_4.id,
            ]
            await wiki_order_hint.store_async()
            print(wiki_order_hint)
            ```
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
            ```python
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
            ```
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

    @skip_async_to_sync
    @classmethod
    async def get_async(
        cls,
        owner_id: str = None,
        id: str = None,
        *,
        offset: int = 0,
        limit: int = 20,
        synapse_client: Optional["Synapse"] = None,
    ) -> AsyncGenerator["WikiHistorySnapshot", None]:
        """
        Get the history of a wiki page as a list of WikiHistorySnapshot objects.

        Arguments:
            owner_id: The Synapse ID of the owner entity.
            id: The ID of the wiki page.
            offset: The index of the pagination offset.
            limit: Limits the size of the page returned.
            synapse_client: Optionally provide a Synapse client.
        Yields:
            Individual WikiHistorySnapshot objects from each page of the response.

        Example: Get the history of a wiki page
            ```python
            async def main():
                async for item in WikiHistorySnapshot.get_async(owner_id=project.id, id=wiki_page.id):
                    print(f"History: {item}")
            asyncio.run(main())
            ```
        """
        if not owner_id:
            raise ValueError("Must provide owner_id to get wiki history.")
        if not id:
            raise ValueError("Must provide id to get wiki history.")
        async for item in get_wiki_history(
            owner_id=owner_id,
            wiki_id=id,  # use id instead of wiki_id to match other classes
            offset=offset,
            limit=limit,
            synapse_client=synapse_client,
        ):
            item = cls().fill_from_dict(wiki_history=item)
            yield item

    @classmethod
    def get(
        cls,
        owner_id: str = None,
        id: str = None,
        *,
        offset: int = 0,
        limit: int = 20,
        synapse_client: Optional["Synapse"] = None,
    ) -> Generator["WikiHistorySnapshot", None, None]:
        """
        Get the history of a wiki page as a list of WikiHistorySnapshot objects.

        Arguments:
            owner_id: The Synapse ID of the owner entity.
            id: The ID of the wiki page.
            offset: The index of the pagination offset.
            limit: Limits the size of the page returned.
            synapse_client: Optionally provide a Synapse client.
        Yields:
            Individual WikiHistorySnapshot objects from each page of the response.

        Example: Get the history of a wiki page
            ```python
            for history in WikiHistorySnapshot.get(owner_id=project.id, id=wiki_page.id):
                print(f"History: {history}")
            ```
        """
        return wrap_async_generator_to_sync_generator(
            async_gen_func=cls().get_async,
            owner_id=owner_id,
            id=id,
            offset=offset,
            limit=limit,
            synapse_client=synapse_client,
        )


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

    @skip_async_to_sync
    @classmethod
    async def get_async(
        cls,
        owner_id: str = None,
        *,
        offset: int = 0,
        limit: int = 20,
        synapse_client: Optional["Synapse"] = None,
    ) -> AsyncGenerator["WikiHeader", None]:
        """
        Get the header tree (hierarchy) of wiki pages for an entity.

        Arguments:
            owner_id: The Synapse ID of the owner entity.
            offset: The index of the pagination offset.
            limit: Limits the size of the page returned.
            synapse_client: Optionally provide a Synapse client.
        Yields:
            Individual WikiHeader objects for the entity.

        Example: Get the header tree (hierarchy) of wiki pages for an entity
            ```python
            async def main():
                async for item in WikiHeader.get_async(owner_id=project.id):
                    print(f"Header: {item}")
            asyncio.run(main())
            ```
        """
        if not owner_id:
            raise ValueError("Must provide owner_id to get wiki header tree.")
        async for item in get_wiki_header_tree(
            owner_id=owner_id,
            offset=offset,
            limit=limit,
            synapse_client=synapse_client,
        ):
            item = cls().fill_from_dict(wiki_header=item)
            yield item

    @classmethod
    def get(
        cls,
        owner_id: str,
        *,
        offset: int = 0,
        limit: int = 20,
        synapse_client: Optional["Synapse"] = None,
    ) -> Generator["WikiHeader", None, None]:
        """
        Get the header tree (hierarchy) of wiki pages for an entity.

        Arguments:
            owner_id: The ID of the owner entity.
            offset: The index of the pagination offset.
            limit: Limits the size of the page returned.
            synapse_client: Optionally provide a Synapse client.
        Yields:
            Individual WikiHeader objects for the entity.

        Example: Get the header tree (hierarchy) of wiki pages for an entity
            ```python
            for header in WikiHeader.get(owner_id=project.id):
                print(f"Header: {header}")
            ```
        """
        return wrap_async_generator_to_sync_generator(
            async_gen_func=cls().get_async,
            owner_id=owner_id,
            offset=offset,
            limit=limit,
            synapse_client=synapse_client,
        )


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
        if not isinstance(wiki_content, str):
            raise SyntaxError(f"Expected a string, got {type(wiki_content).__name__}")
        # Get the cache directory path to save the newly created gzipped file
        cache_dir = os.path.join(synapse_client.cache.cache_root_dir, "wiki_content")
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        if os.path.isfile(wiki_content):
            if wiki_content.endswith(".gz"):
                file_path = wiki_content
            else:
                # If it's a regular html or markdown file, compress it
                with open(wiki_content, "rb") as f_in:
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

        Example: Unzip a gzipped file
            ```python
            file_path = "path/to/file.md.gz"
            unzipped_file_path = WikiPage.unzip_gzipped_file(file_path)
            ```
        """
        with gzip.open(file_path, "rb") as f_in:
            unzipped_content_bytes = f_in.read()

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

            unzipped_file_path = os.path.join(
                os.path.dirname(file_path),
                os.path.basename(file_path).replace(".gz", ""),
            )
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

        Example: Reformat the attachment file name
            ```python
            attachment_file_name = "file.txt"
            attachment_file_name_reformatted = WikiPage.reformat_attachment_file_name(attachment_file_name)
            print(f"Reformatted attachment file name: {attachment_file_name_reformatted}")
            ```
        """
        attachment_file_name_reformatted = attachment_file_name.replace(".", "%2E")
        attachment_file_name_reformatted = attachment_file_name_reformatted.replace(
            "_", "%5F"
        )
        return attachment_file_name_reformatted

    @staticmethod
    def _should_gzip_file(file_path: str) -> bool:
        """Check if a file should be gzipped.

        Files that are already gzipped or are image files (png, jpg, jpeg) should not be gzipped.

        Arguments:
            file_path: The path or name of the file to check.
        Returns:
            True if the file should be gzipped, False otherwise.
        """
        return (
            not file_path.endswith(".gz")
            and not file_path.endswith(".png")
            and not file_path.endswith(".jpg")
            and not file_path.endswith(".jpeg")
        )

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
                    self.markdown_file_handle_id = file_handle.get("id")
            finally:
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
                if WikiPage._should_gzip_file(attachment):
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

            tasks = [
                asyncio.create_task(task_of_uploading_attachment(attachment))
                for attachment in self.attachments
            ]
            results = await asyncio.gather(*tasks)
            self.attachment_file_handle_ids = results
            return self

    async def _determine_wiki_action(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Literal[
        "create_root_wiki_page", "update_existing_wiki_page", "create_sub_wiki_page"
    ]:
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
            headers = WikiHeader.get_async(
                owner_id=self.owner_id,
                synapse_client=synapse_client,
            )
            await anext(headers)
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
            ```python
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
            ```
        """
        client = Synapse.get_client(synapse_client=synapse_client)

        wiki_action = await self._determine_wiki_action(synapse_client=client)
        # get the markdown file handle and attachment file handles if the wiki action is valid
        if wiki_action:
            # Update self with the returned WikiPage objects that have file handle IDs set
            self = await self._get_markdown_file_handle(synapse_client=client)
            self = await self._get_attachment_file_handles(synapse_client=client)

        if wiki_action == "create_root_wiki_page":
            client.logger.info(
                "No wiki page exists within the owner. Create a new wiki page."
            )
            wiki_data = await post_wiki_page(
                owner_id=self.owner_id,
                request=self.to_synapse_request(),
                synapse_client=client,
            )
            client.logger.info(
                f"Created wiki page: {wiki_data.get('title')} with ID: {wiki_data.get('id')}."
            )
        elif wiki_action == "update_existing_wiki_page":
            client.logger.info(
                "A wiki page already exists within the owner. Update the existing wiki page."
            )
            existing_wiki_dict = await get_wiki_page(
                owner_id=self.owner_id,
                wiki_id=self.id,
                wiki_version=self.wiki_version,
                synapse_client=client,
            )
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
            wiki_data = await put_wiki_page(
                owner_id=self.owner_id,
                wiki_id=self.id,
                request=updated_wiki.to_synapse_request(),
                synapse_client=client,
            )
            client.logger.info(
                f"Updated wiki page: {wiki_data.get('title')} with ID: {wiki_data.get('id')}."
            )

        else:
            client.logger.info(
                f"Creating sub-wiki page under parent ID: {self.parent_id}"
            )
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
            ```python
            wiki_page_restored = await WikiPage(owner_id=project.id, id=wiki_page.id, wiki_version="0").restore_async()
            print(wiki_page_restored)
            ```
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to restore a wiki page.")
        if not self.id:
            raise ValueError("Must provide id to restore a wiki page.")
        if not self.wiki_version:
            raise ValueError("Must provide wiki_version to restore a wiki page.")

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
            ```python
            wiki_page = await WikiPage(owner_id=project.id, id=wiki_page.id).get_async()
            print(wiki_page)
            ```
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get a wiki page.")
        if not self.id and not self.title:
            raise ValueError("Must provide id or title to get a wiki page.")

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
            ```python
            wiki_page = await WikiPage(owner_id=project.id, id=wiki_page.id).delete_async()
            print(f"Wiki page {wiki_page.title} deleted successfully.")
            ```
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
            ```python
            attachment_handles = await WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment_handles_async()
            print(f"Attachment handles: {attachment_handles['list']}")
            ```
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
            ```python
            attachment_file_or_url = await WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment_async(file_name="attachment.txt", download_file=False)
            print(f"Attachment URL: {attachment_file_or_url}")
            ```
        Example: Download the attachment file for a wiki page
            This example shows how to download the attachment file for a wiki page.
            ```python
            attachment_file_path = await WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment_async(file_name="attachment.txt", download_file=True, download_location="~/temp")
            print(f"Attachment file path: {attachment_file_path}")
            ```
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get attachment URL.")
        if not self.id:
            raise ValueError("Must provide id to get attachment URL.")
        if not file_name:
            raise ValueError("Must provide file_name to get attachment URL.")

        client = Synapse.get_client(synapse_client=synapse_client)

        if WikiPage._should_gzip_file(file_name):
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
            file_size = int(WikiPage._get_file_size(filehandle_dict, file_name))
            if file_size < SINGLE_THREAD_DOWNLOAD_SIZE_LIMIT:
                downloaded_file_path = download_from_url(
                    url=presigned_url_info.url,
                    destination=download_location,
                    url_is_presigned=True,
                    synapse_client=client,
                )
            else:
                downloaded_file_path = await download_from_url_multi_threaded(
                    presigned_url=presigned_url_info,
                    destination=download_location,
                    synapse_client=client,
                )
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
            ```python
            attachment_preview_url = await WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment_preview_async(file_name="attachment.txt.gz", download_file=False)
            print(f"Attachment preview URL: {attachment_preview_url}")
            ```
        Example: Download the attachment preview file for a wiki page
            This example shows how to download the attachment preview file for a wiki page.
            ```python
            attachment_preview_file_path = WikiPage(owner_id=project.id, id=wiki_page.id).get_attachment_preview(file_name="attachment.txt.gz", download_file=True, download_location="~/temp")
            print(f"Attachment preview file path: {attachment_preview_file_path}")
            ```
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
        if download_file:
            if not download_location:
                raise ValueError("Must provide download_location to download a file.")

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
            file_size = int(WikiPage._get_file_size(filehandle_dict, file_name))
            if file_size < SINGLE_THREAD_DOWNLOAD_SIZE_LIMIT:
                downloaded_file_path = download_from_url(
                    url=presigned_url_info.url,
                    destination=download_location,
                    url_is_presigned=True,
                    synapse_client=client,
                )
            else:
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
            ```python
            markdown_url = await WikiPage(owner_id=project.id, id=wiki_page.id).get_markdown_file_async(download_file=False)
            print(f"Markdown URL: {markdown_url}")
            ```
        Example: Download the markdown file for a wiki page
            This example shows how to download the markdown file for a wiki page.
            ```python
            markdown_file_path = await WikiPage(owner_id=project.id, id=wiki_page.id).get_markdown_file_async(download_file=True, download_location="~/temp")
            print(f"Markdown file path: {markdown_file_path}")
            ```
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
        if download_file:
            if not download_location:
                raise ValueError("Must provide download_location to download a file.")

            downloaded_file_path = download_from_url(
                url=markdown_url,
                destination=download_location,
                url_is_presigned=True,
                synapse_client=client,
            )
            unzipped_file_path = WikiPage.unzip_gzipped_file(downloaded_file_path)
            client.logger.info(
                f"Downloaded and unzipped the markdown file for wiki page {self.id} to {unzipped_file_path}."
            )
            os.remove(downloaded_file_path)
            client.logger.debug(f"Removed the gzipped file {downloaded_file_path}.")
            return unzipped_file_path
        else:
            return markdown_url

    async def _get_markdown_text(self, synapse_client: Synapse) -> str:
        """Download the markdown of this wiki page and return it as text.

        Arguments:
            synapse_client: The Synapse client to use for the download.

        Returns:
            The markdown content of this wiki page as a string.
        """
        markdown_url = await get_markdown_url(
            owner_id=self.owner_id,
            wiki_id=self.id,
            wiki_version=self.wiki_version,
            synapse_client=synapse_client,
        )
        cache_dir = os.path.join(synapse_client.cache.cache_root_dir, "wiki_content")
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)
        downloaded_file_path = download_from_url(
            url=markdown_url,
            destination=cache_dir,
            url_is_presigned=True,
            synapse_client=synapse_client,
        )
        try:
            with gzip.open(downloaded_file_path, "rt", encoding="utf-8") as f_in:
                return f_in.read()
        finally:
            os.remove(downloaded_file_path)

    async def _copy_attachment_file_handles(self, synapse_client: Synapse) -> list[str]:
        """Copy the attachment file handles of this wiki page.

        Arguments:
            synapse_client: The Synapse client to use for the copy.

        Returns:
            The IDs of the newly copied file handles.

        Raises:
            ValueError: If any file handle copy fails.
        """
        if not self.attachment_file_handle_ids:
            return []

        attachment_handles = await self.get_attachment_handles_async(
            synapse_client=synapse_client
        )
        # Get rid of the previews
        no_previews = [
            file_handle
            for file_handle in attachment_handles.get("list", [])
            if not file_handle.get("isPreview")
        ]
        if not no_previews:
            return []

        copy_requests = [
            {
                "originalFile": {
                    "fileHandleId": file_handle["id"],
                    "associateObjectId": self.id,
                    "associateObjectType": "WikiAttachment",
                },
                "newContentType": file_handle.get("contentType"),
                "newFileName": file_handle.get("fileName"),
            }
            for file_handle in no_previews
        ]
        copy_results = await post_file_handles_copy(
            copy_requests=copy_requests,
            synapse_client=synapse_client,
        )
        for copy_result in copy_results:
            if copy_result.get("failureCode") is not None:
                raise ValueError(
                    f"{copy_result['failureCode']} dataFileHandleId: "
                    f"{copy_result['originalFileHandleId']}"
                )
        return [copy_result["newFileHandle"]["id"] for copy_result in copy_results]

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Copy_Wiki: Owner ID {self.owner_id}, Wiki ID {self.id}"
    )
    async def copy_async(
        self,
        destination_owner_id: str,
        destination_sub_page_id: str | None = None,
        update_links: bool = True,
        entity_map: dict[str, str] | None = None,
        *,
        synapse_client: Synapse | None = None,
    ) -> list["WikiHeader"]:
        """Copy the wiki page tree of the owner entity to another entity and
        update internal links.

        If id is set on this WikiPage, only the sub-tree rooted at that wiki page
        is copied. Otherwise the entire wiki of the owner entity is copied.

        Arguments:
            destination_owner_id: The Synapse ID of the entity that the wiki
                will be copied to.
            destination_sub_page_id: Optional ID of a wiki page that already
                exists in the destination. The root of the copied tree is written
                into that page, replacing its title, markdown, and attachments,
                and the rest of the copied pages are created beneath it.
                Required when the destination entity already has a root wiki
                page.
            update_links: Update all the internal links so that they point at the
                copied wiki pages. For example, syn1234/wiki/34345 becomes
                syn3345/wiki/49508. Defaults to True.
            entity_map: A mapping of old Synapse IDs to new Synapse IDs, for
                example {"syn1234": "syn2345"}. If provided, the Synapse IDs
                referenced in the markdown of the copied wiki pages are updated,
                for example syn1234 becomes syn2345. If omitted, Synapse IDs
                are left unchanged.
            synapse_client: If not passed in and caching was not disabled by
                    Synapse.allow_client_caching(False) this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            A list of WikiHeader objects for the destination entity.

        Raises:
            ValueError: If owner_id is not provided or is not a Synapse ID,
                if destination_owner_id is not a Synapse ID, if a key or
                value of entity_map is not
                a Synapse ID, if id is set but no wiki page with that ID
                exists in the owner entity, if destination_sub_page_id does
                not exist in the destination entity, if the destination
                entity already has a root wiki page and
                destination_sub_page_id is not provided, or if copying an
                attachment file handle fails.

        Example: Copy the entire wiki of an entity to another entity
            This example shows how to copy all wiki pages from one project to another.
            ```python
            from synapseclient import Synapse
            from synapseclient.models import WikiPage

            syn = Synapse()
            syn.login()

            new_wiki_headers = await WikiPage(owner_id="syn123").copy_async(
                destination_owner_id="syn456"
            )
            print(new_wiki_headers)
            ```
        Example: Copy a wiki sub-tree and update Synapse ID references
            This example shows how to copy a specific wiki page and its sub-pages,
            rewriting references to syn1234 so they point at syn2345.
            ```python
            new_wiki_headers = await WikiPage(owner_id="syn123", id="34345").copy_async(
                destination_owner_id="syn456",
                entity_map={"syn1234": "syn2345"},
            )
            print(new_wiki_headers)
            ```
        """
        entity_sub_page_id, destination_sub_page_id = _validate_and_format_copy_inputs(
            owner_id=self.owner_id,
            destination_owner_id=destination_owner_id,
            entity_sub_page_id=self.id,
            destination_sub_page_id=destination_sub_page_id,
            entity_map=entity_map,
        )

        client = Synapse.get_client(synapse_client=synapse_client)

        # Getting the wiki header tree fails when there is no wiki
        old_wiki_headers = []
        try:
            async for item in get_wiki_header_tree(
                owner_id=self.owner_id,
                synapse_client=client,
            ):
                old_wiki_headers.append(item)
        except SynapseHTTPError as e:
            if e.response.status_code == 404:
                if entity_sub_page_id:
                    raise ValueError(
                        f"The wiki page {entity_sub_page_id} does not exist "
                        f"in the owner entity {self.owner_id}."
                    ) from e
                return []
            raise

        if destination_sub_page_id:
            destination_wiki_page = await _get_existing_destination_wiki_page(
                destination_owner_id=destination_owner_id,
                destination_sub_page_id=destination_sub_page_id,
                synapse_client=client,
            )
        else:
            await _ensure_destination_has_no_root_wiki(
                destination_owner_id=destination_owner_id,
                synapse_client=client,
            )
            destination_wiki_page = None

        if entity_sub_page_id:
            old_wiki_headers = _collect_wiki_sub_tree_headers(
                wiki_headers=old_wiki_headers,
                sub_page_id=entity_sub_page_id,
            )
            if not old_wiki_headers:
                raise ValueError(
                    f"The wiki page {entity_sub_page_id} does not exist "
                    f"in the owner entity {self.owner_id}."
                )

        if not old_wiki_headers:
            return []

        new_wikis, wiki_id_map = await _copy_wiki_pages(
            old_wiki_headers=old_wiki_headers,
            source_owner_id=self.owner_id,
            destination_owner_id=destination_owner_id,
            destination_wiki_page=destination_wiki_page,
            destination_sub_page_id=destination_sub_page_id,
            synapse_client=client,
        )

        if update_links:
            client.logger.info("Updating internal links")
            new_wikis = _update_internal_links(
                new_wikis=new_wikis,
                wiki_id_map=wiki_id_map,
                source_owner_id=self.owner_id,
                destination_owner_id=destination_owner_id,
            )
            client.logger.info("Done updating internal links.")

        if entity_map:
            client.logger.info("Updating Synapse references")
            new_wikis = _update_synapse_id_references(
                new_wikis=new_wikis,
                wiki_id_map=wiki_id_map,
                entity_map=entity_map,
            )
            client.logger.info("Done updating Synapse IDs.")

        if update_links or entity_map:
            client.logger.info("Storing new wiki pages")
            for new_wiki_id in wiki_id_map.values():
                new_wiki = new_wikis[new_wiki_id]
                new_wiki = await new_wiki._get_markdown_file_handle(
                    synapse_client=client
                )
                wiki_data = await put_wiki_page(
                    owner_id=destination_owner_id,
                    wiki_id=new_wiki_id,
                    request=new_wiki.to_synapse_request(),
                    synapse_client=client,
                )
                new_wikis[new_wiki_id] = new_wiki.fill_from_dict(synapse_wiki=wiki_data)
                client.logger.info(f"Stored wiki page {new_wiki_id}")

        new_wiki_headers = []
        async for item in get_wiki_header_tree(
            owner_id=destination_owner_id,
            synapse_client=client,
        ):
            new_wiki_headers.append(WikiHeader().fill_from_dict(wiki_header=item))
        return new_wiki_headers


async def _copy_wiki_pages(
    old_wiki_headers: list[dict[str, str]],
    source_owner_id: str,
    destination_owner_id: str,
    destination_wiki_page: Optional["WikiPage"],
    destination_sub_page_id: str | None,
    synapse_client: Synapse,
) -> tuple[dict[str, "WikiPage"], dict[str, str]]:
    """Create a copy of each source wiki page in the destination entity.

    Iterates over the source wiki headers in order, creating a corresponding
    wiki page in the destination for each one and copying its markdown and
    attachment file handles. The header list is expected to be ordered so that
    a page appears before any of its children, since each child is created with
    a parent_id looked up from the pages copied earlier.

    A page with a parentId is created beneath its already-copied parent. The
    root of the copied tree is written into destination_wiki_page when one was
    provided, otherwise it is created as a new page under destination_sub_page_id
    (which may be None to create a root page).

    Arguments:
        old_wiki_headers: The source wiki headers to copy, ordered parent before
            child.
        source_owner_id: The Synapse ID of the entity whose wiki is copied.
        destination_owner_id: The Synapse ID of the entity the wiki is copied to.
        destination_wiki_page: An existing destination wiki page that the root of
            the copied tree is written into, or None to create a new root page.
        destination_sub_page_id: The ID of the destination page the copied root
            is created beneath when destination_wiki_page is None, or None to
            create a root page.
        synapse_client: The Synapse client to use for the copy.

    Returns:
        A tuple of the copied wiki pages keyed by their new wiki ID and a
        mapping of old wiki IDs to new wiki IDs.
    """
    wiki_id_map = {}
    new_wikis: dict[str, WikiPage] = {}
    for wiki_header in old_wiki_headers:
        old_wiki = await WikiPage(
            owner_id=source_owner_id, id=wiki_header["id"]
        ).get_async(synapse_client=synapse_client)
        synapse_client.logger.info(f"Got wiki {wiki_header['id']}")
        markdown = await old_wiki._get_markdown_text(synapse_client=synapse_client)
        new_file_handle_ids = await old_wiki._copy_attachment_file_handles(
            synapse_client=synapse_client
        )

        if wiki_header.get("parentId"):
            new_wiki = WikiPage(
                owner_id=destination_owner_id,
                title=old_wiki.title or "",
                markdown=markdown,
                parent_id=wiki_id_map[wiki_header["parentId"]],
                attachment_file_handle_ids=new_file_handle_ids,
            )
            new_wiki = await new_wiki._get_markdown_file_handle(
                synapse_client=synapse_client
            )
            wiki_data = await post_wiki_page(
                owner_id=destination_owner_id,
                request=new_wiki.to_synapse_request(),
                synapse_client=synapse_client,
            )
            new_wiki.fill_from_dict(synapse_wiki=wiki_data)
        elif destination_wiki_page is not None:
            # Write the root of the copied tree into the existing
            # destination wiki page
            destination_wiki_page.title = old_wiki.title or ""
            destination_wiki_page.markdown = markdown
            destination_wiki_page.attachment_file_handle_ids = new_file_handle_ids
            destination_wiki_page = (
                await destination_wiki_page._get_markdown_file_handle(
                    synapse_client=synapse_client
                )
            )
            wiki_data = await put_wiki_page(
                owner_id=destination_owner_id,
                wiki_id=destination_wiki_page.id,
                request=destination_wiki_page.to_synapse_request(),
                synapse_client=synapse_client,
            )
            new_wiki = destination_wiki_page.fill_from_dict(synapse_wiki=wiki_data)
        else:
            new_wiki = WikiPage(
                owner_id=destination_owner_id,
                title=old_wiki.title or "",
                markdown=markdown,
                parent_id=destination_sub_page_id,
                attachment_file_handle_ids=new_file_handle_ids,
            )
            new_wiki = await new_wiki._get_markdown_file_handle(
                synapse_client=synapse_client
            )
            wiki_data = await post_wiki_page(
                owner_id=destination_owner_id,
                request=new_wiki.to_synapse_request(),
                synapse_client=synapse_client,
            )
            new_wiki.fill_from_dict(synapse_wiki=wiki_data)

        new_wikis[new_wiki.id] = new_wiki
        wiki_id_map[old_wiki.id] = new_wiki.id

    return new_wikis, wiki_id_map


def _coerce_sub_page_id(
    sub_page_id: str | int | float | None, argument_description: str
) -> str | None:
    """Coerce a wiki sub page ID to an integer string.

    Rejects values that a plain int() conversion would silently mangle:
    fractional floats truncate, and bools and negative numbers convert to
    nonsense page IDs. Integer-valued floats such as 4.0 are accepted since
    they convert losslessly.

    Arguments:
        sub_page_id: The wiki page ID to coerce.
        argument_description: How to refer to the argument in error messages.

    Returns:
        The ID coerced to an integer string, or None if not provided.

    Raises:
        ValueError: If the ID is not a non-negative whole number or numeric
            string.
    """
    if sub_page_id is None:
        return None
    if isinstance(sub_page_id, float) and sub_page_id.is_integer():
        sub_page_id = int(sub_page_id)
    if isinstance(sub_page_id, bool) or not str(sub_page_id).isdecimal():
        raise ValueError(
            f"{argument_description} must be a numeric wiki page ID or None, "
            f"got {sub_page_id}."
        )
    return str(int(sub_page_id))


def _validate_and_format_copy_inputs(
    owner_id: str | None,
    destination_owner_id: str,
    entity_sub_page_id: str | int | float | None,
    destination_sub_page_id: str | int | float | None,
    entity_map: dict[str, str] | None = None,
) -> tuple[str | None, str | None]:
    """Validate the inputs of a wiki copy and coerce the sub page IDs.

    Arguments:
        owner_id: The Synapse ID of the entity whose wiki is copied.
        destination_owner_id: The Synapse ID of the entity that the wiki is
            copied to.
        entity_sub_page_id: Optional ID of the wiki page that is the root of
            the sub-tree to copy.
        destination_sub_page_id: Optional ID of a wiki page that already
            exists in the destination.
        entity_map: Optional mapping of old Synapse IDs to new Synapse IDs.

    Returns:
        A tuple of entity_sub_page_id and destination_sub_page_id, each
        coerced to an integer string, or None if not provided.

    Raises:
        ValueError: If owner_id is not provided or is not a Synapse ID, if
            destination_owner_id is not a Synapse ID, if a key or value of
            entity_map is not a Synapse ID, or if a sub page ID is not
            numeric.
    """
    if not owner_id:
        raise ValueError("Must provide owner_id to copy a wiki.")
    if not is_synapse_id_str(owner_id):
        raise ValueError(
            f"Wiki owner_id must be a Synapse ID such as syn123, got {owner_id}."
        )

    if not is_synapse_id_str(destination_owner_id):
        raise ValueError(
            "destination_owner_id must be a Synapse ID such as syn123, "
            f"got {destination_owner_id}."
        )

    for old_synapse_id, new_synapse_id in (entity_map or {}).items():
        if not is_synapse_id_str(old_synapse_id):
            raise ValueError(
                "entity_map keys must be Synapse IDs such as syn123, "
                f"got {old_synapse_id}."
            )
        if not is_synapse_id_str(new_synapse_id):
            raise ValueError(
                "entity_map values must be Synapse IDs such as syn123, "
                f"got {new_synapse_id}."
            )

    destination_sub_page_id = _coerce_sub_page_id(
        sub_page_id=destination_sub_page_id,
        argument_description="destination_sub_page_id",
    )
    entity_sub_page_id = _coerce_sub_page_id(
        sub_page_id=entity_sub_page_id,
        argument_description="The id of the WikiPage",
    )

    return (entity_sub_page_id, destination_sub_page_id)


async def _ensure_destination_has_no_root_wiki(
    destination_owner_id: str,
    synapse_client: Synapse,
) -> None:
    """Verify that a copied wiki tree can become the root wiki of the destination.

    A Synapse entity can have at most one root wiki page, so copying a wiki
    without a destination_sub_page_id is only possible when the destination
    has no wiki yet. Checking upfront fails fast before any attachment file
    handles are copied.

    Arguments:
        destination_owner_id: The Synapse ID of the entity the wiki is copied to.
        synapse_client: The Synapse client to use for the lookup.

    Raises:
        ValueError: If the destination entity already has a root wiki page.
    """
    root_wiki_data = await _fetch_wiki_page_data(
        owner_id=destination_owner_id,
        wiki_id=None,
        synapse_client=synapse_client,
    )
    if root_wiki_data is not None:
        raise ValueError(
            f"The destination entity {destination_owner_id} already has a "
            "root wiki page. Provide destination_sub_page_id to copy the "
            "wiki beneath one of its existing pages."
        )


async def _get_existing_destination_wiki_page(
    destination_owner_id: str,
    destination_sub_page_id: str,
    synapse_client: Synapse,
) -> "WikiPage":
    """Fetch the destination wiki page that a copied wiki tree is written into.

    Arguments:
        destination_owner_id: The Synapse ID of the entity the wiki is copied to.
        destination_sub_page_id: The ID of a wiki page that already exists in
            the destination.
        synapse_client: The Synapse client to use for the lookup.

    Returns:
        The destination wiki page.

    Raises:
        ValueError: If no wiki page with the given ID exists in the destination.
    """
    destination_wiki_data = await _fetch_wiki_page_data(
        owner_id=destination_owner_id,
        wiki_id=destination_sub_page_id,
        synapse_client=synapse_client,
    )
    if destination_wiki_data is None:
        raise ValueError(
            f"The destination_sub_page_id {destination_sub_page_id} does not "
            f"exist in the destination entity {destination_owner_id}."
        )
    destination_wiki_page = WikiPage().fill_from_dict(
        synapse_wiki=destination_wiki_data
    )
    destination_wiki_page.owner_id = destination_owner_id
    return destination_wiki_page


async def _fetch_wiki_page_data(
    owner_id: str,
    wiki_id: str | None,
    synapse_client: Synapse,
) -> dict | None:
    """Fetch a wiki page, returning None instead of raising if it does not exist.

    Arguments:
        owner_id: The Synapse ID of the entity that owns the wiki.
        wiki_id: The ID of the wiki page to fetch. If None, the root wiki page
            of the entity is fetched.
        synapse_client: The Synapse client to use for the lookup.

    Returns:
        The wiki page data, or None if the page does not exist.
    """
    try:
        return await get_wiki_page(
            owner_id=owner_id,
            wiki_id=wiki_id,
            synapse_client=synapse_client,
        )
    except SynapseHTTPError as e:
        if e.response.status_code == 404:
            return None
        raise


def _collect_wiki_sub_tree_headers(
    wiki_headers: list[dict[str, str]],
    sub_page_id: str,
    collected_headers: list[dict[str, str]] | None = None,
) -> list[dict[str, str]] | None:
    """Collect the wiki header for sub_page_id and all of its descendants.

    Synapse returns a whole wiki as one flat list of headers, where each header
    only records its parent. This function picks out the requested page, then
    every page underneath it by recursing on each child it finds, and returns
    that branch as a list with each parent appearing before its children. All
    other pages are ignored. The requested page has its parentId removed since
    it becomes the root of the copied tree.

    Arguments:
        wiki_headers: The flat list of all wiki headers for the owner entity.
        sub_page_id: The ID of the wiki page that is the root of the sub-tree.
        collected_headers: Used internally to accumulate matches during recursion.

    Returns:
        The wiki headers for the sub-tree rooted at sub_page_id. The root header
        has its parentId removed so it is treated as a root page when copied.
    """
    sub_page_id = str(sub_page_id)
    for wiki_header in wiki_headers:
        if wiki_header["id"] == sub_page_id:
            if collected_headers is None:
                # The root of the sub-tree is treated as a root page (no parent)
                root_header = {
                    key: value
                    for key, value in wiki_header.items()
                    if key != "parentId"
                }
                collected_headers = [root_header]
            else:
                collected_headers.append(wiki_header)
        elif wiki_header.get("parentId") == sub_page_id:
            collected_headers = _collect_wiki_sub_tree_headers(
                wiki_headers=wiki_headers,
                sub_page_id=wiki_header["id"],
                collected_headers=collected_headers,
            )
    return collected_headers


def _update_internal_links(
    new_wikis: dict[str, "WikiPage"],
    wiki_id_map: dict[str, str],
    source_owner_id: str,
    destination_owner_id: str,
) -> dict[str, "WikiPage"]:
    """Rewrite internal wiki links in the markdown of copied wiki pages.

    Copied markdown still contains links to the source wiki, written as paths
    like source_owner_id/wiki/old_wiki_id. Since the copied pages have new IDs,
    this function must run after all pages are copied, when the complete
    old-to-new ID mapping is known. It replaces each such path with
    destination_owner_id/wiki/new_wiki_id, then replaces any remaining references
    to the source owner ID with the destination owner ID. Only the in-memory
    WikiPage objects are modified; the caller is responsible for storing the
    updated markdown.

    Arguments:
        new_wikis: The copied wiki pages keyed by their new wiki ID.
        wiki_id_map: A mapping of old wiki IDs to new wiki IDs.
        source_owner_id: The Synapse ID of the entity the wiki was copied from.
        destination_owner_id: The Synapse ID of the entity the wiki was copied to.

    Returns:
        The copied wiki pages with updated markdown.
    """
    for new_wiki_id in wiki_id_map.values():
        markdown = new_wikis[new_wiki_id].markdown or ""
        for old_wiki_id, mapped_wiki_id in wiki_id_map.items():
            old_reference = f"{source_owner_id}/wiki/{old_wiki_id}\\b"
            new_reference = f"{destination_owner_id}/wiki/{mapped_wiki_id}"
            markdown = re.sub(old_reference, new_reference, markdown)
        markdown = re.sub(source_owner_id + "\\b", destination_owner_id, markdown)
        new_wikis[new_wiki_id].markdown = markdown
    return new_wikis


def _update_synapse_id_references(
    new_wikis: dict[str, "WikiPage"],
    wiki_id_map: dict[str, str],
    entity_map: dict[str, str],
) -> dict[str, "WikiPage"]:
    """Rewrite Synapse ID references in the markdown of copied wiki pages.

    Wiki markdown often mentions other entities (files, folders, tables) by
    their Synapse IDs. When those entities were also copied, the copied
    markdown still points at the originals. This function replaces each old
    entity ID from entity_map with its new counterpart, wherever it appears
    in the markdown of the copied pages. A trailing word boundary in the
    pattern prevents a shorter ID from matching inside a longer one. While
    _update_internal_links handles the wiki's links to its own pages, this
    function handles references to everything else that was copied. Only the
    in-memory WikiPage objects are modified; the caller is responsible for
    storing the updated markdown.

    Arguments:
        new_wikis: The copied wiki pages keyed by their new wiki ID.
        wiki_id_map: A mapping of old wiki IDs to new wiki IDs.
        entity_map: A mapping of old Synapse IDs to new Synapse IDs.

    Returns:
        The copied wiki pages with updated markdown.
    """
    for new_wiki_id in wiki_id_map.values():
        markdown = new_wikis[new_wiki_id].markdown or ""
        for old_synapse_id, new_synapse_id in entity_map.items():
            markdown = re.sub(old_synapse_id + "\\b", new_synapse_id, markdown)
        new_wikis[new_wiki_id].markdown = markdown
    return new_wikis
