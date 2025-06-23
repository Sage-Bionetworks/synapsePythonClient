"""Script to work with Synapse wiki pages."""

import gzip
import os
import shutil
import uuid
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional, Union

from synapseclient import Synapse
from synapseclient.api.wiki_service import (
    delete_wiki_page,
    get_attachment_handles,
    get_attachment_preview_url,
    get_attachment_url,
    get_markdown_url,
    get_wiki_header_tree,
    get_wiki_history,
    get_wiki_order_hint,
    get_wiki_page,
    post_wiki,
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
from synapseclient.core.utils import delete_none_keys
from synapseclient.models.protocols.wikipage_protocol import (
    WikiHeaderSynchronousProtocol,
    WikiHistorySnapshotSynchronousProtocol,
    WikiOrderHintSynchronousProtocol,
    WikiPageSynchronousProtocol,
)


@dataclass
@async_to_sync
class WikiOrderHint(WikiOrderHintSynchronousProtocol):
    """
    A WikiOrderHint contains the order hint for the root wiki that corresponds to the given owner ID and type.

    Attributes:
        owner_id: The ID of the owner object (e.g., entity, evaluation, etc.).
        owner_object_type: The type of the owner object.
        id_list: The list of sub wiki ids that in the order that they should be placed relative to their siblings.
        etag: The etag of this object.
    """

    owner_id: Optional[str] = None
    """The ID of the owner object (e.g., entity, evaluation, etc.)."""

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
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get wiki order hint.")
        order_hint_dict = await get_wiki_order_hint(
            owner_id=self.owner_id,
            synapse_client=synapse_client,
        )
        return self.fill_from_dict(order_hint_dict)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Update_Wiki_Order_Hint: {self.owner_id}"
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
        owner_id: str,
        id: str,
        *,
        offset: int = 0,
        limit: int = 20,
        synapse_client: Optional["Synapse"] = None,
    ) -> List["WikiHistorySnapshot"]:
        """
        Get the history of a wiki page as a list of WikiHistorySnapshot objects.

        Arguments:
            owner_id: The ID of the owner entity.
            id: The ID of the wiki page.
            offset: The index of the pagination offset.
            limit: Limits the size of the page returned.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            A list of WikiHistorySnapshot objects for the wiki page.
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
        owner_id: str,
        *,
        offset: int = 0,
        limit: int = 20,
        synapse_client: Optional["Synapse"] = None,
    ) -> List["WikiHeader"]:
        """
        Get the header tree (hierarchy) of wiki pages for an entity.

        Arguments:
            owner_id: The ID of the owner entity.
            offset: The index of the pagination offset.
            limit: Limits the size of the page returned.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            A list of WikiHeader objects for the entity.
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
        owner_id: The ID of the owning object (e.g., entity, evaluation, etc.).
        created_on: The timestamp when this page was created.
        created_by: The ID of the user that created this page.
        modified_on: The timestamp when this page was last modified.
        modified_by: The ID of the user that last modified this page.
        version_number: The version number of this wiki page.
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
    """A list of file attachments associated with this page."""

    owner_id: Optional[str] = None
    """The ID of the owning object (e.g., entity, evaluation, etc.)."""

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
        self.markdown = synapse_wiki.get("markdown", None)
        self.attachments = synapse_wiki.get("attachments", [])
        self.owner_id = synapse_wiki.get("ownerId", None)
        self.created_on = synapse_wiki.get("createdOn", None)
        self.created_by = synapse_wiki.get("createdBy", None)
        self.modified_on = synapse_wiki.get("modifiedOn", None)
        self.modified_by = synapse_wiki.get("modifiedBy", None)
        self.wiki_version = synapse_wiki.get("wikiVersion", None)
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
            "ownerId": self.owner_id,
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
            The path to the gzipped file.
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
            # If it's already a gzipped file, save a copy to the cache
            if wiki_content.endswith(".gz"):
                file_path = os.path.join(cache_dir, os.path.basename(wiki_content))
                shutil.copyfile(wiki_content, file_path)
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
            file_path = os.path.join(cache_dir, f"wiki_markdown_{uuid.uuid4()}.md.gz")
            with gzip.open(file_path, "wt", encoding="utf-8") as f_out:
                f_out.write(wiki_content)

        return file_path

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

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            The created wiki page.

        Raises:
            ValueError: If owner_id is not provided or if required fields are missing.
        """
        client = Synapse.get_client(synapse_client=synapse_client)
        if not self.owner_id:
            raise ValueError("Must provide owner_id to modify a wiki page.")
        # Convert markdown to gzipped file if needed
        if self.markdown:
            file_path = self._to_gzip_file(
                wiki_content=self.markdown, synapse_client=client
            )
            # Upload the gzipped file to get a file handle
            file_handle = await upload_file_handle(
                syn=client,
                parent_entity_id=self.owner_id,
                path=file_path,
            )

            client.logger.info(
                f"Uploaded file handle {file_handle.get('id')} for wiki page markdown."
            )
            # delete the temp gzip file
            os.remove(file_path)
            client.logger.debug(f"Deleted temp gzip file {file_path}")

            # Set the markdown file handle ID from the upload response
            self.markdown_file_handle_id = file_handle.get("id")

        # Convert attachments to gzipped file if needed
        if self.attachments:
            file_handles = []
            for attachment in self.attachments:
                file_path = self._to_gzip_file(
                    wiki_content=attachment, synapse_client=client
                )
                file_handle = await upload_file_handle(
                    syn=client,
                    parent_entity_id=self.owner_id,
                    path=file_path,
                )
                file_handles.append(file_handle.get("id"))
                client.logger.info(
                    f"Uploaded file handle {file_handle.get('id')} for wiki page attachment."
                )
                # delete the temp gzip file
                os.remove(file_path)
                client.logger.debug(f"Deleted temp gzip file {file_path}")
            # Set the attachment file handle IDs from the upload response
            self.attachment_file_handle_ids = file_handles

        # Handle root wiki page creation if parent_id is not given
        if not self.parent_id:
            try:
                WikiHeader.get(
                    owner_id=self.owner_id,
                )
            except SynapseHTTPError as e:
                if e.response.status_code == 404:
                    client.logger.debug(
                        "No wiki page exists within the owner. Create a new wiki page."
                    )
                    # Create the wiki page
                    wiki_data = await post_wiki(
                        owner_id=self.owner_id,
                        request=self.to_synapse_request(),
                    )
                    client.logger.info(
                        f"Created wiki page: {wiki_data.get('title')} with ID: {wiki_data.get('id')}."
                    )
                else:
                    raise e
            else:
                client.logger.info(
                    "A wiki page already exists within the owner. Update the existing wiki page."
                )
                # Update the existing wiki page
                if not (self.id):
                    raise ValueError("Must provide id to update a wiki page.")

                # retrieve the wiki page
                existing_wiki = await get_wiki_page(
                    owner_id=self.owner_id,
                    wiki_id=self.id,
                    wiki_version=self.wiki_version,
                )
                # Update existing_wiki with current object's attributes if they are not None
                updates = {
                    k: v
                    for k, v in {
                        "id": self.id,
                        "title": self.title,
                        "markdown": self.markdown,
                        "parentWikiId": self.parent_id,
                        "attachments": self.attachments,
                        "markdownFileHandleId": self.markdown_file_handle_id,
                        "attachmentFileHandleIds": self.attachment_file_handle_ids,
                    }.items()
                    if v is not None
                }
                existing_wiki.update(updates)
                # update the wiki page
                wiki_data = await put_wiki_page(
                    owner_id=self.owner_id,
                    wiki_id=self.id,
                    request=existing_wiki,
                )
                client.logger.info(
                    f"Updated wiki page: {wiki_data.get('title')} with ID: {wiki_data.get('id')}."
                )

        # Handle sub-wiki page creation if parent_id is given
        else:
            client.logger.info(
                f"Creating sub-wiki page under parent ID: {self.parent_id}"
            )
            # Create the sub-wiki page directly
            wiki_data = await post_wiki(
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

        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get a wiki page.")

        # If we have an ID, use it directly (TO SIMPLIFY)
        elif self.id:
            wiki_data = await get_wiki_page(
                owner_id=self.owner_id,
                wiki_id=self.id,
                wiki_version=self.wiki_version,
                synapse_client=synapse_client,
            )
        # If we only have a title, find the wiki page with matching title
        else:
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

            wiki_data = await get_wiki_page(
                owner_id=self.owner_id,
                wiki_id=matching_header["id"],
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
    ) -> list:
        """
        Get the file handles of all attachments on this wiki page.

        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The list of FileHandles for all file attachments of this WikiPage.
        Raises:
            ValueError: If owner_id or id is not provided.
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
        file_name: str,
        *,
        download_file: bool = True,
        download_location: Optional[str] = None,
        redirect: Optional[bool] = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> Union[str, None]:
        """
        Download the wiki page attachment to a local file or return the URL.

        Arguments:
            file_name: The name of the file to get.
            download_file: Whether associated files should be downloaded. Default is True.
            download_location: The directory to download the file to. Required if download_file is True.
            redirect: When set to false, the URL will be returned as text/plain instead of redirecting. Default is False.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            If download_file is True, the attachment file will be downloaded to the download_location. Otherwise, the URL will be returned.
        Raises:
            ValueError: If owner_id or id is not provided.
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get attachment URL.")
        if not self.id:
            raise ValueError("Must provide id to get attachment URL.")
        if not file_name:
            raise ValueError("Must provide file_name to get attachment URL.")

        client = Synapse.get_client(synapse_client=synapse_client)
        attachment_url = await get_attachment_url(
            owner_id=self.owner_id,
            wiki_id=self.id,
            file_name=file_name,
            wiki_version=self.wiki_version,
            redirect=redirect,
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
            if file_size < 8388608:
                download_from_url(
                    url=presigned_url_info.url,
                    destination=download_location,
                    url_is_presigned=True,
                )
            else:
                # download the file
                download_from_url_multi_threaded(
                    presigned_url=presigned_url_info.url, destination=download_location
                )
            client.logger.debug(
                f"Downloaded file {presigned_url_info.file_name} to {download_location}"
            )
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
        redirect: Optional[bool] = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> Union[str, None]:
        """
        Download the wiki page attachment preview to a local file or return the URL.

        Arguments:
            file_name: The name of the file to get.
            download_file: Whether associated files should be downloaded. Default is True.
            download_location: The directory to download the file to. Required if download_file is True.
            redirect: When set to false, the URL will be returned as text/plain instead of redirecting. Default is False.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            If download_file is True, the attachment preview file will be downloaded to the download_location. Otherwise, the URL will be returned.
        Raises:
            ValueError: If owner_id or id is not provided.
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get attachment preview URL.")
        if not self.id:
            raise ValueError("Must provide id to get attachment preview URL.")
        if not file_name:
            raise ValueError("Must provide file_name to get attachment preview URL.")

        client = Synapse.get_client(synapse_client=synapse_client)
        attachment_preview_url = await get_attachment_preview_url(
            owner_id=self.owner_id,
            wiki_id=self.id,
            file_name=file_name,
            wiki_version=self.wiki_version,
            redirect=redirect,
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
            if file_size < 8388608:
                download_from_url(
                    url=presigned_url_info.url,
                    destination=download_location,
                    url_is_presigned=True,
                )
            else:
                # download the file
                download_from_url_multi_threaded(
                    presigned_url=presigned_url_info.url, destination=download_location
                )
                client.logger.debug(
                    f"Downloaded the preview file {presigned_url_info.file_name} to {download_location}"
                )
        else:
            return attachment_preview_url

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Markdown_URL: Owner ID {self.owner_id}, Wiki ID {self.id}, Wiki Version {self.wiki_version}"
    )
    async def get_markdown_async(
        self,
        *,
        download_file_name: Optional[str] = None,
        download_file: bool = True,
        download_location: Optional[str] = None,
        redirect: Optional[bool] = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> Union[str, None]:
        """
        Get the markdown URL of this wiki page.

        Arguments:
            download_file: Whether associated files should be downloaded. Default is True.
            download_location: The directory to download the file to. Required if download_file is True.
            redirect: When set to false, the URL will be returned as text/plain instead of redirecting. Default is False.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            If download_file is True, the markdown file will be downloaded to the download_location. Otherwise, the URL will be returned.
        Raises:
            ValueError: If owner_id or id is not provided.
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
            redirect=redirect,
            synapse_client=client,
        )
        # download the file if download_file is True
        if download_file:
            if not download_location:
                raise ValueError("Must provide download_location to download a file.")
            if not download_file_name:
                raise ValueError("Must provide download_file_name to download a file.")

            # construct PresignedUrlInfo for downloading
            presigned_url_info = PresignedUrlInfo(
                url=markdown_url,
                file_name=download_file_name,
                expiration_utc=_pre_signed_url_expiration_time(markdown_url),
            )
            download_from_url(
                url=presigned_url_info.url,
                destination=download_location,
                url_is_presigned=True,
            )
            client.logger.debug(
                f"Downloaded file {presigned_url_info.file_name} to {download_location}"
            )
        else:
            return markdown_url
