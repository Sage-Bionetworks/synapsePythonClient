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
    async def update_async(
        self,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> "WikiOrderHint":
        """
        Update the order hint of a wiki page tree.

        Arguments:
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The updated WikiOrderHint object for the entity.
        Raises:
            ValueError: If owner_id or request is not provided.
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to update wiki order hint.")

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
        method_to_trace_name=lambda self, **kwargs: f"Get_Wiki_History for Owner ID {kwargs['owner_id']}, Wiki ID {kwargs['wiki_id']}"
    )
    async def get_async(
        cls,
        owner_id: str,
        wiki_id: str,
        *,
        offset: int = 0,
        limit: int = 20,
        synapse_client: Optional["Synapse"] = None,
    ) -> list:
        """
        Get the history of a wiki page as a list of WikiHistorySnapshot objects.

        Arguments:
            owner_id: The ID of the owner entity.
            wiki_id: The ID of the wiki page.
            offset: The index of the pagination offset.
            limit: Limits the size of the page returned.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            A list of WikiHistorySnapshot objects for the wiki page.
        """
        if not owner_id:
            raise ValueError("Must provide owner_id to get wiki history.")
        if not wiki_id:
            raise ValueError("Must provide wiki_id to get wiki history.")
        snapshots = []
        async for item in get_wiki_history(
            owner_id=owner_id,
            wiki_id=wiki_id,
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
    ) -> list:
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

    wiki_version: Optional[int] = None
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
        entity = {
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
        delete_none_keys(entity)
        result = {
            "entity": entity,
        }
        delete_none_keys(result)
        return result

    def _markdown_to_gzip_file(
        self,
        markdown: str,
        synapse_client: Optional[Synapse] = None,
    ) -> str:
        """Convert markdown to a gzipped file and save it in the synapse cache to get a file handle id later.

        Arguments:
            markdown: The markdown content as plain text, basic HTML, or Markdown, or a file path to such content.
            synapse_client: The Synapse client to use for cache access.

        Returns:
            The path to the gzipped file.
        """
        if not isinstance(markdown, str):
            raise TypeError(
                f"Expected markdownto be a str, got {type(markdown).__name__}"
            )

        client = Synapse.get_client(synapse_client=synapse_client)

        # Get the cache directory path to save the newly created gzipped file
        cache_dir = os.path.join(client.cache.cache_root_dir, "wiki_markdown")
        if not os.path.exists(cache_dir):
            os.makedirs(cache_dir)

        # Check if markdown looks like a file path and exists
        if os.path.isfile(markdown):
            # If it's already a gzipped file, save a copy to the cache
            if markdown.endswith(".gz"):
                file_path = os.path.join(cache_dir, os.path.basename(markdown))
                shutil.copyfile(markdown, file_path)
            else:
                # If it's a regular html or markdown file, compress it
                with open(markdown, "rb") as f_in:
                    # Open the output gzip file
                    file_path = os.path.join(
                        cache_dir, os.path.basename(markdown) + ".gz"
                    )
                    with gzip.open(file_path, "wb") as f_out:
                        f_out.writelines(f_in)

        else:
            # If it's a plain text, write it to a gzipped file and save it in the synapse cache
            file_path = os.path.join(cache_dir, f"wiki_markdown_{uuid.uuid4()}.md.gz")
            with gzip.open(file_path, "wt", encoding="utf-8") as f_out:
                f_out.write(markdown)

        return file_path

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Create_Wiki_Page: {self.owner_id}"
    )
    async def create_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
        force_version: bool = False,
    ) -> "WikiPage":
        """Create a new wiki page.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.
            force_version: If True, the wiki page will be created with a new version number.

        Returns:
            The created wiki page.

        Raises:
            ValueError: If owner_id is not provided or if required fields are missing.
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to create a wiki page.")
        # check if the attachments exists
        if not self.markdown:
            raise ValueError("Must provide markdown content to create a wiki page.")

        # Convert markdown to gzipped file if needed
        file_path = self._markdown_to_gzip_file(self.markdown, synapse_client)

        # Upload the gzipped file to get a file handle
        file_handle = await upload_file_handle(
            syn=synapse_client,
            parent_entity_id=self.owner_id,
            path=file_path,
        )

        # delete the temp gzip file
        os.remove(file_path)

        # Set the markdown file handle ID from the upload response
        self.markdown_file_handle_id = file_handle.get("id")

        # Create the wiki page
        wiki_data = await post_wiki(
            owner_id=self.owner_id,
            request=self.to_synapse_request(),
            synapse_client=synapse_client,
        )

        if force_version and self.wiki_version is not None:
            wiki_data = await put_wiki_version(
                owner_id=self.owner_id,
                wiki_id=self.id,
                wiki_version=self.wiki_version,
                request=wiki_data,
                synapse_client=synapse_client,
            )

        else:
            raise ValueError("Must provide wiki_version to force a new version.")

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
                wiki_version=self.version_number,
                synapse_client=synapse_client,
            )
        # If we only have a title, find the wiki page with matching title
        else:
            results = await get_wiki_header_tree(
                owner_id=self.owner_id,
                synapse_client=synapse_client,
            )
            async for result in results:
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
                wiki_version=self.version_number,
                synapse_client=synapse_client,
            )

        self.fill_from_dict(wiki_data)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Update_Wiki_Page: {self.owner_id}, Wiki ID {self.id}, Wiki Version {self.wiki_version}"
    )
    async def update_async(
        self,
        *,
        force_version: bool = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> "WikiPage":
        """
        Update a wiki page. If force_version is True, restore a specific version of the content.

        Arguments:
            force_version: If True, update a specific version of the wiki page (restore).
            synapse_client: Optionally provide a Synapse client.

        Returns:
            The updated WikiPage object.

        Raises:
            ValueError: If required fields are missing.
        """

        if not self.owner_id:
            raise ValueError("Must provide both owner_id to update a wiki page.")
        if not self.id:
            raise ValueError("Must provide id to update a wiki page.")

        if force_version:
            if self.wiki_version is None:
                raise ValueError("Must provide wiki_version to force a new version.")
            wiki_data = await put_wiki_version(
                owner_id=self.owner_id,
                wiki_id=self.id,
                wiki_version=self.wiki_version,
                request=self.to_synapse_request(),
                synapse_client=synapse_client,
            )
        else:
            wiki_data = await put_wiki_page(
                owner_id=self.owner_id,
                wiki_id=self.id,
                request=self.to_synapse_request(),
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
    async def get_attachment_url_async(
        self,
        file_name: str,
        *,
        redirect: Optional[bool] = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> dict:
        """
        Get the URL of a wiki page attachment.

        Arguments:
            file_name: The name of the file to get.
            redirect: When set to false, the URL will be returned as text/plain instead of redirecting. Default is False.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The URL that can be used to download a file for a given WikiPage file attachment.
        Raises:
            ValueError: If owner_id or id is not provided.
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get attachment URL.")
        if not self.id:
            raise ValueError("Must provide id to get attachment URL.")
        if not file_name:
            raise ValueError("Must provide file_name to get attachment URL.")

        return await get_attachment_url(
            owner_id=self.owner_id,
            wiki_id=self.id,
            file_name=file_name,
            wiki_version=self.wiki_version,
            redirect=redirect,
            synapse_client=synapse_client,
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Attachment_Preview_URL: Owner ID {self.owner_id}, Wiki ID {self.id}, File Name {kwargs['file_name']}"
    )
    async def get_attachment_preview_url_async(
        self,
        file_name: str,
        *,
        wiki_version: Optional[int] = None,
        redirect: Optional[bool] = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> dict:
        """
        Get the preview URL of a wiki page attachment.

        Arguments:
            file_name: The name of the file to get.
            wiki_version: Optional version of the wiki page. If not provided, uses self.wiki_version.
            redirect: When set to false, the URL will be returned as text/plain instead of redirecting. Default is False.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The URL that can be used to download a preview file for a given WikiPage file attachment.
        Raises:
            ValueError: If owner_id or id is not provided.
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get attachment preview URL.")
        if not self.id:
            raise ValueError("Must provide id to get attachment preview URL.")
        if not file_name:
            raise ValueError("Must provide file_name to get attachment preview URL.")

        return await get_attachment_preview_url(
            owner_id=self.owner_id,
            wiki_id=self.id,
            file_name=file_name,
            wiki_version=self.wiki_version,
            redirect=redirect,
            synapse_client=synapse_client,
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Get_Markdown_URL: Owner ID {self.owner_id}, Wiki ID {self.id}, Wiki Version {self.wiki_version}"
    )
    async def get_markdown_url_async(
        self,
        *,
        redirect: Optional[bool] = False,
        synapse_client: Optional["Synapse"] = None,
    ) -> dict:
        """
        Get the markdown URL of this wiki page.

        Arguments:
            redirect: When set to false, the URL will be returned as text/plain instead of redirecting. Default is False.
            synapse_client: Optionally provide a Synapse client.
        Returns:
            The URL that can be used to download the markdown file for this WikiPage.
        Raises:
            ValueError: If owner_id or id is not provided.
        """
        if not self.owner_id:
            raise ValueError("Must provide owner_id to get markdown URL.")
        if not self.id:
            raise ValueError("Must provide id to get markdown URL.")

        return await get_markdown_url(
            owner_id=self.owner_id,
            wiki_id=self.id,
            wiki_version=self.wiki_version,
            redirect=redirect,
            synapse_client=synapse_client,
        )
