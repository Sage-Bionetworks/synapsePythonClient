import asyncio
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Dict, List, Optional, Set, Union

from tqdm import tqdm

from synapseclient import Synapse
from synapseclient.api import (
    delete_entity_acl,
    get_entity_acl,
    get_user_group_headers_batch,
)
from synapseclient.api.entity_services import get_entity_benefactor
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.exceptions import SynapseHTTPError
from synapseclient.core.models.acl import AclListResult
from synapseclient.core.transfer_bar import shared_download_progress_bar
from synapseclient.models.protocols.access_control_protocol import (
    AccessControllableSynchronousProtocol,
)

if TYPE_CHECKING:
    from synapseclient.core.models.permission import Permissions
    from synapseclient.models import File, Folder


@dataclass
class BenefactorTracker:
    """
    Tracks benefactor relationships during ACL deletion operations to handle
    cascading changes when entities' inheritance changes.

    Attributes:
        entity_benefactors: Mapping of entity_id -> benefactor_id
        benefactor_children: Mapping of benefactor_id -> [child_entity_ids]
        deleted_acls: Set of entity_ids whose ACLs have been deleted
        processed_entities: Set of entity_ids that have been processed
    """

    entity_benefactors: Dict[str, str] = field(default_factory=dict)
    """Mapping of entity_id -> benefactor_id"""

    benefactor_children: Dict[str, List[str]] = field(default_factory=dict)
    """Mapping of benefactor_id -> [child_entity_ids]"""

    deleted_acls: Set[str] = field(default_factory=set)
    """Set of entity_ids whose ACLs have been deleted"""

    processed_entities: Set[str] = field(default_factory=set)
    """Set of entity_ids that have been processed"""

    async def track_entity_benefactor(
        self,
        entity_ids: List[str],
        synapse_client: "Synapse",
        progress_bar: Optional[tqdm] = None,
    ) -> None:
        """
        Track entities and their benefactor relationships.

        Arguments:
            entity_ids: List of entity IDs to track
            synapse_client: The Synapse client to use for API calls
            progress_bar: Progress bar to update after operation
        """
        entities_to_process = [
            entity_id
            for entity_id in entity_ids
            if entity_id not in self.processed_entities
        ]

        if not entities_to_process:
            if progress_bar:
                progress_bar.update(1)
            return

        async def task_with_entity_id(entity_id: str):
            """Wrapper to pair entity_id with the task result."""
            result = await get_entity_benefactor(
                entity_id=entity_id, synapse_client=synapse_client
            )
            return entity_id, result

        tasks = [
            asyncio.create_task(task_with_entity_id(entity_id))
            for entity_id in entities_to_process
        ]

        if tasks:
            if progress_bar:
                progress_bar.total += len(tasks)
                progress_bar.refresh()

        for completed_task in asyncio.as_completed(tasks):
            entity_id, benefactor_result = await completed_task
            benefactor_id = benefactor_result.id

            self.entity_benefactors[entity_id] = benefactor_id

            if benefactor_id not in self.benefactor_children:
                self.benefactor_children[benefactor_id] = []

            if entity_id != benefactor_id:
                self.benefactor_children[benefactor_id].append(entity_id)

            self.processed_entities.add(entity_id)
            if progress_bar:
                progress_bar.update(1)

    def mark_acl_deleted(self, entity_id: str) -> List[str]:
        """
        Mark an entity's ACL as deleted and return entities that will be affected.

        Arguments:
            entity_id: The ID of the entity whose ACL was deleted

        Returns:
            List of entity IDs that will need their benefactor relationships updated
        """
        self.deleted_acls.add(entity_id)

        affected_entities = self.benefactor_children.get(entity_id, [])

        if entity_id in self.entity_benefactors:
            new_benefactor = self.entity_benefactors[entity_id]

            for affected_entity in affected_entities:
                old_benefactor = self.entity_benefactors.get(affected_entity)
                if old_benefactor == entity_id:
                    self.entity_benefactors[affected_entity] = new_benefactor

                    if new_benefactor not in self.benefactor_children:
                        self.benefactor_children[new_benefactor] = []
                    if affected_entity not in self.benefactor_children[new_benefactor]:
                        self.benefactor_children[new_benefactor].append(affected_entity)

        if entity_id in self.benefactor_children:
            del self.benefactor_children[entity_id]

        return affected_entities

    def will_acl_deletion_affect_others(self, entity_id: str) -> bool:
        """
        Check if deleting this entity's ACL will affect other entities.

        Arguments:
            entity_id: The ID of the entity

        Returns:
            True if other entities will be affected, False otherwise
        """
        return len(self.benefactor_children.get(entity_id, [])) > 0


@async_to_sync
class AccessControllable(AccessControllableSynchronousProtocol):
    """
    Mixin for objects that can be controlled by an Access Control List (ACL).

    In order to use this mixin, the class must have an `id` attribute.
    """

    id: Optional[str] = None
    """The unique immutable ID for this entity. A new ID will be generated for new Files.
    Once issued, this ID is guaranteed to never change or be re-issued."""

    async def get_permissions_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Permissions":
        """
        Get the [permissions][synapseclient.core.models.permission.Permissions]
        that the caller has on an Entity.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A Permissions object


        Example: Using this function:
            Getting permissions for a Synapse Entity

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import File

            syn = Synapse()
            syn.login()

            async def main():
                permissions = await File(id="syn123").get_permissions_async()

            asyncio.run(main())
            ```

            Getting access types list from the Permissions object

            ```
            permissions.access_types
            ```
        """
        loop = asyncio.get_event_loop()

        return await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).get_permissions(
                entity=self.id
            ),
        )

    async def get_acl_async(
        self,
        principal_id: int = None,
        check_benefactor: bool = True,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List[str]:
        """
        Get the [ACL][synapseclient.core.models.permission.Permissions.access_types]
        that a user or group has on an Entity.

        Note: If the entity does not have local sharing settings, or ACL set directly
        on it, this will look up the ACL on the benefactor of the entity. The
        benefactor is the entity that the current entity inherits its permissions from.
        The benefactor is usually the parent entity, but it can be any ancestor in the
        hierarchy. For example, a newly created Project will be its own benefactor,
        while a new FileEntity's benefactor will start off as its containing Project or
        Folder. If the entity already has local sharing settings, the benefactor would
        be itself.

        Arguments:
            principal_id: Identifier of a user or group (defaults to PUBLIC users)
            check_benefactor: If True (default), check the benefactor for the entity
                to get the ACL. If False, only check the entity itself.
                This is useful for checking the ACL of an entity that has local sharing
                settings, but you want to check the ACL of the entity itself and not
                the benefactor it may inherit from.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            An array containing some combination of
                ['READ', 'UPDATE', 'CREATE', 'DELETE', 'DOWNLOAD', 'MODERATE',
                'CHANGE_PERMISSIONS', 'CHANGE_SETTINGS']
                or an empty array
        """
        loop = asyncio.get_event_loop()

        return await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).get_acl(
                entity=self.id,
                principal_id=principal_id,
                check_benefactor=check_benefactor,
            ),
        )

    async def set_permissions_async(
        self,
        principal_id: int = None,
        access_type: List[str] = None,
        modify_benefactor: bool = False,
        warn_if_inherits: bool = True,
        overwrite: bool = True,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict[str, Union[str, list]]:
        """
        Sets permission that a user or group has on an Entity.
        An Entity may have its own ACL or inherit its ACL from a benefactor.

        Arguments:
            principal_id: Identifier of a user or group. `273948` is for all
                registered Synapse users and `273949` is for public access.
                None implies public access.
            access_type: Type of permission to be granted. One or more of CREATE,
                READ, DOWNLOAD, UPDATE, DELETE, CHANGE_PERMISSIONS.

                **Defaults to ['READ', 'DOWNLOAD']**
            modify_benefactor: Set as True when modifying a benefactor's ACL. The term
                'benefactor' is used to indicate which Entity an Entity inherits its
                ACL from. For example, a newly created Project will be its own
                benefactor, while a new FileEntity's benefactor will start off as its
                containing Project. If the entity already has local sharing settings
                the benefactor would be itself. It may also be the immediate parent,
                somewhere in the parent tree, or the project itself.
            warn_if_inherits: When `modify_benefactor` is True, this does not have any
                effect. When `modify_benefactor` is False, and `warn_if_inherits` is
                True, a warning log message is produced if the benefactor for the
                entity you passed into the function is not itself, i.e., it's the
                parent folder, or another entity in the parent tree.
            overwrite: By default this function overwrites existing permissions for
                the specified user. Set this flag to False to add new permissions
                non-destructively.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            An Access Control List object

        Example: Setting permissions
            Grant all registered users download access

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import File

            syn = Synapse()
            syn.login()

            async def main():
                await File(id="syn123").set_permissions_async(principal_id=273948, access_type=['READ','DOWNLOAD'])

            asyncio.run(main())
            ```

            Grant the public view access

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import File

            syn = Synapse()
            syn.login()

            async def main():
                await File(id="syn123").set_permissions_async(principal_id=273949, access_type=['READ'])

            asyncio.run(main())
            ```
        """
        if access_type is None:
            access_type = ["READ", "DOWNLOAD"]
        loop = asyncio.get_event_loop()

        return await loop.run_in_executor(
            None,
            lambda: Synapse.get_client(synapse_client=synapse_client).setPermissions(
                entity=self.id,
                principalId=principal_id,
                accessType=access_type,
                modify_benefactor=modify_benefactor,
                warn_if_inherits=warn_if_inherits,
                overwrite=overwrite,
            ),
        )

    async def delete_permissions_async(
        self,
        include_self: bool = True,
        include_container_content: bool = False,
        recursive: bool = False,
        target_entity_types: Optional[List[str]] = None,
        dry_run: bool = False,
        show_acl_details: bool = True,
        show_files_in_containers: bool = True,
        *,
        synapse_client: Optional[Synapse] = None,
        _benefactor_tracker: Optional[BenefactorTracker] = None,
    ) -> None:
        """
        Delete the entire Access Control List (ACL) for a given Entity. This is not
        scoped to a specific user or group, but rather removes all permissions
        associated with the Entity. After this operation, the Entity will inherit
        permissions from its benefactor, which is typically its parent entity or
        the Project it belongs to.

        In order to remove permissions for a specific user or group, you
        should use the `set_permissions_async` method with the `access_type` set to
        an empty list.

        By default, Entities such as FileEntity and Folder inherit their permission from
        their containing Project. For such Entities the Project is the Entity's 'benefactor'.
        This permission inheritance can be overridden by creating an ACL for the Entity.
        When this occurs the Entity becomes its own benefactor and all permission are
        determined by its own ACL.

        If the ACL of an Entity is deleted, then its benefactor will automatically be set
        to its parent's benefactor.

        **Special notice for Projects:** The ACL for a Project cannot be deleted, you
        must individually update or revoke the permissions for each user or group.

        Arguments:
            include_self: If True (default), delete the ACL of the current entity.
                If False, skip deleting the ACL of the current entity.
            include_container_content: If True, delete ACLs from contents directly within
                containers (files and folders inside self). This must be set to
                True for recursive to have any effect. Defaults to False.
            recursive: If True and the entity is a container (e.g., Project or Folder),
                recursively process child containers. Note that this must be used with
                include_container_content=True to have any effect. Setting recursive=True
                with include_container_content=False will raise a ValueError.
                Only works on classes that support the `sync_from_synapse_async` method.
            target_entity_types: Specify which entity types to process when deleting ACLs.
                Allowed values are "folder" and "file" (case-insensitive).
                If None, defaults to ["folder", "file"]. This does not affect the
                entity type of the current entity, which is always processed if
                `include_self=True`.
            dry_run: If True, log the changes that would be made instead of actually
                performing the deletions. When enabled, all ACL deletion operations are
                simulated and logged at info level. Defaults to False.
            show_acl_details: When dry_run=True, controls whether current ACL details are
                displayed for entities that will have their permissions changed. If True (default),
                shows detailed ACL information. If False, hides ACL details for cleaner output.
                Has no effect when dry_run=False.
            show_files_in_containers: When dry_run=True, controls whether files within containers
                are displayed in the preview. If True (default), shows all files. If False, hides
                files when their only change is benefactor inheritance (but still shows files with
                local ACLs being deleted). Has no effect when dry_run=False.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
            _benefactor_tracker: Internal use tracker for managing benefactor relationships.
                Used for recursive functionality to track which entities will be affected

        Returns:
            None

        Raises:
            ValueError: If the entity does not have an ID or if an invalid entity type is provided.
            SynapseHTTPError: If there are permission issues or if the entity already inherits permissions.
            Exception: For any other errors that may occur during the process.

        Note: The caller must be granted ACCESS_TYPE.CHANGE_PERMISSIONS on the Entity to
        call this method.

        Example: Delete permissions for a single entity
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import File

            syn = Synapse()
            syn.login()

            async def main():
                await File(id="syn123").delete_permissions_async()

            asyncio.run(main())
            ```

        Example: Delete permissions recursively for a folder and all its children
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Folder

            syn = Synapse()
            syn.login()

            async def main():
                # Delete permissions for this folder only (does not affect children)
                await Folder(id="syn123").delete_permissions_async()

                # Delete permissions for all files and folders directly within this folder,
                # but not the folder itself
                await Folder(id="syn123").delete_permissions_async(
                    include_self=False,
                    include_container_content=True
                )

                # Delete permissions for all items in the entire hierarchy (folders and their files)
                # Both recursive and include_container_content must be True
                await Folder(id="syn123").delete_permissions_async(
                    recursive=True,
                    include_container_content=True
                )

                # Delete permissions only for folder entities within this folder recursively
                # and their contents
                await Folder(id="syn123").delete_permissions_async(
                    recursive=True,
                    include_container_content=True,
                    target_entity_types=["folder"]
                )

                # Delete permissions only for files within this folder and all subfolders
                await Folder(id="syn123").delete_permissions_async(
                    include_self=False,
                    recursive=True,
                    include_container_content=True,
                    target_entity_types=["file"]
                )

                # Dry run example: Log what would be deleted without making changes
                await Folder(id="syn123").delete_permissions_async(
                    recursive=True,
                    include_container_content=True,
                    dry_run=True
                )
            asyncio.run(main())
            ```
        """
        if not self.id:
            raise ValueError("The entity must have an ID to delete permissions.")

        client = Synapse.get_client(synapse_client=synapse_client)

        if include_self and self.__class__.__name__.lower() == "project":
            client.logger.warning(
                "The ACL for a Project cannot be deleted, you must individually update or "
                "revoke the permissions for each user or group. Continuing without deleting "
                "the Project's ACL."
            )
            include_self = False

        normalized_types = self._normalize_target_entity_types(target_entity_types)

        is_top_level = not _benefactor_tracker
        benefactor_tracker = _benefactor_tracker or BenefactorTracker()

        should_process_children = (recursive or include_container_content) and hasattr(
            self, "sync_from_synapse_async"
        )
        all_entities = [self] if include_self else []

        custom_message = "Deleting ACLs [Dry Run]..." if dry_run else "Deleting ACLs..."
        with shared_download_progress_bar(
            file_size=1, synapse_client=client, custom_message=custom_message, unit=None
        ) as progress_bar:
            if progress_bar:
                progress_bar.update(1)  # Initial setup complete

            if should_process_children:
                if recursive and not include_container_content:
                    raise ValueError(
                        "When recursive=True, include_container_content must also be True. "
                        "Setting recursive=True with include_container_content=False has no effect."
                    )

                if progress_bar:
                    progress_bar.total += 1
                    progress_bar.refresh()

                all_entities = await self._collect_entities(
                    client=client,
                    target_entity_types=normalized_types,
                    include_container_content=include_container_content,
                    recursive=recursive,
                    progress_bar=progress_bar,
                )
                if progress_bar:
                    progress_bar.update(1)

                entity_ids = [entity.id for entity in all_entities if entity.id]
                if entity_ids:
                    if progress_bar:
                        progress_bar.total += 1
                        progress_bar.refresh()
                    await benefactor_tracker.track_entity_benefactor(
                        entity_ids=entity_ids,
                        synapse_client=client,
                        progress_bar=progress_bar,
                    )
                else:
                    if progress_bar:
                        progress_bar.total += 1
                        progress_bar.refresh()
                        progress_bar.update(1)

            if is_top_level:
                if progress_bar:
                    progress_bar.total += 1
                    progress_bar.refresh()
                await self._build_and_log_run_tree(
                    client=client,
                    benefactor_tracker=benefactor_tracker,
                    collected_entities=all_entities,
                    include_self=include_self,
                    show_acl_details=show_acl_details,
                    show_files_in_containers=show_files_in_containers,
                    progress_bar=progress_bar,
                    dry_run=dry_run,
                )

            if dry_run:
                return

            if include_self:
                if progress_bar:
                    progress_bar.total += 1
                    progress_bar.refresh()
                await self._delete_current_entity_acl(
                    client=client,
                    benefactor_tracker=benefactor_tracker,
                    progress_bar=progress_bar,
                )

            if should_process_children:
                if include_container_content:
                    if progress_bar:
                        progress_bar.total += 1
                        progress_bar.refresh()
                    await self._process_container_contents(
                        client=client,
                        target_entity_types=normalized_types,
                        benefactor_tracker=benefactor_tracker,
                        progress_bar=progress_bar,
                        recursive=recursive,
                        include_container_content=include_container_content,
                    )
                    if progress_bar:
                        progress_bar.update(1)  # Process container contents complete

    def _normalize_target_entity_types(
        self, target_entity_types: Optional[List[str]]
    ) -> List[str]:
        """
        Normalize and validate the target entity types.

        Arguments:
            target_entity_types: A list of entity types to validate. If None, returns default types.

        Returns:
            List[str]: A normalized list (lowercase) of valid entity types.
        """
        default_types = ["folder", "file"]

        if target_entity_types is None:
            return default_types

        normalized_types = [t.lower() for t in target_entity_types]

        return normalized_types

    async def _delete_current_entity_acl(
        self,
        client: Synapse,
        benefactor_tracker: BenefactorTracker,
        progress_bar: Optional[tqdm] = None,
    ) -> None:
        """
        Delete the ACL for the current entity with benefactor relationship tracking.

        Arguments:
            client: The Synapse client instance to use for API calls.
            benefactor_tracker: Tracker for managing benefactor relationships.
            progress_bar: Progress bar to update after operation.

        Returns:
            None
        """

        await benefactor_tracker.track_entity_benefactor(
            entity_ids=[self.id], synapse_client=client, progress_bar=progress_bar
        )

        try:
            await delete_entity_acl(entity_id=self.id, synapse_client=client)
            client.logger.debug(f"Deleted ACL for entity {self.id}")

            if benefactor_tracker:
                affected_entities = benefactor_tracker.mark_acl_deleted(self.id)
                if affected_entities:
                    client.logger.info(
                        f"ACL deletion for entity {self.id} caused {len(affected_entities)} "
                        f"entities to inherit from a new benefactor: {affected_entities}"
                    )

            if progress_bar:
                progress_bar.update(1)

        except SynapseHTTPError as e:
            if (
                e.response.status_code == 403
                and "Resource already inherits its permissions." in e.response.text
            ):
                client.logger.debug(
                    f"Entity {self.id} already inherits permissions from its parent."
                )
                if progress_bar:
                    progress_bar.update(1)
            else:
                raise

    async def _process_container_contents(
        self,
        client: Synapse,
        target_entity_types: List[str],
        benefactor_tracker: BenefactorTracker,
        progress_bar: Optional[tqdm] = None,
        recursive: bool = False,
        include_container_content: bool = True,
    ) -> None:
        """
        Process the contents of a container entity, optionally recursively.

        Arguments:
            client: The Synapse client instance to use for API calls.
            target_entity_types: A list of normalized entity types to process.
            benefactor_tracker: Tracker for managing benefactor relationships.
            progress_bar: Optional progress bar to update as tasks complete.
            recursive: If True, process folders recursively; if False, process only direct contents.
            include_container_content: Whether to include the content of containers in processing.

        Returns:
            None
        """
        if "file" in target_entity_types and hasattr(self, "files"):
            if benefactor_tracker:
                track_tasks = [
                    benefactor_tracker.track_entity_benefactor(
                        entity_ids=[file.id],
                        synapse_client=client,
                        progress_bar=progress_bar,
                    )
                    for file in self.files
                ]

                if progress_bar and track_tasks:
                    progress_bar.total += len(track_tasks)
                    progress_bar.refresh()

                for completed_task in asyncio.as_completed(track_tasks):
                    await completed_task
                    if progress_bar:
                        progress_bar.update(1)

            async def process_single_file(file):
                await file.delete_permissions_async(
                    recursive=False,
                    include_self=True,
                    target_entity_types=["file"],
                    dry_run=False,
                    _benefactor_tracker=benefactor_tracker,
                    synapse_client=client,
                )

            file_tasks = [process_single_file(file) for file in self.files]

            if progress_bar and file_tasks:
                progress_bar.total += len(file_tasks)
                progress_bar.refresh()

            for completed_task in asyncio.as_completed(file_tasks):
                await completed_task
                if progress_bar:
                    progress_bar.update(1)

        if hasattr(self, "folders"):
            await self._process_folder_permission_deletion(
                client=client,
                recursive=recursive,
                benefactor_tracker=benefactor_tracker,
                progress_bar=progress_bar,
                target_entity_types=target_entity_types,
                include_container_content=include_container_content,
            )

    async def _process_folder_permission_deletion(
        self,
        client: Synapse,
        recursive: bool,
        benefactor_tracker: BenefactorTracker,
        target_entity_types: List[str],
        progress_bar: Optional[tqdm] = None,
        include_container_content: bool = False,
    ) -> None:
        """
        Process folder permission deletion either directly or recursively.

        Arguments:
            client: The Synapse client instance to use for API calls.
            recursive: If True, process folders recursively; if False, process only direct folders.
            benefactor_tracker: Tracker for managing benefactor relationships.
                Only used for non-recursive processing.
            target_entity_types: A list of normalized entity types to process.
            progress_bar: Optional progress bar to update as tasks complete.
            include_container_content: Whether to include the content of containers in processing.
                Only used for recursive processing.

        Returns:
            None

        Raises:
            Exception: For any errors that may occur during processing, which are caught and logged.
        """
        if not recursive and benefactor_tracker:
            track_tasks = [
                benefactor_tracker.track_entity_benefactor(
                    entity_ids=[folder.id],
                    synapse_client=client,
                    progress_bar=progress_bar,
                )
                for folder in self.folders
            ]

            if progress_bar and track_tasks:
                progress_bar.total += len(track_tasks)
                progress_bar.refresh()

            for completed_task in asyncio.as_completed(track_tasks):
                await completed_task
                if progress_bar:
                    progress_bar.update(1)  # Each tracking task complete

        async def process_single_folder(folder):
            should_delete_folder_acl = (
                "folder" in target_entity_types and include_container_content
            )

            await folder.delete_permissions_async(
                include_self=should_delete_folder_acl,
                recursive=recursive,
                # This is needed to ensure we do not delete children ACLs when not
                # recursive, but still allow us to delete the ACL on the folder
                include_container_content=include_container_content and recursive,
                target_entity_types=target_entity_types,
                dry_run=False,
                _benefactor_tracker=benefactor_tracker,
                synapse_client=client,
            )

        folder_tasks = [process_single_folder(folder) for folder in self.folders]

        if progress_bar and folder_tasks:
            progress_bar.total += len(folder_tasks)
            progress_bar.refresh()

        for completed_task in asyncio.as_completed(folder_tasks):
            await completed_task
            if progress_bar:
                progress_bar.update(1)  # Each folder task complete

    async def list_acl_async(
        self,
        recursive: bool = False,
        include_container_content: bool = False,
        target_entity_types: Optional[List[str]] = None,
        log_tree: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
        _progress_bar: Optional[tqdm] = None,  # Internal parameter for recursive calls
    ) -> AclListResult:
        """
        List the Access Control Lists (ACLs) for this entity and optionally its children.

        This function returns the local sharing settings for the entity and optionally
        its children. It provides a mapping of all ACLs for the given container/entity.

        **Important Note:** This function returns the LOCAL sharing settings only, not
        the effective permissions that each Synapse User ID/Team has on the entities.
        More permissive permissions could be granted via a Team that the user has access
        to that has permissions on the entity, or through inheritance from parent entities.

        Arguments:
            recursive: If True and the entity is a container (e.g., Project or Folder),
                recursively process child containers. Note that this must be used with
                include_container_content=True to have any effect. Setting recursive=True
                with include_container_content=False will raise a ValueError.
                Only works on classes that support the `sync_from_synapse_async` method.
            include_container_content: If True, include ACLs from contents directly within
                containers (files and folders inside self). This must be set to
                True for recursive to have any effect. Defaults to False.
            target_entity_types: Specify which entity types to process when listing ACLs.
                Allowed values are "folder" and "file" (case-insensitive).
                If None, defaults to ["folder", "file"].
            log_tree: If True, logs the ACL results to console in ASCII tree format showing
                entity hierarchies and their ACL permissions in a tree-like structure.
                Defaults to False.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.
            _progress_bar: Internal parameter. Progress bar instance to use for updates
                when called recursively. Should not be used by external callers.

        Returns:
            An AclListResult object containing a structured representation of ACLs where:
            - entity_acls: A list of EntityAcl objects, each representing one entity's ACL
            - Each EntityAcl contains acl_entries (a list of AclEntry objects)
            - Each AclEntry contains the principal_id and their list of permissions

        Raises:
            ValueError: If the entity does not have an ID or if an invalid entity type is provided.
            SynapseHTTPError: If there are permission issues accessing ACLs.
            Exception: For any other errors that may occur during the process.

        Example: List ACLs for a single entity
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import File

            syn = Synapse()
            syn.login()

            async def main():
                acl_result = await File(id="syn123").list_acl_async()
                print(acl_result)

                # Access entity ACLs (entity_acls is a list, not a dict)
                for entity_acl in acl_result.all_entity_acls:
                    if entity_acl.entity_id == "syn123":
                        # Access individual ACL entries
                        for acl_entry in entity_acl.acl_entries:
                            if acl_entry.principal_id == "273948":
                                print(f"Principal 273948 has permissions: {acl_entry.permissions}")

                # I can also access the ACL for the file itself
                print(acl_result.entity_acl)

                print(acl_result)

            asyncio.run(main())
            ```

        Example: List ACLs recursively for a folder and all its children
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Folder

            syn = Synapse()
            syn.login()

            async def main():
                acl_result = await Folder(id="syn123").list_acl_async(
                    recursive=True,
                    include_container_content=True
                )

                # Access each entity's ACL (entity_acls is a list)
                for entity_acl in acl_result.all_entity_acls:
                    print(f"Entity {entity_acl.entity_id} has ACL with {len(entity_acl.acl_entries)} principals")

                # I can also access the ACL for the folder itself
                print(acl_result.entity_acl)

                # List ACLs for only folder entities
                folder_acl_result = await Folder(id="syn123").list_acl_async(
                    recursive=True,
                    include_container_content=True,
                    target_entity_types=["folder"]
                )

            asyncio.run(main())
            ```

        Example: List ACLs with ASCII tree visualization
            When `log_tree=True`, the ACLs will be logged in a tree format. Additionally,
            the `ascii_tree` attribute of the AclListResult will contain the ASCII tree
            representation of the ACLs.

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Folder

            syn = Synapse()
            syn.login()

            async def main():
                acl_result = await Folder(id="syn123").list_acl_async(
                    recursive=True,
                    include_container_content=True,
                    log_tree=True, # Enable ASCII tree logging
                )

                # The ASCII tree representation of the ACLs will also be available
                # in acl_result.ascii_tree
                print(acl_result.ascii_tree)

            asyncio.run(main())
            ```
        """
        if not self.id:
            raise ValueError("The entity must have an ID to list ACLs.")

        normalized_types = self._normalize_target_entity_types(target_entity_types)
        client = Synapse.get_client(synapse_client=synapse_client)

        all_acls: Dict[str, Dict[str, List[str]]] = {}
        all_entities = []

        # Only update progress bar for self ACL if we're the top-level call (not recursive)
        # When _progress_bar is passed, it means this is a recursive call and the parent
        # is managing progress updates
        update_progress_for_self = _progress_bar is None
        acl = await self._get_current_entity_acl(
            client=client,
            progress_bar=_progress_bar if update_progress_for_self else None,
        )
        if acl is not None:
            all_acls[self.id] = acl
        all_entities.append(self)

        should_process_children = (recursive or include_container_content) and hasattr(
            self, "sync_from_synapse_async"
        )

        if should_process_children and (recursive and not include_container_content):
            raise ValueError(
                "When recursive=True, include_container_content must also be True. "
                "Setting recursive=True with include_container_content=False has no effect."
            )

        if should_process_children and _progress_bar is None:
            with shared_download_progress_bar(
                file_size=1,
                synapse_client=client,
                custom_message="Collecting ACLs...",
                unit=None,
            ) as progress_bar:
                await self._process_children_with_progress(
                    client=client,
                    normalized_types=normalized_types,
                    include_container_content=include_container_content,
                    recursive=recursive,
                    all_entities=all_entities,
                    all_acls=all_acls,
                    progress_bar=progress_bar,
                )
                # Ensure progress bar reaches 100% completion
                if progress_bar:
                    remaining = (
                        progress_bar.total - progress_bar.n
                        if progress_bar.total > progress_bar.n
                        else 0
                    )
                    if remaining > 0:
                        progress_bar.update(remaining)
        elif should_process_children:
            await self._process_children_with_progress(
                client=client,
                normalized_types=normalized_types,
                include_container_content=include_container_content,
                recursive=recursive,
                all_entities=all_entities,
                all_acls=all_acls,
                progress_bar=_progress_bar,
            )
        current_acl = all_acls.get(self.id)
        acl_result = AclListResult.from_dict(
            all_acl_dict=all_acls, current_acl_dict=current_acl
        )

        if log_tree:
            logged_tree = await self._log_acl_tree(acl_result, all_entities, client)
            acl_result.ascii_tree = logged_tree

        return acl_result

    async def _get_current_entity_acl(
        self, client: Synapse, progress_bar: Optional[tqdm] = None
    ) -> Optional[Dict[str, List[str]]]:
        """
        Get the ACL for the current entity.

        Arguments:
            client: The Synapse client instance to use for API calls.
            progress_bar: Progress bar to update after operation.

        Returns:
            A dictionary mapping principal IDs to permission lists, or None if no ACL exists.
        """
        try:
            acl_response = await get_entity_acl(
                entity_id=self.id, synapse_client=client
            )
            result = self._parse_acl_response(acl_response)
            if progress_bar:
                progress_bar.update(1)
            return result
        except SynapseHTTPError as e:
            if e.response.status_code == 404:
                client.logger.debug(
                    f"Entity {self.id} inherits permissions from its parent (no local ACL)."
                )
                if progress_bar:
                    progress_bar.update(1)
                return None
            else:
                raise

    def _parse_acl_response(self, acl_response: Dict[str, Any]) -> Dict[str, List[str]]:
        """
        Parse the ACL response from the API into the expected format.

        Arguments:
            acl_response: The raw ACL response from the API.

        Returns:
            A dictionary mapping principal IDs to permission lists.
        """
        parsed_acl = {}

        if "resourceAccess" in acl_response:
            for resource_access in acl_response["resourceAccess"]:
                principal_id = str(resource_access.get("principalId", ""))
                access_types = resource_access.get("accessType", [])
                if principal_id and access_types:
                    parsed_acl[principal_id] = access_types

        return parsed_acl

    async def _collect_entities(
        self,
        client: Synapse,
        target_entity_types: List[str],
        progress_bar: Optional[tqdm] = None,
        include_container_content: bool = False,
        recursive: bool = False,
        collect_acls: bool = False,
        collect_self: bool = False,
        all_acls: Optional[Dict[str, Dict[str, List[str]]]] = None,
    ) -> List[Union["File", "Folder"]]:
        """
        Unified method to collect entities, their ACLs, or both based on parameters.

        This method replaces multiple specialized collection methods with a single,
        configurable approach that can:
        1. Collect entity objects
        2. Collect ACLs from entities (collect_acls=True)
        3. Handle both direct container contents and recursive collection
        4. Filter by entity types

        Arguments:
            client: The Synapse client instance to use for API calls.
            target_entity_types: A list of normalized entity types to process.
            progress_bar: Progress bar instance to update as tasks complete.
            include_container_content: Whether to include the content of containers.
            recursive: Whether to process recursively.
            collect_acls: Whether to collect ACLs from entities.
            collect_self: If True, include the current entity in the results.
            all_acls: Dictionary to accumulate ACL results if collecting ACLs.

        Returns:
            Returns a list of entity objects
        """
        entities = []

        if collect_self:
            entities.append(self)

            if collect_acls and all_acls is not None:
                if progress_bar:
                    progress_bar.total += 1
                    progress_bar.refresh()

                entity_acls = await self.list_acl_async(
                    recursive=False,
                    include_container_content=False,
                    target_entity_types=target_entity_types,
                    synapse_client=client,
                    _progress_bar=progress_bar,
                )
                all_acls.update(entity_acls.to_dict())

                if progress_bar:
                    progress_bar.update(1)

        should_process_children = (recursive or include_container_content) and hasattr(
            self, "sync_from_synapse_async"
        )
        if should_process_children:
            if not self._synced_from_synapse:
                await self.sync_from_synapse_async(
                    recursive=False,
                    download_file=False,
                    include_activity=False,
                    synapse_client=client,
                )

            if include_container_content:
                if "file" in target_entity_types and hasattr(self, "files"):
                    if collect_acls and all_acls is not None and progress_bar:
                        progress_bar.total += len(self.files)
                        progress_bar.refresh()

                    if collect_acls and all_acls is not None:
                        file_acl_tasks = []
                        for file in getattr(self, "files", []):
                            entities.append(file)
                            file_acl_tasks.append(
                                file.list_acl_async(
                                    recursive=False,
                                    include_container_content=False,
                                    target_entity_types=["file"],
                                    synapse_client=client,
                                    _progress_bar=progress_bar,
                                )
                            )

                        for completed_task in asyncio.as_completed(file_acl_tasks):
                            file_acls = await completed_task
                            all_acls.update(file_acls.to_dict())
                            if progress_bar:
                                progress_bar.update(1)
                    else:
                        for file in getattr(self, "files", []):
                            entities.append(file)

                if "folder" in target_entity_types and hasattr(self, "folders"):
                    if collect_acls and all_acls is not None and progress_bar:
                        progress_bar.total += len(self.folders)
                        progress_bar.refresh()

                    if collect_acls and all_acls is not None:
                        folder_acl_tasks = []
                        for folder in getattr(self, "folders", []):
                            entities.append(folder)
                            folder_acl_tasks.append(
                                folder.list_acl_async(
                                    recursive=False,
                                    include_container_content=False,
                                    target_entity_types=["folder"],
                                    synapse_client=client,
                                    _progress_bar=progress_bar,
                                )
                            )

                        for completed_task in asyncio.as_completed(folder_acl_tasks):
                            folder_acls = await completed_task
                            all_acls.update(folder_acls.to_dict())
                            if progress_bar:
                                progress_bar.update(1)
                    else:
                        for folder in getattr(self, "folders", []):
                            entities.append(folder)

            if recursive and hasattr(self, "folders"):
                collect_tasks = []
                for folder in self.folders:
                    collect_tasks.append(
                        folder._collect_entities(
                            client=client,
                            target_entity_types=target_entity_types,
                            include_container_content=include_container_content,
                            recursive=recursive,
                            collect_acls=collect_acls,
                            collect_self=False,
                            all_acls=all_acls,
                            progress_bar=progress_bar,
                        )
                    )

                for completed_task in asyncio.as_completed(collect_tasks):
                    result = await completed_task
                    if result is not None:
                        entities.extend(result)

        return entities

    async def _build_and_log_run_tree(
        self,
        client: Synapse,
        benefactor_tracker: BenefactorTracker,
        progress_bar: Optional[tqdm] = None,
        collected_entities: List["AccessControllable"] = None,
        include_self: bool = True,
        show_acl_details: bool = True,
        show_files_in_containers: bool = True,
        dry_run: bool = True,
    ) -> None:
        """
        Build and log comprehensive tree showing ACL deletion impacts.

        Creates a detailed visualization of which entities will have ACLs deleted,
        how inheritance will change, and which permissions will be affected.
        The tree uses visual indicators to show current vs future state.

        Arguments:
            client: The Synapse client instance to use for API calls.
            benefactor_tracker: Tracker containing all entity relationships.
            progress_bar: Optional progress bar to update during dry run analysis.
            collected_entities: List of entity objects that have been collected.
            include_self: Whether to include self in the deletion analysis.
            show_acl_details: Whether to display current ACL details for entities that will change.
            show_files_in_containers: Whether to show files within containers.
        """
        tree_data = await self._build_tree_data(
            client=client, collected_entities=collected_entities
        )
        if not tree_data["entities_by_id"]:
            client.logger.info(
                "[DRY RUN] No entities available for deletion impact analysis."
            )
            return

        tree_output = await self._format_dry_run_tree_async(
            tree_data["tree_structure"],
            benefactor_tracker,
            include_self,
            tree_data["entities_by_id"],
            show_acl_details=show_acl_details,
            show_files_in_containers=show_files_in_containers,
            synapse_client=client,
        )

        if dry_run:
            client.logger.info("=== DRY RUN: Permission Deletion Impact Analysis ===")
        else:
            client.logger.info("=== Permission Deletion Impact Analysis ===")
        client.logger.info(tree_output)

        if dry_run:
            client.logger.info("=== End of Dry Run Analysis ===")
        else:
            client.logger.info("=== End of Permission Deletion Analysis ===")

        if progress_bar:
            remaining = (
                progress_bar.total - progress_bar.n
                if progress_bar.total > progress_bar.n
                else 0
            )
            if remaining > 0:
                progress_bar.update(remaining)
            else:
                progress_bar.update(1)

    async def _build_tree_data(
        self,
        client: Synapse,
        collected_entities: List["AccessControllable"] = None,
    ) -> Dict[str, Any]:
        """
        Build comprehensive tree data including entities and ACL structure.

        Consolidates entity preparation, ACL fetching, and tree structure building
        into a single operation that can be reused by different tree formatting functions.

        Arguments:
            client: The Synapse client instance to use for API calls.
            collected_entities: List of entity objects that have been collected.
            benefactor_tracker: Optional tracker for managing benefactor relationships.

        Returns:
            Dictionary containing entities_by_id, acl_result, and tree_structure.
        """
        entities_by_id = self._prepare_entities_for_tree(
            collected_entities=collected_entities
        )
        acl_result = await self._build_acl_result_from_entities(
            entities_by_id=entities_by_id, client=client
        )
        tree_structure = await self._build_acl_tree_structure(
            acl_result, list(entities_by_id.values())
        )

        return {
            "entities_by_id": entities_by_id,
            "acl_result": acl_result,
            "tree_structure": tree_structure,
        }

    def _prepare_entities_for_tree(
        self, collected_entities: List[Union["File", "Folder"]] = None
    ) -> Dict[str, Union["File", "Folder"]]:
        """
        Prepare entity dictionary for tree building operations.

        Arguments:
            collected_entities: List of entity objects that have been collected.

        Returns:
            Dictionary mapping entity IDs to entity objects.
        """
        if collected_entities:
            entities_by_id = {
                entity.id: entity
                for entity in collected_entities
                if hasattr(entity, "id") and entity.id
            }
            if hasattr(self, "id") and self.id and self.id not in entities_by_id:
                entities_by_id[self.id] = self
        else:
            entities_by_id = {}
            if hasattr(self, "id") and self.id:
                entities_by_id[self.id] = self

        return entities_by_id

    async def _build_acl_result_from_entities(
        self, entities_by_id: Dict[str, Union["File", "Folder"]], client: Synapse
    ) -> AclListResult:
        """
        Build AclListResult from a dictionary of entities by fetching their ACL information.

        Arguments:
            entities_by_id: Dictionary mapping entity IDs to entity objects.
            client: The Synapse client instance to use for API calls.

        Returns:
            AclListResult containing ACL information for all entities.
        """
        from synapseclient.core.models.acl import AclEntry, EntityAcl

        async def fetch_entity_acl(entity_id: str) -> Optional[EntityAcl]:
            """Helper function to fetch ACL for a single entity."""
            try:
                acl_response = await get_entity_acl(
                    entity_id=entity_id, synapse_client=client
                )
                acl_info = self._parse_acl_response(acl_response)

                acl_entries = []
                for principal_id, permissions in acl_info.items():
                    acl_entries.append(
                        AclEntry(
                            principal_id=int(principal_id), permissions=permissions
                        )
                    )

                return EntityAcl(entity_id=entity_id, acl_entries=acl_entries)

            except SynapseHTTPError as e:
                if e.response.status_code != 404:
                    raise
                return None

        entity_ids = list(entities_by_id.keys())
        acl_tasks = [fetch_entity_acl(entity_id) for entity_id in entity_ids]

        entity_acls = []
        for completed_task in asyncio.as_completed(acl_tasks):
            try:
                result = await completed_task
                if result is not None:
                    entity_acls.append(result)
            except SynapseHTTPError as e:
                if e.response.status_code != 404:
                    raise
                continue
            except Exception as e:
                raise e

        return AclListResult(all_entity_acls=entity_acls)

    async def _fetch_user_group_info_from_tree(
        self, tree_structure: Dict[str, Any], synapse_client: "Synapse"
    ) -> Dict[str, str]:
        """
        Fetch user and group information for all principals found in a tree structure.

        Extracts all principal IDs from ACL entries within the tree structure and
        fetches their corresponding user/group names in a single batch operation.

        Arguments:
            tree_structure: Tree structure containing entity metadata with ACL information.
            synapse_client: Synapse client for API calls.

        Returns:
            Dictionary mapping principal IDs to user/group names.
        """
        entity_metadata = tree_structure.get("entity_metadata", {})
        return await self._fetch_user_group_info(entity_metadata, synapse_client)

    async def _fetch_user_group_info(
        self, entity_metadata: Dict[str, Any], synapse_client: "Synapse"
    ) -> Dict[str, str]:
        """
        Fetch user and group information for all principals found in ACLs.

        Scans through entity metadata to collect all unique principal IDs from
        ACL entries, then fetches their user/group information in a single batch call.

        Arguments:
            entity_metadata: Dictionary containing entity metadata with ACL information.
            synapse_client: Synapse client for API calls.

        Returns:
            Dictionary mapping principal IDs to user/group names.
        """
        all_principal_ids = self._extract_principal_ids_from_metadata(entity_metadata)

        if not all_principal_ids:
            return {}

        user_group_header_batch = await get_user_group_headers_batch(
            list(all_principal_ids), synapse_client=synapse_client
        )

        if not user_group_header_batch:
            return {}

        return {
            user_group_header["ownerId"]: user_group_header["userName"]
            for user_group_header in user_group_header_batch
        }

    def _extract_principal_ids_from_metadata(
        self, entity_metadata: Dict[str, Any]
    ) -> Set[str]:
        """
        Extract all unique principal IDs from entity metadata ACL entries.

        Arguments:
            entity_metadata: Dictionary containing entity metadata with ACL information.

        Returns:
            Set of principal ID strings found in the metadata.
        """
        all_principal_ids = set()
        for entity_id, metadata in entity_metadata.items():
            acl = metadata.get("acl")
            if acl and hasattr(acl, "acl_entries") and acl.acl_entries:
                for acl_entry in acl.acl_entries:
                    if acl_entry.principal_id:
                        all_principal_ids.add(str(acl_entry.principal_id))
        return all_principal_ids

    async def _format_dry_run_tree_async(
        self,
        tree_structure: Dict[str, Any],
        benefactor_tracker: BenefactorTracker,
        include_self: bool = True,
        entities_by_id: Optional[Dict[str, Union["File", "Folder"]]] = None,
        show_acl_details: bool = True,
        show_files_in_containers: bool = True,
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> str:
        """
        Format tree structure for dry run analysis showing ACL deletion impacts.

        Creates a comprehensive visualization showing which entities will have ACLs deleted,
        how inheritance will change, and which permissions will be affected. Uses icons
        and status indicators to clearly show current vs future state.

        Arguments:
            tree_structure: The tree structure dictionary from _build_acl_tree_structure.
            benefactor_tracker: Tracker containing benefactor relationships.
            include_self: Whether to include self in the deletion analysis.
            entities_by_id: Dictionary mapping entity IDs to entity objects.
            show_acl_details: Whether to display current ACL details for entities that will change.
            show_files_in_containers: Whether to show files within containers.
            synapse_client: Synapse client for API calls.

        Returns:
            Formatted ASCII tree string showing deletion impact analysis.
        """
        entity_metadata = tree_structure["entity_metadata"]
        children_map = tree_structure["children_map"]
        root_entities = tree_structure["root_entities"]

        user_group_info_map = await self._fetch_user_group_info_from_tree(
            tree_structure, synapse_client
        )

        await self._augment_tree_with_missing_entities(
            entity_metadata,
            children_map,
            benefactor_tracker,
            include_self,
            entities_by_id,
            synapse_client,
        )

        lines = self._build_dry_run_tree_lines(
            entity_metadata,
            children_map,
            root_entities,
            benefactor_tracker,
            include_self,
            show_acl_details,
            show_files_in_containers,
            user_group_info_map,
        )

        return "\n".join(lines)

    async def _augment_tree_with_missing_entities(
        self,
        entity_metadata: Dict[str, Any],
        children_map: Dict[str, List[str]],
        benefactor_tracker: BenefactorTracker,
        include_self: bool,
        entities_by_id: Optional[Dict[str, Union["File", "Folder"]]],
        synapse_client: Optional["Synapse"],
    ) -> None:
        """
        Add missing entity information needed for comprehensive tree display.

        Fetches metadata for entities that are affected by ACL deletions but not
        already present in the tree structure, ensuring complete impact visualization.

        Arguments:
            entity_metadata: Dictionary to augment with missing entity information.
            children_map: Dictionary mapping parent entities to their children.
            benefactor_tracker: Tracker containing benefactor relationships.
            include_self: Whether to include self in the deletion analysis.
            entities_by_id: Dictionary mapping entity IDs to entity objects.
            synapse_client: Synapse client for API calls.
        """
        all_missing_entity_ids = set()
        for entity_id in entity_metadata.keys():
            metadata = entity_metadata[entity_id]
            has_local_acl = metadata.get("acl") is not None
            is_self_entity = hasattr(self, "id") and entity_id == self.id
            will_be_deleted = has_local_acl and (not is_self_entity or include_self)

            if will_be_deleted:
                affected_children = benefactor_tracker.benefactor_children.get(
                    entity_id, []
                )
                children = children_map.get(entity_id, [])
                children_not_in_tree = [
                    child_id
                    for child_id in affected_children
                    if child_id not in children
                ]
                all_missing_entity_ids.update(children_not_in_tree)

        if all_missing_entity_ids:
            missing_entity_details = await self._fetch_missing_entity_details(
                list(all_missing_entity_ids),
                entity_metadata,
                entities_by_id,
                synapse_client,
            )
            entity_metadata.update(missing_entity_details)

    async def _fetch_missing_entity_details(
        self,
        entity_ids: List[str],
        entity_metadata: Dict[str, Any],
        entities_by_id: Optional[Dict[str, Union["File", "Folder"]]],
        synapse_client: Optional["Synapse"],
    ) -> Dict[str, Dict[str, str]]:
        """
        Fetch entity details for entities not already in metadata.

        Arguments:
            entity_ids: List of entity IDs to fetch details for.
            entity_metadata: Existing entity metadata to check against.
            entities_by_id: Dictionary mapping entity IDs to entity objects.
            synapse_client: Synapse client for API calls.

        Returns:
            Dictionary mapping entity IDs to their metadata.
        """
        missing_entities = {}
        tasks = []
        entity_id_to_task = {}

        for entity_id in entity_ids:
            if entity_id not in entity_metadata:
                if entities_by_id and entity_id in entities_by_id:
                    entity = entities_by_id[entity_id]
                    missing_entities[entity_id] = {
                        "name": getattr(entity, "name", f"Entity {entity_id}"),
                        "type": entity.__class__.__name__,
                        "parent_id": getattr(entity, "parent_id", None),
                        "acl": None,
                    }
                else:
                    task = get_entity_benefactor(
                        entity_id=entity_id, synapse_client=synapse_client
                    )
                    tasks.append(task)
                    entity_id_to_task[entity_id] = task

        if not tasks:
            return missing_entities

        for completed_task in asyncio.as_completed(tasks):
            header = await completed_task

            entity_id = None
            for eid, task in entity_id_to_task.items():
                if task == completed_task:
                    entity_id = eid
                    break

            if entity_id:
                entity_type = self._normalize_entity_type(header.type)
                missing_entities[entity_id] = {
                    "name": header.name or f"Entity {entity_id}",
                    "type": entity_type,
                    "parent_id": None,
                    "acl": None,
                }

        return missing_entities

    def _normalize_entity_type(self, entity_type: str) -> str:
        """
        Normalize entity type string to a readable format.

        Arguments:
            entity_type: Raw entity type string from API.

        Returns:
            Normalized entity type string.
        """
        if not entity_type:
            return "Unknown"

        if entity_type.startswith("org.sagebionetworks.repo.model."):
            entity_type = entity_type.split(".")[-1]
            if entity_type == "FileEntity":
                return "File"

        return entity_type

    def _build_dry_run_tree_lines(
        self,
        entity_metadata: Dict[str, Any],
        children_map: Dict[str, List[str]],
        root_entities: List[str],
        benefactor_tracker: BenefactorTracker,
        include_self: bool,
        show_acl_details: bool,
        show_files_in_containers: bool,
        user_group_info_map: Dict[str, str],
    ) -> List[str]:
        """
        Build the lines for the dry run tree display.

        Creates the actual tree visualization with all status indicators,
        benefactor changes, and impact analysis.

        Arguments:
            entity_metadata: Dictionary containing entity metadata with ACL information.
            children_map: Dictionary mapping parent entities to their children.
            root_entities: List of root entity IDs for the tree.
            benefactor_tracker: Tracker containing benefactor relationships.
            include_self: Whether to include self in the deletion analysis.
            show_acl_details: Whether to display current ACL details.
            show_files_in_containers: Whether to show files within containers.
            user_group_info_map: Dictionary mapping principal IDs to user/group names.

        Returns:
            List of strings representing the tree lines.
        """
        lines = []

        def find_ultimate_benefactor_for_affected(entity_id: str) -> str:
            """Recursively find the ultimate benefactor for an affected entity."""
            parent_id = entity_metadata.get(entity_id, {}).get("parent_id")
            if not parent_id:
                return entity_id

            if parent_id in entity_metadata:
                parent_metadata = entity_metadata[parent_id]
                parent_has_local_acl = parent_metadata.get("acl") is not None
                parent_is_self = hasattr(self, "id") and parent_id == self.id
                parent_will_be_deleted = parent_has_local_acl and (
                    not parent_is_self or include_self
                )

                if parent_will_be_deleted:
                    return find_ultimate_benefactor_for_affected(parent_id)
                else:
                    if parent_has_local_acl:
                        return parent_id
                    else:
                        return find_ultimate_benefactor_for_affected(parent_id)
            else:
                return parent_id

        def format_entity_with_benefactor_info(entity_id: str) -> str:
            """Format entity information with benefactor and impact details."""
            metadata = entity_metadata[entity_id]
            name = metadata["name"]
            entity_type = metadata["type"]

            current_benefactor = benefactor_tracker.entity_benefactors.get(
                entity_id, entity_id
            )
            affected_children = benefactor_tracker.benefactor_children.get(
                entity_id, []
            )

            has_local_acl = metadata.get("acl") is not None
            is_self_entity = hasattr(self, "id") and entity_id == self.id
            will_be_deleted = has_local_acl and (not is_self_entity or include_self)

            base_info = f"{name} ({entity_id}) [{entity_type}]"

            if will_be_deleted:
                status_info = " ⚠️  WILL DELETE LOCAL ACL"
                ultimate_benefactor = find_ultimate_benefactor_for_affected(entity_id)
                if ultimate_benefactor and ultimate_benefactor != entity_id:
                    status_info += f" → Will inherit from {ultimate_benefactor}"
                if affected_children:
                    status_info += f" → Affects {len(affected_children)} children"
            elif has_local_acl and is_self_entity and not include_self:
                status_info = " 🚫 LOCAL ACL WILL NOT BE DELETED"
            else:
                will_benefactor_change = False
                new_benefactor = current_benefactor

                for ancestor_id in entity_metadata.keys():
                    ancestor_metadata = entity_metadata[ancestor_id]
                    ancestor_has_local_acl = ancestor_metadata.get("acl") is not None
                    ancestor_is_self = hasattr(self, "id") and ancestor_id == self.id
                    ancestor_will_be_deleted = ancestor_has_local_acl and (
                        not ancestor_is_self or include_self
                    )

                    if (
                        ancestor_will_be_deleted
                        and entity_id
                        in benefactor_tracker.benefactor_children.get(ancestor_id, [])
                    ):
                        will_benefactor_change = True
                        new_benefactor = find_ultimate_benefactor_for_affected(
                            entity_id
                        )
                        break

                if will_benefactor_change:
                    status_info = f" 📍 Will inherit from {new_benefactor} (currently {current_benefactor})"
                else:
                    if current_benefactor != entity_id:
                        status_info = f" ↗️  Inherits from {current_benefactor}"
                    else:
                        status_info = " 🔒 No local ACL"

            return base_info + status_info

        def format_principal_with_impact(
            entity_id: str, principal_id: str, permissions: List[str]
        ) -> Optional[str]:
            """Format principal information with permission impact."""
            permission_list = permissions if permissions else ["None"]

            team_name = user_group_info_map.get(str(principal_id))
            if team_name:
                principal_info = f"{team_name} ({principal_id})"
            else:
                principal_info = f"Principal {principal_id}"

            is_self_entity = hasattr(self, "id") and entity_id == self.id
            will_be_deleted = not is_self_entity or include_self

            if will_be_deleted:
                return f"🔑 {principal_info}: {permission_list} → WILL BE REMOVED"
            else:
                return None

        def is_entity_relevant(entity_id: str) -> bool:
            """Determine if an entity should be included in the trimmed tree."""
            metadata = entity_metadata[entity_id]
            has_local_acl = metadata.get("acl") is not None
            is_self_entity = hasattr(self, "id") and entity_id == self.id
            will_be_deleted = has_local_acl and (not is_self_entity or include_self)
            entity_type = metadata.get("type", "").lower()

            if will_be_deleted:
                return True

            will_benefactor_change = False
            for ancestor_id in entity_metadata.keys():
                ancestor_metadata = entity_metadata[ancestor_id]
                ancestor_has_local_acl = ancestor_metadata.get("acl") is not None
                ancestor_is_self = hasattr(self, "id") and ancestor_id == self.id
                ancestor_will_be_deleted = ancestor_has_local_acl and (
                    not ancestor_is_self or include_self
                )

                if (
                    ancestor_will_be_deleted
                    and entity_id
                    in benefactor_tracker.benefactor_children.get(ancestor_id, [])
                ):
                    will_benefactor_change = True
                    break

            if not show_files_in_containers and "file" in entity_type:
                if will_be_deleted:
                    return True
                else:
                    return False

            if will_benefactor_change:
                return True

            return False

        def get_relevant_children(entity_id: str) -> List[str]:
            """Get only the children that are relevant for the trimmed tree."""
            all_children = children_map.get(entity_id, [])
            relevant_children = []

            for child_id in all_children:
                if is_entity_relevant(child_id):
                    relevant_children.append(child_id)
                elif has_relevant_descendants(child_id):
                    relevant_children.append(child_id)

            return relevant_children

        def has_relevant_descendants(entity_id: str) -> bool:
            """Check if an entity has any relevant descendants."""
            children = children_map.get(entity_id, [])
            for child_id in children:
                if is_entity_relevant(child_id) or has_relevant_descendants(child_id):
                    return True
            return False

        def build_tree_recursive(
            entity_id: str,
            prefix: str = "",
            is_last: bool = True,
            is_root: bool = False,
        ) -> None:
            """Recursively build the enhanced dry run tree."""
            if is_root:
                lines.append(f"{format_entity_with_benefactor_info(entity_id)}")
                extension_prefix = ""
            else:
                current_prefix = "└── " if is_last else "├── "
                lines.append(
                    f"{prefix}{current_prefix}{format_entity_with_benefactor_info(entity_id)}"
                )
                extension_prefix = prefix + ("    " if is_last else "│   ")

            metadata = entity_metadata[entity_id]
            acl = metadata.get("acl")
            acl_principals = []

            if (
                show_acl_details
                and acl
                and hasattr(acl, "acl_entries")
                and acl.acl_entries
            ):
                for acl_entry in acl.acl_entries:
                    principal_id = acl_entry.principal_id
                    permissions = acl_entry.permissions if acl_entry.permissions else []
                    acl_principals.append((principal_id, permissions))

                for i, (principal_id, permissions) in enumerate(acl_principals):
                    is_last_principal = (
                        i == len(acl_principals) - 1
                    ) and not get_relevant_children(entity_id)
                    principal_prefix = "└── " if is_last_principal else "├── "
                    formatted_principal = format_principal_with_impact(
                        entity_id, principal_id, permissions
                    )
                    if formatted_principal:
                        lines.append(
                            f"{extension_prefix}{principal_prefix}{formatted_principal}"
                        )

            affected_children = benefactor_tracker.benefactor_children.get(
                entity_id, []
            )
            relevant_children = get_relevant_children(entity_id)

            metadata = entity_metadata[entity_id]
            has_local_acl = metadata.get("acl") is not None
            is_self_entity = hasattr(self, "id") and entity_id == self.id
            will_be_deleted = has_local_acl and (not is_self_entity or include_self)

            if will_be_deleted and affected_children:
                children_not_in_tree = [
                    child_id
                    for child_id in affected_children
                    if child_id not in relevant_children
                    and is_entity_relevant(child_id)
                ]

                if children_not_in_tree:
                    all_children = relevant_children + children_not_in_tree
                    all_children.sort(
                        key=lambda child_id: entity_metadata.get(child_id, {}).get(
                            "name", f"Entity {child_id}"
                        )
                    )

                    for i, child_id in enumerate(all_children):
                        is_last_child = i == len(all_children) - 1

                        if child_id in children_not_in_tree:
                            child_metadata = entity_metadata.get(child_id, {})
                            child_name = child_metadata.get(
                                "name", f"Entity {child_id}"
                            )
                            child_type = child_metadata.get("type", "Unknown")
                            change_prefix = "└── " if is_last_child else "├── "

                            new_benefactor = find_ultimate_benefactor_for_affected(
                                child_id
                            )
                            current_benefactor = (
                                benefactor_tracker.entity_benefactors.get(
                                    child_id, child_id
                                )
                            )

                            lines.append(
                                f"{extension_prefix}{change_prefix}{child_name} ({child_id}) "
                                f"[{child_type}] 📍 Will inherit from {new_benefactor} (currently {current_benefactor})"
                            )
                        else:
                            build_tree_recursive(
                                child_id, extension_prefix, is_last_child, False
                            )
                    return

            if relevant_children:
                relevant_children.sort(
                    key=lambda child_id: entity_metadata[child_id]["name"]
                )
                for i, child_id in enumerate(relevant_children):
                    is_last_child = i == len(relevant_children) - 1
                    build_tree_recursive(
                        child_id, extension_prefix, is_last_child, False
                    )

        entities_with_acls = 0
        total_affected_children = 0
        for entity_id in entity_metadata.keys():
            metadata = entity_metadata[entity_id]
            has_local_acl = metadata.get("acl") is not None
            is_self_entity = hasattr(self, "id") and entity_id == self.id
            will_be_deleted = has_local_acl and (not is_self_entity or include_self)

            if will_be_deleted:
                entities_with_acls += 1
                affected_children_count = len(
                    benefactor_tracker.benefactor_children.get(entity_id, [])
                )
                total_affected_children += affected_children_count

        lines.append(
            f"📊 Summary: {entities_with_acls} entities with local ACLs to delete, "
            f"{total_affected_children + entities_with_acls} entities will change inheritance"
        )

        lines.append("")
        lines.append("🔍 Legend:")
        lines.append("  ⚠️  = Local ACL will be deleted")
        lines.append("  🚫 = Local ACL will NOT be deleted")
        lines.append("  ↗️ = Currently inherits permissions")
        lines.append("  🔒 = No local ACL (inherits from parent)")
        lines.append("  🔑 = Permission that will be removed")
        lines.append("  📍 = New inheritance after deletion")
        lines.append("")

        relevant_roots = [
            root_id
            for root_id in root_entities
            if is_entity_relevant(root_id) or has_relevant_descendants(root_id)
        ]

        if len(relevant_roots) == 1:
            build_tree_recursive(relevant_roots[0], "", True, True)
        else:
            relevant_roots.sort(
                key=lambda entity_id: entity_metadata[entity_id]["name"] or ""
            )
            for i, root_id in enumerate(relevant_roots):
                is_last_root = i == len(relevant_roots) - 1
                build_tree_recursive(root_id, "", is_last_root, True)

        return lines

    async def _log_acl_tree(
        self,
        acl_result: AclListResult,
        entities: List[Union["File", "Folder"]],
        client: Synapse,
    ) -> str:
        """
        Generate and log an ASCII tree representation of ACL results.

        Creates a hierarchical tree structure from the ACL results and displays
        entity hierarchies with their ACL permissions in a tree-like format using
        ASCII characters. This provides a clear view of the current permission
        structure across the entity hierarchy.

        Arguments:
            acl_result: The ACL list result containing entity ACL information.
            entities: List of entity objects that have been processed.
            client: The Synapse client instance for API calls and logging.
        """
        if not acl_result or not acl_result.all_entity_acls:
            client.logger.info("No ACL results to display in tree format.")
            return

        tree_structure = await self._build_acl_tree_structure(acl_result, entities)
        tree_output = await self._format_ascii_tree_async(
            tree_structure, synapse_client=client
        )

        client.logger.info("ACL Tree Structure:")
        client.logger.info(tree_output)
        return tree_output

    async def _build_acl_tree_structure(
        self, acl_result: AclListResult, entities: List[Union["File", "Folder"]]
    ) -> Dict[str, Any]:
        """
        Build a hierarchical tree structure from ACL results.

        Creates a comprehensive tree structure that includes entity metadata,
        parent-child relationships, and ACL information. This structure can be
        used by different formatting functions to display trees in various ways.

        Arguments:
            acl_result: The ACL list result containing entity ACL information.
            entities: List of entity objects to use for metadata instead of API calls.

        Returns:
            Dictionary containing entity_metadata, children_map, and root_entities.
        """
        entity_metadata = {}
        children_map = {}
        all_entity_ids = set()

        for entity_acl in acl_result.all_entity_acls:
            if entity_acl.entity_id:
                all_entity_ids.add(entity_acl.entity_id)

        entities_by_id = {}
        if entities:
            for entity in entities:
                if hasattr(entity, "id") and entity.id:
                    entities_by_id[entity.id] = entity
                    all_entity_ids.add(entity.id)

        for entity_id in all_entity_ids:
            if entity_id in entities_by_id:
                entity = entities_by_id[entity_id]
                entity_metadata[entity_id] = {
                    "name": getattr(entity, "name", f"Entity {entity_id}"),
                    "type": entity.__class__.__name__,
                    "parent_id": getattr(entity, "parent_id", None),
                    "acl": next(
                        (
                            acl
                            for acl in acl_result.all_entity_acls
                            if acl.entity_id == entity_id
                        ),
                        None,
                    ),
                }
            else:
                entity_metadata[entity_id] = {
                    "name": f"Entity {entity_id}",
                    "type": "Unknown",
                    "parent_id": None,
                    "acl": next(
                        (
                            acl
                            for acl in acl_result.all_entity_acls
                            if acl.entity_id == entity_id
                        ),
                        None,
                    ),
                }

        entities_needing_parents = set(all_entity_ids)
        while entities_needing_parents:
            current_entity_id = entities_needing_parents.pop()
            if current_entity_id in entity_metadata:
                parent_id = entity_metadata[current_entity_id]["parent_id"]
                if parent_id and parent_id not in entity_metadata:
                    if parent_id in entities_by_id:
                        parent_entity = entities_by_id[parent_id]
                        entity_metadata[parent_id] = {
                            "name": getattr(
                                parent_entity, "name", f"Entity {parent_id}"
                            ),
                            "type": parent_entity.__class__.__name__,
                            "parent_id": getattr(parent_entity, "parent_id", None),
                            "acl": None,
                        }
                        all_entity_ids.add(parent_id)
                        entities_needing_parents.add(parent_id)

        for entity_id in all_entity_ids:
            if entity_id in entity_metadata:
                parent_id = entity_metadata[entity_id]["parent_id"]
                if parent_id and parent_id in entity_metadata:
                    if parent_id not in children_map:
                        children_map[parent_id] = []
                    children_map[parent_id].append(entity_id)

        root_entities = []
        for entity_id in all_entity_ids:
            parent_id = entity_metadata[entity_id]["parent_id"]
            if not parent_id or parent_id not in all_entity_ids:
                root_entities.append(entity_id)

        if not root_entities:
            root_entities = list(all_entity_ids)

        if hasattr(self, "id") and self.id in all_entity_ids:
            if self.id in root_entities:
                root_entities.remove(self.id)
            root_entities.insert(0, self.id)

        return {
            "entity_metadata": entity_metadata,
            "children_map": children_map,
            "root_entities": root_entities,
        }

    async def _format_ascii_tree_async(
        self,
        tree_structure: Dict[str, Any],
        *,
        synapse_client: Optional["Synapse"] = None,
    ) -> str:
        """
        Format the tree structure as ASCII art with ACL status icons.

        Creates a visual tree representation showing entity hierarchies with
        ACL status indicators. Uses ASCII art characters to show parent-child
        relationships and displays permission information for each entity.

        Arguments:
            tree_structure: The tree structure dictionary.
            synapse_client: Synapse client for fetching user/group information.

        Returns:
            A string containing the ASCII tree representation with ACL status indicators.
        """
        entity_metadata = tree_structure["entity_metadata"]
        children_map = tree_structure["children_map"]
        root_entities = tree_structure["root_entities"]

        user_group_info_map = await self._fetch_user_group_info_from_tree(
            tree_structure,
            synapse_client or Synapse.get_client(synapse_client=synapse_client),
        )

        lines = []
        lines.append("🔍 Legend:")
        lines.append("  🔒 = No local ACL (inherits from parent)")
        lines.append("  🔑 = Local ACL (custom permissions)")
        lines.append("")

        def get_acl_status_icon(entity_id: str) -> str:
            """Get the appropriate ACL status icon for an entity."""
            metadata = entity_metadata[entity_id]
            acl = metadata.get("acl")
            return (
                "🔑"
                if (acl and hasattr(acl, "acl_entries") and acl.acl_entries)
                else "🔒"
            )

        def should_show_entity(entity_id: str) -> bool:
            """Determine if an entity should be shown in the tree."""
            metadata = entity_metadata[entity_id]
            acl = metadata.get("acl")

            if acl and hasattr(acl, "acl_entries") and acl.acl_entries:
                return True

            children = children_map.get(entity_id, [])
            return any(should_show_entity(child_id) for child_id in children)

        def format_entity_info(entity_id: str) -> str:
            """Format entity information with ACL status icon."""
            metadata = entity_metadata[entity_id]
            name = metadata["name"]
            entity_type = metadata["type"]
            acl_icon = get_acl_status_icon(entity_id)
            return f"{acl_icon} {name} ({entity_id}) [{entity_type}]"

        def format_principal_info(principal_id: str, permissions: List[str]) -> str:
            """Format principal information with permissions."""
            permission_list = permissions if permissions else ["None"]
            team_name = user_group_info_map.get(str(principal_id))

            if team_name:
                return (
                    f"   └── {team_name} ({principal_id}): {', '.join(permission_list)}"
                )
            else:
                return f"   └── Principal {principal_id}: {', '.join(permission_list)}"

        def build_tree_recursive(
            entity_id: str, prefix: str = "", is_last: bool = True
        ) -> None:
            """Recursively build the ASCII tree."""
            if not should_show_entity(entity_id):
                return

            current_prefix = "└── " if is_last else "├── "
            lines.append(f"{prefix}{current_prefix}{format_entity_info(entity_id)}")

            metadata = entity_metadata[entity_id]
            acl = metadata.get("acl")

            if acl and hasattr(acl, "acl_entries") and acl.acl_entries:
                extension_prefix = prefix + ("    " if is_last else "│   ")
                for acl_entry in acl.acl_entries:
                    principal_id = acl_entry.principal_id
                    permissions = acl_entry.permissions if acl_entry.permissions else []
                    lines.append(
                        f"{extension_prefix}{format_principal_info(principal_id, permissions)}"
                    )

            children = children_map.get(entity_id, [])
            if children:
                visible_children = [
                    child_id for child_id in children if should_show_entity(child_id)
                ]
                visible_children.sort(
                    key=lambda child_id: entity_metadata[child_id]["name"]
                )
                extension_prefix = prefix + ("    " if is_last else "│   ")

                for i, child_id in enumerate(visible_children):
                    is_last_child = i == len(visible_children) - 1
                    build_tree_recursive(child_id, extension_prefix, is_last_child)

        visible_roots = [
            root_id for root_id in root_entities if should_show_entity(root_id)
        ]

        if not visible_roots:
            lines.append("No entities with local ACLs found.")
            return "\n".join(lines)

        if len(visible_roots) == 1:
            build_tree_recursive(visible_roots[0])
        else:
            visible_roots.sort(key=lambda entity_id: entity_metadata[entity_id]["name"])
            for i, root_id in enumerate(visible_roots):
                is_last_root = i == len(visible_roots) - 1
                build_tree_recursive(root_id, "", is_last_root)

        return "\n".join(lines)

    async def _process_children_with_progress(
        self,
        client: Synapse,
        normalized_types: List[str],
        include_container_content: bool,
        recursive: bool,
        all_entities: List,
        all_acls: Dict[str, Dict[str, List[str]]],
        progress_bar: Optional[tqdm] = None,
    ) -> None:
        """
        Process children entities with optional progress tracking.

        Arguments:
            client: The Synapse client instance
            normalized_types: List of normalized entity types to process
            include_container_content: Whether to include container content
            recursive: Whether to process recursively
            all_entities: List to append entities to
            all_acls: Dictionary to store ACL results
            progress_bar: Progress bar for tracking
        """
        operations_completed = 0

        if not self._synced_from_synapse:
            if progress_bar:
                progress_bar.total += 1
                progress_bar.refresh()

            await self.sync_from_synapse_async(
                recursive=False,
                download_file=False,
                include_activity=False,
                synapse_client=client,
            )

            operations_completed += 1
            if progress_bar:
                progress_bar.update(1)

        if progress_bar:
            progress_bar.total += 1
            progress_bar.refresh()

        child_entities = await self._collect_entities(
            client=client,
            target_entity_types=normalized_types,
            include_container_content=include_container_content,
            recursive=recursive,
            collect_acls=True,
            collect_self=False,
            all_acls=all_acls,
            progress_bar=progress_bar,
        )

        operations_completed += 1
        if progress_bar:
            progress_bar.update(1)

        for entity in child_entities:
            if entity != self:
                all_entities.append(entity)
