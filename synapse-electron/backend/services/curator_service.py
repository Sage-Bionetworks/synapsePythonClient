"""
Curator service for Synapse Desktop Client.

This module provides the CuratorManager service class for handling
curation task and record set operations, including CSV template generation.
"""
import logging
from pathlib import Path
from typing import Any, Dict, Optional

# Import the CSV template generation function
from services.curator.extract_json_schema_titles import (
    extract_schema_properties_from_file,
)

from synapseclient import Synapse
from synapseclient.models import CurationTask, Folder, RecordSet
from synapseclient.models.curation import (
    FileBasedMetadataTaskProperties,
    RecordBasedMetadataTaskProperties,
)

# Set up logger for this module
logger = logging.getLogger(__name__)


class CuratorManager:
    """
    Handles all curator-related operations.

    Manages curation tasks, record sets, and CSV template generation
    for the Synapse platform, providing a clean interface between
    the GUI and the underlying synapseclient library.
    """

    def __init__(self) -> None:
        """
        Initialize the curator manager.

        Returns:
            None

        Raises:
            None: Initialization does not perform operations that could fail.
        """
        self.synapse_client: Optional[Synapse] = None

    def set_synapse_client(self, synapse_client: Synapse) -> None:
        """
        Set the Synapse client instance to use.

        Arguments:
            synapse_client: Authenticated Synapse client instance

        Returns:
            None

        Raises:
            None: This method does not raise exceptions.
        """
        self.synapse_client = synapse_client

    # Curation Task operations
    async def list_curation_tasks(self, project_id: str) -> Dict[str, Any]:
        """
        List all curation tasks for a given project.

        Arguments:
            project_id: The Synapse project ID to list tasks from

        Returns:
            Dict containing success status and list of tasks

        Raises:
            Exception: Various exceptions from synapseclient operations
        """
        try:
            logger.info(f"Listing curation tasks for project: {project_id}")

            tasks = []
            async for task_dict in CurationTask.list_async(
                project_id=project_id, synapse_client=self.synapse_client
            ):
                # Convert task dict to our response format
                task_info = self._convert_task_dict_to_info(task_dict)
                tasks.append(task_info)

            logger.info(f"Found {len(tasks)} curation tasks")
            return {"success": True, "tasks": tasks}

        except Exception as e:
            logger.error(f"Error listing curation tasks: {e}")
            return {"success": False, "error": str(e)}

    async def create_curation_task(
        self,
        project_id: str,
        data_type: str,
        instructions: str,
        task_type: str,
        upload_folder_id: Optional[str] = None,
        file_view_id: Optional[str] = None,
        record_set_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new curation task.

        Arguments:
            project_id: The Synapse project ID
            data_type: Data type name for the task
            instructions: Instructions for contributors
            task_type: Type of task ('file-based' or 'record-based')
            upload_folder_id: Upload folder ID for file-based tasks
            file_view_id: File view ID for file-based tasks
            record_set_id: Record set ID for record-based tasks

        Returns:
            Dict containing success status and task information

        Raises:
            Exception: Various exceptions from synapseclient operations
        """
        try:
            logger.info(f"Creating curation task for project: {project_id}")

            # Create task properties based on type
            if task_type == "file-based":
                task_properties = FileBasedMetadataTaskProperties(
                    upload_folder_id=upload_folder_id, file_view_id=file_view_id
                )
            elif task_type == "record-based":
                task_properties = RecordBasedMetadataTaskProperties(
                    record_set_id=record_set_id
                )
            else:
                raise ValueError(f"Invalid task type: {task_type}")

            # Create the curation task
            task = CurationTask(
                project_id=project_id,
                data_type=data_type,
                instructions=instructions,
                task_properties=task_properties,
            )

            stored_task = await task.store(synapse_client=self.synapse_client)
            task_info = self._convert_task_to_info(stored_task)

            logger.info(f"Created curation task with ID: {stored_task.task_id}")
            return {"success": True, "task": task_info}

        except Exception as e:
            logger.error(f"Error creating curation task: {e}")
            return {"success": False, "error": str(e)}

    async def update_curation_task(
        self,
        task_id: int,
        project_id: Optional[str] = None,
        data_type: Optional[str] = None,
        instructions: Optional[str] = None,
        task_type: Optional[str] = None,
        upload_folder_id: Optional[str] = None,
        file_view_id: Optional[str] = None,
        record_set_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update an existing curation task.

        Arguments:
            task_id: The task ID to update
            project_id: The Synapse project ID (optional)
            data_type: Data type name for the task (optional)
            instructions: Instructions for contributors (optional)
            task_type: Type of task ('file-based' or 'record-based') (optional)
            upload_folder_id: Upload folder ID for file-based tasks (optional)
            file_view_id: File view ID for file-based tasks (optional)
            record_set_id: Record set ID for record-based tasks (optional)

        Returns:
            Dict containing success status and updated task information

        Raises:
            Exception: Various exceptions from synapseclient operations
        """
        try:
            logger.info(f"Updating curation task: {task_id}")

            # Get the existing task first
            task = await CurationTask(task_id=task_id).get_async(
                synapse_client=self.synapse_client
            )

            # Update fields if provided
            if project_id is not None:
                task.project_id = project_id
            if data_type is not None:
                task.data_type = data_type
            if instructions is not None:
                task.instructions = instructions

            # Update task properties if task type is provided
            if task_type is not None:
                if task_type == "file-based":
                    task.task_properties = FileBasedMetadataTaskProperties(
                        upload_folder_id=upload_folder_id, file_view_id=file_view_id
                    )
                elif task_type == "record-based":
                    task.task_properties = RecordBasedMetadataTaskProperties(
                        record_set_id=record_set_id
                    )

            # Store the updated task
            updated_task = await task.store(synapse_client=self.synapse_client)
            task_info = self._convert_task_to_info(updated_task)

            logger.info(f"Updated curation task: {task_id}")
            return {"success": True, "task": task_info}

        except Exception as e:
            logger.error(f"Error updating curation task: {e}")
            return {"success": False, "error": str(e)}

    async def delete_curation_task(self, task_id: int) -> Dict[str, Any]:
        """
        Delete a curation task.

        Arguments:
            task_id: The task ID to delete

        Returns:
            Dict containing success status

        Raises:
            Exception: Various exceptions from synapseclient operations
        """
        try:
            logger.info(f"Deleting curation task: {task_id}")

            task = CurationTask(task_id=task_id)
            await task.delete(synapse_client=self.synapse_client)

            logger.info(f"Deleted curation task: {task_id}")
            return {"success": True}

        except Exception as e:
            logger.error(f"Error deleting curation task: {e}")
            return {"success": False, "error": str(e)}

    # Record Set operations
    async def enumerate_record_sets(
        self, container_id: str, recursive: bool = True
    ) -> Dict[str, Any]:
        """
        Enumerate record sets in a container.

        Arguments:
            container_id: Container ID (Project or Folder) to enumerate
            recursive: Whether to enumerate recursively

        Returns:
            Dict containing success status and list of record sets

        Raises:
            Exception: Various exceptions from synapseclient operations
        """
        try:
            logger.info(f"Enumerating record sets in container: {container_id}")

            # Get the container
            container = await Folder(id=container_id).get_async(
                synapse_client=self.synapse_client
            )

            record_sets = []

            # Walk through the container to find record sets
            async for dirpath, dirs, nondirs in container.walk(
                include_types=["recordset"],
                recursive=recursive,
                synapse_client=self.synapse_client,
            ):
                for entity_header in nondirs:
                    # Check if this is a RecordSet by trying to get it as one
                    try:
                        record_set = await RecordSet(id=entity_header.id).get_async(
                            synapse_client=self.synapse_client
                        )
                        record_set_info = self._convert_record_set_to_info(
                            record_set, dirpath[0]
                        )
                        record_sets.append(record_set_info)
                    except Exception:
                        # Not a RecordSet, skip
                        pass

            logger.info(f"Found {len(record_sets)} record sets")
            return {"success": True, "record_sets": record_sets}

        except Exception as e:
            logger.error(f"Error enumerating record sets: {e}")
            return {"success": False, "error": str(e)}

    async def create_record_set(
        self,
        name: str,
        parent_id: str,
        description: Optional[str] = None,
        csv_table_descriptor_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Create a new record set.

        Arguments:
            name: Name of the record set
            parent_id: Parent container ID
            description: Description of the record set
            csv_table_descriptor_id: CSV table descriptor ID

        Returns:
            Dict containing success status and record set information

        Raises:
            Exception: Various exceptions from synapseclient operations
        """
        try:
            logger.info(f"Creating record set: {name}")

            record_set = RecordSet(
                name=name, parent_id=parent_id, description=description
            )

            if csv_table_descriptor_id:
                logger.warning("CSV table descriptor setting is not yet implemented.")
                # TODO: Set CSV table descriptor when available
                pass

            stored_record_set = await record_set.store_async(
                synapse_client=self.synapse_client
            )
            record_set_info = self._convert_record_set_to_info(stored_record_set, "")

            logger.info(f"Created record set with ID: {stored_record_set.id}")
            return {"success": True, "record_set": record_set_info}

        except Exception as e:
            logger.error(f"Error creating record set: {e}")
            return {"success": False, "error": str(e)}

    async def update_record_set(
        self,
        record_set_id: str,
        name: Optional[str] = None,
        description: Optional[str] = None,
        csv_table_descriptor_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Update an existing record set.

        Arguments:
            record_set_id: Record set ID to update
            name: Name of the record set (optional)
            description: Description of the record set (optional)
            csv_table_descriptor_id: CSV table descriptor ID (optional)

        Returns:
            Dict containing success status and updated record set information

        Raises:
            Exception: Various exceptions from synapseclient operations
        """
        try:
            logger.info(f"Updating record set: {record_set_id}")

            # Get the existing record set
            record_set = await RecordSet(id=record_set_id).get_async(
                synapse_client=self.synapse_client
            )

            # Update fields if provided
            if name is not None:
                record_set.name = name
            if description is not None:
                record_set.description = description

            if csv_table_descriptor_id:
                logger.warning("CSV table descriptor setting is not yet implemented.")
                # TODO: Set CSV table descriptor when available
                pass

            # Store the updated record set
            updated_record_set = await record_set.store_async(
                synapse_client=self.synapse_client
            )
            record_set_info = self._convert_record_set_to_info(updated_record_set, "")

            logger.info(f"Updated record set: {record_set_id}")
            return {"success": True, "record_set": record_set_info}

        except Exception as e:
            logger.error(f"Error updating record set: {e}")
            return {"success": False, "error": str(e)}

    async def delete_record_set(
        self, record_set_id: str, version_only: bool = False
    ) -> Dict[str, Any]:
        """
        Delete a record set.

        Arguments:
            record_set_id: Record set ID to delete
            version_only: Whether to delete only the current version

        Returns:
            Dict containing success status

        Raises:
            Exception: Various exceptions from synapseclient operations
        """
        try:
            logger.info(f"Deleting record set: {record_set_id}")

            record_set = RecordSet(id=record_set_id)
            await record_set.delete_async(
                version_only=version_only, synapse_client=self.synapse_client
            )

            logger.info(f"Deleted record set: {record_set_id}")
            return {"success": True}

        except Exception as e:
            logger.error(f"Error deleting record set: {e}")
            return {"success": False, "error": str(e)}

    # CSV Template operations
    async def generate_csv_template(
        self, data_model_jsonld_path: str, schema_uri: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Generate CSV template from JSON-LD data model.

        Arguments:
            data_model_jsonld_path: Path to the JSON-LD data model file
            schema_uri: JSON schema URI (optional)

        Returns:
            Dict containing success status and CSV headers

        Raises:
            Exception: Various exceptions from file operations
        """
        try:
            logger.info(f"Generating CSV template from: {data_model_jsonld_path}")

            # Check if file exists
            if not Path(data_model_jsonld_path).exists():
                raise FileNotFoundError(
                    f"Data model file not found: {data_model_jsonld_path}"
                )

            # Extract schema properties and generate DataFrame
            df = extract_schema_properties_from_file(data_model_jsonld_path)
            headers = df.columns.tolist()

            logger.info(f"Generated CSV template with {len(headers)} columns")
            return {
                "success": True,
                "headers": headers,
                "preview_data": [],  # Could add preview data if needed
            }

        except Exception as e:
            logger.error(f"Error generating CSV template: {e}")
            return {"success": False, "error": str(e)}

    # Helper methods
    def _convert_task_dict_to_info(self, task_dict: Dict[str, Any]) -> Dict[str, Any]:
        """
        Convert task dictionary from API to task info format.

        Arguments:
            task_dict: Task dictionary from synapseclient API

        Returns:
            Dict with task information in API response format

        Raises:
            None: This method handles missing keys gracefully.
        """
        task_properties = task_dict.get("taskProperties", {})
        concrete_type = task_properties.get("concreteType", "")

        if "FileBasedMetadataTaskProperties" in concrete_type:
            task_type = "file-based"
            upload_folder_id = task_properties.get("uploadFolderId")
            file_view_id = task_properties.get("fileViewId")
            record_set_id = None
        else:
            task_type = "record-based"
            upload_folder_id = None
            file_view_id = None
            record_set_id = task_properties.get("recordSetId")

        return {
            "task_id": task_dict.get("taskId"),
            "project_id": task_dict.get("projectId"),
            "data_type": task_dict.get("dataType"),
            "instructions": task_dict.get("instructions"),
            "task_type": task_type,
            "upload_folder_id": upload_folder_id,
            "file_view_id": file_view_id,
            "record_set_id": record_set_id,
            "created_on": task_dict.get("createdOn"),
            "created_by": task_dict.get("createdBy"),
            "modified_on": task_dict.get("modifiedOn"),
            "modified_by": task_dict.get("modifiedBy"),
        }

    def _convert_task_to_info(self, task: CurationTask) -> Dict[str, Any]:
        """
        Convert CurationTask object to task info format.

        Arguments:
            task: CurationTask object

        Returns:
            Dict with task information in API response format

        Raises:
            None: This method handles missing attributes gracefully.
        """
        task_properties = task.task_properties

        if isinstance(task_properties, FileBasedMetadataTaskProperties):
            task_type = "file-based"
            upload_folder_id = task_properties.upload_folder_id
            file_view_id = task_properties.file_view_id
            record_set_id = None
        else:
            task_type = "record-based"
            upload_folder_id = None
            file_view_id = None
            record_set_id = getattr(task_properties, "record_set_id", None)

        return {
            "task_id": task.task_id,
            "project_id": task.project_id,
            "data_type": task.data_type,
            "instructions": task.instructions,
            "task_type": task_type,
            "upload_folder_id": upload_folder_id,
            "file_view_id": file_view_id,
            "record_set_id": record_set_id,
            "created_on": task.created_on,
            "created_by": task.created_by,
            "modified_on": task.modified_on,
            "modified_by": task.modified_by,
        }

    def _convert_record_set_to_info(
        self, record_set: RecordSet, path: str
    ) -> Dict[str, Any]:
        """
        Convert RecordSet object to record set info format.

        Arguments:
            record_set: RecordSet object
            path: Full hierarchical path

        Returns:
            Dict with record set information in API response format

        Raises:
            None: This method handles missing attributes gracefully.
        """
        return {
            "id": record_set.id,
            "name": record_set.name,
            "description": record_set.description,
            "parent_id": record_set.parent_id,
            "path": path,
            "created_on": record_set.created_on,
            "created_by": record_set.created_by,
            "modified_on": record_set.modified_on,
            "modified_by": record_set.modified_by,
        }
