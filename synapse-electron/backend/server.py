#!/usr/bin/env python3
"""
FastAPI backend server for Synapse Desktop Client.

This module provides the main REST API server that bridges the existing
Python Synapse functionality with the Electron frontend. It handles
authentication, file operations, and bulk operations through a clean
HTTP API interface.
"""

import argparse
import asyncio
import logging
import os
import sys
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

import uvicorn
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

# Add the parent directory to the path to import synapseclient modules
current_dir = Path(__file__).parent
parent_dir = current_dir.parent.parent
sys.path.insert(0, str(parent_dir))

try:
    from models import (  # API Models; Domain Models
        BulkDownloadRequest,
        BulkItemModel,
        BulkUploadRequest,
        DownloadRequest,
        EnumerateRequest,
        LoginRequest,
        ScanDirectoryRequest,
        UploadRequest,
    )
    from services import ConfigManager, SynapseClientManager
    from utils import (
        broadcast_message,
        get_home_and_downloads_directories,
        get_queued_messages,
        initialize_logging,
        run_async_task_in_background,
        scan_directory_for_files,
        setup_electron_environment,
        setup_logging,
        start_websocket_server,
    )
except ImportError as e:
    print(f"Error importing desktop client models: {e}")
    print(
        "Make sure you're running this from the correct directory and models are accessible"
    )
    sys.exit(1)

# Configure logging with default level
setup_logging("info")
logger = logging.getLogger(__name__)

# Global instances
synapse_client: Optional[SynapseClientManager] = None
config_manager: Optional[ConfigManager] = None


@asynccontextmanager
async def lifespan(app: FastAPI) -> Any:
    """
    Manage application lifespan events.

    Handles startup and shutdown logic for the FastAPI application,
    including logging initialization and cleanup.

    Arguments:
        app: The FastAPI application instance.

    Returns:
        An async context manager that handles startup and shutdown.

    Raises:
        Exception: If logging initialization fails during startup.
    """
    # Startup
    setup_logging()
    await initialize_logging()
    yield
    # Shutdown - add any cleanup here if needed


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.

    Sets up the FastAPI application with CORS middleware and proper configuration
    for the Synapse Desktop Client backend API.

    Arguments:
        None

    Returns:
        FastAPI: Configured FastAPI application instance with CORS middleware.

    Raises:
        Exception: If application creation or configuration fails.
    """
    app = FastAPI(
        title="Synapse Desktop Client API",
        description="Backend API for Synapse Desktop Client",
        version="0.1.0",
        lifespan=lifespan,
    )

    # Configure CORS for Electron app
    app.add_middleware(
        CORSMiddleware,
        allow_origins=["http://localhost:*", "file://*"],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    return app


app = create_app()


# API Routes - Logging


@app.get("/logs/poll")
async def poll_log_messages() -> Dict[str, Any]:
    """
    Poll for new log messages from the queue.

    Retrieves queued log messages from the logging system and returns them
    to the frontend for display in the log viewer.

    Arguments:
        None

    Returns:
        Dict[str, Any]: JSON response containing queued log messages with fields:
            - success: Boolean indicating if polling was successful
            - messages: List of log message dictionaries
            - count: Number of messages returned
            - error: Error message if polling failed (optional)

    Raises:
        Exception: If retrieving log messages fails, returns error in response.
    """
    try:
        messages = get_queued_messages()
        return {
            "success": True,
            "messages": [msg.model_dump() for msg in messages],
            "count": len(messages),
        }
    except Exception as e:
        logger.error("Error polling log messages: %s", e)
        return {"success": False, "messages": [], "count": 0, "error": str(e)}


# API Routes - Health and System


@app.get("/health")
async def health_check() -> Dict[str, str]:
    """
    Health check endpoint for service monitoring.

    Provides a simple health check endpoint that can be used by monitoring
    systems to verify that the backend service is running and responsive.

    Arguments:
        None

    Returns:
        Dict[str, str]: JSON response indicating service health status with fields:
            - status: Health status (always "healthy" if reachable)
            - service: Service name identifier

    Raises:
        None: This endpoint should always succeed if the service is running.
    """
    return {"status": "healthy", "service": "synapse-backend"}


@app.get("/test/logging")
async def test_logging() -> Dict[str, str]:
    """
    Test endpoint to verify logging functionality.

    Emits test log messages at different levels to verify that the logging
    system is working correctly and messages are being captured properly.

    Arguments:
        None

    Returns:
        Dict[str, str]: JSON response confirming logging test with fields:
            - message: Success message if test completed
            - error: Error message if test failed

    Raises:
        Exception: If logging test fails, returns error message in response.
    """
    try:
        logger.info("Test logging endpoint called")
        logger.debug(
            "Test debug message - should only appear if debug level is enabled"
        )
        return {"message": "Test logging messages sent"}
    except Exception as e:
        logger.error("Test logging failed: %s", e)
        return {"error": str(e)}


@app.get("/system/home-directory")
async def get_home_directory() -> Dict[str, str]:
    """
    Get the user's home and downloads directory paths.

    Retrieves the current user's home directory and downloads directory paths,
    creating the downloads directory if it doesn't exist.

    Arguments:
        None

    Returns:
        Dict[str, str]: JSON response with directory paths containing:
            - home_directory: User's home directory path
            - downloads_directory: User's downloads directory path

    Raises:
        HTTPException: If directories cannot be accessed or created.
    """
    try:
        directories = get_home_and_downloads_directories()
        return directories
    except Exception as e:
        logger.error("Error getting home directory: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


# API Routes - Authentication


@app.get("/auth/profiles")
async def get_profiles() -> Dict[str, List[Dict[str, str]]]:
    """
    Get available authentication profiles from configuration.

    Retrieves all available Synapse authentication profiles from the user's
    configuration file and formats them for display in the frontend.

    Arguments:
        None

    Returns:
        Dict[str, List[Dict[str, str]]]: JSON response with list of available profiles containing:
            - profiles: List of profile dictionaries with 'name' and 'display_name' fields

    Raises:
        HTTPException: If profiles cannot be retrieved from configuration.
    """
    try:
        global config_manager
        if not config_manager:
            config_manager = ConfigManager()

        profiles = config_manager.get_available_profiles()
        logger.info("Available profiles: %s", profiles)

        profile_data = []
        for profile in profiles:
            profile_data.append(
                {"name": profile, "display_name": profile.replace("_", " ").title()}
            )

        return {"profiles": profile_data}
    except Exception as e:
        logger.error("Error getting profiles: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/auth/login")
async def login(request: LoginRequest) -> Dict[str, str]:
    """
    Authenticate user with Synapse.

    Handles both manual authentication (with username/token) and profile-based
    authentication using stored configuration profiles.

    Arguments:
        request: Login request containing authentication details and mode

    Returns:
        Dict[str, str]: JSON response with authentication result containing:
            - username: Authenticated username
            - name: Display name (same as username)
            - user_id: User ID (empty string for compatibility)
            - token: Authentication status token

    Raises:
        HTTPException: If authentication fails or required parameters are missing.
    """
    try:
        global synapse_client, config_manager
        if not synapse_client:
            synapse_client = SynapseClientManager()
        if not config_manager:
            config_manager = ConfigManager()

        result = await _perform_authentication(request)

        if result["success"]:
            return {
                "username": result.get("username", request.username),
                "name": result.get("username", request.username),
                "user_id": "",
                "token": "authenticated",
            }
        else:
            raise HTTPException(status_code=401, detail=result["error"])

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Login error: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


async def _perform_authentication(request: LoginRequest) -> Dict[str, Any]:
    """
    Perform the actual authentication based on request mode.

    Handles the authentication logic for both manual and profile-based login modes.

    Arguments:
        request: Login request containing authentication details and mode

    Returns:
        Dict[str, Any]: Dictionary with authentication result containing:
            - success: Boolean indicating if authentication was successful
            - username: Username if successful
            - error: Error message if authentication failed

    Raises:
        HTTPException: If required parameters are missing for the authentication mode.
    """
    # Use default (non-debug) mode
    debug_mode = False

    if request.mode == "manual":
        if not request.username or not request.token:
            raise HTTPException(
                status_code=400, detail="Username and token are required"
            )
        return await synapse_client.login_manual(
            request.username, request.token, debug_mode
        )
    else:
        if not request.profile:
            raise HTTPException(status_code=400, detail="Profile is required")
        return await synapse_client.login_with_profile(request.profile, debug_mode)


@app.post("/auth/logout")
async def logout() -> Dict[str, str]:
    """
    Logout current user.

    Terminates the current Synapse session and clears authentication state
    for the authenticated user.

    Arguments:
        None

    Returns:
        Dict[str, str]: JSON response confirming logout with:
            - message: Confirmation message of successful logout

    Raises:
        HTTPException: If logout operation fails.
    """
    try:
        global synapse_client
        if synapse_client:
            synapse_client.logout()
        return {"message": "Logged out successfully"}
    except Exception as e:
        logger.error("Logout error: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


# API Routes - File Operations


@app.post("/files/download")
async def download_file(request: DownloadRequest) -> Dict[str, str]:
    """
    Download a file from Synapse.

    Initiates a background download task for the specified Synapse entity.
    The download runs asynchronously to avoid blocking the API response.

    Arguments:
        request: Download request with file details including synapse_id, version, and download_path

    Returns:
        Dict[str, str]: JSON response confirming download started with:
            - message: Confirmation that download has started
            - synapse_id: The Synapse ID being downloaded

    Raises:
        HTTPException: If user is not authenticated or download cannot be started.
    """
    try:
        global synapse_client
        if not synapse_client or not synapse_client.is_logged_in:
            raise HTTPException(status_code=401, detail="Not authenticated")

        async def download_task() -> None:
            try:
                logger.info("Starting download of %s", request.synapse_id)

                result = await synapse_client.download_file(
                    synapse_id=request.synapse_id,
                    version=int(request.version) if request.version else None,
                    download_path=request.download_path,
                    progress_callback=None,
                    detail_callback=None,
                )

                if result["success"]:
                    logger.info("✅ Successfully downloaded %s", request.synapse_id)
                    logger.info(
                        "Downloaded to %s", result.get("path", "download location")
                    )
                else:
                    logger.error("❌ Download failed: %s", result["error"])
            except Exception as e:
                logger.exception("Download task error: %s", e)
                logger.error("❌ Download error: %s", str(e))
            finally:
                logger.info("Download task completed for %s", request.synapse_id)

        run_async_task_in_background(download_task, f"download_{request.synapse_id}")
        return {"message": "Download started", "synapse_id": request.synapse_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Download endpoint error: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/files/upload")
async def upload_file(request: UploadRequest) -> Dict[str, str]:
    """
    Upload a file to Synapse.

    Initiates a background upload task for the specified file. Supports both
    creating new entities and updating existing ones based on the request mode.

    Arguments:
        request: Upload request with file details including file_path, mode, parent_id/entity_id, and name

    Returns:
        Dict[str, str]: JSON response confirming upload started with:
            - message: Confirmation that upload has started
            - file_path: The local file path being uploaded

    Raises:
        HTTPException: If user is not authenticated or upload cannot be started.
    """
    try:
        global synapse_client
        if not synapse_client or not synapse_client.is_logged_in:
            raise HTTPException(status_code=401, detail="Not authenticated")

        async def upload_task() -> None:
            try:
                logger.info("Starting upload of %s", request.file_path)

                result = await synapse_client.upload_file(
                    file_path=request.file_path,
                    parent_id=request.parent_id if request.mode == "new" else None,
                    entity_id=request.entity_id if request.mode == "update" else None,
                    name=request.name,
                    progress_callback=None,
                    detail_callback=None,
                )

                if result["success"]:
                    logger.info("✅ Successfully uploaded %s", request.file_path)
                    logger.info("Uploaded as %s", result.get("entity_id", "new entity"))
                else:
                    logger.error("❌ Upload failed: %s", result["error"])
            except Exception as e:
                logger.exception("Upload task error: %s", e)
                logger.error("❌ Upload error: %s", str(e))
            finally:
                logger.info("Upload task completed for %s", request.file_path)

        run_async_task_in_background(
            upload_task, f"upload_{os.path.basename(request.file_path)}"
        )
        return {"message": "Upload started", "file_path": request.file_path}

    except HTTPException:
        raise
    except Exception as e:
        logger.error("Upload endpoint error: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


# API Routes - Bulk Operations


@app.post("/files/scan-directory")
async def scan_directory(request: ScanDirectoryRequest) -> Dict[str, Any]:
    """
    Scan a directory for files to upload.

    Recursively scans the specified directory and returns file metadata
    for all files found, including size, type, and path information.

    Arguments:
        request: Directory scan request with directory_path and recursive flag

    Returns:
        Dict[str, Any]: JSON response with file listing and summary containing:
            - success: Boolean indicating scan success
            - files: List of file metadata dictionaries
            - summary: Summary statistics about scanned files

    Raises:
        HTTPException: If directory doesn't exist, is not a directory, or scanning fails.
    """
    try:
        result = scan_directory_for_files(request.directory_path, request.recursive)
        return result
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e)) from e
    except Exception as e:
        logger.error("Directory scan error: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


@app.post("/bulk/enumerate")
async def enumerate_container(
    request: EnumerateRequest,
) -> Dict[str, List[Dict[str, Any]]]:
    """
    Enumerate contents of a Synapse container.

    Lists all files and folders within the specified Synapse container (Project or Folder),
    with optional recursive enumeration of subdirectories.

    Arguments:
        request: Container enumeration request with container_id and recursive flag

    Returns:
        Dict[str, List[Dict[str, Any]]]: JSON response with container contents containing:
            - items: List of item dictionaries with metadata (id, name, type, size, etc.)

    Raises:
        HTTPException: If user is not authenticated or enumeration fails.
    """
    try:
        global synapse_client
        if not synapse_client or not synapse_client.is_logged_in:
            raise HTTPException(status_code=401, detail="Not authenticated")

        logger.info("Enumerating container %s", request.container_id)

        result = await synapse_client.enumerate_container(
            request.container_id, request.recursive
        )

        if result["success"]:
            items = _convert_bulk_items_to_dict(result["items"])
            logger.info("Found %d items in container", len(items))
            return {"items": items}
        else:
            raise HTTPException(status_code=500, detail=result["error"])

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Enumerate error: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


def _convert_bulk_items_to_dict(items: List[Any]) -> List[Dict[str, Any]]:
    """
    Convert BulkItem objects to JSON-serializable dictionaries.

    Transforms BulkItem objects or existing dictionaries into a standardized
    dictionary format suitable for JSON serialization and frontend consumption.

    Arguments:
        items: List of BulkItem objects or dictionaries to convert

    Returns:
        List[Dict[str, Any]]: List of dictionaries with standardized item metadata including:
            - id: Item Synapse ID
            - name: Item name
            - type: Item type (file/folder)
            - size: File size (for files)
            - parent_id: Parent container ID
            - path: Item path

    Raises:
        None: Handles conversion errors gracefully by preserving existing dictionaries.
    """
    converted_items = []
    for item in items:
        if hasattr(item, "synapse_id"):  # BulkItem object
            converted_items.append(
                {
                    "id": item.synapse_id,
                    "name": item.name,
                    "type": item.item_type.lower(),
                    "size": item.size,
                    "parent_id": item.parent_id,
                    "path": item.path if item.path else "",
                }
            )
        else:  # Already a dict
            converted_items.append(item)
    return converted_items


@app.post("/bulk/download")
async def bulk_download(request: BulkDownloadRequest) -> Dict[str, Any]:
    """
    Bulk download files from Synapse.

    Downloads multiple files from Synapse to the specified local directory.
    Only file items are processed; folders are filtered out automatically.

    Arguments:
        request: Bulk download request with items list, download_path, and options

    Returns:
        Dict[str, Any]: JSON response with download results containing:
            - success: Boolean indicating overall operation success
            - message: Summary message
            - item_count: Number of items processed
            - summary: Detailed operation summary

    Raises:
        HTTPException: If user is not authenticated, no files selected, or download fails.
    """
    logger.info("Bulk download endpoint")
    try:
        global synapse_client
        if not synapse_client or not synapse_client.is_logged_in:
            raise HTTPException(status_code=401, detail="Not authenticated")

        file_items_data = _filter_file_items(request.items)

        if not file_items_data:
            raise HTTPException(
                status_code=400,
                detail="No files selected for download. Only files can be bulk downloaded.",
            )

        logger.info(
            "Starting bulk download of %d files (filtered from %d selected items)",
            len(file_items_data),
            len(request.items),
        )

        bulk_items = _convert_dict_to_bulk_items(file_items_data)

        result = await synapse_client.bulk_download(
            items=bulk_items,
            download_path=request.download_path,
            recursive=request.create_subfolders,
            progress_callback=None,
            detail_callback=None,
        )

        return await _handle_bulk_download_result(result, file_items_data)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Bulk download endpoint error")
        raise HTTPException(status_code=500, detail=str(e)) from e


def _filter_file_items(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Filter items to only include files for bulk download.

    Removes non-file items (folders, etc.) from the items list since
    bulk download operations only support files.

    Arguments:
        items: List of item dictionaries that may contain files and folders

    Returns:
        List[Dict[str, Any]]: Filtered list containing only file items

    Raises:
        None: This function does not raise exceptions.
    """
    return [item for item in items if item.get("type", "file").lower() == "file"]


def _convert_dict_to_bulk_items(
    file_items_data: List[Dict[str, Any]]
) -> List[BulkItemModel]:
    """
    Convert dictionary items to BulkItem objects.

    Transforms dictionary representations of file items into BulkItemModel
    objects that can be used by the Synapse client for bulk operations.

    Arguments:
        file_items_data: List of file item dictionaries with metadata

    Returns:
        List[BulkItemModel]: List of BulkItemModel objects ready for bulk operations

    Raises:
        None: This function does not raise exceptions.
    """
    bulk_items = []
    for item_data in file_items_data:
        bulk_item = BulkItemModel(
            synapse_id=item_data["id"],
            name=item_data["name"],
            item_type=item_data.get("type", "file"),
            size=item_data.get("size"),
            parent_id=item_data.get("parent_id"),
            path=item_data.get("path", ""),
        )
        bulk_items.append(bulk_item)
    return bulk_items


async def _handle_bulk_download_result(
    result: Dict[str, Any], file_items_data: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Handle the result of bulk download operation.

    Processes the result from a bulk download operation and broadcasts
    completion messages via WebSocket to notify the frontend.

    Arguments:
        result: The result dictionary from the bulk download operation
        file_items_data: The original file items data that were downloaded

    Returns:
        Dict[str, Any]: A response dictionary with the result of the bulk download operation containing:
            - success: Boolean indicating operation success
            - message: Summary message
            - item_count: Number of items processed
            - summary: Detailed operation summary

    Raises:
        HTTPException: If the bulk download operation failed.
    """
    if result["success"]:
        logger.info("Successfully downloaded %d files", len(file_items_data))
        await broadcast_message(
            {
                "type": "complete",
                "operation": "bulk-download",
                "success": True,
                "data": {"message": f"Downloaded {len(file_items_data)} files"},
            }
        )
        return {
            "success": True,
            "message": "Bulk download completed successfully",
            "item_count": len(file_items_data),
            "summary": result.get(
                "summary", f"Downloaded {len(file_items_data)} files"
            ),
        }
    else:
        logger.error("Bulk download failed: %s", result["error"])
        await broadcast_message(
            {
                "type": "complete",
                "operation": "bulk-download",
                "success": False,
                "data": {"error": result["error"]},
            }
        )
        raise HTTPException(status_code=500, detail=result["error"])


@app.post("/bulk/upload")
async def bulk_upload(request: BulkUploadRequest) -> Dict[str, Any]:
    """
    Bulk upload files to Synapse with proper folder hierarchy.

    Uploads multiple files to Synapse with optional preservation of the
    local folder structure. Only file items are processed for upload.

    Arguments:
        request: Bulk upload request with parent_id, files list, and options

    Returns:
        Dict[str, Any]: JSON response with upload results containing:
            - success: Boolean indicating overall operation success
            - message: Summary message
            - successful_uploads: Number of successful uploads
            - failed_uploads: Number of failed uploads
            - total_items: Total number of items processed
            - summary: Detailed operation summary

    Raises:
        HTTPException: If user is not authenticated, no files selected, or upload fails.
    """
    try:
        global synapse_client
        if not synapse_client or not synapse_client.is_logged_in:
            raise HTTPException(status_code=401, detail="Not authenticated")

        file_items_data = _filter_file_items(request.files)

        if not file_items_data:
            raise HTTPException(
                status_code=400,
                detail="No files selected for upload. Only files can be bulk uploaded.",
            )

        logger.info(
            "Starting bulk upload of %d files (filtered from %d selected items)",
            len(file_items_data),
            len(request.files),
        )

        result = await _perform_bulk_upload(request, file_items_data)
        return result

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Bulk upload endpoint error")
        logger.info("Bulk upload error: %s", str(e))
        await broadcast_message(
            {
                "type": "complete",
                "operation": "bulk-upload",
                "success": False,
                "data": {"error": str(e)},
            }
        )
        raise HTTPException(status_code=500, detail=str(e)) from e


async def _perform_bulk_upload(
    request: BulkUploadRequest, file_items_data: List[Dict[str, Any]]
) -> Dict[str, Any]:
    """
    Perform the actual bulk upload operation.

    Handles the complete bulk upload process including folder hierarchy creation
    and file uploads with proper parent-child relationships.

    Arguments:
        request: The bulk upload request with configuration options
        file_items_data: List of filtered file items to upload

    Returns:
        Dict[str, Any]: Upload result summary with success counts and details

    Raises:
        HTTPException: If no valid files found to upload or upload preparation fails.
    """

    folder_map, root_files = _build_folder_hierarchy(request, file_items_data)

    total_items = len(root_files) + len(
        [f for f in folder_map.values() if "/" not in f.name]
    )
    if total_items == 0:
        raise HTTPException(status_code=400, detail="No valid files found to upload")

    logger.info(
        "Created folder hierarchy: %d folders, %d root files",
        len([f for f in folder_map.values() if "/" not in f.name]),
        len(root_files),
    )

    upload_tasks = _create_upload_tasks(root_files, folder_map)
    upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)

    return _process_upload_results(upload_results, total_items, root_files, folder_map)


def _build_folder_hierarchy(
    request: BulkUploadRequest, file_items_data: List[Dict[str, Any]]
) -> tuple[Dict[str, Any], List[Any]]:
    """
    Build folder hierarchy for preserving structure during upload.

    Creates the necessary folder structure in memory before upload to preserve
    the local directory hierarchy in Synapse when requested.

    Arguments:
        request: The bulk upload request with hierarchy preservation settings
        file_items_data: List of file items with path information

    Returns:
        tuple[Dict[str, Any], List[Any]]: A tuple containing:
            - folder_map: Dictionary mapping folder paths to folder objects
            - root_files: List of files that belong at the root level

    Raises:
        None: Skips files with invalid paths and logs warnings.
    """
    from synapseclient.models.file import File

    folder_map = {}
    root_files = []

    for file_data in file_items_data:
        file_path = file_data.get("path")
        relative_path = file_data.get("relative_path", "")

        if not file_path or not os.path.exists(file_path):
            logger.info(
                "Skipping file with invalid path: %s", file_data.get("name", "Unknown")
            )
            continue

        file_obj = File(
            path=file_path,
            name=file_data.get("name"),
            description=file_data.get("description", None),
        )

        if request.preserve_folder_structure and relative_path:
            _add_file_to_folder_hierarchy(
                file_obj, relative_path, folder_map, request.parent_id
            )
        else:
            file_obj.parent_id = request.parent_id
            root_files.append(file_obj)

    return folder_map, root_files


def _add_file_to_folder_hierarchy(
    file_obj: Any, relative_path: str, folder_map: Dict[str, Any], parent_id: str
) -> None:
    """
    Add a file to the appropriate folder in the hierarchy.

    Processes the relative path to create the necessary folder structure and
    assigns the file to the correct parent folder.

    Arguments:
        file_obj: The file object to add to the hierarchy
        relative_path: The relative path of the file from the upload root
        folder_map: Dictionary tracking created folders by path
        parent_id: The parent ID for the root of the hierarchy

    Returns:
        None: Modifies folder_map and file_obj in place

    Raises:
        None: This function handles errors gracefully.
    """
    from synapseclient.models.folder import Folder

    relative_path = relative_path.replace("\\", "/").strip("/")
    path_parts = relative_path.split("/")

    if len(path_parts) > 1:
        folder_parts = path_parts[:-1]
        current_path = ""
        current_parent_id = parent_id

        for i, folder_name in enumerate(folder_parts):
            if current_path:
                current_path += "/"
            current_path += folder_name

            if current_path not in folder_map:
                folder_obj = Folder(
                    name=folder_name,
                    parent_id=current_parent_id,
                    files=[],
                    folders=[],
                )
                folder_map[current_path] = folder_obj

                if i > 0:
                    parent_path = "/".join(folder_parts[:i])
                    if parent_path in folder_map:
                        folder_map[parent_path].folders.append(folder_obj)

            current_parent_id = None

        folder_path = "/".join(folder_parts)
        if folder_path in folder_map:
            folder_map[folder_path].files.append(file_obj)


def _create_upload_tasks(
    root_files: List[Any], folder_map: Dict[str, Any]
) -> List[Any]:
    """
    Create async upload tasks for all files and folders.

    Generates a list of async tasks that can be executed concurrently to
    upload all files and create all folders in the hierarchy.

    Arguments:
        root_files: List of files that belong at the root level
        folder_map: Dictionary mapping folder paths to folder objects

    Returns:
        List[Any]: List of async tasks for uploading items

    Raises:
        None: This function does not raise exceptions.
    """
    upload_tasks = []

    for file_obj in root_files:
        upload_tasks.append(_upload_item(file_obj, "file", file_obj.name))

    top_level_folders = [
        folder for path, folder in folder_map.items() if "/" not in path
    ]
    for folder_obj in top_level_folders:
        upload_tasks.append(_upload_item(folder_obj, "folder", folder_obj.name))

    return upload_tasks


async def _upload_item(item: Any, item_type: str, item_name: str) -> Dict[str, Any]:
    """
    Upload a file or folder and return result.

    Performs the actual upload of a single item (file or folder) to Synapse
    and returns detailed result information.

    Arguments:
        item: The item object (File or Folder) to upload
        item_type: Type of item being uploaded ("file" or "folder")
        item_name: Display name of the item for logging

    Returns:
        Dict[str, Any]: Upload result dictionary containing:
            - success: Boolean indicating upload success
            - item_name: Name of the item
            - item_type: Type of the item
            - entity_id: Synapse ID of uploaded item (if successful)
            - error: Error message (if failed)
            - path: Local file path (if applicable)

    Raises:
        Exception: Catches and handles upload errors, returning them in result dict.
    """
    try:
        logger.info("Uploading %s: %s", item_type, item_name)

        stored_item = await item.store_async(synapse_client=synapse_client.client)

        return {
            "success": True,
            "item_name": item_name,
            "item_type": item_type,
            "entity_id": stored_item.id,
            "path": getattr(item, "path", None),
        }
    except Exception as e:
        logger.error("Failed to upload %s: %s", item_name, str(e))
        return {
            "success": False,
            "item_name": item_name,
            "item_type": item_type,
            "error": str(e),
            "path": getattr(item, "path", None),
        }


def _process_upload_results(
    upload_results: List[Any],
    total_items: int,
    root_files: List[Any],
    folder_map: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Process upload results and return final response.

    Analyzes the results from all upload tasks and compiles a summary
    response with success/failure counts and details.

    Arguments:
        upload_results: List of results from upload tasks (may include exceptions)
        total_items: Total number of items that were attempted to upload
        root_files: List of root-level files that were uploaded
        folder_map: Dictionary of folders that were created

    Returns:
        Dict[str, Any]: Final upload summary containing:
            - success: Always True (individual failures are counted)
            - message: Summary message
            - successful_uploads: Number of successful uploads
            - failed_uploads: Number of failed uploads
            - total_items: Total number of items processed
            - summary: Detailed summary string

    Raises:
        None: This function processes exceptions and includes them in results.
    """
    processed_results = []
    for i, result in enumerate(upload_results):
        if isinstance(result, Exception):
            item_name = "Unknown"
            if i < len(root_files):
                item_name = root_files[i].name
            elif i - len(root_files) < len(
                [f for f in folder_map.values() if "/" not in f.name]
            ):
                top_level_folders = [
                    f for f in folder_map.values() if "/" not in f.name
                ]
                item_name = top_level_folders[i - len(root_files)].name

            processed_results.append(
                {"success": False, "item_name": item_name, "error": str(result)}
            )
        else:
            processed_results.append(result)

    successful_uploads = sum(1 for r in processed_results if r.get("success", False))
    failed_uploads = total_items - successful_uploads

    logger.info(
        "Bulk upload completed: %d successful, %d failed",
        successful_uploads,
        failed_uploads,
    )

    return {
        "success": True,
        "message": "Bulk upload completed successfully",
        "successful_uploads": successful_uploads,
        "failed_uploads": failed_uploads,
        "total_items": total_items,
        "summary": f"Uploaded {successful_uploads} items, {failed_uploads} failed",
    }


def main() -> None:
    """
    Main entry point for the backend server.

    Parses command line arguments and starts the FastAPI server with WebSocket
    support and appropriate configuration for the Synapse Desktop Client environment.

    Arguments:
        None: Uses command line arguments via argparse

    Returns:
        None: Runs the server until interrupted

    Raises:
        SystemExit: If argument parsing fails or server startup fails.
    """
    parser = argparse.ArgumentParser(description="Synapse Desktop Client Backend")
    parser.add_argument("--host", default="127.0.0.1", help="Host to bind to")
    parser.add_argument("--port", type=int, default=8000, help="Port to bind to")
    parser.add_argument("--ws-port", type=int, default=8001, help="WebSocket port")
    parser.add_argument("--reload", action="store_true", help="Enable auto-reload")

    args = parser.parse_args()

    # Setup environment for proper operation when launched from Electron
    setup_electron_environment()

    # Start WebSocket server
    start_websocket_server(args.ws_port)

    # Start FastAPI server
    logger.info("Starting Synapse Backend Server on %s:%s", args.host, args.port)
    logger.info("WebSocket server on ws://%s:%s", args.host, args.ws_port)

    uvicorn.run(
        app, host=args.host, port=args.port, reload=args.reload, log_level="info"
    )


if __name__ == "__main__":
    main()
