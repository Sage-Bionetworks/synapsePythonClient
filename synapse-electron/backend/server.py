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
from typing import Any, Dict

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
synapse_client: SynapseClientManager = None
config_manager: ConfigManager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Manage application lifespan events.

    Handles startup and shutdown logic for the FastAPI application,
    including logging initialization and cleanup.
    """
    # Startup
    setup_logging()
    await initialize_logging()
    yield
    # Shutdown - add any cleanup here if needed


def create_app() -> FastAPI:
    """
    Create and configure the FastAPI application.

    Returns:
        Configured FastAPI application instance
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
async def poll_log_messages():
    """
    Poll for new log messages from the queue.

    Returns:
        JSON response containing queued log messages
    """
    try:
        messages = get_queued_messages()
        return {
            "success": True,
            "messages": [msg.dict() for msg in messages],
            "count": len(messages),
        }
    except Exception as e:
        logger.error("Error polling log messages: %s", e)
        return {"success": False, "messages": [], "count": 0, "error": str(e)}


# API Routes - Health and System


@app.get("/health")
async def health_check():
    """
    Health check endpoint for service monitoring.

    Returns:
        JSON response indicating service health status
    """
    return {"status": "healthy", "service": "synapse-backend"}


@app.get("/test/logging")
async def test_logging():
    """
    Test endpoint to verify logging functionality.

    Returns:
        JSON response confirming logging is working
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
async def get_home_directory():
    """
    Get the user's home and downloads directory paths.

    Returns:
        JSON response with directory paths

    Raises:
        HTTPException: If directories cannot be accessed
    """
    try:
        directories = get_home_and_downloads_directories()
        return directories
    except Exception as e:
        logger.error("Error getting home directory: %s", e)
        raise HTTPException(status_code=500, detail=str(e)) from e


# API Routes - Authentication


@app.get("/auth/profiles")
async def get_profiles():
    """
    Get available authentication profiles from configuration.

    Returns:
        JSON response with list of available profiles

    Raises:
        HTTPException: If profiles cannot be retrieved
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
async def login(request: LoginRequest):
    """
    Authenticate user with Synapse.

    Args:
        request: Login request containing authentication details and log level

    Returns:
        JSON response with authentication result

    Raises:
        HTTPException: If authentication fails
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

    Args:
        request: Login request containing authentication details

    Returns:
        Dictionary with authentication result
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
async def logout():
    """
    Logout current user.

    Returns:
        JSON response confirming logout

    Raises:
        HTTPException: If logout fails
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
async def download_file(request: DownloadRequest):
    """
    Download a file from Synapse.

    Args:
        request: Download request with file details

    Returns:
        JSON response confirming download started

    Raises:
        HTTPException: If download cannot be started
    """
    try:
        global synapse_client
        if not synapse_client or not synapse_client.is_logged_in:
            raise HTTPException(status_code=401, detail="Not authenticated")

        async def download_task():
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
async def upload_file(request: UploadRequest):
    """
    Upload a file to Synapse.

    Args:
        request: Upload request with file details

    Returns:
        JSON response confirming upload started

    Raises:
        HTTPException: If upload cannot be started
    """
    try:
        global synapse_client
        if not synapse_client or not synapse_client.is_logged_in:
            raise HTTPException(status_code=401, detail="Not authenticated")

        async def upload_task():
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
async def scan_directory(request: ScanDirectoryRequest):
    """
    Scan a directory for files to upload.

    Args:
        request: Directory scan request

    Returns:
        JSON response with file listing and summary

    Raises:
        HTTPException: If directory cannot be scanned
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
async def enumerate_container(request: EnumerateRequest):
    """
    Enumerate contents of a Synapse container.

    Args:
        request: Container enumeration request

    Returns:
        JSON response with container contents

    Raises:
        HTTPException: If enumeration fails
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


def _convert_bulk_items_to_dict(items):
    """
    Convert BulkItem objects to JSON-serializable dictionaries.

    Args:
        items: List of BulkItem objects or dictionaries

    Returns:
        List of dictionaries
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
async def bulk_download(request: BulkDownloadRequest):
    """
    Bulk download files from Synapse.

    Args:
        request: Bulk download request

    Returns:
        JSON response with download results

    Raises:
        HTTPException: If bulk download fails
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

        return _handle_bulk_download_result(result, file_items_data)

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Bulk download endpoint error")
        raise HTTPException(status_code=500, detail=str(e)) from e


def _filter_file_items(items):
    """Filter items to only include files for bulk download."""
    return [item for item in items if item.get("type", "file").lower() == "file"]


def _convert_dict_to_bulk_items(file_items_data):
    """Convert dictionary items to BulkItem objects."""
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


async def _handle_bulk_download_result(result, file_items_data):
    """Handle the result of bulk download operation."""
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
async def bulk_upload(request: BulkUploadRequest):
    """
    Bulk upload files to Synapse with proper folder hierarchy.

    Args:
        request: Bulk upload request

    Returns:
        JSON response with upload results

    Raises:
        HTTPException: If bulk upload fails
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


async def _perform_bulk_upload(request: BulkUploadRequest, file_items_data):
    """Perform the actual bulk upload operation."""

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


def _build_folder_hierarchy(request: BulkUploadRequest, file_items_data):
    """Build folder hierarchy for preserving structure during upload."""
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


def _add_file_to_folder_hierarchy(file_obj, relative_path, folder_map, parent_id):
    """Add a file to the appropriate folder in the hierarchy."""
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


def _create_upload_tasks(root_files, folder_map):
    """Create async upload tasks for all files and folders."""
    upload_tasks = []

    for file_obj in root_files:
        upload_tasks.append(_upload_item(file_obj, "file", file_obj.name))

    top_level_folders = [
        folder for path, folder in folder_map.items() if "/" not in path
    ]
    for folder_obj in top_level_folders:
        upload_tasks.append(_upload_item(folder_obj, "folder", folder_obj.name))

    return upload_tasks


async def _upload_item(item, item_type: str, item_name: str) -> Dict[str, Any]:
    """Upload a file or folder and return result."""
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


def _process_upload_results(upload_results, total_items, root_files, folder_map):
    """Process upload results and return final response."""
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


def main():
    """
    Main entry point for the backend server.

    Parses command line arguments and starts the server with
    appropriate configuration for the environment.
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
