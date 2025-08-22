#!/usr/bin/env python3
"""
FastAPI backend server for Synapse Desktop Client
Bridges the existing Python Synapse functionality with the Electron frontend
"""

import argparse
import asyncio
import json
import logging
import os
import sys
import threading
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Any, Dict, List, Optional

import uvicorn
import websockets
from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# Add the parent directory to the path to import synapsegui modules
current_dir = Path(__file__).parent
parent_dir = current_dir.parent.parent  # Go up two levels to reach the project root
sys.path.insert(0, str(parent_dir))

try:
    # Import existing Synapse functionality
    from synapsegui.models.bulk_item import BulkItem
    from synapsegui.models.config import ConfigManager
    from synapsegui.models.synapse_client import SynapseClientManager
except ImportError as e:
    print(f"Error importing synapsegui modules: {e}")
    print(
        "Make sure you're running this from the correct directory and synapsegui is accessible"
    )
    sys.exit(1)

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# WebSocket clients set
connected_clients = set()


# Custom logging handler to forward logs to Electron
class ElectronLogHandler(logging.Handler):
    """Custom logging handler that forwards logs to Electron via WebSocket"""

    def emit(self, record):
        """Send log record to Electron frontend"""
        try:
            # Skip WebSocket-related logs to prevent feedback loops
            if record.name.startswith("websockets"):
                return

            # Format the log message
            message = self.format(record)

            # Determine log level
            level_mapping = {
                logging.DEBUG: "debug",
                logging.INFO: "info",
                logging.WARNING: "warning",
                logging.ERROR: "error",
                logging.CRITICAL: "critical",
            }
            level = level_mapping.get(record.levelno, "info")

            # Create enhanced log message
            log_data = {
                "type": "log",
                "message": message,
                "level": level,
                "logger_name": record.name,
                "timestamp": record.created,
                "source": "python-logger",
                "auto_scroll": True,
                "raw_message": record.getMessage(),  # Original message without formatting
                "filename": getattr(record, "filename", ""),
                "line_number": getattr(record, "lineno", 0),
            }

            # Send to Electron via REST polling instead of WebSocket
            try:
                # Simply add all log messages to the queue for UI polling
                log_message_queue.append(log_data)
                # Keep queue size reasonable - remove old messages if it gets too large
                if len(log_message_queue) > 1000:
                    log_message_queue.pop(0)

            except Exception as e:
                # Don't let logging errors crash the app - just print to console
                print(f"Error queuing log message: {e}")
        except Exception as e:
            # Don't let logging errors crash the app
            print(f"Error in ElectronLogHandler: {e}")


# Simple message queue for logs to avoid asyncio issues
log_message_queue = []


# Set up the custom handler for the root logger to catch all logs
electron_handler = ElectronLogHandler()
electron_handler.setLevel(logging.DEBUG)
formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
electron_handler.setFormatter(formatter)

# Add handler only to root logger to catch all log messages
root_logger = logging.getLogger()
root_logger.addHandler(electron_handler)
root_logger.setLevel(logging.INFO)

# Explicitly set WebSocket logger to WARNING to prevent debug spam
websockets_logger = logging.getLogger("websockets")
websockets_logger.setLevel(logging.WARNING)

# Pydantic models for API requests


class LoginRequest(BaseModel):
    mode: str  # 'manual' or 'config'
    username: Optional[str] = None
    token: Optional[str] = None
    profile: Optional[str] = None


class DownloadRequest(BaseModel):
    synapse_id: str
    version: Optional[str] = None
    download_path: str


class UploadRequest(BaseModel):
    file_path: str
    mode: str  # 'new' or 'update'
    parent_id: Optional[str] = None
    entity_id: Optional[str] = None
    name: Optional[str] = None


class EnumerateRequest(BaseModel):
    container_id: str
    recursive: bool = True


class BulkDownloadRequest(BaseModel):
    items: List[Dict[str, Any]]
    download_path: str
    create_subfolders: bool = True


class BulkUploadRequest(BaseModel):
    parent_id: str
    files: List[Dict[str, Any]]  # List of file objects with path, name, size etc
    preserve_folder_structure: bool = True


class ScanDirectoryRequest(BaseModel):
    directory_path: str
    recursive: bool = True


# Global synapse client manager instance
synapse_client = None
config_manager = None


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan events"""
    # Startup
    # Give a moment for WebSocket connections to establish
    await asyncio.sleep(1)
    await initialize_logging()

    yield

    # Shutdown - add any cleanup here if needed
    pass


def create_app() -> FastAPI:
    """Create and configure the FastAPI application"""
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

# WebSocket connection handler


async def handle_websocket_client(websocket, path=None):
    """Handle WebSocket connections from Electron frontend"""
    connected_clients.add(websocket)
    logger.info(
        f"WebSocket client connected from path: {path}. Total clients: {len(connected_clients)}"
    )

    # Send connection status
    try:
        await websocket.send(
            json.dumps({"type": "connection_status", "connected": True})
        )

        async for message in websocket:
            # Handle incoming WebSocket messages if needed
            logger.info(f"Received WebSocket message: {message}")
    except websockets.exceptions.ConnectionClosed:
        logger.info("WebSocket client disconnected")
    except Exception as e:
        logger.error(f"WebSocket error: {e}")
    finally:
        connected_clients.discard(websocket)
        logger.info(
            f"WebSocket client removed. Total clients: {len(connected_clients)}"
        )


async def broadcast_message(message: dict):
    """Broadcast message to all connected WebSocket clients"""
    if not connected_clients:
        # Only log once per message type to avoid spam
        message_type = message.get("type", "unknown")
        if message_type not in [
            "log",
            "progress",
        ]:  # Don't spam for frequent message types
            logger.debug(
                f"No WebSocket clients connected to send message: {message_type}"
            )
        return

    # Add debug logging for completion messages
    if message.get("type") == "complete":
        logger.info(f"Broadcasting completion message: {message}")

    # Add auto-scroll flag for log messages
    if message.get("type") == "log":
        message["auto_scroll"] = True

    # Add timestamp if not present
    if "timestamp" not in message:
        import time

        message["timestamp"] = time.time()

    # Create a copy of the set to avoid "set changed size during iteration" error
    clients_copy = connected_clients.copy()
    disconnected = set()
    message_json = json.dumps(message)
    for client in clients_copy:
        try:
            await client.send(message_json)
        except websockets.exceptions.ConnectionClosed:
            disconnected.add(client)
        except Exception as e:
            logger.warning(f"Failed to send message to client: {e}")
            disconnected.add(client)
    # Remove disconnected clients from the original set
    for client in disconnected:
        connected_clients.discard(client)


# API Routes


async def initialize_logging():
    """Initialize logging and send startup message"""
    try:
        logger.info("Synapse Backend Server started successfully")
        logger.info("Logging system initialized - all log messages will appear here")
        logger.info("Backend server logging initialized")
    except Exception as e:
        print(f"Failed to initialize logging: {e}")


@app.get("/logs/poll")
async def poll_log_messages():
    """Poll for new log messages from the queue"""
    try:
        # Get all messages and clear the queue atomically
        messages = log_message_queue.copy()
        log_message_queue.clear()

        return {"success": True, "messages": messages, "count": len(messages)}
    except Exception as e:
        logger.error(f"Error polling log messages: {e}")
        return {"success": False, "messages": [], "count": 0, "error": str(e)}


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy", "service": "synapse-backend"}


@app.get("/test/logging")
async def test_logging():
    """Test endpoint to verify logging functionality"""
    try:
        logger.info("This is a test INFO message")
        logger.error("This is a test ERROR message")
        logger.warning("Test log via Python logger WARNING")
        logger.debug("Test log via Python logger DEBUG")
        return {"message": "Test logging messages sent"}
    except Exception as e:
        logger.error(f"Test logging failed: {e}")
        return {"error": str(e)}


@app.get("/system/home-directory")
async def get_home_directory():
    """Get the user's home directory path"""
    try:
        home_dir = os.path.expanduser("~")
        downloads_dir = os.path.join(home_dir, "Downloads")

        # Verify the Downloads directory exists, create if it doesn't
        if not os.path.exists(downloads_dir):
            try:
                os.makedirs(downloads_dir, exist_ok=True)
                logger.info(f"Created Downloads directory: {downloads_dir}")
            except Exception as e:
                logger.warning(f"Could not create Downloads directory: {e}")
                # Fall back to home directory if Downloads can't be created
                downloads_dir = home_dir

        return {"home_directory": home_dir, "downloads_directory": downloads_dir}
    except Exception as e:
        logger.error(f"Error getting home directory: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Authentication endpoints


@app.get("/auth/profiles")
async def get_profiles():
    """Get available authentication profiles"""
    try:
        global config_manager
        if not config_manager:
            config_manager = ConfigManager()

        profiles = config_manager.get_available_profiles()
        logger.info(f"Available profiles: {profiles}")
        profile_data = []

        for profile in profiles:
            profile_data.append(
                {"name": profile, "display_name": profile.replace("_", " ").title()}
            )

        return {"profiles": profile_data}
    except Exception as e:
        logger.error(f"Error getting profiles: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/auth/login")
async def login(request: LoginRequest):
    """Authenticate user with Synapse"""
    try:
        global synapse_client, config_manager
        if not synapse_client:
            synapse_client = SynapseClientManager()
        if not config_manager:
            config_manager = ConfigManager()

        if request.mode == "manual":
            if not request.username or not request.token:
                raise HTTPException(
                    status_code=400, detail="Username and token are required"
                )

            # Handle manual login
            result = await synapse_client.login_manual(request.username, request.token)
        else:
            if not request.profile:
                raise HTTPException(status_code=400, detail="Profile is required")

            # Handle config-based login
            result = await synapse_client.login_with_profile(request.profile)

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
        logger.error(f"Login error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/auth/logout")
async def logout():
    """Logout current user"""
    try:
        global synapse_client
        if synapse_client:
            synapse_client.logout()
        return {"message": "Logged out successfully"}
    except Exception as e:
        logger.error(f"Logout error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# File operation endpoints


@app.post("/files/download")
async def download_file(request: DownloadRequest):
    """Download a file from Synapse"""
    try:
        global synapse_client
        if not synapse_client or not synapse_client.is_logged_in:
            raise HTTPException(status_code=401, detail="Not authenticated")

        async def download_task():
            try:
                logger.info(f"Starting download of {request.synapse_id}")

                # Eliminate callbacks entirely - just use direct async calls
                # This prevents all the asyncio task issues
                result = await synapse_client.download_file(
                    synapse_id=request.synapse_id,
                    version=int(request.version) if request.version else None,
                    download_path=request.download_path,
                    progress_callback=None,  # Disable progress callbacks
                    detail_callback=None,  # Disable detail callbacks
                )

                # Manually log completion instead of broadcast_message (since we're in a thread)
                if result["success"]:
                    logger.info(f"✅ Successfully downloaded {request.synapse_id}")
                    logger.info(
                        f"Downloaded to {result.get('path', 'download location')}"
                    )
                else:
                    logger.error(f"❌ Download failed: {result['error']}")
            except Exception as e:
                logger.exception(f"Download task error: {e}")
                logger.error(f"❌ Download error: {str(e)}")
            finally:
                # Always ensure completion message is sent
                logger.info(f"Download task completed for {request.synapse_id}")

        # Start the task in the background using a separate thread
        import asyncio

        # Create a background task that will run the download
        def run_download_in_background():
            try:
                # Run the async download task in a new event loop
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(download_task())
            except Exception as e:
                logger.error(f"Background download error: {e}")
            finally:
                loop.close()

        # Start in a thread to avoid blocking
        import threading

        thread = threading.Thread(target=run_download_in_background)
        thread.daemon = True
        thread.start()

        return {"message": "Download started", "synapse_id": request.synapse_id}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Download endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/files/upload")
async def upload_file(request: UploadRequest):
    """Upload a file to Synapse"""
    try:
        global synapse_client
        if not synapse_client or not synapse_client.is_logged_in:
            raise HTTPException(status_code=401, detail="Not authenticated")

        async def upload_task():
            try:
                logger.info(f"Starting upload of {request.file_path}")
                # Eliminate callbacks entirely - just use direct async calls
                # This prevents all the asyncio task issues
                result = await synapse_client.upload_file(
                    file_path=request.file_path,
                    parent_id=request.parent_id if request.mode == "new" else None,
                    entity_id=request.entity_id if request.mode == "update" else None,
                    name=request.name,
                    progress_callback=None,  # Disable progress callbacks
                    detail_callback=None,  # Disable detail callbacks
                )

                # Manually log completion instead of broadcast_message (since we're in a thread)
                if result["success"]:
                    logger.info(f"✅ Successfully uploaded {request.file_path}")
                    logger.info(f"Uploaded as {result.get('entity_id', 'new entity')}")
                else:
                    logger.error(f"❌ Upload failed: {result['error']}")
            except Exception as e:
                logger.exception(f"Upload task error: {e}")
                logger.error(f"❌ Upload error: {str(e)}")
            finally:
                # Always ensure completion message is sent
                logger.info(f"Upload task completed for {request.file_path}")

        # Start the task in the background using a separate thread
        def run_upload_in_background():
            try:
                # Run the async upload task in a new event loop
                loop = asyncio.new_event_loop()
                asyncio.set_event_loop(loop)
                loop.run_until_complete(upload_task())
            except Exception as e:
                logger.error(f"Background upload error: {e}")
            finally:
                loop.close()

        # Start in a thread to avoid blocking
        import threading

        thread = threading.Thread(target=run_upload_in_background)
        thread.daemon = True
        thread.start()

        return {"message": "Upload started", "file_path": request.file_path}

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Upload endpoint error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


# Bulk operation endpoints


@app.post("/files/scan-directory")
async def scan_directory(request: ScanDirectoryRequest):
    """Scan a directory for files to upload"""
    try:
        import mimetypes

        directory_path = request.directory_path
        if not os.path.exists(directory_path):
            raise HTTPException(status_code=400, detail="Directory does not exist")

        if not os.path.isdir(directory_path):
            raise HTTPException(status_code=400, detail="Path is not a directory")

        logger.info(f"Scanning directory: {directory_path}")

        files = []
        total_size = 0

        def scan_directory_recursive(current_path: str, base_path: str):
            nonlocal total_size
            items = []

            try:
                for item in os.listdir(current_path):
                    item_path = os.path.join(current_path, item)
                    relative_path = os.path.relpath(item_path, base_path)

                    if os.path.isfile(item_path):
                        try:
                            file_size = os.path.getsize(item_path)
                            mime_type, _ = mimetypes.guess_type(item_path)

                            file_info = {
                                "id": item_path,  # Use full path as ID
                                "name": item,
                                "type": "file",
                                "size": file_size,
                                "path": item_path,
                                "relative_path": relative_path,
                                "parent_path": current_path,
                                "mime_type": mime_type or "application/octet-stream",
                            }
                            items.append(file_info)
                            total_size += file_size

                        except (OSError, IOError) as e:
                            logger.warning(f"Could not access file {item_path}: {e}")
                            continue

                    elif os.path.isdir(item_path) and request.recursive:
                        # Add folder to list
                        folder_info = {
                            "id": item_path,
                            "name": item,
                            "type": "folder",
                            "size": 0,
                            "path": item_path,
                            "relative_path": relative_path,
                            "parent_path": current_path,
                        }
                        items.append(folder_info)

                        # Recursively scan subdirectories
                        sub_items = scan_directory_recursive(item_path, base_path)
                        items.extend(sub_items)

            except (PermissionError, OSError) as e:
                logger.warning(f"Could not access directory {current_path}: {e}")

            return items

        files = scan_directory_recursive(directory_path, directory_path)

        # Count files vs folders
        file_count = sum(1 for f in files if f["type"] == "file")
        folder_count = sum(1 for f in files if f["type"] == "folder")

        logger.info(f"Found {file_count} files and {folder_count} folders")

        return {
            "success": True,
            "files": files,
            "summary": {
                "total_items": len(files),
                "file_count": file_count,
                "folder_count": folder_count,
                "total_size": total_size,
            },
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.error(f"Directory scan error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/bulk/enumerate")
async def enumerate_container(request: EnumerateRequest):
    """Enumerate contents of a Synapse container"""
    try:
        global synapse_client
        if not synapse_client or not synapse_client.is_logged_in:
            raise HTTPException(status_code=401, detail="Not authenticated")

        logger.info(f"Enumerating container {request.container_id}")

        result = await synapse_client.enumerate_container(
            request.container_id, request.recursive
        )

        if result["success"]:
            # Convert items to JSON-serializable format
            items = []
            for item in result["items"]:
                if hasattr(item, "synapse_id"):  # BulkItem object
                    items.append(
                        {
                            "id": item.synapse_id,
                            "name": item.name,
                            "type": item.item_type.lower(),  # Use item_type and convert to lowercase
                            "size": item.size,
                            "parent_id": item.parent_id,
                            "path": item.path if item.path else "",
                        }
                    )
                else:  # Already a dict
                    items.append(item)

            logger.info(f"Found {len(items)} items in container")
            return {"items": items}
        else:
            raise HTTPException(status_code=500, detail=result["error"])

    except HTTPException:
        raise
    except Exception as e:
        logger.exception(f"Enumerate error: {e}")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/bulk/download")
async def bulk_download(request: BulkDownloadRequest):
    """Bulk download files from Synapse"""
    logger.info("Bulk download endpoint")
    try:
        global synapse_client
        if not synapse_client or not synapse_client.is_logged_in:
            raise HTTPException(status_code=401, detail="Not authenticated")

        # Execute the download task and wait for completion
        try:
            # Filter to only include files for bulk download
            file_items_data = [
                item
                for item in request.items
                if item.get("type", "file").lower() == "file"
            ]

            if not file_items_data:
                raise HTTPException(
                    status_code=400,
                    detail="No files selected for download. Only files can be bulk downloaded.",
                )

            logger.info(
                f"Starting bulk download of {len(file_items_data)} files "
                f"(filtered from {len(request.items)} selected items)"
            )
            # Convert items back to BulkItem objects with path information
            bulk_items = []
            for item_data in file_items_data:
                bulk_item = BulkItem(
                    synapse_id=item_data["id"],
                    name=item_data["name"],
                    item_type=item_data.get("type", "file"),
                    size=item_data.get("size"),
                    parent_id=item_data.get("parent_id"),
                    path=item_data.get("path", ""),
                )
                bulk_items.append(bulk_item)

            # Create async wrapper functions for the callbacks
            async def async_progress_callback(p, m):
                pass  # Remove progress callback functionality

            async def async_detail_callback(m):
                logger.info(m)

            result = await synapse_client.bulk_download(
                items=bulk_items,
                download_path=request.download_path,
                recursive=request.create_subfolders,
                progress_callback=async_progress_callback,
                detail_callback=async_detail_callback,
            )

            if result["success"]:
                logger.info(f"Successfully downloaded {len(file_items_data)} files")
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
                logger.error(f"Bulk download failed: {result['error']}")
                await broadcast_message(
                    {
                        "type": "complete",
                        "operation": "bulk-download",
                        "success": False,
                        "data": {"error": result["error"]},
                    }
                )
                raise HTTPException(status_code=500, detail=result["error"])

        except Exception as e:
            logger.error(f"Bulk download task error: {e}")
            logger.info(f"Bulk download error: {str(e)}", True)
            await broadcast_message(
                {
                    "type": "complete",
                    "operation": "bulk-download",
                    "success": False,
                    "data": {"error": str(e)},
                }
            )
            raise HTTPException(status_code=500, detail=str(e))

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Bulk download endpoint error")
        raise HTTPException(status_code=500, detail=str(e))


@app.post("/bulk/upload")
async def bulk_upload(request: BulkUploadRequest):
    """Bulk upload files to Synapse with proper folder hierarchy"""
    try:
        global synapse_client
        if not synapse_client or not synapse_client.is_logged_in:
            raise HTTPException(status_code=401, detail="Not authenticated")

        # Filter to only include files for bulk upload
        file_items_data = [
            file for file in request.files if file.get("type", "file").lower() == "file"
        ]

        if not file_items_data:
            raise HTTPException(
                status_code=400,
                detail="No files selected for upload. Only files can be bulk uploaded.",
            )

        logger.info(
            f"Starting bulk upload of {len(file_items_data)} files "
            f"(filtered from {len(request.files)} selected items)"
        )
        # Import required models
        from synapseclient.models.file import File
        from synapseclient.models.folder import Folder

        # Build folder hierarchy and organize files
        folder_map = {}  # path -> Folder object
        root_files = []  # Files that go directly in the parent folder

        for file_data in file_items_data:
            file_path = file_data.get("path")
            relative_path = file_data.get("relative_path", "")

            if not file_path or not os.path.exists(file_path):
                logger.info(
                    f"Skipping file with invalid path: {file_data.get('name', 'Unknown')}",
                    True,
                )
                continue

            # Create File object
            file_obj = File(
                path=file_path,
                name=file_data.get("name"),
                description=file_data.get("description", None),
            )

            if request.preserve_folder_structure and relative_path:
                # Normalize path separators and split into folder parts
                relative_path = relative_path.replace("\\", "/").strip("/")
                path_parts = relative_path.split("/")

                if len(path_parts) > 1:
                    # File is in a subfolder - build folder hierarchy
                    folder_parts = path_parts[:-1]  # All parts except filename
                    current_path = ""
                    current_parent_id = request.parent_id

                    # Create folder hierarchy
                    for i, folder_name in enumerate(folder_parts):
                        if current_path:
                            current_path += "/"
                        current_path += folder_name

                        if current_path not in folder_map:
                            # Create new folder
                            folder_obj = Folder(
                                name=folder_name,
                                parent_id=current_parent_id,
                                files=[],
                                folders=[],
                            )
                            folder_map[current_path] = folder_obj

                            # Add to parent folder's folders list
                            if i > 0:
                                parent_path = "/".join(folder_parts[:i])
                                if parent_path in folder_map:
                                    folder_map[parent_path].folders.append(folder_obj)

                        # Update parent_id for next level
                        current_parent_id = None  # Will be set when folder is stored

                    # Add file to its containing folder
                    folder_path = "/".join(folder_parts)
                    if folder_path in folder_map:
                        folder_map[folder_path].files.append(file_obj)
                else:
                    # File is in root directory
                    file_obj.parent_id = request.parent_id
                    root_files.append(file_obj)
            else:
                # No folder structure preservation - all files go to parent
                file_obj.parent_id = request.parent_id
                root_files.append(file_obj)

        # Get all unique folders (top-level folders only for initial upload)
        top_level_folders = []
        for path, folder_obj in folder_map.items():
            if "/" not in path:  # Top-level folder
                top_level_folders.append(folder_obj)

        total_items = len(root_files) + len(top_level_folders)
        if total_items == 0:
            logger.error("No valid files found to upload")
            await broadcast_message(
                {
                    "type": "complete",
                    "operation": "bulk-upload",
                    "success": False,
                    "data": {"error": "No valid files found to upload"},
                }
            )
            raise HTTPException(
                status_code=400, detail="No valid files found to upload"
            )

        logger.info(
            f"Created folder hierarchy: {len(top_level_folders)} folders, {len(root_files)} root files"
        )

        # Track progress
        completed_items = 0
        upload_results = []

        async def upload_item(item, item_type: str, item_name: str) -> dict:
            """Upload a file or folder and return result"""
            nonlocal completed_items
            try:
                logger.info(f"Uploading {item_type}: {item_name}")

                # Store the item (folder or file)
                stored_item = await item.store_async(synapse_client=synapse_client.client)

                completed_items += 1
                return {
                    "success": True,
                    "item_name": item_name,
                    "item_type": item_type,
                    "entity_id": stored_item.id,
                    "path": getattr(item, "path", None),
                }
            except Exception as e:
                completed_items += 1
                logger.error(f"Failed to upload {item_name}: {str(e)}")

                return {
                    "success": False,
                    "item_name": item_name,
                    "item_type": item_type,
                    "error": str(e),
                    "path": getattr(item, "path", None),
                }

        # Create upload tasks for all items
        upload_tasks = []

        # Add root files
        for file_obj in root_files:
            upload_tasks.append(upload_item(file_obj, "file", file_obj.name))

        # Add top-level folders (which will handle their subfolders/files automatically)
        for folder_obj in top_level_folders:
            upload_tasks.append(upload_item(folder_obj, "folder", folder_obj.name))

        # Wait for all uploads to complete
        upload_results = await asyncio.gather(*upload_tasks, return_exceptions=True)

        # Process results and handle any exceptions
        processed_results = []
        for i, result in enumerate(upload_results):
            if isinstance(result, Exception):
                item_name = "Unknown"
                if i < len(root_files):
                    item_name = root_files[i].name
                elif i - len(root_files) < len(top_level_folders):
                    item_name = top_level_folders[i - len(root_files)].name

                processed_results.append(
                    {"success": False, "item_name": item_name, "error": str(result)}
                )
            else:
                processed_results.append(result)

        # Calculate final statistics
        successful_uploads = sum(
            1 for r in processed_results if r.get("success", False)
        )
        failed_uploads = total_items - successful_uploads

        logger.info(
            f"Bulk upload completed: {successful_uploads} successful, {failed_uploads} failed"
        )

        await broadcast_message(
            {
                "type": "complete",
                "operation": "bulk-upload",
                "success": True,
                "data": {
                    "message": f"Uploaded {successful_uploads} items, {failed_uploads} failed",
                    "successful_uploads": successful_uploads,
                    "failed_uploads": failed_uploads,
                    "results": processed_results,
                },
            }
        )

        return {
            "success": True,
            "message": "Bulk upload completed successfully",
            "successful_uploads": successful_uploads,
            "failed_uploads": failed_uploads,
            "total_items": total_items,
            "summary": f"Uploaded {successful_uploads} items, {failed_uploads} failed",
        }

    except HTTPException:
        raise
    except Exception as e:
        logger.exception("Bulk upload endpoint error")
        logger.info(f"Bulk upload error: {str(e)}", True)
        await broadcast_message(
            {
                "type": "complete",
                "operation": "bulk-upload",
                "success": False,
                "data": {"error": str(e)},
            }
        )
        raise HTTPException(status_code=500, detail=str(e))


def start_websocket_server(port: int):
    """Start the WebSocket server in a separate thread"""

    def run_websocket_server():
        async def websocket_server():
            # Create a wrapper that handles both old and new websockets library signatures
            async def websocket_handler(websocket, path=None):
                await handle_websocket_client(websocket, path)

            try:
                async with websockets.serve(websocket_handler, "localhost", port):
                    logger.info("WebSocket server started on ws://localhost:%s", port)
                    await asyncio.Future()  # Run forever
            except Exception as e:
                logger.error(f"WebSocket server error: {e}")

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(websocket_server())

    ws_thread = threading.Thread(target=run_websocket_server)
    ws_thread.daemon = True
    ws_thread.start()


def setup_electron_environment():
    """
    Setup environment variables and directories when running from Electron.
    This fixes issues with temporary directories and cache paths in packaged apps.
    """
    try:
        # Detect if we're running from within Electron's environment
        is_electron_context = (
            'electron' in sys.executable.lower() or
            'resources' in os.getcwd() or
            os.path.exists(os.path.join(os.getcwd(), '..', '..', 'app.asar'))
        )

        if is_electron_context:
            logger.info("Detected Electron environment, setting up proper directories")

            # Get user's home directory for cache/temp storage
            if os.name == 'nt':  # Windows
                cache_base = os.getenv('LOCALAPPDATA') or os.getenv('APPDATA')
                if cache_base:
                    app_cache_dir = os.path.join(cache_base, 'SynapseDesktopClient')
                else:
                    app_cache_dir = os.path.join(os.path.expanduser('~'), 'AppData', 'Local', 'SynapseDesktopClient')
            else:  # Unix-like systems
                cache_base = os.getenv('XDG_CACHE_HOME')
                if cache_base:
                    app_cache_dir = os.path.join(cache_base, 'SynapseDesktopClient')
                else:
                    app_cache_dir = os.path.join(os.path.expanduser('~'), '.cache', 'SynapseDesktopClient')

            # Create the cache directory if it doesn't exist
            os.makedirs(app_cache_dir, exist_ok=True)

            # Set up synapse cache directory
            synapse_cache = os.path.join(app_cache_dir, 'synapse')
            os.makedirs(synapse_cache, exist_ok=True)
            os.environ['SYNAPSE_CACHE'] = synapse_cache

            # Set up temporary directory in user space
            temp_dir = os.path.join(app_cache_dir, 'temp')
            os.makedirs(temp_dir, exist_ok=True)
            os.environ['TMPDIR'] = temp_dir
            os.environ['TMP'] = temp_dir
            os.environ['TEMP'] = temp_dir

            # Change working directory to user's home to avoid permission issues
            user_home = os.path.expanduser('~')
            os.chdir(user_home)

            logger.info(f"Set SYNAPSE_CACHE to: {synapse_cache}")
            logger.info(f"Set temp directories to: {temp_dir}")
            logger.info(f"Changed working directory to: {user_home}")
        else:
            logger.info("Not in Electron environment, using default settings")

    except Exception as e:
        logger.warning(f"Failed to setup Electron environment: {e}")
        # Don't fail if environment setup has issues


def main():
    """Main entry point for the backend server"""
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
