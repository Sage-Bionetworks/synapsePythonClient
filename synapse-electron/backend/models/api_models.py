"""
API request and response models for the Synapse Desktop Client backend.

This module contains all Pydantic models used for API request validation
and response serialization.
"""

from typing import Any, Dict, List, Optional

from pydantic import BaseModel, Field


class LoginRequest(BaseModel):
    """Request model for user authentication."""

    mode: str = Field(..., description="Authentication mode: 'manual' or 'config'")
    username: Optional[str] = Field(None, description="Username for manual authentication")
    token: Optional[str] = Field(None, description="Authentication token for manual login")
    profile: Optional[str] = Field(None, description="Profile name for config-based authentication")


class DownloadRequest(BaseModel):
    """Request model for single file download."""

    synapse_id: str = Field(..., description="Synapse entity ID to download")
    version: Optional[str] = Field(None, description="Specific version to download")
    download_path: str = Field(..., description="Local directory path for download")


class UploadRequest(BaseModel):
    """Request model for single file upload."""

    file_path: str = Field(..., description="Local path to the file to upload")
    mode: str = Field(..., description="Upload mode: 'new' or 'update'")
    parent_id: Optional[str] = Field(None, description="Parent entity ID for new files")
    entity_id: Optional[str] = Field(None, description="Entity ID to update for existing files")
    name: Optional[str] = Field(None, description="Name for the entity")


class EnumerateRequest(BaseModel):
    """Request model for container enumeration."""

    container_id: str = Field(..., description="Synapse ID of the container to enumerate")
    recursive: bool = Field(True, description="Whether to enumerate recursively")


class BulkDownloadRequest(BaseModel):
    """Request model for bulk file download."""

    items: List[Dict[str, Any]] = Field(..., description="List of items to download")
    download_path: str = Field(..., description="Local directory path for downloads")
    create_subfolders: bool = Field(True, description="Whether to create subfolders")


class BulkUploadRequest(BaseModel):
    """Request model for bulk file upload."""

    parent_id: str = Field(..., description="Parent entity ID for uploads")
    files: List[Dict[str, Any]] = Field(..., description="List of file objects to upload")
    preserve_folder_structure: bool = Field(True, description="Whether to preserve folder structure")


class ScanDirectoryRequest(BaseModel):
    """Request model for directory scanning."""

    directory_path: str = Field(..., description="Directory path to scan")
    recursive: bool = Field(True, description="Whether to scan recursively")


class AuthResponse(BaseModel):
    """Response model for authentication."""

    username: str = Field(..., description="Authenticated username")
    name: str = Field(..., description="Display name")
    user_id: str = Field(..., description="User ID")
    token: str = Field(..., description="Authentication status")


class OperationResponse(BaseModel):
    """Generic response model for operations."""

    success: bool = Field(..., description="Whether the operation was successful")
    message: Optional[str] = Field(None, description="Operation message")
    error: Optional[str] = Field(None, description="Error message if operation failed")


class DirectoryInfo(BaseModel):
    """Response model for directory information."""

    home_directory: str = Field(..., description="User's home directory path")
    downloads_directory: str = Field(..., description="User's downloads directory path")


class ProfileInfo(BaseModel):
    """Model for authentication profile information."""

    name: str = Field(..., description="Profile name")
    display_name: str = Field(..., description="Profile display name")


class ProfilesResponse(BaseModel):
    """Response model for available authentication profiles."""

    profiles: List[ProfileInfo] = Field(..., description="List of available profiles")


class BulkItemsResponse(BaseModel):
    """Response model for bulk items listing."""

    items: List[Dict[str, Any]] = Field(..., description="List of bulk items")


class FileInfo(BaseModel):
    """Model for file information during directory scanning."""

    id: str = Field(..., description="File identifier")
    name: str = Field(..., description="File name")
    type: str = Field(..., description="Item type: 'file' or 'folder'")
    size: int = Field(..., description="File size in bytes")
    path: str = Field(..., description="Full file path")
    relative_path: str = Field(..., description="Relative path from scan root")
    parent_path: str = Field(..., description="Parent directory path")
    mime_type: Optional[str] = Field(None, description="MIME type of the file")


class ScanSummary(BaseModel):
    """Summary information for directory scanning."""

    total_items: int = Field(..., description="Total number of items found")
    file_count: int = Field(..., description="Number of files found")
    folder_count: int = Field(..., description="Number of folders found")
    total_size: int = Field(..., description="Total size of all files in bytes")


class ScanDirectoryResponse(BaseModel):
    """Response model for directory scanning."""

    success: bool = Field(..., description="Whether the scan was successful")
    files: List[FileInfo] = Field(..., description="List of files found")
    summary: ScanSummary = Field(..., description="Summary of scan results")


class BulkItem(BaseModel):
    """Model for items in bulk operations."""

    id: str = Field(..., description="Item identifier")
    name: str = Field(..., description="Item name")
    type: str = Field(..., description="Item type")
    size: Optional[int] = Field(None, description="Item size in bytes")
    parent_id: Optional[str] = Field(None, description="Parent item ID")
    path: str = Field("", description="Item path")


class EnumerateResponse(BaseModel):
    """Response model for container enumeration."""

    items: List[BulkItem] = Field(..., description="List of enumerated items")


class UploadResult(BaseModel):
    """Model for individual upload results."""

    success: bool = Field(..., description="Whether the upload was successful")
    item_name: str = Field(..., description="Name of the uploaded item")
    item_type: str = Field(..., description="Type of the uploaded item")
    entity_id: Optional[str] = Field(None, description="Entity ID if successful")
    error: Optional[str] = Field(None, description="Error message if failed")
    path: Optional[str] = Field(None, description="Local file path")


class BulkUploadResponse(BaseModel):
    """Response model for bulk upload operations."""

    success: bool = Field(..., description="Whether the operation was successful")
    message: str = Field(..., description="Operation summary message")
    successful_uploads: int = Field(..., description="Number of successful uploads")
    failed_uploads: int = Field(..., description="Number of failed uploads")
    total_items: int = Field(..., description="Total number of items processed")
    summary: str = Field(..., description="Detailed summary")


class LogMessage(BaseModel):
    """Model for log messages."""

    type: str = Field(..., description="Message type")
    message: str = Field(..., description="Log message content")
    level: str = Field(..., description="Log level")
    logger_name: str = Field(..., description="Logger name")
    timestamp: float = Field(..., description="Message timestamp")
    source: str = Field(..., description="Message source")
    auto_scroll: bool = Field(True, description="Whether to auto-scroll in UI")
    raw_message: str = Field(..., description="Raw log message")
    filename: str = Field("", description="Source filename")
    line_number: int = Field(0, description="Source line number")


class LogPollResponse(BaseModel):
    """Response model for log polling."""

    success: bool = Field(..., description="Whether polling was successful")
    messages: List[LogMessage] = Field(..., description="List of log messages")
    count: int = Field(..., description="Number of messages returned")
    error: Optional[str] = Field(None, description="Error message if polling failed")


class HealthCheckResponse(BaseModel):
    """Response model for health check endpoint."""

    status: str = Field(..., description="Service health status")
    service: str = Field(..., description="Service name")


class TestLoggingResponse(BaseModel):
    """Response model for test logging endpoint."""

    message: Optional[str] = Field(None, description="Success message")
    error: Optional[str] = Field(None, description="Error message if test failed")


class DownloadStartResponse(BaseModel):
    """Response model for download start endpoint."""

    message: str = Field(..., description="Status message")
    synapse_id: str = Field(..., description="Synapse ID being downloaded")


class UploadStartResponse(BaseModel):
    """Response model for upload start endpoint."""

    message: str = Field(..., description="Status message")
    file_path: str = Field(..., description="File path being uploaded")


class LogoutResponse(BaseModel):
    """Response model for logout endpoint."""

    message: str = Field(..., description="Logout confirmation message")


class BulkDownloadResponse(BaseModel):
    """Response model for bulk download operations."""

    success: bool = Field(..., description="Whether the operation was successful")
    message: str = Field(..., description="Operation summary message")
    item_count: int = Field(..., description="Number of items processed")
    summary: str = Field(..., description="Detailed summary of the operation")


class CompletionMessage(BaseModel):
    """Model for operation completion messages sent via WebSocket."""

    type: str = Field("complete", description="Message type")
    operation: str = Field(..., description="Operation that completed")
    success: bool = Field(..., description="Whether operation was successful")
    data: Dict[str, Any] = Field(..., description="Operation result data")
    timestamp: Optional[float] = Field(None, description="Message timestamp")


class ProgressMessage(BaseModel):
    """Model for progress update messages sent via WebSocket."""

    type: str = Field("progress", description="Message type")
    operation: str = Field(..., description="Operation in progress")
    progress: int = Field(..., description="Progress percentage (0-100)")
    message: str = Field(..., description="Progress description")
    timestamp: Optional[float] = Field(None, description="Message timestamp")


class ConnectionStatusMessage(BaseModel):
    """Model for WebSocket connection status messages."""

    type: str = Field("connection_status", description="Message type")
    connected: bool = Field(..., description="Connection status")
    timestamp: Optional[float] = Field(None, description="Message timestamp")


class ErrorResponse(BaseModel):
    """Generic error response model."""

    error: str = Field(..., description="Error message")
    detail: Optional[str] = Field(None, description="Additional error details")
    status_code: Optional[int] = Field(None, description="HTTP status code")
