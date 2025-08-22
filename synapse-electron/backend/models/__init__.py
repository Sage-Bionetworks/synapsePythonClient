"""
Models package for Synapse Desktop Client backend.

This package contains all data models and schemas used in the application.
Data models are pure data structures without business logic.
"""

from .api_models import (
    LoginRequest,
    LogLevelRequest,
    DownloadRequest,
    UploadRequest,
    EnumerateRequest,
    BulkDownloadRequest,
    BulkUploadRequest,
    ScanDirectoryRequest,
    AuthResponse,
    OperationResponse,
    DirectoryInfo,
    ProfileInfo,
    ProfilesResponse,
    BulkItemsResponse,
    FileInfo,
    ScanSummary,
    ScanDirectoryResponse,
    BulkItem,
    EnumerateResponse,
    UploadResult,
    BulkUploadResponse,
    LogMessage,
    LogPollResponse,
    HealthCheckResponse,
    TestLoggingResponse,
    DownloadStartResponse,
    UploadStartResponse,
    LogoutResponse,
    BulkDownloadResponse,
    CompletionMessage,
    ProgressMessage,
    ConnectionStatusMessage,
    ErrorResponse,
)

from .domain_models import BulkItem as BulkItemModel

__all__ = [
    # API Request Models
    "LoginRequest",
    "LogLevelRequest",
    "DownloadRequest",
    "UploadRequest",
    "EnumerateRequest",
    "BulkDownloadRequest",
    "BulkUploadRequest",
    "ScanDirectoryRequest",

    # API Response Models
    "AuthResponse",
    "OperationResponse",
    "DirectoryInfo",
    "ProfileInfo",
    "ProfilesResponse",
    "BulkItemsResponse",
    "FileInfo",
    "ScanSummary",
    "ScanDirectoryResponse",
    "BulkItem",
    "EnumerateResponse",
    "UploadResult",
    "BulkUploadResponse",
    "LogMessage",
    "LogPollResponse",
    "HealthCheckResponse",
    "TestLoggingResponse",
    "DownloadStartResponse",
    "UploadStartResponse",
    "LogoutResponse",
    "BulkDownloadResponse",
    "CompletionMessage",
    "ProgressMessage",
    "ConnectionStatusMessage",
    "ErrorResponse",

    # Domain Models
    "BulkItemModel",
]
