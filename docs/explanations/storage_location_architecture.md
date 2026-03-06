# Storage Location Architecture

This document provides an in-depth architectural overview of the StorageLocation
system in the Synapse Python Client. It explains the design decisions, class
relationships, and data flows that enable flexible storage configuration.

---

## On This Page

<div class="grid cards" markdown>

-   **[Domain Model](#domain-model)**

    Core classes, enums, and their relationships

-   **[Storage Types](#storage-type-mapping)**

    How storage types map to REST API types and choosing the right one

-   **[Entity Inheritance](#entity-inheritance-hierarchy)**

    How Projects and Folders gain storage capabilities

-   **[Operation Flows](#operation-flows)**

    Sequence diagrams for store, setup, and STS operations

-   **[Settings & API](#project-setting-lifecycle)**

    Project settings lifecycle and REST API architecture

-   **[Migration](#migration-flow)**

    Two-phase file migration process

</div>

---

## Overview

The StorageLocation setting enables Synapse users to configure a location where files are uploaded to and downloaded from via Synapse.
By default, Synapse stores files in its internal S3 storage, but
users can configure projects and folders to use external storage backends such as
AWS S3 buckets, Google Cloud Storage, SFTP servers, or a local file server using a proxy server.

### Key Concepts
- [**StorageLocationSetting**](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/project/StorageLocationSetting.html): A configuration specifying file storage and download locations.
- [**ProjectSetting**](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/project/ProjectSetting.html): A configuration applied to projects that allows customization of file storage locations.
- [**UploadType**](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/file/UploadType.html): An enumeration that defines the types of file upload destinations that Synapse supports.
- **STS Credentials**: Temporary AWS credentials for direct S3 access.
- **StorageLocation Migration**: The process of transferring the files associated with Synapse entities between storage locations while preserving the entities’ structure and identifiers.

---

<br>

# Part 1: Data Model

This section covers the core classes, enumerations, and type mappings.

<br>

## Domain Model

The following class diagram shows the core classes and their relationships in the
StorageLocation system.

```mermaid
classDiagram
    direction TB

    class StorageLocation {
        +int storage_location_id
        +StorageLocationType storage_type
        +UploadType upload_type
        +str bucket
        +str base_key
        +bool sts_enabled
        +str banner
        +str description
        +str etag
        +str created_on
        +int created_by
        +str url
        +bool supports_subfolders
        +str endpoint_url
        +str proxy_url
        +str secret_key
        +str benefactor_id
        +store() StorageLocation
        +get() StorageLocation
        +fill_from_dict(dict) StorageLocation
    }

    class StorageLocationType {
        <<enumeration>>
        SYNAPSE_S3
        EXTERNAL_S3
        EXTERNAL_GOOGLE_CLOUD
        EXTERNAL_SFTP
        EXTERNAL_OBJECT_STORE
        PROXY
    }

    class UploadType {
        <<enumeration>>
        S3
        GOOGLE_CLOUD_STORAGE
        SFTP
        HTTPS
        NONE
    }

    class StorageLocationConfigurable {
        <<mixin>>
        +set_storage_location(storage_location_id) ProjectSetting
        +get_project_setting(setting_type) ProjectSetting
        +delete_project_setting(setting_id)
        +get_sts_storage_token(permission, output_format) dict
        +index_files_for_migration(dest_storage_location_id, db_path) MigrationResult
        +migrate_indexed_files(db_path) MigrationResult
    }

    class Project {
        +str id
        +str name
        +str description
    }

    class Folder {
        +str id
        +str name
        +str parent_id
    }

    class UploadDestinationListSetting {
        <<enumeration>>
        concreteType
        id
        projectId
        settingsType
        etag
        locations
    }

    class ProjectSetting {
        <<enumeration>>
        concreteType
        id
        projectId
        settingsType
        etag

    }
    StorageLocation --> StorageLocationType : storage_type
    StorageLocation --> UploadType : upload_type
    StorageLocationConfigurable <|-- Project : implements
    StorageLocationConfigurable <|-- Folder : implements
    StorageLocationConfigurable ..> ProjectSetting : returns
    StorageLocationConfigurable ..> UploadDestinationListSetting : uses

```

<br>


### Key Components
[synapseclient.models.StorageLocation] | The model representing a storage location setting in Synapse |
[synapseclient.models.StorageLocationType] | Enumeration defining the supported storage backend types |
[synapseclient.models.UploadType] | Enumeration defining the upload protocol for each storage type |
[synapseclient.models.mixins.StorageLocationConfigurable] | Mixin providing storage management methods to entities |
[synapseclient.models.mixins.UploadDestinationListSetting] | Enumeration defining the setting type contains the list of upload locations for files in entities |
[synapseclient.models.mixins.ProjectSetting] | Enumeration defining the project based setting

---

<br>

## Storage Type Mapping (TODO: double checking if EXTERNAL_HTTP works as expected)

Each `StorageLocationType` maps to a specific REST API `concreteType` and has a
default `UploadType`. This mapping allows the system to parse
responses from the API and construct requests.

```mermaid
flowchart LR
    subgraph StorageLocationType
        SYNAPSE_S3["SYNAPSE_S3"]
        EXTERNAL_S3["EXTERNAL_S3"]
        EXTERNAL_GOOGLE_CLOUD["EXTERNAL_GOOGLE_CLOUD"]
        EXTERNAL_SFTP["EXTERNAL_SFTP"]
        EXTERNAL_OBJECT_STORE["EXTERNAL_OBJECT_STORE"]
        PROXY["PROXY"]
    end

    subgraph concreteType
        S3SLS["S3StorageLocationSetting"]
        ExtS3SLS["ExternalS3StorageLocationSetting"]
        ExtGCSSLS["ExternalGoogleCloudStorageLocationSetting"]
        ExtSLS["ExternalStorageLocationSetting"]
        ExtObjSLS["ExternalObjectStorageLocationSetting"]
        ProxySLS["ProxyStorageLocationSettings"]
    end

    subgraph UploadType
        S3["S3"]
        GCS["GOOGLECLOUDSTORAGE"]
        SFTP["SFTP"]
        HTTPS["HTTPS"]
    end

    SYNAPSE_S3 --> S3SLS --> S3
    EXTERNAL_S3 --> ExtS3SLS --> S3
    EXTERNAL_GOOGLE_CLOUD --> ExtGCSSLS --> GCS
    EXTERNAL_SFTP --> ExtSLS --> SFTP
    EXTERNAL_OBJECT_STORE --> ExtObjSLS --> S3
    PROXY --> ProxySLS --> HTTPS
```

<br>

### Storage Type Attributes

Different storage types support different configuration attributes:

| Attribute | Type | S3StorageLocationSetting | ExternalS3StorageLocationSetting | ExternalObjectStorageLocationSetting | ExternalStorageLocationSetting | ExternalGoogleCloudStorageLocationSetting | ProxyStorageLocationSettings |
|-----------|------|--------------------------|----------------------------------|--------------------------------------|--------------------------------|-------------------------------------------|------------------------------|
| **Common (all types)** |
| `concreteType` | string (enum) | ✓ (required) | ✓ (required) | ✓ (required) | ✓ (required) | ✓ (required) | ✓ (required) |
| `storageLocationId` | integer (int32) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `uploadType` | string | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `banner` | string | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `description` | string | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `etag` | string | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `createdOn` | string | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| `createdBy` | integer (int32) | ✓ | ✓ | ✓ | ✓ | ✓ | ✓ |
| **Type-specific** |
| `baseKey` | string | ✓ | ✓ | — | — | ✓ | — |
| `stsEnabled` | boolean | ✓ | ✓ | — | — | — | — |
| `bucket` | string | — | ✓ (required) | ✓ (required) | — | ✓ (required) | — |
| `endpointUrl` | string | — | ✓ | ✓ (required) | — | — | — |
| `url` | string | — | — | — | ✓ | — | — |
| `supportsSubfolders` | boolean | — | — | — | ✓ | — | — |
| `proxyUrl` | string | — | — | — | — | — | ✓ |
| `secretKey` | string | — | — | — | — | — | ✓ |
| `benefactorId` | string | — | — | — | — | — | ✓ |

## Summary by type

| Setting type | Description | Type-specific attributes |
|--------------|-------------|---------------------------|
| **S3StorageLocationSetting** | Default Synapse storage on Amazon S3. | `baseKey`, `stsEnabled` |
| **ExternalS3StorageLocationSetting** | External S3 bucket connected with Synapse (Synapse-accessed). | `bucket` (required), `baseKey`, `stsEnabled`, `endpointUrl` |
| **ExternalObjectStorageLocationSetting** | S3-compatible object storage **not** accessed by Synapse. | `bucket` (required), `endpointUrl` (required) |
| **ExternalStorageLocationSetting** | SFTP or HTTPS upload destination. | `url`, `supportsSubfolders` |
| **ExternalGoogleCloudStorageLocationSetting** | External Google Cloud Storage bucket connected with Synapse. | `bucket` (required), `baseKey` |
| **ProxyStorageLocationSettings** | HTTPS proxy for all upload/download operations. | `proxyUrl`, `secretKey`, `benefactorId` |


<br>

### Choosing a Storage Type

Use this decision tree to select the appropriate storage type for your use case:

```mermaid
flowchart TB
    Start{Need custom storage?}
    Start -->|No| DEFAULT[Use default Synapse storage]
    Start -->|Yes| Q1{Want Synapse to<br/>manage storage?}

    Q1 -->|Yes| SYNAPSE_S3[Use SYNAPSE_S3]
    Q1 -->|No| Q2{What storage<br/>backend?}

    Q2 -->|AWS S3| Q3{Synapse accesses<br/>bucket directly?}
    Q2 -->|Google Cloud| EXTERNAL_GOOGLE_CLOUD[Use EXTERNAL_GOOGLE_CLOUD]
    Q2 -->|SFTP Server| EXTERNAL_SFTP[Use EXTERNAL_SFTP]
    Q2 -->|Proxy Server| PROXY[Use PROXY]
    Q2 -->|AWS S3 | EXTERNAL_OBJECT_STORE[Use EXTERNAL_OBJECT_STORE]

    Q3 -->|Yes| Q4{Need STS<br/>credentials?}
    Q3 -->|No| EXTERNAL_OBJECT_STORE

    Q4 -->|Yes| EXTERNAL_S3_STS[Use EXTERNAL_S3<br/>with sts_enabled=True]
    Q4 -->|No| EXTERNAL_S3[Use EXTERNAL_S3]

    SYNAPSE_S3 --> Benefits1[Benefits:<br/>- Zero configuration<br/>- Managed by Synapse<br/>- STS available]
    EXTERNAL_S3 --> Benefits2[Benefits:<br/>- Use your own bucket<br/>- Control access & costs<br/>- Optional STS]
    EXTERNAL_S3_STS --> Benefits2
    EXTERNAL_GOOGLE_CLOUD --> Benefits3[Benefits:<br/>- GCP native<br/>- Use existing GCS buckets]
    EXTERNAL_SFTP --> Benefits4[Benefits:<br/>- Legacy systems<br/>- Synapse never touches data]
    EXTERNAL_OBJECT_STORE --> Benefits5[Benefits:<br/>- OpenStack, MinIO, etc<br/>- Synapse never touches data]
    PROXY --> Benefits6[Benefits:<br/>- Custom access control<br/>- Data transformation]
    DEFAULT --> Benefits0[Benefits:<br/>- No configuration needed<br/>- Synapse-managed S3]
```

---

<br>

## Entity Inheritance Hierarchy

Projects and Folders inherit storage configuration capabilities through the
`StorageLocation` mixin. This pattern allows consistent storage
management across container entities.

```mermaid
classDiagram
    direction TB

    class StorageLocation {
        <<mixin>>
        +set_storage_location()
        +get_project_setting()
        +delete_project_setting()
        +get_sts_storage_token()
        +index_files_for_migration()
        +migrate_indexed_files()
    }

    class Project {
        +str id
        +str name
        +str description
        +str etag
    }

    class Folder {
        +str id
        +str name
        +str parent_id
        +str etag
    }

    StorageLocation <|-- Project
    StorageLocation <|-- Folder
```

The mixin pattern allows `Project` and `Folder` to share storage location
functionality without code duplication. Both classes inherit the same
methods from `StorageLocation`.

---

<br>
<br>

# Part 2: Operation Flows

This section contains sequence diagrams for key operations.

<br>

## Operation Flows

### Store Operation

The `store()` method creates a new storage location in Synapse. Creating a storage location is idempotent per user. Repeating a creation request with the same properties will return the previously created storage location rather than creating a new one.

```mermaid
sequenceDiagram
    participant User
    participant StorageLocation
    participant _to_synapse_request as _to_synapse_request()
    participant API as storage_location_services
    participant Synapse as Synapse REST API

    User->>StorageLocation: store()
    activate StorageLocation

    StorageLocation->>_to_synapse_request: Build request body
    activate _to_synapse_request

    Note over _to_synapse_request: Validate storage_type is set
    Note over _to_synapse_request: Build concreteType from storage_type
    Note over _to_synapse_request: Determine uploadType
    Note over _to_synapse_request: Add type-specific fields

    _to_synapse_request-->>StorageLocation: Request body dict
    deactivate _to_synapse_request

    StorageLocation->>API: create_storage_location_setting(body)
    activate API

    API->>Synapse: POST /storageLocation
    activate Synapse

    Synapse-->>API: Response with storageLocationId
    deactivate Synapse

    API-->>StorageLocation: Response dict
    deactivate API

    StorageLocation->>StorageLocation: fill_from_dict(response)
    Note over StorageLocation: Parse storageLocationId
    Note over StorageLocation: Parse concreteType → storage_type
    Note over StorageLocation: Parse uploadType → upload_type
    Note over StorageLocation: Extract type-specific fields

    StorageLocation-->>User: StorageLocation (populated)
    deactivate StorageLocation
```

<br>

### Setup S3 Convenience Flow

The `setup_s3()` class method creates a folder with S3 storage in a single call.
```mermaid
sequenceDiagram
    participant User
    participant setup_s3 as StorageLocation.setup_s3()
    participant StorageLocation
    participant Folder
    participant Mixin as StorageLocation
    participant API as storage_location_services
    participant Synapse as Synapse REST API

    User->>setup_s3: setup_s3(parent, folder_name, bucket_name)
    activate setup_s3

    Note over setup_s3: Validate: folder_name XOR folder

    alt folder_name provided
        setup_s3->>Folder: Folder(name, parent_id).store()
        activate Folder
        Folder->>Synapse: POST /entity
        Synapse-->>Folder: Folder response
        Folder-->>setup_s3: New Folder
        deactivate Folder
    else folder ID provided
        setup_s3->>Folder: Folder(id).get()
        activate Folder
        Folder->>Synapse: GET /entity/{id}
        Synapse-->>Folder: Folder response
        Folder-->>setup_s3: Existing Folder
        deactivate Folder
    end

    alt bucket_name provided
        Note over setup_s3: storage_type = EXTERNAL_S3
    else bucket_name is None
        Note over setup_s3: storage_type = SYNAPSE_S3
    end

    setup_s3->>StorageLocation: StorageLocation(...).store()
    activate StorageLocation
    StorageLocation->>Synapse: POST /storageLocation
    Synapse-->>StorageLocation: StorageLocation response
    StorageLocation-->>setup_s3: StorageLocation
    deactivate StorageLocation

    setup_s3->>Mixin: folder.set_storage_location(storage_location_id)
    activate Mixin

    Mixin->>API: get_project_setting(project_id, "upload")
    API->>Synapse: GET /projectSettings/{id}/type/upload
    Synapse-->>API: Setting or empty

    alt Setting exists
        API-->>Mixin: Existing setting
        Mixin->>API: update_project_setting(body)
        API->>Synapse: PUT /projectSettings
    else No setting
        Mixin->>API: create_project_setting(body)
        API->>Synapse: POST /projectSettings
    end

    Synapse-->>API: Project setting response
    API-->>Mixin: Updated setting
    deactivate Mixin

    setup_s3-->>User: (Folder, StorageLocation)
    deactivate setup_s3
```

<br>

### STS Token Retrieval

STS (AWS Security Token Service) enables direct S3 access using temporary credentials.

When a Synapse client is constructed (`Synapse.__init__`), it creates an in-memory token cache:

- `self._sts_token_store = sts_transfer.StsTokenStore()` (see `synapseclient/client.py`)

The store caches STS tokens per entity and permission so repeated access to the same storage location can reuse credentials without a round-trip to the REST API.

```mermaid
sequenceDiagram
    participant User
    participant Entity as Folder/Project
    participant Mixin as StorageLocation
    participant STS as sts_transfer module
    participant Client as Synapse Client
    participant TokenStore as _sts_token_store (StsTokenStore)
    participant Synapse as Synapse REST API

    Note over Client,TokenStore: Client.__init__ creates self._sts_token_store = sts_transfer.StsTokenStore()

    User->>Entity: get_sts_storage_token(permission, output_format)
    activate Entity

    Entity->>Mixin: get_sts_storage_token_async()
    activate Mixin

    Mixin->>Client: Synapse.get_client()
    Client-->>Mixin: Synapse client instance

    Mixin->>STS: sts_transfer.get_sts_credentials()
    activate STS

    STS->>Client: syn._sts_token_store.get_token(...)
    activate Client
    Client->>TokenStore: get_token(entity_id, permission, min_remaining_life)
    activate TokenStore

    alt token cached and not expired
        TokenStore-->>Client: Cached token
    else cache miss or token expired
        TokenStore->>Synapse: GET /entity/{id}/sts?permission={permission}
        activate Synapse
        Synapse-->>TokenStore: STS credentials response
        deactivate Synapse
        TokenStore-->>Client: New token (cached)
    end
    deactivate TokenStore
    Client-->>STS: Token
    deactivate Client

    Note over STS: Parse credentials

    alt output_format == "boto"
        Note over STS: Format for boto3 client kwargs
        STS-->>Mixin: {aws_access_key_id, aws_secret_access_key, aws_session_token}
    else output_format == "json"
        Note over STS: Return JSON string
        STS-->>Mixin: JSON credentials string
    else output_format == "shell" / "bash"
        Note over STS: Format as export commands
        STS-->>Mixin: Shell export commands
    end
    deactivate STS

    Mixin-->>Entity: Formatted credentials
    deactivate Mixin

    Entity-->>User: Credentials
    deactivate Entity
```

<br>

#### Credential Output Formats

| Format | Description | Use Case |
|--------|-------------|----------|
| `boto` | Dict with `aws_access_key_id`, `aws_secret_access_key`, `aws_session_token` | Pass directly to `boto3.client('s3', **creds)` |
| `json` | JSON string | Store or pass to external tools |
| `shell` / `bash` | `export AWS_ACCESS_KEY_ID=...` format | Execute in shell |
| `cmd` | Windows SET commands | Windows command prompt |
| `powershell` | PowerShell variable assignments | PowerShell scripts |

---

<br>
<br>

# Part 3: Settings & Infrastructure

This section covers project settings, API architecture, and the async/sync pattern.

<br>

## Project Setting Lifecycle

Project settings control which storage location(s) are used for uploads to an
entity. The following state diagram shows the lifecycle of a project setting.

```mermaid
stateDiagram-v2
    [*] --> NoSetting: Entity created

    NoSetting --> Created: set_storage_location()
    Note right of NoSetting: Inherits from parent\nor uses Synapse default

    Created --> Updated: set_storage_location()\nwith different locations
    Updated --> Updated: set_storage_location()\nwith different locations

    Created --> Deleted: delete_project_setting()
    Updated --> Deleted: delete_project_setting()

    Deleted --> NoSetting: Returns to default

    state Created {
        [*] --> Active
        Active: locations = [storage_location_id]
        Active: settingsType = "upload"
    }

    state Updated {
        [*] --> Modified
        Modified: locations = [new_id, ...]
        Modified: settingsType = "upload"
    }
```

<br>

### Setting Types

| Type | Purpose | Status |
|------|---------|--------|
| `upload` | Configures upload destination storage location(s) | **Supported** |

Other setting types may be added in the future.

---

<br>

## API Layer Architecture

The storage location services module provides async functions that wrap the
Synapse REST API endpoints. This layer handles serialization and error handling.

```mermaid
flowchart TB
    subgraph "Model Layer"
        SL[StorageLocation]
        SLCM[StorageLocation Mixin]
    end

    subgraph "API Layer (storage_location_services.py)"
        create_sls[create_storage_location_setting]
        get_sls[get_storage_location_setting]
        get_ps[get_project_setting]
        create_ps[create_project_setting]
        update_ps[update_project_setting]
        delete_ps[delete_project_setting]
    end

    subgraph "REST Endpoints"
        POST_SL["POST /storageLocation"]
        GET_SL["GET /storageLocation/{id}"]
        GET_PS["GET /projectSettings/{id}/type/{type}"]
        POST_PS["POST /projectSettings"]
        PUT_PS["PUT /projectSettings"]
        DELETE_PS["DELETE /projectSettings/{id}"]
    end

    SL --> create_sls --> POST_SL
    SL --> get_sls --> GET_SL

    SLCM --> get_ps --> GET_PS
    SLCM --> create_ps --> POST_PS
    SLCM --> update_ps --> PUT_PS
    SLCM --> delete_ps --> DELETE_PS
```

<br>

### REST API Reference

| Method | Endpoint | Description |
|--------|----------|-------------|
| POST | `/storageLocation` | Create a new storage location setting |
| GET | `/storageLocation/{id}` | Retrieve a storage location by ID |
| GET | `/projectSettings/{projectId}/type/{type}` | Get project settings for an entity |
| POST | `/projectSettings` | Create a new project setting |
| PUT | `/projectSettings` | Update an existing project setting |
| DELETE | `/projectSettings/{id}` | Delete a project setting |

---

<br>

## Async/Sync Pattern

The StorageLocation system follows the Python client's `@async_to_sync` pattern,
providing both async and sync versions of all methods.

```mermaid
flowchart LR
    subgraph "User Code"
        SyncCall["folder.set_storage_location()"]
        AsyncCall["await folder.set_storage_location_async()"]
    end

    subgraph "@async_to_sync Decorator"
        Wrapper["Sync wrapper"]
        AsyncMethod["Async implementation"]
    end

    subgraph "Event Loop"
        RunSync["wrap_async_to_sync()"]
        AsyncIO["asyncio"]
    end

    SyncCall --> Wrapper
    Wrapper --> RunSync
    RunSync --> AsyncIO
    AsyncIO --> AsyncMethod

    AsyncCall --> AsyncMethod
```

<br>

### Method Pairs

| Sync Method | Async Method |
|-------------|--------------|
| `StorageLocation.store()` | `StorageLocation.store_async()` |
| `StorageLocation.get()` | `StorageLocation.get_async()` |
| `StorageLocation.setup_s3()` | `StorageLocation.setup_s3_async()` |
| `folder.set_storage_location()` | `folder.set_storage_location_async()` |
| `folder.get_project_setting()` | `folder.get_project_setting_async()` |
| `folder.delete_project_setting()` | `folder.delete_project_setting_async()` |
| `folder.get_sts_storage_token()` | `folder.get_sts_storage_token_async()` |
| `folder.index_files_for_migration()` | `folder.index_files_for_migration_async()` |
| `folder.migrate_indexed_files()` | `folder.migrate_indexed_files_async()` |

---

<br>
<br>

# Part 4: Migration

This section covers the file migration system.

<br>

## Migration Flow

File migration is a two-phase process that moves files from one storage location
to another while preserving Synapse metadata.

```mermaid
sequenceDiagram
    participant User
    participant Entity as Project/Folder
    participant IndexFn as index_files_for_migration
    participant DB as SQLite Database
    participant MigrateFn as migrate_indexed_files
    participant Synapse as Synapse REST API

    rect rgb(240, 248, 255)
        Note over User,Synapse: Phase 1: Index Files
        User->>Entity: index_files_for_migration(dest_id, db_path)
        activate Entity

        Entity->>IndexFn: Start indexing
        activate IndexFn

        IndexFn->>Synapse: Query entity tree
        Synapse-->>IndexFn: File list

        loop For each file
            IndexFn->>Synapse: Get file metadata
            Synapse-->>IndexFn: File info
            IndexFn->>DB: Record file for migration
        end

        IndexFn-->>Entity: MigrationResult (indexed counts)
        deactivate IndexFn

        Entity-->>User: MigrationResult
        deactivate Entity
    end

    rect rgb(240, 248, 255)
        Note over User,Synapse: Phase 2: Migrate Files
        User->>Entity: migrate_indexed_files(db_path)
        activate Entity

        Entity->>MigrateFn: Start migration
        activate MigrateFn

        MigrateFn->>DB: Read indexed files

        loop For each indexed file
            MigrateFn->>Synapse: Copy file to new storage
            Synapse-->>MigrateFn: Success/Failure
            MigrateFn->>DB: Update status
        end

        MigrateFn-->>Entity: MigrationResult (migrated counts)
        deactivate MigrateFn

        Entity-->>User: MigrationResult
        deactivate Entity
    end
```

<br>

### Migration Strategies

| Strategy | Description |
|----------|-------------|
| `new` | Create new file versions in destination (default) |
| `all` | Migrate all versions of each file |
| `latest` | Only migrate the latest version |
| `skip` | Skip if file already exists in destination |

---

<br>
<br>

# Learn More

| Resource | Description |
|----------|-------------|
| [Storage Location Tutorial](../tutorials/python/storage_location.md) | Step-by-step guide to using storage locations |
| [StorageLocation API Reference][synapseclient.models.StorageLocation] | Complete API documentation |
| [StorageLocation Mixin][synapseclient.models.mixins.StorageLocation] | Mixin methods for Projects and Folders |
| [Custom Storage Locations (Synapse Docs)](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html) | Official Synapse documentation |
