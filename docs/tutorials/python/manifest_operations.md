# Manifest Operations

This tutorial covers how to work with manifest TSV files for bulk file operations in Synapse. Manifest files provide a way to track file metadata, download files with their annotations, and upload files with provenance information.

## Overview

A manifest file is a tab-separated values (TSV) file that contains metadata about files in Synapse. The manifest includes:

- File paths and Synapse IDs
- Parent container IDs
- Annotations
- Provenance information (used/executed references)

## Generating Manifests During Download

When syncing files from Synapse, you can automatically generate a manifest file that captures all file metadata.

### Using sync_from_synapse with Manifest Generation

```python
from synapseclient.models import Project
import synapseclient

synapseclient.login()

# Download a project with manifest generation at each directory level
project = Project(id="syn123456").sync_from_synapse(
    path="/path/to/download",
    generate_manifest="all"
)

# Or generate a single manifest at the root level only
project = Project(id="syn123456").sync_from_synapse(
    path="/path/to/download",
    generate_manifest="root"
)
```

### Manifest Generation Options

The `generate_manifest` parameter accepts three values:

| Value | Description |
|-------|-------------|
| `"suppress"` | (Default) Do not create any manifest files |
| `"root"` | Create a single manifest at the root download path |
| `"all"` | Create a manifest in each directory level |

### Generating Manifest Separately

You can also generate a manifest after syncing:

```python
from synapseclient.models import Project
import synapseclient

synapseclient.login()

# First sync without manifest
project = Project(id="syn123456").sync_from_synapse(
    path="/path/to/download"
)

# Then generate manifest separately
manifest_path = project.generate_manifest(
    path="/path/to/download",
    manifest_scope="root"
)
print(f"Manifest created at: {manifest_path}")
```

## Manifest File Format

The generated manifest file (`SYNAPSE_METADATA_MANIFEST.tsv`) contains the following columns:

| Column | Description |
|--------|-------------|
| `path` | Local file path |
| `parent` | Synapse ID of the parent container |
| `name` | File name in Synapse |
| `id` | Synapse file ID |
| `synapseStore` | Whether the file is stored in Synapse |
| `contentType` | MIME type of the file |
| `used` | Provenance - entities used to create this file |
| `executed` | Provenance - code/scripts executed |
| `activityName` | Name of the provenance activity |
| `activityDescription` | Description of the provenance activity |
| *custom columns* | Any annotations on the files |

### Example Manifest

```tsv
path	parent	name	id	synapseStore	contentType	used	executed	activityName	activityDescription	study	dataType
/data/file1.csv	syn123	file1.csv	syn456	True	text/csv			Data Processing		Study1	RNA-seq
/data/file2.csv	syn123	file2.csv	syn789	True	text/csv	syn456		Analysis	Processed from file1	Study1	RNA-seq
```

## Uploading Files from a Manifest

You can upload files to Synapse using a manifest file:

```python
from synapseclient.models import Project
import synapseclient

synapseclient.login()

# Upload files from a manifest
files = Project.from_manifest(
    manifest_path="/path/to/manifest.tsv",
    parent_id="syn123456"
)

for file in files:
    print(f"Uploaded: {file.name} ({file.id})")
```

### Dry Run Validation

Before uploading, you can validate the manifest:

```python
from synapseclient.models import Project

# Validate without uploading
is_valid, errors = Project.validate_manifest(
    manifest_path="/path/to/manifest.tsv"
)

if is_valid:
    print("Manifest is valid, ready for upload")
else:
    for error in errors:
        print(f"Error: {error}")
```

Or use the `dry_run` option to validate the manifest and see what would be uploaded without making changes:

```python
# Dry run - validates and returns what would be uploaded, but doesn't upload
files = Project.from_manifest(
    manifest_path="/path/to/manifest.tsv",
    parent_id="syn123456",
    dry_run=True  # Validate only, no actual upload
)
print(f"Would upload {len(files)} files")
```

The `dry_run` parameter is useful for:

- Validating manifest format before committing to an upload
- Testing your manifest configuration
- Previewing which files will be affected

## Working with Annotations

Annotations in the manifest are automatically handled:

### On Download

When generating a manifest, all file annotations are included as additional columns:

```python
project = Project(id="syn123456").sync_from_synapse(
    path="/path/to/download",
    generate_manifest="root"
)
# Annotations appear as columns in the manifest
```

### On Upload

Any columns in the manifest that aren't standard fields become annotations:

```tsv
path	parent	study	dataType	specimenType
/data/file1.csv	syn123	Study1	RNA-seq	tissue
```

```python
files = Project.from_manifest(
    manifest_path="/path/to/manifest.tsv",
    parent_id="syn123456",
    merge_existing_annotations=True  # Merge with existing annotations
)
```

## Working with Provenance

### On Download

Provenance information is captured in the `used`, `executed`, `activityName`, and `activityDescription` columns:

```python
project = Project(id="syn123456").sync_from_synapse(
    path="/path/to/download",
    include_activity=True,  # Include provenance
    generate_manifest="root"
)
```

### On Upload

You can specify provenance in the manifest:

```tsv
path	parent	used	executed	activityName	activityDescription
/data/output.csv	syn123	syn456;syn789	https://github.com/repo/script.py	Analysis	Generated from input files
```

- Multiple references are separated by semicolons (`;`)
- References can be Synapse IDs, URLs, or local file paths

## Synapse Download List Integration

The manifest functionality integrates with Synapse's Download List feature. You can generate a manifest directly from your Synapse download list, which is useful for exporting metadata about files you've queued for download in the Synapse web interface.

### Generating Manifest from Download List

```python
from synapseclient.models import Project
import synapseclient

synapseclient.login()

# Generate a manifest from your Synapse download list
manifest_path = Project.generate_download_list_manifest(
    download_path="/path/to/save/manifest"
)
print(f"Manifest downloaded to: {manifest_path}")
```

### Custom CSV Formatting

You can customize the manifest format:

```python
from synapseclient.models import Project
import synapseclient

synapseclient.login()

# Generate a tab-separated manifest
manifest_path = Project.generate_download_list_manifest(
    download_path="/path/to/save/manifest",
    csv_separator="\t",  # Tab-separated
    include_header=True
)
```

### Using DownloadListManifestRequest Directly

For more control over the manifest generation process, use the `DownloadListManifestRequest` class directly:

```python
from synapseclient.models import DownloadListManifestRequest, CsvTableDescriptor
import synapseclient

synapseclient.login()

# Create a request with custom CSV formatting
request = DownloadListManifestRequest(
    csv_table_descriptor=CsvTableDescriptor(
        separator="\t",
        quote_character='"',
        is_first_line_header=True
    )
)

# Send the job and wait for completion
request.send_job_and_wait()

# Download the generated manifest
manifest_path = request.download_manifest(download_path="/path/to/download")
print(f"Manifest file handle: {request.result_file_handle_id}")
```

## Best Practices

1. **Use `generate_manifest="root"` for simple cases** - Creates a single manifest at the root level, easier to manage.

2. **Use `generate_manifest="all"` for complex hierarchies** - Creates manifests at each directory level, useful for large projects with many subdirectories.

3. **Validate manifests before upload** - Use `validate_manifest()` or `dry_run=True` to catch errors early.

4. **Include provenance information** - Set `include_activity=True` when syncing to capture provenance in the manifest.

5. **Backup your manifest** - The manifest is a valuable record of your data and its metadata.

## Async API

All manifest operations are available as async methods:

```python
import asyncio
from synapseclient.models import Project
import synapseclient

async def main():
    synapseclient.login()

    # Async sync with manifest
    project = Project(id="syn123456")
    await project.sync_from_synapse_async(
        path="/path/to/download",
        generate_manifest="root"
    )

    # Async manifest generation
    manifest_path = await project.generate_manifest_async(
        path="/path/to/download",
        manifest_scope="root"
    )

    # Async upload from manifest
    files = await Project.from_manifest_async(
        manifest_path="/path/to/manifest.tsv",
        parent_id="syn123456"
    )

asyncio.run(main())
```

## See Also

- [Download Data in Bulk](download_data_in_bulk.md)
- [Upload Data in Bulk](upload_data_in_bulk.md)
- [Manifest TSV Format](../../explanations/manifest_tsv.md)
