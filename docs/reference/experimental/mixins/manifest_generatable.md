# ManifestGeneratable Mixin

The `ManifestGeneratable` mixin provides manifest TSV file generation and reading capabilities for container entities (Projects and Folders).

## Overview

This mixin enables:

- Generating manifest TSV files after syncing from Synapse
- Uploading files from manifest TSV files
- Validating manifest files before upload

## Usage

The mixin is automatically available on `Project` and `Folder` classes:

```python
from synapseclient.models import Project, Folder

# Project and Folder both have manifest capabilities
project = Project(id="syn123")
folder = Folder(id="syn456")
```

## API Reference

::: synapseclient.models.mixins.manifest.ManifestGeneratable
    options:
      show_root_heading: true
      show_source: false
      members:
        - generate_manifest
        - generate_manifest_async
        - from_manifest
        - from_manifest_async
        - validate_manifest
        - validate_manifest_async
        - get_manifest_data
        - get_manifest_data_async

## Constants

### MANIFEST_FILENAME

The default filename for generated manifests: `SYNAPSE_METADATA_MANIFEST.tsv`

```python
from synapseclient.models import MANIFEST_FILENAME

print(MANIFEST_FILENAME)  # "SYNAPSE_METADATA_MANIFEST.tsv"
```

### DEFAULT_GENERATED_MANIFEST_KEYS

The default columns included in generated manifest files:

```python
from synapseclient.models import DEFAULT_GENERATED_MANIFEST_KEYS

print(DEFAULT_GENERATED_MANIFEST_KEYS)
# ['path', 'parent', 'name', 'id', 'synapseStore', 'contentType',
#  'used', 'executed', 'activityName', 'activityDescription']
```

## See Also

- [Manifest Operations Tutorial](../../../tutorials/python/manifest_operations.md)
- [StorableContainer Mixin](storable_container.md)
- [Manifest TSV Format](../../../explanations/manifest_tsv.md)
