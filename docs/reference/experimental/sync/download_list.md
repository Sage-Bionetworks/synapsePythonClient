[](){ #download-list-reference-sync }
# Download List

The Synapse Download List (cart) lets you queue files for bulk download via the Synapse
web UI or API. Files are downloaded individually rather than packaged into a zip because
download lists can exceed 100 GB. Successfully downloaded files are removed from the cart
automatically, so interrupted runs are safely resumable.

## Example

```python
from synapseclient import Synapse
from synapseclient.models import DownloadList

syn = Synapse()
syn.login()

# Download all files in the cart to a local directory
manifest_path = DownloadList().download_files(download_location="./downloads")
```

## API Reference

::: synapseclient.models.DownloadList
    options:
        inherited_members: true
        members:
        - download_files
        - get_manifest
        - add_files
        - remove_files
        - clear

---

[](){ #download-list-item-reference-sync }
## DownloadListItem

Identifies a specific file version in the download list. Used as input to
add_files and remove_files.

::: synapseclient.models.DownloadListItem
