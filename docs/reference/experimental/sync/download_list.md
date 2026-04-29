[](){ #download-list-reference-sync }
# Download List

The Synapse Download List (cart) lets you queue files for bulk download via the Synapse
web UI or API. Files are downloaded individually rather than packaged into a zip because
download lists can exceed 100 GB. Successfully downloaded files are removed from the cart
automatically, so interrupted runs are safely resumable.

## Example

```python
from synapseclient import Synapse
from synapseclient.operations import download_list_files

syn = Synapse()
syn.login()

# Download all files in the cart to a local directory
manifest_path = download_list_files(download_location="./downloads")
```

## API Reference

::: synapseclient.operations.download_list_files

::: synapseclient.operations.download_list_manifest

::: synapseclient.operations.download_list_add

::: synapseclient.operations.download_list_remove

::: synapseclient.operations.download_list_clear

---

[](){ #download-list-item-reference-sync }
## DownloadListItem

Identifies a specific file version in the download list. Used as input to
download_list_add and download_list_remove.

::: synapseclient.operations.DownloadListItem
