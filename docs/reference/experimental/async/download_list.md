# Download List

The Synapse Download List (cart) lets you queue files for bulk download via the Synapse
web UI or API. Files are downloaded individually rather than packaged into a zip because
download lists can exceed 100 GB. Successfully downloaded files are removed from the cart
automatically, so interrupted runs are safely resumable.

## API Reference

[](){ #download-list-reference-async }
::: synapseclient.models.DownloadList
    options:
        inherited_members: true
        members:
        - download_files_async
        - get_manifest_async
        - add_files_async
        - remove_files_async
        - clear_async

---

[](){ #download-list-item-reference-async }
## DownloadListItem

Identifies a specific file version in the download list. Used as input to
add_files_async and remove_files_async.

::: synapseclient.models.DownloadListItem
