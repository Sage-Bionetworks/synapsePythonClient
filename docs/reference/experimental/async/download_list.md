# Download List

The Synapse Download List (cart) lets you queue files for bulk download via the Synapse
web UI or API. Files are downloaded individually rather than packaged into a zip because
download lists can exceed 100 GB. Successfully downloaded files are removed from the cart
automatically, so interrupted runs are safely resumable.

## API Reference

[](){ #download-list-reference-async }

::: synapseclient.operations.download_list_files_async

::: synapseclient.operations.download_list_manifest_async

::: synapseclient.operations.download_list_add_async

::: synapseclient.operations.download_list_remove_async

::: synapseclient.operations.download_list_clear_async

---

[](){ #download-list-item-reference-async }
## DownloadListItem

Identifies a specific file version in the download list. Used as input to
download_list_add_async and download_list_remove_async.

::: synapseclient.operations.DownloadListItem
