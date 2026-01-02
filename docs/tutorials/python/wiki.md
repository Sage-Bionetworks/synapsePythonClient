# Wikis
Wikis in Synapse provide rich documentation and collaborative content for projects, folders, files, datasets, and other entities. They support markdown formatting, file attachments, hierarchical organization, version history, and custom ordering. Wikis are managed through the Synapse Python client using models like:

- **[WikiPage](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/v2/wiki/V2WikiPage.html)**: The main Wiki page model for creating and managing Wiki content
- **[WikiHeader](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/v2/wiki/V2WikiHeader.html)**: Represents Wiki page headers and hierarchy information
- **[WikiHistorySnapshot](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/v2/wiki/V2WikiHistorySnapshot.html)**: Provides access to Wiki version history
- **[WikiOrderHint](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/v2/wiki/V2WikiOrderHint.html)**: Manages the order of Wiki pages within an entity

**Important:** Wikis inherit the sharing permissions of its associated Synapse entities. Anyone with view access or higher can see the Wiki’s content. If a project, folder, or file is shared publicly, the linked Wiki will also be publicly visible, including to users who are not logged in to Synapse. For this reason, protected human data must not be stored in Synapse Wikis.

You can view your Wiki pages in the [Synapse web UI](https://www.synapse.org/) by navigating to your project and clicking on the Wiki tab.


## Tutorial Purpose

In this tutorial you will:

1. Create, read, update, and restore Wiki pages
2. Create Wiki pages from markdown text and files, and download markdown content
3. Create Wiki pages with attachments, retrieve attachment handles and URLs, and download attachments and previews
4. Retrieve Wiki page hierarchy using WikiHeader
5. Access Wiki version history using WikiHistorySnapshot
6. Get, set, and update Wiki page ordering using WikiOrderHint
7. Delete Wiki pages

## Prerequisites

* Make sure that you have completed the [Installation](../installation.md) and [Authentication](../authentication.md) setup.
* This tutorial assumes you have a basic understanding of Synapse projects. If you need to create a project, see the [Projects tutorial](project.md).

## 1. Create a Wiki page
### Initial setup
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=15-30}
```
A Wiki page requires an owner object, a title, and markdown. Here is an example to create a new root Wiki page for your project with plain text markdown:
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=34-38}
```

Alternatively, you can create a Wiki page from an existing markdown file.
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=41-46}
```
<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
Uploaded file handle ... for Wiki page markdown.
No Wiki page exists within the owner. Create a new Wiki page.
Created Wiki page: My Root Wiki Page with ID: ...
```
</details>

## 2. Update a Wiki page

To update an existing Wiki page, create a new WikiPage object with the same `id` and new content:

```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=49-54}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>

```
Uploaded file handle ... for Wiki page markdown.
A Wiki page already exists within the owner. Update the existing Wiki page.
Updated Wiki page: My First Root Wiki Page NEW with ID: ...
```
</details>

## 3. Restore a Wiki page to a previous version

You can restore a Wiki page to any previous version by specifying the `Wiki_version` parameter:
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=57-59}
```

Check if the content is restored.
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=62-70}
```

## 4. Get a Wiki page
You can retrieve Wiki pages in several ways. To find a Wiki page id, you can get the WikiHeader tree to see all Wiki pages in the hierarchy (see the WikiHeader section below).

Once you know the Wiki page id, you can retrieve a specific Wiki page:
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=74}
```

Alternatively, you can retrieve a Wiki page by its title:
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=77}
```

Verify that the retrieved Wiki page matches the original Wiki page
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=80-88}
```

## 5. Create a sub-Wiki page
You can create a sub-Wiki page under an existing Wiki page.
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=89-94}
```
<details class="example">
  <summary>You'll notice the output looks like:</summary>
```
Uploaded file handle ... for wiki page markdown.
Creating sub-wiki page under parent ID: ...
Created sub-wiki page: Sub Wiki Page 1 with ID: ... under parent: ...
```
</details>

## 6. Create a Wiki page from markdown
### Create a Wiki page directly from a string:
You can create a Wiki page from a single-line or multi-line Python string. Here is an example of creating a Wiki page from a multi-line Python string:

```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=100-121}
```

### Create a Wiki page from a markdown file

You can also create a Wiki page from an existing markdown file. Markdown files may be uploaded in either non-gzipped or gzipped format:

```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=126-134}
```

## 7. Download Wiki page markdown
You can download the markdown content of a Wiki page back to a file.

### Download the markdown file URL for a Wiki page
```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=139-144}
```

### Download the markdown file for a Wiki page that is created from plain text, the downloaded file will be named `wiki_markdown_<wiki_page_title>.md`
```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=147-149}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>
```
Your markdown content in plain text
Downloaded and unzipped the markdown file for wiki page ... to path/to/wiki_markdown_Sub Page 2 created from markdown text.md.
```
</details>

### Download the markdown file for a Wiki page that is created from a markdown file
```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=152-154}
```
<details class="example">
  <summary>You'll notice the output looks like:</summary>
```
Downloaded and unzipped the markdown file for wiki page ... to path/to/sample_wiki.md.
```
</details>

## 8. Create Wiki pages with attachments

Wiki pages can include file attachments, which are useful for sharing supplementary materials such as images, data files, or documents. Attachment files may be uploaded in either non-gzipped or gzipped format:

### Create a Wiki page with attachments

First, create a file to attach. Then create a Wiki page with the attachment. Note that attachment file names in markdown need special formatting: replace `.` with `%2E` and `_` with `%5F`. You can utilize the static method `WikiPage.reformat_attachment_file_name` to reformat the file name.

```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=158-170}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>
```
Uploaded file handle ... for wiki page attachment.
Creating sub-wiki page under parent ID: ...
Created sub-wiki page: Sub Page 4 with Attachments with ID: ... under parent: ...
```
</details>

To include images in your Wiki page, you DO NOT need to reformat the file name for image files (e.g., PNG, JPG, JPEG).
```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=173-183}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>
```
Uploaded file handle ... for wiki page attachment.
Creating sub-wiki page under parent ID: ...
Created sub-wiki page: Sub Page 5 with Attachments with ID: ... under parent: ...
```
</details>

## 9. Retrieve attachment handles and URLs, and download attachments and previews
### Get attachment handles
Retrieve the file handles of all attachments on a Wiki page:

```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=186-188}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>
```
{'list': [{'id': '...', 'etag': ..., 'concreteType': 'org.sagebionetworks.repo.model.file.S3FileHandle', 'contentType': '...', 'contentMd5': '...', 'fileName': '...', 'storageLocationId': 1,..., 'isPreview': False}]}
```
</details>

### Get attachment URL without downloading

You can retrieve the URL of an attachment without downloading it. Attachment file name can be in either non-gzipped or gzipped format.

```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=191-196}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>
```
'https://data.prod.sagebase.org/...'
```
</details>

### Download an attachment file

Download an attachment file to your local machine and unzip it using `WikiPage.unzip_gzipped_file` function.
```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=199-205}
```

### Get attachment preview URL
You can also retrieve preview URLs for attachments. When using `get_attachment_preview`, specify the original file name, not the file name returned in the attachment handle response when isPreview=True. The file name can be in either non-gzipped or gzipped format.
The downloaded file will still be named according to the file name provided in the response when isPreview=True. Note that image attachments do not have preview files.
```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=209-214}
```

### Download an attachment preview

Download the preview version of an attachment:

```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=217-223}
```

The downloaded preview file will be named `preview.<your attachment file type>` (or according to the file name in the attachment handle response when `isPreview=True`).

## 10. Retrieve Wiki page hierarchy using WikiHeader

WikiHeader allows you to retrieve the hierarchical structure of Wiki pages within an entity.

### Get Wiki header tree

Retrieve the complete Wiki page hierarchy for a project:

```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=226}
```

<details class="example">
  <summary>You'll notice the output shows the Wiki hierarchy:</summary>
```
[WikiHeader(id='...', title='My Root Wiki Page', ...), WikiHeader(id='...', title='Sub Wiki Page 1', ...)]
```
</details>

## 11. Access Wiki version history using WikiHistorySnapshot

WikiHistorySnapshot provides access to the version history of Wiki pages, allowing you to see all previous versions and their metadata.

### Get Wiki history

Retrieve the version history for a specific Wiki page:

```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=230}
```
<details class="example">
  <summary>You'll notice the output shows the history of versions:</summary>
```
[WikiHistorySnapshot(version='..', modified_on='...', modified_by='...'), ..., WikiHistorySnapshot(version='...',...)]
```
</details>

## 12. Get, set, and update Wiki page ordering using WikiOrderHint

WikiOrderHint allows you to control the order in which Wiki pages are displayed. By default, Wiki pages don't have an explicit order, so you need to set it explicitly.

### Get the current order hint

First, retrieve the current order hint (which may be empty initially):

```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=234}
```

<details class="example">
  <summary>You'll notice the output looks like:</summary>
```
WikiOrderHint(owner_id='...', owner_object_type='ENTITY', id_list=[], etag='...')
```
</details>

### Set the Wiki order hint

Set the order of Wiki pages by providing a list of Wiki page IDs in the desired order:

```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=237-245}
```

<details class="example">
  <summary>You'll notice the output shows the updated order:</summary>
```
WikiOrderHint(id_list=['...', '...', ...])
```
</details>

### Update Wiki order hint

You can update the order hint at any time by retrieving it, modifying the `id_list`, and storing it again:

```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=248-257}
```

## 13. Delete Wiki pages

Delete a Wiki page by providing the owner ID and Wiki page ID:

```python
{!docs/tutorials/python/tutorial_scripts/Wiki.py!lines=260-266}
```

## Source Code for this Tutorial

<details class="quote">
  <summary>Click to show me</summary>
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!}
```
</details>

## References

- [WikiPage][synapseclient.models.WikiPage]
- [WikiHeader][synapseclient.models.WikiHeader]
- [WikiHistorySnapshot][synapseclient.models.WikiHistorySnapshot]
- [WikiOrderHint][synapseclient.models.WikiOrderHint]
- [Project][synapseclient.models.Project]
- [syn.login][synapseclient.Synapse.login]
- [Related tutorial: Projects](project.md)
