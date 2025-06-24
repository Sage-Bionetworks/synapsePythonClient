# Wikis on Projects

# Synapse Wiki Models Tutorial

This tutorial demonstrates how to work with Wiki models in the Synapse Python client. Wikis in Synapse provide a way to create rich documentation and collaborative content for projects, folders, files, datasets, and other entities.

## Overview

The Synapse Wiki models include:
- **WikiPage**: The main wiki page model for creating and managing wiki content
- **WikiHeader**: Represents wiki page headers and hierarchy information
- **WikiHistorySnapshot**: Provides access to wiki version history
- **WikiOrderHint**: Manages the order of wiki pages within an entity

This tutorial shows how to:
1. Create, read, and update wiki pages
2. Work with WikiPage Markdown
3. Work with WikiPage Attachments
4. Work with WikiHeader
5. Work with WikiHistorySnapshot
6. Work with WikiOrderHint
7. Delete wiki pages

## Basic Setup
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=14-36}
```

## 1. Create, read, and update wiki pages
### Create a new wiki page for the project with plain text markdown
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=40-44}
```

### OR you can create a wiki page with an existing markdown file. More instructions can be found in section 2.
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=47-52}
```

### Create a new wiki page with updated content
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=55-60}
```

### Restore the wiki page to the original version
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=63-74}
```

### Create a sub-wiki page
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=77-82}
```

### Get an existing wiki page for the project, now you can see one root wiki page and one sub-wiki page
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=85-86}
```

### Retrieving a Wiki Page
Note: You need to know the wiki page ID or wiki page title to retrieve it
#### Retrieve a Wiki Page with wiki page ID
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=89-90}
```

#### Retrieve a Wiki Page with wiki page title
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=93-94}
```

#### Check if the retrieved wiki page is the same as the original wiki page
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=97-102}
```

## 2. WikiPage Markdown Operations
### Create wiki page from markdown text
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=106-129}
```

### Create wiki page from a markdown file
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=133-143}
```

### Download the markdown file
Note: If the markdown is generated from plain text using the client, the downloaded file will be named wiki_markdown_<wiki_page_title>.md.gz. If it is generated from an existing markdown file, the downloaded file will retain the original filename with the .gz suffix appended.
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=146-173}
```

## 3. WikiPage Attachments Operations
### Create a wiki page with attachments
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=176-191}
```
### Get the file handles of all attachments on this wiki page.
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=194-197}
```

### Get attachment URL without downloading
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=200-206}
```
### Download an attachment
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=209-219}
```

### Download an attachment preview URL without downloading
Download an attachment preview. Instead of using the file_name from the attachmenthandle response when isPreview=True, you should use the original file name in the get_attachment_preview request. The downloaded file will still be named according to the file_name provided in the response when isPreview=True.
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=223-229}
```

#### Download an attachment preview
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=232-240}
```

## 4. WikiHeader - Working with Wiki Hierarchy
### Getting Wiki Header Tree
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=244-246}
```

## 5. WikiHistorySnapshot - Version History
### Accessing Wiki History
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=250-251}
```

## 6. WikiOrderHint - Managing Wiki Order
Note: You need to have order hint set before pulling.
### Set the wiki order hint
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=255-266}
```

### Update wiki order hint
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=269-278}
```

### Deleting a Wiki Page
Note: You need to know the owner ID and wiki page ID to delete a wiki page
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=281}
```

## clean up
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=284}
```
