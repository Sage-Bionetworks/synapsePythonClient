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
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=14-29}
```

## 1. Create, read, and update wiki pages
### Create a new wiki page for the project with plain text markdown
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=33-37}
```

### OR you can create a wiki page with an existing markdown file
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=40-45}
```

### Create a new wiki page with updated content
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=48-53}
```

### Restore the wiki page to the original version
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=56-64}
```

### Create a sub-wiki page
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=67-72}
```

### Get an existing wiki page for the project, now you can see one root wiki page and one sub-wiki page
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=75-76}
```

### Retrieving a Wiki Page
Note: You need to know the wiki page ID or wiki page title to retrieve it
#### Retrieve a Wiki Page with wiki page ID
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=79-80}
```

#### Retrieve a Wiki Page with wiki page title
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=83-84}
```

#### Check if the retrieved wiki page is the same as the original wiki page
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=87-92}
```

## 2. WikiPage Markdown Operations
### Create wiki page from markdown text
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=96-119}
```

### Create wiki page from a markdown file
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=123-133}
```

### Download the markdown file
Note: If the markdown is generated from plain text using the client, the downloaded file will be named wiki_markdown_<wiki_page_id>.md.gz. If it is generated from an existing markdown file, the downloaded file will retain the original filename with the .gz suffix appended.
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=137-143}
```

## 3. WikiPage Attachments Operations
### Create a wiki page with attachments
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=147-161}
```
### Get the file handles of all attachments on this wiki page.
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=164-165}
```
### Download an attachment
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=167-175}
```

### Get attachment URL without downloading
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=178-182}
```

### Download an attachment preview (WIP)
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=185-191}
```
#### Get attachment preview URL without downloading (WIP)


## 4. WikiHeader - Working with Wiki Hierarchy
### Getting Wiki Header Tree
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=197-200}
```

## 5. WikiHistorySnapshot - Version History

### Accessing Wiki History
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=204-208}
```

## 6. WikiOrderHint - Managing Wiki Order
### Get wiki order hint (No id_list returned, same result getting from direct endpoint calls)
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=212-213}
```
### Update wiki order hint
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=216-222}
```

### Deleting a Wiki Page
Note: You need to know the owner ID and wiki page ID to delete a wiki page
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=225}
```

## clean up
```python
{!docs/tutorials/python/tutorial_scripts/wiki.py!lines=228}
```
