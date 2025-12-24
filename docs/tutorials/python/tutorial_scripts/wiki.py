#!/usr/bin/env python3
"""
Tutorial script demonstrating the Synapse Wiki models functionality.

This script shows how to:
1. Create, read, update, and restore wiki pages
2. Create wiki pages from markdown text and files, and download markdown content
3. Create wiki pages with attachments, retrieve attachment handles and URLs, and download attachments and previews
4. Retrieve wiki page hierarchy using WikiHeader
5. Access wiki version history using WikiHistorySnapshot
6. Get, set, and update wiki page ordering using WikiOrderHint
7. Delete wiki pages

"""
import os

from synapseclient import Synapse
from synapseclient.models import (
    Project,
    WikiHeader,
    WikiHistorySnapshot,
    WikiOrderHint,
    WikiPage,
)

syn = Synapse()
syn.login()

# Get the project
project = Project(name="My uniquely named project about Alzheimer's Disease").get()

# Section1: Create, read, and update wiki pages
# Create a new wiki page for the project with plain text markdown
root_wiki_page = WikiPage(
    owner_id=project.id,
    title="My Root Wiki Page",
    markdown="# Welcome to My Root Wiki\n\nThis is a sample root wiki page created with the Synapse client.",
).store()

# OR you can create a wiki page with an existing markdown file. More instructions can be found in section 2.
markdown_file_path = "path/to/your_markdown_file.md"
root_wiki_page = WikiPage(
    owner_id=project.id,
    title="My First Root Wiki Page Version with existing markdown file",
    markdown=markdown_file_path,
).store()

# Update the wiki page
root_wiki_page_new = WikiPage(
    owner_id=project.id,
    title="My First Root Wiki Page NEW",
    markdown="# Welcome to My Root Wiki NEW\n\nThis is a sample root wiki page created with the Synapse client.",
    id=root_wiki_page.id,
).store()

# Restore the wiki page to the original version
wiki_page_restored = WikiPage(
    owner_id=project.id, id=root_wiki_page.id, wiki_version="0"
).restore()

# check if the content is restored
assert (
    root_wiki_page.markdown_file_handle_id == wiki_page_restored.markdown_file_handle_id
), "Markdown file handle ID does not match after restore"
assert (
    root_wiki_page.id == wiki_page_restored.id
), "Wiki page ID does not match after restore"
assert (
    root_wiki_page.title == wiki_page_restored.title
), "Wiki page title does not match after restore"

# Get the wiki page
# Once you know the Wiki page id, you can retrieve the Wiki page with the id
retrieved_wiki = WikiPage(owner_id=project.id, id=root_wiki_page.id).get()

# Or you can retrieve the Wiki page with the title
retrieved_wiki = WikiPage(owner_id=project.id, title=root_wiki_page.title).get()

# Check if the retrieved Wiki page is the same as the original Wiki page
assert (
    root_wiki_page.markdown_file_handle_id == retrieved_wiki.markdown_file_handle_id
), "Markdown file handle ID does not match retrieved wiki page"
assert (
    root_wiki_page.id == retrieved_wiki.id
), "Wiki page ID does not match retrieved wiki page"
assert (
    root_wiki_page.title == retrieved_wiki.title
), "Wiki page title does not match retrieved wiki page"

# Create a sub-wiki page
sub_wiki_1 = WikiPage(
    owner_id=project.id,
    title="Sub Wiki Page 1",
    parent_id=root_wiki_page.id,
    markdown="# Sub Page 1\n\nThis is a sub-page of another wiki.",
).store()

# Section 2: WikiPage Markdown Operations
# Create wiki page from markdown text
markdown_content = """# Sample Markdown Content

## Section 1
This is a sample markdown file with multiple sections.

## Section 2
- List item 1
- List item 2
- List item 3

## Code Example
```python
def hello_world():
    print("Hello, World!")
```
"""

# Create wiki page from markdown text
sub_wiki_2 = WikiPage(
    owner_id=project.id,
    parent_id=root_wiki_page.id,
    title="Sub Page 2 created from markdown text",
    markdown=markdown_content,
).store()

# Create a wiki page from a markdown file
markdown_file_path = "~/temp/temp_markdown_file.md.gz"

# Create wiki page from markdown file
sub_wiki_3 = WikiPage(
    owner_id=project.id,
    parent_id=root_wiki_page.id,
    title="Sub Page 3 created from markdown file",
    markdown=markdown_file_path,
).store()

# Download the markdown file
# Note: If the markdown is generated from plain text using the client, the downloaded file will be named wiki_markdown_<wiki_page_title>.md.gz. If it is generated from an existing markdown file, the downloaded file will retain the original filename.
# Download the markdown file for sub_wiki_2 that is created from markdown text
wiki_page_markdown_2_url = WikiPage(
    owner_id=project.id,
    id=sub_wiki_2.id,
).get_markdown_file(
    download_file=False,
)

# Download the markdown file for sub_wiki_2 that is created from markdown text
wiki_page_markdown_2 = WikiPage(
    owner_id=project.id, id=sub_wiki_2.id
).get_markdown_file(download_file=True, download_location=".")

# Download the markdown file for sub_wiki_3 that is created from a markdown file
wiki_page_markdown_3 = WikiPage(
    owner_id=project.id, id=sub_wiki_3.id
).get_markdown_file(download_file=True, download_location=".")

# Section 3: WikiPage with Attachments
# Create a temporary file for the attachment
attachment_file_name = "temp_attachment.txt"

# reformat '.' and '_' in the attachment file name to be a valid attachment path
attachment_file_name_reformatted = WikiPage.reformat_attachment_file_name(
    os.path.basename(attachment_file_name)
)
sub_wiki_4 = WikiPage(
    owner_id=project.id,
    parent_id=root_wiki_page.id,
    title="Sub Page 4 with Attachments",
    markdown=f"# Sub Page 4 with Attachments\n\nThis is a attachment: ${{previewattachment?fileName={attachment_file_name_reformatted}}}",
    attachments=[attachment_file_name],
).store()

# Inlucde images in the markdown file
image_file_path = "path/to/test_image.png"
# use the original file name instead of the gzipped file name for images
image_file_name = os.path.basename(image_file_path)
markdown_content = f"# Sub Page 5 with images\n\nThis is an attached image: ${{image?fileName=test_image.png&align=None&scale=100&responsive=true&altText=}}"
sub_wiki_5 = WikiPage(
    owner_id=project.id,
    parent_id=root_wiki_page.id,
    title="Sub Page 5 with Images",
    markdown=markdown_content,
    attachments=[image_file_path],
).store()

# Get attachment handles
attachment_handles = WikiPage(
    owner_id=project.id, id=sub_wiki_4.id
).get_attachment_handles()

# Get attachment URL without downloading
wiki_page_attachment_url = WikiPage(
    owner_id=project.id, id=sub_wiki_4.id
).get_attachment(
    file_name=os.path.basename(attachment_file_name),
    download_file=False,
)

# Download an attachment
wiki_page_attachment = WikiPage(owner_id=project.id, id=sub_wiki_4.id).get_attachment(
    file_name=os.path.basename(attachment_file_name),
    download_file=True,
    download_location=".",
)
# Unzip the attachment file
unzipped_attachment_file_path = WikiPage.unzip_gzipped_file(wiki_page_attachment)

# Download an attachment preview. Instead of using the file_name from the attachmenthandle response when isPreview=True, you should use the original file name in the get_attachment_preview request. The downloaded file will still be named according to the file_name provided in the response when isPreview=True.
# Get attachment preview URL without downloading
attachment_preview_url = WikiPage(
    owner_id=project.id, id=sub_wiki_4.id
).get_attachment_preview(
    file_name=os.path.basename(attachment_file_name),
    download_file=False,
)

# Download an attachment preview
attachment_preview = WikiPage(
    owner_id=project.id, id=sub_wiki_4.id
).get_attachment_preview(
    file_name=os.path.basename(attachment_file_name),
    download_file=True,
    download_location=".",
)

# Section 4: WikiHeader - Working with Wiki Hierarchy
# Get wiki header tree (hierarchy)
headers = WikiHeader.get(owner_id=project.id)

# Section 5. WikiHistorySnapshot - Version History
# Get wiki history for root_wiki_page
history = WikiHistorySnapshot.get(owner_id=project.id, id=root_wiki_page.id)

# Section 6. WikiOrderHint - Ordering Wiki Pages
# Set the wiki order hint
order_hint = WikiOrderHint(owner_id=project.id).get()
# As you can see from the printed message, the order hint is not set by default, so you need to set it explicitly at the beginning.
order_hint.id_list = [
    root_wiki_page.id,
    sub_wiki_3.id,
    sub_wiki_4.id,
    sub_wiki_1.id,
    sub_wiki_2.id,
    sub_wiki_5.id,
]
order_hint.store()

# Update wiki order hint
order_hint = WikiOrderHint(owner_id=project.id).get()
order_hint.id_list = [
    root_wiki_page.id,
    sub_wiki_1.id,
    sub_wiki_2.id,
    sub_wiki_3.id,
    sub_wiki_4.id,
    sub_wiki_5.id,
]
order_hint.store()

# Delete a wiki page
sub_wiki_6 = WikiPage(
    owner_id=project.id,
    parent_id=root_wiki_page.id,
    title="Sub Page 6 to be deleted",
    markdown=f"# Sub Page 6 to be deleted\n\nThis is a sub page to be deleted.",
).store()
wiki_page_to_delete = WikiPage(owner_id=project.id, id=sub_wiki_6.id).delete()
