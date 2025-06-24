#!/usr/bin/env python3
"""
Tutorial script demonstrating the Synapse Wiki models functionality.

This script shows how to:
1. Create, read, and update wiki pages
2. Work with WikiPage Markdown
3. Work with WikiPage Attachments
4. Work with WikiHeader
5. Work with WikiHistorySnapshot
6. Work with WikiOrderHint
7. Delete wiki pages
"""
import gzip
import os
import uuid

from synapseclient import Synapse
from synapseclient.models import (
    Project,
    WikiHeader,
    WikiHistorySnapshot,
    WikiOrderHint,
    WikiPage,
)

# Initialize Synapse client
syn = Synapse()
syn.login()

# Create a Synapse Project to work with
my_test_project = Project(
    name=f"My Test Project_{uuid.uuid4()}",
    description="This is a test project for the wiki tutorial.",
).store()
print(f"Created project: {my_test_project.name} with ID: {my_test_project.id}")

# Section1: Create, read, and update wiki pages
# Create a new wiki page for the project with plain text markdown
root_wiki_page = WikiPage(
    owner_id=my_test_project.id,
    title="My Root Wiki Page",
    markdown="# Welcome to My Root Wiki\n\nThis is a sample root wiki page created with the Synapse client.",
).store()

# OR you can create a wiki page with an existing markdown file. More instructions can be found in section 2.
markdown_file_path = "path/to/your_markdown_file.md"
root_wiki_page = WikiPage(
    owner_id=my_test_project.id,
    title="My First Root Wiki Page Version with existing markdown file",
    markdown=markdown_file_path,
).store()

# Create a new wiki page with updated content
root_wiki_page_new = WikiPage(
    owner_id=my_test_project.id,
    title="My First Root Wiki Page NEW",
    markdown="# Welcome to My Root Wiki NEW\n\nThis is a sample root wiki page created with the Synapse client.",
    id=root_wiki_page.id,
).store()

# Restore the wiki page to the original version
wiki_page_restored = WikiPage(
    owner_id=my_test_project.id, id=root_wiki_page.id, wiki_version="0"
).restore()

# check if the content is restored
comparisons = [
    root_wiki_page.markdown_file_handle_id
    == wiki_page_restored.markdown_file_handle_id,
    root_wiki_page.id == wiki_page_restored.id,
    root_wiki_page.title == wiki_page_restored.title,
]
print(f"All fields match: {all(comparisons)}")

# Create a sub-wiki page
sub_wiki_1 = WikiPage(
    owner_id=my_test_project.id,
    title="Sub Wiki Page 1",
    parent_id=root_wiki_page.id,  # Use the ID of the parent wiki page we created '633033'
    markdown="# Sub Page 1\n\nThis is a sub-page of another wiki.",
).store()

# Get an existing wiki page for the project, now you can see one root wiki page and one sub-wiki page
wiki_header_tree = WikiHeader.get(owner_id=my_test_project.id)
print(wiki_header_tree)

# Once you know the wiki page id, you can retrieve the wiki page with the id
retrieved_wiki = WikiPage(owner_id=my_test_project.id, id=sub_wiki_1.id).get()
print(f"Retrieved wiki page with title: {retrieved_wiki.title}")

# Or you can retrieve the wiki page with the title
retrieved_wiki = WikiPage(owner_id=my_test_project.id, title=sub_wiki_1.title).get()
print(f"Retrieved wiki page with title: {retrieved_wiki.title}")

# Check if the retrieved wiki page is the same as the original wiki page
comparisons = [
    sub_wiki_1.markdown_file_handle_id == retrieved_wiki.markdown_file_handle_id,
    sub_wiki_1.id == retrieved_wiki.id,
    sub_wiki_1.title == retrieved_wiki.title,
]
print(f"All fields match: {all(comparisons)}")

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
    owner_id=my_test_project.id,
    parent_id=root_wiki_page.id,
    title="Sub Page 2 created from markdown text",
    markdown=markdown_content,
).store()

# Create a wiki page from a markdown file
# Create a temporary markdown gzipped file from the markdown_content
markdown_file_path = "temp_markdown_file.md.gz"
with gzip.open(markdown_file_path, "wt", encoding="utf-8") as gz:
    gz.write("This is a markdown file")

# Create wiki page from markdown file
sub_wiki_3 = WikiPage(
    owner_id=my_test_project.id,
    parent_id=root_wiki_page.id,
    title="Sub Page 3 created from markdown file",
    markdown=markdown_file_path,
).store()

# Download the markdown file
# delete the markdown file after uploading to test the download function
os.remove(markdown_file_path)
# Note: If the markdown is generated from plain text using the client, the downloaded file will be named wiki_markdown_<wiki_page_title>.md.gz. If it is generated from an existing markdown file, the downloaded file will retain the original filename with the .gz suffix appended.
# Download the markdown file for sub_wiki_2 that is created from markdown text
wiki_page_markdown_2 = WikiPage(
    owner_id=my_test_project.id, id=sub_wiki_2.id
).get_markdown(
    download_file=True,
    download_location=".",
    download_file_name=f"wiki_markdown_{sub_wiki_2.title}.md.gz",
)
print(
    f"Wiki page markdown for sub_wiki_2 successfully downloaded: {os.path.exists(f'wiki_markdown_{sub_wiki_2.title}.md.gz')}"
)
# clean up the downloaded markdown file
os.remove(f"wiki_markdown_{sub_wiki_2.title}.md.gz")

# Download the markdown file for sub_wiki_3 that is created from a markdown file
wiki_page_markdown_3 = WikiPage(
    owner_id=my_test_project.id, id=sub_wiki_3.id
).get_markdown(
    download_file=True, download_location=".", download_file_name=markdown_file_path
)
print(
    f"Wiki page markdown for sub_wiki_3 successfully downloaded: {os.path.exists(markdown_file_path)}"
)
# clean up the downloaded markdown file
os.remove(markdown_file_path)

# Section 3: WikiPage with Attachments
# Create a temporary file for the attachment
attachment_file_name = "temp_attachment.txt.gz"
with gzip.open(attachment_file_name, "wt", encoding="utf-8") as gz:
    gz.write("This is a sample attachment.")

# reformat '.' and '_' in the attachment file name to be a valid attachment path
attachment_file_name_reformatted = attachment_file_name.replace(".", "%2E")
attachment_file_name_reformatted = attachment_file_name_reformatted.replace("_", "%5F")

sub_wiki_4 = WikiPage(
    owner_id=my_test_project.id,
    parent_id=root_wiki_page.id,
    title="Sub Page 4 with Attachments",
    markdown=f"# Sub Page 4 with Attachments\n\nThis is a attachment: ${{previewattachment?fileName={attachment_file_name_reformatted}}}",
    attachments=[attachment_file_name],
).store()

# Get attachment handles
attachment_handles = WikiPage(
    owner_id=my_test_project.id, id=sub_wiki_4.id
).get_attachment_handles()
print(f"Attachment handles: {attachment_handles['list']}")

# Get attachment URL without downloading
wiki_page_attachment_url = WikiPage(
    owner_id=my_test_project.id, id=sub_wiki_4.id
).get_attachment(
    file_name="temp_attachment.txt.gz",
    download_file=False,
)
print(f"Attachment URL: {wiki_page_attachment_url}")

# Download an attachment
# Delete the attachment file after uploading to test the download function
os.remove(attachment_file_name)
wiki_page_attachment = WikiPage(
    owner_id=my_test_project.id, id=sub_wiki_4.id
).get_attachment(
    file_name=attachment_file_name,
    download_file=True,
    download_location=".",
)
print(f"Attachment downloaded: {os.path.exists(attachment_file_name)}")
os.remove(attachment_file_name)

# Download an attachment preview. Instead of using the file_name from the attachmenthandle response when isPreview=True, you should use the original file name in the get_attachment_preview request. The downloaded file will still be named according to the file_name provided in the response when isPreview=True.
# Get attachment preview URL without downloading
attachment_preview_url = WikiPage(
    owner_id=my_test_project.id, id=sub_wiki_4.id
).get_attachment_preview(
    file_name="temp_attachment.txt.gz",
    download_file=False,
)
print(f"Attachment preview URL: {attachment_preview_url}")

# Download an attachment preview
attachment_preview = WikiPage(
    owner_id=my_test_project.id, id=sub_wiki_4.id
).get_attachment_preview(
    file_name="temp_attachment.txt.gz",
    download_file=True,
    download_location=".",
)
# From the attachment preview URL or attachment handle response, the downloaded preview file is preview.txt
os.remove("preview.txt")

# Section 4: WikiHeader - Working with Wiki Hierarchy
# Get wiki header tree (hierarchy)
headers = WikiHeader.get(owner_id=my_test_project.id)
print(f"Found {len(headers)} wiki pages in hierarchy")
print(f"Wiki header tree: {headers}")

# Section 5. WikiHistorySnapshot - Version History
# Get wiki history for root_wiki_page
history = WikiHistorySnapshot.get(owner_id=my_test_project.id, id=root_wiki_page.id)
print(f"History: {history}")

# Section 6. WikiOrderHint - Ordering Wiki Pages
# Set the wiki order hint
order_hint = WikiOrderHint(owner_id=my_test_project.id).get()
print(f"Order hint for {my_test_project.id}: {order_hint.id_list}")
# As you can see from the printed message, the order hint is not set by default, so you need to set it explicitly at the beginning.
order_hint.id_list = [
    root_wiki_page.id,
    sub_wiki_3.id,
    sub_wiki_4.id,
    sub_wiki_1.id,
    sub_wiki_2.id,
]
order_hint.store()
print(f"Order hint for {my_test_project.id}: {order_hint}")

# Update wiki order hint
order_hint = WikiOrderHint(owner_id=my_test_project.id).get()
order_hint.id_list = [
    root_wiki_page.id,
    sub_wiki_1.id,
    sub_wiki_2.id,
    sub_wiki_3.id,
    sub_wiki_4.id,
]
order_hint.store()
print(f"Order hint for {my_test_project.id}: {order_hint}")

# Delete a wiki page
wiki_page_to_delete = WikiPage(owner_id=my_test_project.id, id=sub_wiki_3.id).delete()

# clean up
my_test_project.delete()
