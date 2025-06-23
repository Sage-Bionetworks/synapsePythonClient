#!/usr/bin/env python3
"""
Tutorial script demonstrating the Synapse Wiki models functionality.

This script shows how to:
1.
2. Work with wiki headers and hierarchy
3. Access wiki history
4. Manage wiki order hints
5. Handle attachments and markdown content
6. Download wiki content and attachments
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
wiki_page_1 = WikiPage(
    owner_id=my_test_project.id,
    title="My Root Wiki Page",
    markdown="# Welcome to My Root Wiki\n\nThis is a sample root wiki page created with the Synapse client.",
).store()

# OR you can create a wiki page with an existing markdown file
markdown_file_path = "path/to/your_markdown_file.md"
wiki_page_1 = WikiPage(
    owner_id=my_test_project.id,
    title="My First Root Wiki Page Version with existing markdown file",
    markdown=markdown_file_path,
).store()

# Create a new wiki page with updated content
wiki_page_2 = WikiPage(
    owner_id=my_test_project.id,
    title="My First Root Wiki Page Version 1",
    markdown="# Welcome to My Root Wiki Version 1\n\nThis is a sample root wiki page created with the Synapse client.",
    id=wiki_page_1.id,
).store()

# Restore the wiki page to the original version
wiki_page_restored = WikiPage(
    owner_id=my_test_project.id, id=wiki_page_1.id, wiki_version="0"
).restore()

# check if the content is restored
comparisons = [
    wiki_page_1.markdown_file_handle_id == wiki_page_restored.markdown_file_handle_id,
    wiki_page_1.id == wiki_page_restored.id,
    wiki_page_1.title == wiki_page_restored.title,
]
print(f"All fields match: {all(comparisons)}")

# Create a sub-wiki page
sub_wiki = WikiPage(
    owner_id=my_test_project.id,
    title="Sub Wiki Page 1",
    parent_id=wiki_page_1.id,  # Use the ID of the parent wiki page we created '633033'
    markdown="# Sub Page 1\n\nThis is a sub-page of another wiki.",
).store()

# Get an existing wiki page for the project, now you can see one root wiki page and one sub-wiki page
wiki_header_tree = WikiHeader.get(owner_id=my_test_project.id)
print(wiki_header_tree)

# Once you know the wiki page id, you can retrieve the wiki page with the id
retrieved_wiki = WikiPage(owner_id=my_test_project.id, id=sub_wiki.id).get()
print(f"Retrieved wiki page with title: {retrieved_wiki.title}")

# Or you can retrieve the wiki page with the title
retrieved_wiki = WikiPage(owner_id=my_test_project.id, title=wiki_page_1.title).get()
print(f"Retrieved wiki page with title: {retrieved_wiki.title}")

# Check if the retrieved wiki page is the same as the original wiki page
comparisons = [
    wiki_page_1.markdown_file_handle_id == retrieved_wiki.markdown_file_handle_id,
    wiki_page_1.id == retrieved_wiki.id,
    wiki_page_1.title == retrieved_wiki.title,
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
markdown_wiki = WikiPage(
    owner_id=my_test_project.id,
    parent_id=wiki_page_1.id,
    title="Sub Page 2 created from markdown text",
    markdown=markdown_content,
).store()

# Create a wiki page from a markdown file
# Create a temporary markdown gzipped file from the markdown_content
markdown_file_path = "temp_markdown_file.md.gz"
with gzip.open(markdown_file_path, "wt", encoding="utf-8") as gz:
    gz.write("This is a markdown file")

# Create wiki page from markdown file
markdown_wiki = WikiPage(
    owner_id=my_test_project.id,
    parent_id=wiki_page_1.id,
    title="Sub Page 3 created from markdown file",
    markdown=markdown_file_path,
).store()

# Download the markdown file
# delete the markdown file after downloading --> check if the file is downloaded
os.remove(markdown_file_path)
markdown_file = WikiPage(owner_id=my_test_project.id, id=markdown_wiki.id).get_markdown(
    download_file=True, download_location=".", download_file_name="markdown_file.md"
)

print(f"Markdown file downloaded to: {markdown_file}")

# Section 3: WikiPage with Attachments
# Create a temporary file for the attachment
attachment_file_name = "temp_attachment.txt.gz"
with gzip.open(attachment_file_name, "wt", encoding="utf-8") as gz:
    gz.write("This is a sample attachment.")

# reformat the attachment file name to be a valid attachment path
attachment_file_name_reformatted = attachment_file_name.replace(".", "%2E")
attachment_file_name_reformatted = attachment_file_name_reformatted.replace("_", "%5F")

wiki_with_attachments = WikiPage(
    owner_id=my_test_project.id,
    parent_id=wiki_page_1.id,
    title="Sub Page 4 with Attachments",
    markdown=f"# Sub Page 4 with Attachments\n\nThis is a attachment: ${{previewattachment?fileName={attachment_file_name_reformatted}}}",
    attachments=[attachment_file_name],
).store()

# Get attachment handles
attachment_handles = WikiPage(
    owner_id=my_test_project.id, id=wiki_with_attachments.id
).get_attachment_handles()
print(f"Found {len(attachment_handles['list'])} attachments")

# Delete the attachment file after uploading --> check if the file is deleted
os.remove(attachment_file_name)
# Download an attachment
wiki_page = WikiPage(
    owner_id=my_test_project.id, id=wiki_with_attachments.id
).get_attachment(
    file_name=attachment_file_name,
    download_file=True,
    download_location=".",
)
print(f"Attachment downloaded: {os.path.exists(attachment_file_name)}")

# Get attachment URL without downloading
wiki_page_url = WikiPage(
    owner_id=my_test_project.id, id=wiki_with_attachments.id
).get_attachment(
    file_name="temp_attachment.txt.gz",
    download_file=False,
)
print(f"Attachment URL: {wiki_page_url}")

# Download an attachment preview--? Failed to download the attachment preview, synapseclient.core.exceptions.SynapseHTTPError: 404 Client Error: Cannot find a wiki attachment for OwnerID: syn68493645, ObjectType: ENTITY, WikiPageId: 633100, fileName: preview.txt
attachment_handles = WikiPage(
    owner_id=my_test_project.id, id=wiki_with_attachments.id
).get_attachment_handles()
print(f"Attachment handles: {attachment_handles}")
wiki_page = WikiPage(
    owner_id=my_test_project.id, id=wiki_with_attachments.id
).get_attachment_preview(
    file_name="preview.txt",
    download_file=True,
    download_location=".",
)

# Section 4: WikiHeader - Working with Wiki Hierarchy

# Get wiki header tree (hierarchy)
# Note: Uncomment to actually get the header tree
headers = WikiHeader.get(owner_id=my_test_project.id)
print(f"Found {len(headers)} wiki pages in hierarchy")

# Section 5. WikiHistorySnapshot - Version History
# Get wiki history
history = WikiHistorySnapshot.get(owner_id=my_test_project.id, id=wiki_page_1.id)

print(f"Found {len(history)} versions in history for {wiki_page_1.title}")

# Section 6. WikiOrderHint - Ordering Wiki Pages
# Get wiki order hint --> failed to get the order hint
order_hint = WikiOrderHint(owner_id=my_test_project.id).get()
print(f"Order hint for {my_test_project.id}: {order_hint}")

# Update wiki order hint
order_hint.id_list = [wiki_page_1.id]

print(f"Created order hint for {len(order_hint.id_list)} wiki pages")

# Update order hint
order_hint.id_list = ["633084", "633085", "633086", "633087", "633088"]  # Reorder
order_hint.store()

# Delete a wiki page
wiki_page_to_delete = WikiPage(
    owner_id=my_test_project.id, id=wiki_with_attachments.id
).delete()

# clean up
my_test_project.delete()
