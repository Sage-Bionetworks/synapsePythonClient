======
Upload
======

Files in Synapse are versionable. Please see `Files and Versioning <https://docs.synapse.org/articles/files_and_versioning.html>`_ for more information about how versions in Files works.

Uploading a New Version
=======================
Uploading a new version follows the same steps as uploading a file for the first time - use the same file name and store it in the same location (e.g., the same parentId). It is recommended to add a comment to the new version in order to easily track differences at a glance. The example file raw_data.txt will now have a version of 2 and a comment describing the change::

    # Upload a new version of raw_data.txt, EXPLICIT UPDATE EXAMPLE
    import synapseclient

    # fetch the file in Synapse
    file_to_update = syn.get('syn2222', downloadFile=False)

    # save the local path to the new version of the file
    file_to_update.path = '/path/to/new/version/of/raw_data.txt'

    # add a version comment
    file_to_update.versionComment = 'Added 5 random normally distributed numbers.'

    # store the new file
    updated_file = syn.store(file_to_update)

    # Upload a new version of raw_data.txt, IMPLICIT UPDATE EXAMPLE
    # Assuming that there is a file created with:
    syn.store(File('path/to/old/raw_data.txt', parentId='syn123456'))

    # To create a new version of that file, make sure you store it with the exact same name
    new_file = syn.store(File('path/to/new_version/raw_data.txt',  parentId='syn123456'))

Updating Annotations or Provenance without Changing Versions
============================================================
Any change to a File will automatically update its version. If this isn’t the desired behavior, such as minor cahnges to the metadata, you can set forceVersion=False with the Python or R clients. For command line, the commands set-annotations and set-provenance will update the metadata without creating a new version. Adding/updating annotations and provenance in the web client will also not cause a version change.

Important: Because Provenance is tracked by version, set forceVersion=False for minor changes to avoid breaking Provenance.
Setting annotations without changing version::

    # Get file from Synapse, set download=False since we are only updating annotations
    file = syn.get('syn56789', download=False)

    # Add annotations
    file.annotations = {"fileType":"bam", "assay":"RNA-seq"}

    # Store the file without creating a new version
    file = syn.store(file, forceVersion=False)


Setting Provenance without Changing Version
===========================================

To set Provenance without changing the file version::

    # Get file from Synapse, set download=False since we are only updating provenance
    file = syn.get('syn56789', download=False)

    # Add provenance
    file = syn.setProvenance(file, activity = Activity(used = '/path/to/example_code'))

    # Store the file without creating a new version
    file = syn.store(file, forceVersion=False)

Downloading a Specific Version
==============================

By default, the File downloaded will always be the most recent version. However, a specific version can be downloaded by passing the version parameter::

    entity = syn.get("syn3260973", version=1)
