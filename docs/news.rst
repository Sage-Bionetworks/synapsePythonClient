=============
Release Notes
=============


1.9.0 (2018-09-28)
==================

In version 1.9.0, we deprecated and removed `query()` and `chunkedQuery()`. These functions used the old query services which does not perform well. To query for entities filter by annotations, please use `EntityViewSchema`.

We also deprecated the following functions and will remove them in Synapse Python client version 2.0.
In the `Activity` object:

* `usedEntity()`
* `usedURL()`

In the `Synapse` object:

* `getEntity()`
* `loadEntity()`
* `createEntity()`
* `updateEntity()`
* `deleteEntity()`
* `downloadEntity()`
* `uploadFile()`
* `uploadFileHandle()`
* `uploadSynapseManagedFileHandle()`
* `downloadTableFile()`

Please see our documentation for more details on how to migrate your code away from these functions.

Features
--------

* `SYNPY-806 <https://sagebionetworks.jira.com/browse/SYNPY-806>`_ - Support Folders and Tables in View

Bug Fixes
---------

* `SYNPY-195 <https://sagebionetworks.jira.com/browse/SYNPY-195>`_ - Dangerous exception handling
* `SYNPY-261 <https://sagebionetworks.jira.com/browse/SYNPY-261>`_ - error downloading data from synapse (python client)
* `SYNPY-694 <https://sagebionetworks.jira.com/browse/SYNPY-694>`_ - Uninformative error in `copyWiki` function
* `SYNPY-805 <https://sagebionetworks.jira.com/browse/SYNPY-805>`_ - Uninformative error when getting View that does not exist
* `SYNPY-819 <https://sagebionetworks.jira.com/browse/SYNPY-819>`_ - command-line clients need to be updated to replace the EntityView 'viewType' with 'viewTypeMask'

Tasks
-----

* `SYNPY-759 <https://sagebionetworks.jira.com/browse/SYNPY-759>`_ - Look for all functions that are documented as "Deprecated" and apply the deprecation syntax
* `SYNPY-812 <https://sagebionetworks.jira.com/browse/SYNPY-812>`_ - Add Github issue template
* `SYNPY-824 <https://sagebionetworks.jira.com/browse/SYNPY-824>`_ - Remove the deprecated function query() and chunkedQuery()

Improvements
------------

* `SYNPY-583 <https://sagebionetworks.jira.com/browse/SYNPY-583>`_ - Better error message for create Link object
* `SYNPY-810 <https://sagebionetworks.jira.com/browse/SYNPY-810>`_ - simplify docs for deleting rows
* `SYNPY-814 <https://sagebionetworks.jira.com/browse/SYNPY-814>`_ - fix docs links in python client __init__.py
* `SYNPY-822 <https://sagebionetworks.jira.com/browse/SYNPY-822>`_ - Switch to use news.rst instead of multiple release notes files
* `SYNPY-823 <https://sagebionetworks.jira.com/browse/SYNPY-759>`_ - Pin keyring to version 12.0.2 to use SecretStorage 2.x


1.8.2 (2018-08-17)
==================

In this release, we have been performed some house-keeping on the code base. The two major changes are:

 * making `syn.move()` available to move an entity to a new parent in Synapse. For example::

    import synapseclient
    from synapseclient import Folder

    syn = synapseclient.login()

    file = syn.get("syn123")
    folder = Folder("new folder", parent="syn456")
    folder = syn.store(folder)

    # moving file to the newly created folder
    syn.move(file, folder)

 * exposing the ability to use the Synapse Python client with single threaded. This feature is useful when running Python script in an environment that does not support multi-threading. However, this will negatively impact upload speed. To use single threaded::

    import synapseclient
    synapseclient.config.single_threaded = True

Bug Fixes
---------

*   `SYNPY-535 <https://sagebionetworks.jira.com/browse/SYNPY-535>`_ - Synapse Table update: Connection Reset
*   `SYNPY-603 <https://sagebionetworks.jira.com/browse/SYNPY-603>`_ - Python client and synapser cannot handle table column type LINK
*   `SYNPY-688 <https://sagebionetworks.jira.com/browse/SYNPY-688>`_ - Recursive get (sync) broken for empty folders.
*   `SYNPY-744 <https://sagebionetworks.jira.com/browse/SYNPY-744>`_ - KeyError when trying to download using Synapse Client 1.8.1
*   `SYNPY-750 <https://sagebionetworks.jira.com/browse/SYNPY-750>`_ - Error in downloadTableColumns for file view
*   `SYNPY-758 <https://sagebionetworks.jira.com/browse/SYNPY-758>`_ - docs in Sphinx don't show for synapseclient.table.RowSet
*   `SYNPY-760 <https://sagebionetworks.jira.com/browse/SYNPY-760>`_ - Keyring related error on Linux
*   `SYNPY-766 <https://sagebionetworks.jira.com/browse/SYNPY-766>`_ - as\_table\_columns() returns a list of columns out of order for python 3.5 and 2.7
*   `SYNPY-776 <https://sagebionetworks.jira.com/browse/SYNPY-776>`_ - Cannot log in to Synapse - error(54, 'Connection reset by peer')
*   `SYNPY-795 <https://sagebionetworks.jira.com/browse/SYNPY-795>`_ - Not recognizable column in query result

Features
--------

*   `SYNPY-582 <https://sagebionetworks.jira.com/browse/SYNPY-582>`_ - move file or folder in the client
*   `SYNPY-788 <https://sagebionetworks.jira.com/browse/SYNPY-788>`_ - Add option to use syn.store() without exercising multithreads

Tasks
-----

*   `SYNPY-729 <https://sagebionetworks.jira.com/browse/SYNPY-729>`_ - Deprecate query() and chunkedQuery()
*   `SYNPY-797 <https://sagebionetworks.jira.com/browse/SYNPY-797>`_ - Check Python client code base on using PLFM object model
*   `SYNPY-798 <https://sagebionetworks.jira.com/browse/SYNPY-798>`_ - Using github.io to host documentation

Improvements
------------

*   `SYNPY-646 <https://sagebionetworks.jira.com/browse/SYNPY-646>`_ - Error output of synGet is non-informative
*   `SYNPY-743 <https://sagebionetworks.jira.com/browse/SYNPY-743>`_ - lint the entire python client code base


1.8.1 (2018-05-17)
==================

This release is a hotfix for a bug.
Please refer to 1.8.0 release notes for information about additional changes.

Bug Fixes
---------

*   `SYNPY-706 <https://sagebionetworks.jira.com/browse/SYNPY-706>`_ - syn.username can cause attribute not found if user not logged in


1.8.0 (2018-05-07)
==================

This release has 2 major changes:

* The client will no longer store your saved credentials in your synapse cache (`~/synapseCache/.session`). The python client now relies on `keyring <https://pypi.org/project/keyring/>`_ to handle credential storage of your Synapse credentials.
* The client also now uses connection pooling, which means that all method calls that connect to Synapse should now be faster.

The remaining changes are bug fixes and cleanup of test code.

Below are the full list of issues addressed by this release:

Bug Fixes
---------

*   `SYNPY-654 <https://sagebionetworks.jira.com/browse/SYNPY-654>`_ - syn.getColumns does not terminate
*   `SYNPY-658 <https://sagebionetworks.jira.com/browse/SYNPY-658>`_ - Security vunerability on clusters
*   `SYNPY-689 <https://sagebionetworks.jira.com/browse/SYNPY-689>`_ - Wiki's attachments cannot be None
*   `SYNPY-692 <https://sagebionetworks.jira.com/browse/SYNPY-692>`_ - synapseutils.sync.generateManifest() sets contentType incorrectly
*   `SYNPY-693 <https://sagebionetworks.jira.com/browse/SYNPY-693>`_ - synapseutils.sync.generateManifest() UnicodeEncodingError in python 2

Tasks
-----

*   `SYNPY-617 <https://sagebionetworks.jira.com/browse/SYNPY-617>`_ - Remove use of deprecated service to delete table rows
*   `SYNPY-673 <https://sagebionetworks.jira.com/browse/SYNPY-673>`_ - Fix Integration Tests being run on Appveyor
*   `SYNPY-683 <https://sagebionetworks.jira.com/browse/SYNPY-683>`_ - Clean up print()s used in unit/integration tests

Improvements
------------

*   `SYNPY-408 <https://sagebionetworks.jira.com/browse/SYNPY-408>`_ - Add bettter error messages when /filehandle/batch fails.
*   `SYNPY-647 <https://sagebionetworks.jira.com/browse/SYNPY-647>`_ - Use connection pooling for Python client's requests


1.7.5 (2018-01-31)
==================

v1.7.4 release was broken for new users that installed from pip. v1.7.5 has the same changes as v1.7.4 but fixes the pip installation.


1.7.4 (2018-01-29)
==================

This release mostly includes bugfixes and improvements for various Table classes:
 * Fixed bug where you couldn't store a table converted to a `pandas.Dataframe` if it had a INTEGER column with some missing values.
 * `EntityViewSchema` can now automatically add all annotations within your defined `scopes` as columns. Just set the view's `addAnnotationColumns=True` before calling `syn.store()`. This attribute defaults to `True` for all newly created `EntityViewSchemas`. Setting `addAnnotationColumns=True` on existing tables will only add annotation columns that are not already a part of your schema.
 * You can now use `synapseutils.notifyMe` as a decorator to notify you by email when your function has completed. You will also be notified of any Errors if they are thrown while your function runs.

We also added some new features:
 * `syn.findEntityId()` function that allows you to find an Entity by its name and parentId, set parentId to `None` to search for Projects by name.
 * The bulk upload functionality of `synapseutils.syncToSynapse` is available from the command line using: `synapse sync`.

Below are the full list of issues addressed by this release:


Features
--------

*   `SYNPY-506 <https://sagebionetworks.jira.com/browse/SYNPY-506>`_ - need convenience function for /entity/child
*   `SYNPY-517 <https://sagebionetworks.jira.com/browse/SYNPY-517>`_ - sync command line

Improvements
------------

*   `SYNPY-267 <https://sagebionetworks.jira.com/browse/SYNPY-267>`_ - Update Synapse tables for integer types
*   `SYNPY-304 <https://sagebionetworks.jira.com/browse/SYNPY-304>`_ - Table objects should implement len()
*   `SYNPY-416 <https://sagebionetworks.jira.com/browse/SYNPY-416>`_ - warning message for recursive get when a non-Project of Folder entity is passed
*   `SYNPY-482 <https://sagebionetworks.jira.com/browse/SYNPY-482>`_ - Create a sample synapseConfig if none is present
*   `SYNPY-489 <https://sagebionetworks.jira.com/browse/SYNPY-489>`_ - Add a boolean parameter in EntityViewSchema that will indicate whether the client should create columns based on annotations in the specified scopes
*   `SYNPY-494 <https://sagebionetworks.jira.com/browse/SYNPY-494>`_ - Link should be able to take an entity object as the parameter and derive its id
*   `SYNPY-511 <https://sagebionetworks.jira.com/browse/SYNPY-511>`_ - improve exception handling
*   `SYNPY-512 <https://sagebionetworks.jira.com/browse/SYNPY-512>`_ - Remove the use of PaginatedResult's totalNumberOfResult
*   `SYNPY-539 <https://sagebionetworks.jira.com/browse/SYNPY-539>`_ - When creating table Schemas, enforce a limit on the number of columns that can be created.

Bug Fixes
---------

*   `SYNPY-235 <https://sagebionetworks.jira.com/browse/SYNPY-235>`_ - can't print Row objects with dates in them
*   `SYNPY-272 <https://sagebionetworks.jira.com/browse/SYNPY-272>`_ - bug syn.storing rowsets containing Python datetime objects
*   `SYNPY-297 <https://sagebionetworks.jira.com/browse/SYNPY-297>`_ - as_table_columns shouldn't give fractional max size
*   `SYNPY-404 <https://sagebionetworks.jira.com/browse/SYNPY-404>`_ - when we get a SynapseMd5MismatchError we should delete the downloaded file
*   `SYNPY-425 <https://sagebionetworks.jira.com/browse/SYNPY-425>`_ - onweb doesn't work for tables
*   `SYNPY-438 <https://sagebionetworks.jira.com/browse/SYNPY-438>`_ - Need to change 'submit' not to use evaluation/id/accessRequirementUnfulfilled
*   `SYNPY-496 <https://sagebionetworks.jira.com/browse/SYNPY-496>`_ - monitor.NotifyMe can not be used as an annotation decorator
*   `SYNPY-521 <https://sagebionetworks.jira.com/browse/SYNPY-521>`_ - inconsistent error message when username/password is wrong on login
*   `SYNPY-536 <https://sagebionetworks.jira.com/browse/SYNPY-536>`_ - pre-signed upload URL expired warnings using Python client sync function
*   `SYNPY-555 <https://sagebionetworks.jira.com/browse/SYNPY-555>`_ - EntityViewSchema is missing from sphinx documentation
*   `SYNPY-558 <https://sagebionetworks.jira.com/browse/SYNPY-558>`_ - synapseutils.sync.syncFromSynapse throws error when syncing a Table object
*   `SYNPY-595 <https://sagebionetworks.jira.com/browse/SYNPY-595>`_ - Get recursive folders filled with Links fails
*   `SYNPY-605 <https://sagebionetworks.jira.com/browse/SYNPY-605>`_ - Update documentation for getUserProfile to include information about refreshing and memoization

Tasks
-----

*   `SYNPY-451 <https://sagebionetworks.jira.com/browse/SYNPY-451>`_ - Add limit and offset for accessApproval and accessRequirement API calls and remove 0x400 flag default when calling GET /entity/{id}/bundle
*   `SYNPY-546 <https://sagebionetworks.jira.com/browse/SYNPY-546>`_ - Change warning message when user does not DOWNLOAD permissions.


1.7.3 (2017-12-08)
==================

Release 1.7.3 introduces fixes and quality of life changes to Tables and synapseutils:

* Changes to Tables:

    * You no longer have to include the `etag` column in your SQL query when using a `tableQuery()` to update File/Project Views. just `SELECT` the relevant columns and etags will be resolved automatically.
    * The new `PartialRowSet` class allows you to only have to upload changes to individual cells of a table instead of every row that had a value changed. It is recommended to use the `PartialRowSet.from_mapping()` classmethod instead of the `PartialRowSet` constructor.

* Changes to synapseutils:

    * Improved documentation
    * You can now use `~` to refer to your home directory in your manifest.tsv

We also added improved debug logging and use Python's builtin `logging` module instead of printing directly to `sys.stderr`

Below are the full list of issues addressed by this release:

Bug Fixes
---------

*   `SYNPY-419 <https://sagebionetworks.jira.com/browse/SYNPY-419>`_ - support object store from client
*   `SYNPY-499 <https://sagebionetworks.jira.com/browse/SYNPY-499>`_ - metadata manifest file name spelled wrong
*   `SYNPY-504 <https://sagebionetworks.jira.com/browse/SYNPY-504>`_ - downloadTableFile changed return type with no change in documentation or mention in release notes
*   `SYNPY-508 <https://sagebionetworks.jira.com/browse/SYNPY-508>`_ - syncToSynapse does not work if "the file path in "used" or "executed" of the manifest.tsv uses home directory shortcut "~"
*   `SYNPY-516 <https://sagebionetworks.jira.com/browse/SYNPY-516>`_ - synapse sync file does not work if file is a URL
*   `SYNPY-525 <https://sagebionetworks.jira.com/browse/SYNPY-525>`_ - Download CSV file of Synapse Table - 416 error
*   `SYNPY-572 <https://sagebionetworks.jira.com/browse/SYNPY-572>`_ - Users should only be prompted for updates if the first or second part of the version number is changed.

Features
--------

*   `SYNPY-450 <https://sagebionetworks.jira.com/browse/SYNPY-450>`_ - Create convenience functions for synapse project settings
*   `SYNPY-517 <https://sagebionetworks.jira.com/browse/SYNPY-517>`_ - sync command line
*   `SYNPY-519 <https://sagebionetworks.jira.com/browse/SYNPY-519>`_ - Clean up doc string for Sync
*   `SYNPY-545 <https://sagebionetworks.jira.com/browse/SYNPY-545>`_ - no module botocore
*   `SYNPY-577 <https://sagebionetworks.jira.com/browse/SYNPY-577>`_ - Expose new view etags in command line clients

Tasks
-----

*   `SYNPY-569 <https://sagebionetworks.jira.com/browse/SYNPY-569>`_ - 'includeEntityEtag' should be True for Async table csv query downloads

Improvements
------------

*   `SYNPY-304 <https://sagebionetworks.jira.com/browse/SYNPY-304>`_ - Table objects should implement len()
*   `SYNPY-511 <https://sagebionetworks.jira.com/browse/SYNPY-511>`_ - improve exception handling
*   `SYNPY-518 <https://sagebionetworks.jira.com/browse/SYNPY-518>`_ - Clean up sync interface
*   `SYNPY-590 <https://sagebionetworks.jira.com/browse/SYNPY-590>`_ - Need better logging of errors that occur in the Python client.
*   `SYNPY-597 <https://sagebionetworks.jira.com/browse/SYNPY-597>`_ - Add ability to create PartialRowset updates


1.7.1 (2017-11-17)
==================

Release 1.7 is a large bugfix release with several new features. The main ones include:

* We have expanded the `synapseutils packages <docs.synapse.org/python/synapseutils.html#module-synapseutils>`_ to add the ability to:

    * Bulk upload files to synapse (synapseutils.syncToSynapse).
    * Notify you via email on the progress of a function (useful for jobs like large file uploads that may take a long time to complete).
    * The syncFromSynapse function now creates a "manifest" which contains the metadata of downloaded files. (These can also be used to update metadata with the bulk upload function.

* File View tables can now be created from the python client using EntityViewSchema. See `fileviews documentation <http://docs.synapse.org/articles/fileviews.html>`_.
* The python client is now able to upload to user owned S3 Buckets. `Click here for instructions on linking your S3 bucket to synapse <http://docs.synapse.org/articles/custom_storage_location.html>`_.

We've also made various improvements to existing features:

* The LARGETEXT type is now supported in Tables allowing for strings up to 2Mb.
* The `--description` argument when creating/updating entities from the command line client will now create a `Wiki` for that entity. You can also use `--descriptionFile` to write the contents of a markdown file as the entity's `Wiki`
* Two member variables of the File object, `file_entity.cacheDir` and `file_entity.files` is being DEPRECATED in favor of `file_entity.path` for finding the location of a downloaded `File`
* `pandas` `dataframe`s containing `datetime` values can now be properly converted into csv and uploaded to Synapse.

We also added a optional `convert_to_datetime` parameter to `CsvFileTable.asDataFrame()` that will automatically convert Synapse DATE columns into `datetime` objects instead of leaving them as `long` unix timestamps

Below are the full list of bugs and issues addressed by this release:

Features
--------

*   `SYNPY-53 <https://sagebionetworks.jira.com/browse/SYNPY-53>`_ - support syn.get of external FTP links in py client
*   `SYNPY-179 <https://sagebionetworks.jira.com/browse/SYNPY-179>`_ - Upload to user owned S3 bucket
*   `SYNPY-412 <https://sagebionetworks.jira.com/browse/SYNPY-412>`_ - allow query-based download based on view tables from command line client
*   `SYNPY-487 <https://sagebionetworks.jira.com/browse/SYNPY-487>`_ - Add remote monitoring of long running processes
*   `SYNPY-415 <https://sagebionetworks.jira.com/browse/SYNPY-415>`_ - Add Docker and TableViews into Entity.py
*   `SYNPY-89 <https://sagebionetworks.jira.com/browse/SYNPY-89>`_ - Python client: Bulk upload client/command
*   `SYNPY-413 <https://sagebionetworks.jira.com/browse/SYNPY-413>`_ - Update table views via python client
*   `SYNPY-301 <https://sagebionetworks.jira.com/browse/SYNPY-301>`_ - change actual file name from python client
*   `SYNPY-442 <https://sagebionetworks.jira.com/browse/SYNPY-442>`_ - set config file path on command line

Improvements
------------

*   `SYNPY-407 <https://sagebionetworks.jira.com/browse/SYNPY-407>`_ - support LARGETEXT in tables
*   `SYNPY-360 <https://sagebionetworks.jira.com/browse/SYNPY-360>`_ - Duplicate file handles are removed from BulkFileDownloadRequest
*   `SYNPY-187 <https://sagebionetworks.jira.com/browse/SYNPY-187>`_ - Move --description in command line client to create wikis
*   `SYNPY-224 <https://sagebionetworks.jira.com/browse/SYNPY-224>`_ - When uploading to a managed external file handle (e.g. SFTP), fill in storageLocationId
*   `SYNPY-315 <https://sagebionetworks.jira.com/browse/SYNPY-315>`_ - Default behavior for files in cache dir should be replace
*   `SYNPY-381 <https://sagebionetworks.jira.com/browse/SYNPY-381>`_ - Remove references to "files" and "cacheDir".
*   `SYNPY-396 <https://sagebionetworks.jira.com/browse/SYNPY-396>`_ - Create filehandle copies in synapseutils.copy instead of downloading
*   `SYNPY-403 <https://sagebionetworks.jira.com/browse/SYNPY-403>`_ - Use single endpoint for all downloads
*   `SYNPY-435 <https://sagebionetworks.jira.com/browse/SYNPY-435>`_ - Convenience function for new service to get entity's children
*   `SYNPY-471 <https://sagebionetworks.jira.com/browse/SYNPY-471>`_ - docs aren't generated for synapseutils
*   `SYNPY-472 <https://sagebionetworks.jira.com/browse/SYNPY-472>`_ - References to wrong doc site
*   `SYNPY-347 <https://sagebionetworks.jira.com/browse/SYNPY-347>`_ - Missing dtypes in table.DTYPE_2_TABLETYPE
*   `SYNPY-463 <https://sagebionetworks.jira.com/browse/SYNPY-463>`_ - When copying filehandles we should add the files to the cache if we already donwloaded them
*   `SYNPY-475 <https://sagebionetworks.jira.com/browse/SYNPY-475>`_ - Store Tables timeout error

Bug Fixes
---------

*   `SYNPY-190 <https://sagebionetworks.jira.com/browse/SYNPY-190>`_ - syn.login('asdfasdfasdf') should fail
*   `SYNPY-344 <https://sagebionetworks.jira.com/browse/SYNPY-344>`_ - weird cache directories
*   `SYNPY-346 <https://sagebionetworks.jira.com/browse/SYNPY-346>`_ - ValueError: cannot insert ROW_ID, already exists in CsvTableFile constructor
*   `SYNPY-351 <https://sagebionetworks.jira.com/browse/SYNPY-351>`_ - Versioning broken for sftp files
*   `SYNPY-366 <https://sagebionetworks.jira.com/browse/SYNPY-366>`_ - file URLs leads to wrong path
*   `SYNPY-393 <https://sagebionetworks.jira.com/browse/SYNPY-393>`_ - New cacheDir causes cache to be ignored(?)
*   `SYNPY-409 <https://sagebionetworks.jira.com/browse/SYNPY-409>`_ - Python client cannot depend on parsing Amazon pre-signed URLs
*   `SYNPY-418 <https://sagebionetworks.jira.com/browse/SYNPY-418>`_ - Integration test failure against 167
*   `SYNPY-421 <https://sagebionetworks.jira.com/browse/SYNPY-421>`_ - syn.getWikiHeaders has a return limit of 50 (Need to return all headers)
*   `SYNPY-423 <https://sagebionetworks.jira.com/browse/SYNPY-423>`_ - upload rate is off or incorrect
*   `SYNPY-424 <https://sagebionetworks.jira.com/browse/SYNPY-424>`_ - File entities don't handle local_state correctly for setting datafilehandleid
*   `SYNPY-426 <https://sagebionetworks.jira.com/browse/SYNPY-426>`_ - multiple tests failing because of filenameOveride
*   `SYNPY-427 <https://sagebionetworks.jira.com/browse/SYNPY-427>`_ - test dependent on config file
*   `SYNPY-428 <https://sagebionetworks.jira.com/browse/SYNPY-428>`_ - sync function error
*   `SYNPY-431 <https://sagebionetworks.jira.com/browse/SYNPY-431>`_ - download ending early and not restarting from previous spot
*   `SYNPY-443 <https://sagebionetworks.jira.com/browse/SYNPY-443>`_ - tests/integration/integration_test_Entity.py:test_get_with_downloadLocation_and_ifcollision AssertionError
*   `SYNPY-461 <https://sagebionetworks.jira.com/browse/SYNPY-461>`_ - On Windows, command line client login credential prompt fails (python 2.7)
*   `SYNPY-465 <https://sagebionetworks.jira.com/browse/SYNPY-465>`_ - Update tests that set permissions to also include 'DOWNLOAD' permission and tests that test functions using queries
*   `SYNPY-468 <https://sagebionetworks.jira.com/browse/SYNPY-468>`_ - Command line client incompatible with cache changes
*   `SYNPY-470 <https://sagebionetworks.jira.com/browse/SYNPY-470>`_ - default should be read, download for setPermissions
*   `SYNPY-483 <https://sagebionetworks.jira.com/browse/SYNPY-483>`_ - integration test fails for most users
*   `SYNPY-484 <https://sagebionetworks.jira.com/browse/SYNPY-484>`_ - URL expires after retries
*   `SYNPY-486 <https://sagebionetworks.jira.com/browse/SYNPY-486>`_ - Error in integration tests
*   `SYNPY-488 <https://sagebionetworks.jira.com/browse/SYNPY-488>`_ - sync tests for command line client puts file in working directory
*   `SYNPY-142 <https://sagebionetworks.jira.com/browse/SYNPY-142>`_ - PY: Error in login with rememberMe=True
*   `SYNPY-464 <https://sagebionetworks.jira.com/browse/SYNPY-464>`_ - synapse get syn4988808 KeyError: u'preSignedURL'

Tasks
-----

*   `SYNPY-422 <https://sagebionetworks.jira.com/browse/SYNPY-422>`_ - reduce default page size for GET /evaluation/{evalId}/submission/bundle/all
*   `SYNPY-437 <https://sagebionetworks.jira.com/browse/SYNPY-437>`_ - Remove tests for access restrictions on evaluations
*   `SYNPY-402 <https://sagebionetworks.jira.com/browse/SYNPY-402>`_ - Add release notes to Github release tag


1.6.1 (2016-11-02)
==================

What is New
-----------

In version 1.6 we introduce a new sub-module _synapseutils_ that
provide convenience functions for more complicated operations in Synapse such as copying of files wikis and folders. In addition we have introduced several improvements in downloading content from Synapse. As with uploads we are now able to recover from an interrupted download and will retry on network failures.

*   `SYNPY-48 <https://sagebionetworks.jira.com/browse/SYNPY-48>`_  - Automate build and test of Python client on Python 3.x
*   `SYNPY-180 <https://sagebionetworks.jira.com/browse/SYNPY-180>`_  - Pass upload destination in chunked file upload
*   `SYNPY-349 <https://sagebionetworks.jira.com/browse/SYNPY-349>`_  - Link Class
*   `SYNPY-350 <https://sagebionetworks.jira.com/browse/SYNPY-350>`_  - Copy Function
*   `SYNPY-370 <https://sagebionetworks.jira.com/browse/SYNPY-370>`_  - Building to new doc site for Synapse
*   `SYNPY-371 <https://sagebionetworks.jira.com/browse/SYNPY-371>`_  - Support paths in syn.onweb

Improvements
------------

We have improved download robustness and error checking, along with extensive recovery on failed operations. This includes the ability for the client to pause operation when Synapse is updated.

*   `SYNPY-270 <https://sagebionetworks.jira.com/browse/SYNPY-270>`_  - Synapse READ ONLY mode should cause pause in execution
*   `SYNPY-308 <https://sagebionetworks.jira.com/browse/SYNPY-308>`_  - Add md5 checking after downloading a new file handle
*   `SYNPY-309 <https://sagebionetworks.jira.com/browse/SYNPY-309>`_  - Add download recovery by using the 'Range': 'bytes=xxx-xxx' header
*   `SYNPY-353 <https://sagebionetworks.jira.com/browse/SYNPY-353>`_  - Speed up downloads of fast connections
*   `SYNPY-356 <https://sagebionetworks.jira.com/browse/SYNPY-356>`_  - Add support for version flag in synapse cat command line
*   `SYNPY-357 <https://sagebionetworks.jira.com/browse/SYNPY-357>`_  - Remove failure message on retry in multipart_upload
*   `SYNPY-380 <https://sagebionetworks.jira.com/browse/SYNPY-380>`_  - Add speed meter to downloads/uploads
*   `SYNPY-387 <https://sagebionetworks.jira.com/browse/SYNPY-387>`_  - Do exponential backoff on 429 status and print explanatory error message from server
*   `SYNPY-390 <https://sagebionetworks.jira.com/browse/SYNPY-390>`_  - Move recursive download to Python client utils

Bug Fixes
---------

*   `SYNPY-154 <https://sagebionetworks.jira.com/browse/SYNPY-154>`_  - 500 Server Error when storing new version of file from command line
*   `SYNPY-168 <https://sagebionetworks.jira.com/browse/SYNPY-168>`_  - Failure on login gives an ugly error message
*   `SYNPY-253 <https://sagebionetworks.jira.com/browse/SYNPY-253>`_  - Error messages on upload retry inconsistent with behavior
*   `SYNPY-261 <https://sagebionetworks.jira.com/browse/SYNPY-261>`_  - error downloading data from synapse (python client)
*   `SYNPY-274 <https://sagebionetworks.jira.com/browse/SYNPY-274>`_  - Trying to use the client without logging in needs to give a reasonable error
*   `SYNPY-331 <https://sagebionetworks.jira.com/browse/SYNPY-331>`_  - test_command_get_recursive_and_query occasionally fails
*   `SYNPY-337 <https://sagebionetworks.jira.com/browse/SYNPY-337>`_  - Download error on 10 Gb file.
*   `SYNPY-343 <https://sagebionetworks.jira.com/browse/SYNPY-343>`_  - Login failure
*   `SYNPY-351 <https://sagebionetworks.jira.com/browse/SYNPY-351>`_  - Versioning broken for sftp files
*   `SYNPY-352 <https://sagebionetworks.jira.com/browse/SYNPY-352>`_  - file upload max retries exceeded messages
*   `SYNPY-358 <https://sagebionetworks.jira.com/browse/SYNPY-358>`_  - upload failure from python client (threading)
*   `SYNPY-361 <https://sagebionetworks.jira.com/browse/SYNPY-361>`_  - file download fails midway without warning/error
*   `SYNPY-362 <https://sagebionetworks.jira.com/browse/SYNPY-362>`_  - setAnnotations bug when given synapse ID
*   `SYNPY-363 <https://sagebionetworks.jira.com/browse/SYNPY-363>`_  - problems using provenance during upload
*   `SYNPY-382 <https://sagebionetworks.jira.com/browse/SYNPY-382>`_  - Python client is truncating the entity id in download csv from table
*   `SYNPY-383 <https://sagebionetworks.jira.com/browse/SYNPY-383>`_  - Travis failing with paramiko.ssh_exception.SSHException: No hostkey
*   `SYNPY-384 <https://sagebionetworks.jira.com/browse/SYNPY-384>`_  - resuming a download after a ChunkedEncodingError created new file with correct size
*   `SYNPY-388 <https://sagebionetworks.jira.com/browse/SYNPY-388>`_  - Asynchronous creation of Team causes sporadic test failure
*   `SYNPY-391 <https://sagebionetworks.jira.com/browse/SYNPY-391>`_  - downloadTableColumns() function doesn't work when resultsAs=rowset is set for for syn.tableQuery()
*   `SYNPY-397 <https://sagebionetworks.jira.com/browse/SYNPY-397>`_  - Error in syncFromSynapse() integration test on Windows
*   `SYNPY-399 <https://sagebionetworks.jira.com/browse/SYNPY-399>`_ - python client not compatible with newly released Pandas 0.19