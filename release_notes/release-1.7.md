Release Notes - Synapse Python Client - Version py-1.7


# Release 1.7.3


**Release Date:** 08-December-2017  
**Install Instructions:** `pip install --upgrade synapseclient` or see [http://docs.synapse.org/python/#installation](http://docs.synapse.org/python/#installation)  
**Documentation:** [http://docs.synapse.org/python/](http://docs.synapse.org/python/)

Release 1.7.3 introduces fixes and quality of life changes to Tables and synapseutils:
* Changes to Tables:
	* You no longer have to include the `etag` column in your SQL query when using a `tableQuery()` to update File/Project Views. just `SELECT` the relevant columns and etags will be resolved automatically.
	* The new `PartialRowSet` class allows you to only have to upload changes to individual cells of a table instead of every row that had a value changed. It is recommended to use the `PartialRowSet.from_mapping()` classmethod instead of the `PartialRowSet` constructor.
* Changes to synapseutils:
	* Improved documentation
	* You can now use `~` to refer to your home directory in your manifest.tsv

We also added improved debug logging and use Python's bulitin `logging` module instead of printing directly to `sys.stderr`

Below are the full list of issues addressed by this release:
### Bug

*   [[SYNPY-419](https://sagebionetworks.jira.com/browse/SYNPY-419)] - support object store from client
*   [[SYNPY-499](https://sagebionetworks.jira.com/browse/SYNPY-499)] - metadata manifest file name spelled wrong
*   [[SYNPY-504](https://sagebionetworks.jira.com/browse/SYNPY-504)] - downloadTableFile changed return type with no change in documentation or mention in release notes
*   [[SYNPY-508](https://sagebionetworks.jira.com/browse/SYNPY-508)] - syncToSynapse does not work if "the file path in "used" or "executed" of the manifest.tsv uses home directory shortcut "~"
*   [[SYNPY-516](https://sagebionetworks.jira.com/browse/SYNPY-516)] - synapse sync file does not work if file is a URL
*   [[SYNPY-525](https://sagebionetworks.jira.com/browse/SYNPY-525)] - Download CSV file of Synapse Table - 416 error
*   [[SYNPY-572](https://sagebionetworks.jira.com/browse/SYNPY-572)] - Users should only be prompted for updates if the first or second part of the version number is changed.

### New Feature

*   [[SYNPY-450](https://sagebionetworks.jira.com/browse/SYNPY-450)] - Create convenience functions for synapse project settings
*   [[SYNPY-517](https://sagebionetworks.jira.com/browse/SYNPY-517)] - sync command line
*   [[SYNPY-519](https://sagebionetworks.jira.com/browse/SYNPY-519)] - Clean up doc string for Sync
*   [[SYNPY-545](https://sagebionetworks.jira.com/browse/SYNPY-545)] - no module botocore
*   [[SYNPY-577](https://sagebionetworks.jira.com/browse/SYNPY-577)] - Expose new view etags in command line clients

### Task

*   [[SYNPY-569](https://sagebionetworks.jira.com/browse/SYNPY-569)] - 'includeEntityEtag' should be True for Async table csv query downloads

### Improvement

*   [[SYNPY-304](https://sagebionetworks.jira.com/browse/SYNPY-304)] - Table objects should implement len()
*   [[SYNPY-511](https://sagebionetworks.jira.com/browse/SYNPY-511)] - improve exception handling
*   [[SYNPY-518](https://sagebionetworks.jira.com/browse/SYNPY-518)] - Clean up sync interface
*   [[SYNPY-590](https://sagebionetworks.jira.com/browse/SYNPY-590)] - Need better logging of errors that occur in the Python client.
*   [[SYNPY-597](https://sagebionetworks.jira.com/browse/SYNPY-597)] - Add ability to create PartialRowset updates


# Release 1.7.1

**Release Date:** 17-June-2017  
**Install Instructions:** `pip install --upgrade synapseclient` or see [http://docs.synapse.org/python/#installation](http://docs.synapse.org/python/#installation)  
**Documentation:** [http://docs.synapse.org/python/](http://docs.synapse.org/python/)

Release 1.7 is a large bugfix release with several new features. The main ones include:
* We have expanded the [syanpaseutils packages](docs.synapse.org/python/synapseutils.html#module-synapseutils) to add the  abilitity to:
    * Bulk upload files to synapse (synapseutils.syncToSynapse).
    * Notify you via email on the progress of a function (useful for jobs like large file uploads that may take a long time to complete).
    * The syncFromSynapse function now creates a "manifest" which contains the metadata of downloaded files. (These can also be used to update metadata with the bulk upload function.
* File View tables can now be created from the python client using EntityViewSchema. See [fileviews documentation](http://docs.synapse.org/articles/fileviews.html).
* The python client is now able to upload to user owned S3 Buckets. [Click here for instructions on linking your S3 bucket to synapse](http://docs.synapse.org/articles/custom_storage_location.html)

We've also made vairous improvements to existing features:
* The LARGETEXT type is now supported in Tables allowing for strings up to 2Mb.
* The `--description` argument when creating/updating entities from the command line client will now create a `Wiki` for that entity. You can also use `--descriptionFile` to write the contents of a markdownfile as the entity's `Wiki`
* Two member variables of the File object, `file_entity.cacheDir` and `file_entity.files` is being DEPRECATED in favor of `file_entity.path` for finding the location of a downloaded `File`
* `pandas` `dataframe`s containing `datetime` values can now be properly converted into csv and uploaded to Synapse.
We also added a optional `convert_to_datetime` parameter to `CsvFileTable.asDataFrame()` that will automatically convert Synapse DATE columns into `datetime` objects instead of leaving them as `long` unix timestamps


Below are the full list of bugs and issues addressed by this release:

### New Features

*   [[SYNPY-53](https://sagebionetworks.jira.com/browse/SYNPY-53)] - support syn.get of external FTP links in py client
*   [[SYNPY-179](https://sagebionetworks.jira.com/browse/SYNPY-179)] - Upload to user owned S3 bucket
*   [[SYNPY-412](https://sagebionetworks.jira.com/browse/SYNPY-412)] - allow query-based download based on view tables from command line client
*   [[SYNPY-487](https://sagebionetworks.jira.com/browse/SYNPY-487)] - Add remote monitoring of long running processes
*   [[SYNPY-415](https://sagebionetworks.jira.com/browse/SYNPY-415)] - Add Docker and TableViews into Entity.py
*   [[SYNPY-89](https://sagebionetworks.jira.com/browse/SYNPY-89)] - Python client: Bulk upload client/command
*   [[SYNPY-413](https://sagebionetworks.jira.com/browse/SYNPY-413)] - Update table views via python client
*   [[SYNPY-301](https://sagebionetworks.jira.com/browse/SYNPY-301)] - change actual file name from python client
*   [[SYNPY-442](https://sagebionetworks.jira.com/browse/SYNPY-442)] - set config file path on command line


### Improvements

*   [[SYNPY-407](https://sagebionetworks.jira.com/browse/SYNPY-407)] - support LARGETEXT in tables
*   [[SYNPY-360](https://sagebionetworks.jira.com/browse/SYNPY-360)] - Duplicate file handles are removed from BulkFileDownloadRequest
*   [[SYNPY-187](https://sagebionetworks.jira.com/browse/SYNPY-187)] - Move --description in command line client to create wikis
*   [[SYNPY-224](https://sagebionetworks.jira.com/browse/SYNPY-224)] - When uploading to a managed external file handle (e.g. SFTP), fill in storageLocationId
*   [[SYNPY-315](https://sagebionetworks.jira.com/browse/SYNPY-315)] - Default behavior for files in cache dir should be replace
*   [[SYNPY-381](https://sagebionetworks.jira.com/browse/SYNPY-381)] - Remove references to "files" and "cacheDir".
*   [[SYNPY-396](https://sagebionetworks.jira.com/browse/SYNPY-396)] - Create filehandle copies in synapseutils.copy instead of downloading
*   [[SYNPY-403](https://sagebionetworks.jira.com/browse/SYNPY-403)] - Use single endpoint for all downloads
*   [[SYNPY-435](https://sagebionetworks.jira.com/browse/SYNPY-435)] - Convenience function for new service to get entity's children
*   [[SYNPY-471](https://sagebionetworks.jira.com/browse/SYNPY-471)] - docs aren't generated for synapseutils
*   [[SYNPY-472](https://sagebionetworks.jira.com/browse/SYNPY-472)] - References to wrong doc site
*   [[SYNPY-347](https://sagebionetworks.jira.com/browse/SYNPY-347)] - Missing dtypes in table.DTYPE_2_TABLETYPE
*   [[SYNPY-463](https://sagebionetworks.jira.com/browse/SYNPY-463)] - When copying filehandles we should add the files to the cache if we already donwloaded them
*   [[SYNPY-475](https://sagebionetworks.jira.com/browse/SYNPY-475)] - Store Tables timeout error


### Bug Fixes

*   [[SYNPY-190](https://sagebionetworks.jira.com/browse/SYNPY-190)] - syn.login('asdfasdfasdf') should fail
*   [[SYNPY-344](https://sagebionetworks.jira.com/browse/SYNPY-344)] - weird cache directories
*   [[SYNPY-346](https://sagebionetworks.jira.com/browse/SYNPY-346)] - ValueError: cannot insert ROW_ID, already exists in CsvTableFile constructor
*   [[SYNPY-351](https://sagebionetworks.jira.com/browse/SYNPY-351)] - Versioning broken for sftp files
*   [[SYNPY-366](https://sagebionetworks.jira.com/browse/SYNPY-366)] - file URLs leads to wrong path
*   [[SYNPY-393](https://sagebionetworks.jira.com/browse/SYNPY-393)] - New cacheDir causes cache to be ignored(?)
*   [[SYNPY-409](https://sagebionetworks.jira.com/browse/SYNPY-409)] - Python client cannot depend on parsing Amazon pre-signed URLs
*   [[SYNPY-418](https://sagebionetworks.jira.com/browse/SYNPY-418)] - Integration test failure against 167
*   [[SYNPY-421](https://sagebionetworks.jira.com/browse/SYNPY-421)] - syn.getWikiHeaders has a return limit of 50 (Need to return all headers)
*   [[SYNPY-423](https://sagebionetworks.jira.com/browse/SYNPY-423)] - upload rate is off or incorrect
*   [[SYNPY-424](https://sagebionetworks.jira.com/browse/SYNPY-424)] - File entities don't handle local_state correctly for setting datafilehandleid
*   [[SYNPY-426](https://sagebionetworks.jira.com/browse/SYNPY-426)] - multiple tests failing because of filenameOveride
*   [[SYNPY-427](https://sagebionetworks.jira.com/browse/SYNPY-427)] - test dependent on config file
*   [[SYNPY-428](https://sagebionetworks.jira.com/browse/SYNPY-428)] - sync function error
*   [[SYNPY-431](https://sagebionetworks.jira.com/browse/SYNPY-431)] - download ending early and not restarting from previous spot
*   [[SYNPY-443](https://sagebionetworks.jira.com/browse/SYNPY-443)] - tests/integration/integration_test_Entity.py:test_get_with_downloadLocation_and_ifcollision AssertionError
*   [[SYNPY-461](https://sagebionetworks.jira.com/browse/SYNPY-461)] - On Windows, command line client login credential prompt fails (python 2.7)
*   [[SYNPY-465](https://sagebionetworks.jira.com/browse/SYNPY-465)] - Update tests that set permissions to also include 'DOWNLOAD' permission and tests that test functions using queries
*   [[SYNPY-468](https://sagebionetworks.jira.com/browse/SYNPY-468)] - Command line client incompatible with cache changes
*   [[SYNPY-470](https://sagebionetworks.jira.com/browse/SYNPY-470)] - default should be read, download for setPermissions
*   [[SYNPY-483](https://sagebionetworks.jira.com/browse/SYNPY-483)] - integration test fails for most users
*   [[SYNPY-484](https://sagebionetworks.jira.com/browse/SYNPY-484)] - URL expires after retries
*   [[SYNPY-486](https://sagebionetworks.jira.com/browse/SYNPY-486)] - Error in integration tests
*   [[SYNPY-488](https://sagebionetworks.jira.com/browse/SYNPY-488)] - sync tests for command line client puts file in working directory
*   [[SYNPY-142](https://sagebionetworks.jira.com/browse/SYNPY-142)] - PY: Error in login with rememberMe=True
*   [[SYNPY-464](https://sagebionetworks.jira.com/browse/SYNPY-464)] - synapse get syn4988808 KeyError: u'preSignedURL'

### Miscellaneous Tasks

*   [[SYNPY-422](https://sagebionetworks.jira.com/browse/SYNPY-422)] - reduce default page size for GET /evaluation/{evalId}/submission/bundle/all
*   [[SYNPY-437](https://sagebionetworks.jira.com/browse/SYNPY-437)] - Remove tests for access restrictions on evaluations
*   [[SYNPY-402](https://sagebionetworks.jira.com/browse/SYNPY-402)] - Add release notes to Github release tag
