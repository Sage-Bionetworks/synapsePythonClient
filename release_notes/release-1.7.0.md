Release Notes - Synapse Python Client - Version py-1.7

**Release Date:** 16-November-2017  
**Install Instructions:** `pip install --upgrade synapseclient` or see [http://docs.synapse.org/python/#installation](http://docs.synapse.org/python/#installation)  
**Documentation:** [http://docs.synapse.org/python/](http://docs.synapse.org/python/)

## New Features

*   [[SYNPY-53](https://sagebionetworks.jira.com/browse/SYNPY-53)] - support syn.get of external FTP links in py client
*   [[SYNPY-179](https://sagebionetworks.jira.com/browse/SYNPY-179)] - Upload to user owned S3 bucket
*   [[SYNPY-360](https://sagebionetworks.jira.com/browse/SYNPY-360)] - Duplicate file handles are removed from BulkFileDownloadRequest
*   [[SYNPY-398](https://sagebionetworks.jira.com/browse/SYNPY-398)] - Implement batch fileHandle copy API
*   [[SYNPY-407](https://sagebionetworks.jira.com/browse/SYNPY-407)] - support LARGETEXT in tables
*   [[SYNPY-412](https://sagebionetworks.jira.com/browse/SYNPY-412)] - allow query-based download based on view tables from command line client
*   [[SYNPY-487](https://sagebionetworks.jira.com/browse/SYNPY-487)] - Add remote monitoring of long running processes
*   [[SYNPY-415](https://sagebionetworks.jira.com/browse/SYNPY-415)] - Add Docker and TableViews into Entity.py

## Improvement

*   [[SYNPY-89](https://sagebionetworks.jira.com/browse/SYNPY-89)] - Python client: Bulk upload client/command
*   [[SYNPY-187](https://sagebionetworks.jira.com/browse/SYNPY-187)] - Move --description in command line client to create wikis
*   [[SYNPY-224](https://sagebionetworks.jira.com/browse/SYNPY-224)] - When uploading to a managed external file handle (e.g. SFTP), fill in storageLocationId
*   [[SYNPY-315](https://sagebionetworks.jira.com/browse/SYNPY-315)] - Default behavior for files in cache dir should be replace
*   [[SYNPY-381](https://sagebionetworks.jira.com/browse/SYNPY-381)] - Remove references to "files" and "cacheDir".
*   [[SYNPY-396](https://sagebionetworks.jira.com/browse/SYNPY-396)] - Create filehandle copies in synapseutils.copy instead of downloading
*   [[SYNPY-403](https://sagebionetworks.jira.com/browse/SYNPY-403)] - Use single endpoint for all downloads
*   [[SYNPY-413](https://sagebionetworks.jira.com/browse/SYNPY-413)] - Update table views via python client
*   [[SYNPY-435](https://sagebionetworks.jira.com/browse/SYNPY-435)] - Convenience function for new service to get entity's children
*   [[SYNPY-471](https://sagebionetworks.jira.com/browse/SYNPY-471)] - docs aren't generated for synapseutils
*   [[SYNPY-472](https://sagebionetworks.jira.com/browse/SYNPY-472)] - References to wrong doc site

## Bug Fixes

*   [[SYNPY-190](https://sagebionetworks.jira.com/browse/SYNPY-190)] - syn.login('asdfasdfasdf') should fail
*   [[SYNPY-301](https://sagebionetworks.jira.com/browse/SYNPY-301)] - change actual file name from python client
*   [[SYNPY-344](https://sagebionetworks.jira.com/browse/SYNPY-344)] - weird cache directories
*   [[SYNPY-346](https://sagebionetworks.jira.com/browse/SYNPY-346)] - ValueError: cannot insert ROW_ID, already exists in CsvTableFile constructor
*   [[SYNPY-347](https://sagebionetworks.jira.com/browse/SYNPY-347)] - Missing dtypes in table.DTYPE_2_TABLETYPE
*   [[SYNPY-351](https://sagebionetworks.jira.com/browse/SYNPY-351)] - Versioning broken for sftp files
*   [[SYNPY-366](https://sagebionetworks.jira.com/browse/SYNPY-366)] - file URLs leads to wrong path
*   [[SYNPY-393](https://sagebionetworks.jira.com/browse/SYNPY-393)] - New cacheDir causes cache to be ignored(?)
*   [[SYNPY-409](https://sagebionetworks.jira.com/browse/SYNPY-409)] - Python client cannot depend on parsing Amazon pre-signed URLs
*   [[SYNPY-418](https://sagebionetworks.jira.com/browse/SYNPY-418)] - Integration test failure against 167
*   [[SYNPY-421](https://sagebionetworks.jira.com/browse/SYNPY-421)] - syn.getWikiHeaders has a return limit of 50 (Need to return all headers)
*   [[SYNPY-423](https://sagebionetworks.jira.com/browse/SYNPY-423)] - upload rate is off or incorrect
*   [[SYNPY-424](https://sagebionetworks.jira.com/browse/SYNPY-424)] - File entities don't handle local_state correctly for setting datafilehandleid
*   [[SYNPY-425](https://sagebionetworks.jira.com/browse/SYNPY-425)] - onweb doesn't work for tables
*   [[SYNPY-426](https://sagebionetworks.jira.com/browse/SYNPY-426)] - multiple tests failing because of filenameOveride
*   [[SYNPY-427](https://sagebionetworks.jira.com/browse/SYNPY-427)] - test dependent on config file
*   [[SYNPY-428](https://sagebionetworks.jira.com/browse/SYNPY-428)] - sync function error
*   [[SYNPY-431](https://sagebionetworks.jira.com/browse/SYNPY-431)] - download ending early and not restarting from previous spot
*   [[SYNPY-442](https://sagebionetworks.jira.com/browse/SYNPY-442)] - set config file path on command line
*   [[SYNPY-443](https://sagebionetworks.jira.com/browse/SYNPY-443)] - tests/integration/integration_test_Entity.py:test_get_with_downloadLocation_and_ifcollision AssertionError
*   [[SYNPY-461](https://sagebionetworks.jira.com/browse/SYNPY-461)] - On Windows, command line client login credential prompt fails (python 2.7)
*   [[SYNPY-463](https://sagebionetworks.jira.com/browse/SYNPY-463)] - When copying filehandles we should add the files to the cache if we already donwloaded them
*   [[SYNPY-465](https://sagebionetworks.jira.com/browse/SYNPY-465)] - Update tests that set permissions to also include 'DOWNLOAD' permission and tests that test functions using queries
*   [[SYNPY-468](https://sagebionetworks.jira.com/browse/SYNPY-468)] - Command line client incompatible with cache changes
*   [[SYNPY-470](https://sagebionetworks.jira.com/browse/SYNPY-470)] - default should be read, download for setPermissions
*   [[SYNPY-475](https://sagebionetworks.jira.com/browse/SYNPY-475)] - Store Tables timeout error
*   [[SYNPY-483](https://sagebionetworks.jira.com/browse/SYNPY-483)] - integration test fails for most users
*   [[SYNPY-484](https://sagebionetworks.jira.com/browse/SYNPY-484)] - URL expires after retries
*   [[SYNPY-486](https://sagebionetworks.jira.com/browse/SYNPY-486)] - Error in integration tests
*   [[SYNPY-488](https://sagebionetworks.jira.com/browse/SYNPY-488)] - sync tests for command line client puts file in working directory
*   [[SYNPY-142](https://sagebionetworks.jira.com/browse/SYNPY-142)] - PY: Error in login with rememberMe=True
*   [[SYNPY-464](https://sagebionetworks.jira.com/browse/SYNPY-464)] - synapse get syn4988808 KeyError: u'preSignedURL'


## Task

*   [[SYNPY-422](https://sagebionetworks.jira.com/browse/SYNPY-422)] - reduce default page size for GET /evaluation/{evalId}/submission/bundle/all
*   [[SYNPY-437](https://sagebionetworks.jira.com/browse/SYNPY-437)] - Remove tests for access restrictions on evaluations
*   [[SYNPY-402](https://sagebionetworks.jira.com/browse/SYNPY-402)] - Add release notes to Github release tag