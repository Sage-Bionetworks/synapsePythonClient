## Release Notes - Synapse Python Client - Version 1.6.1

**Release Date:** 2-November-2016  
**Install Instructions:** `pip install --upgrade synapseclient` or see [http://docs.synapse.org/python/#installation](http://docs.synapse.org/python/#installation)  
**Documentation:** [http://docs.synapse.org/python/](http://docs.synapse.org/python/)

### What is New

In version 1.6 we introduce a new sub-module _synapseutils_ that  
provide convenience functions for more complicated operations in Synapse such as copying of files wikis and folders. In addition we have introduced several improvements in downloading content from Synapse. As with uploads we are now able to recover from an interrupted download and will retry on network failures.

*   [SYNPY-48](https://sagebionetworks.jira.com/browse/SYNPY-48) - Automate build and test of Python client on Python 3.x

*   [SYNPY-180](https://sagebionetworks.jira.com/browse/SYNPY-180) - Pass upload destination in chunked file upload

*   [SYNPY-349](https://sagebionetworks.jira.com/browse/SYNPY-349) - Link Class

*   [SYNPY-350](https://sagebionetworks.jira.com/browse/SYNPY-350) - Copy Function

*   [SYNPY-370](https://sagebionetworks.jira.com/browse/SYNPY-370) - Building to new doc site for Synapse

*   [SYNPY-371](https://sagebionetworks.jira.com/browse/SYNPY-371) - Support paths in syn.onweb

### Improvements

We have improved download robustness and error checking, along with extensive recovery on failed operations. This includes the ability for the client to pause operation when Synapse is updated.

*   [SYNPY-270](https://sagebionetworks.jira.com/browse/SYNPY-270) - Synapse READ ONLY mode should cause pause in execution

*   [SYNPY-308](https://sagebionetworks.jira.com/browse/SYNPY-308) - Add md5 checking after downloading a new file handle

*   [SYNPY-309](https://sagebionetworks.jira.com/browse/SYNPY-309) - Add download recovery by using the 'Range': 'bytes=xxx-xxx' header

*   [SYNPY-353](https://sagebionetworks.jira.com/browse/SYNPY-353) - Speed up downloads of fast connections

*   [SYNPY-356](https://sagebionetworks.jira.com/browse/SYNPY-356) - Add support for version flag in synapse cat command line

*   [SYNPY-357](https://sagebionetworks.jira.com/browse/SYNPY-357) - Remove failure message on retry in multipart_upload

*   [SYNPY-380](https://sagebionetworks.jira.com/browse/SYNPY-380) - Add speed meter to downloads/uploads

*   [SYNPY-387](https://sagebionetworks.jira.com/browse/SYNPY-387) - Do exponential backoff on 429 status and print explanatory error message from server
*   [SYNPY-390](https://sagebionetworks.jira.com/browse/SYNPY-390) - Move recursive download to Python client utils

### Fixes

*   [SYNPY-154](https://sagebionetworks.jira.com/browse/SYNPY-154) - 500 Server Error when storing new version of file from command line
*   [SYNPY-168](https://sagebionetworks.jira.com/browse/SYNPY-168) - Failure on login gives an ugly error message
*   [SYNPY-253](https://sagebionetworks.jira.com/browse/SYNPY-253) - Error messages on upload retry inconsistent with behavior
*   [SYNPY-261](https://sagebionetworks.jira.com/browse/SYNPY-261) - error downloading data from synapse (python client)
*   [SYNPY-274](https://sagebionetworks.jira.com/browse/SYNPY-274) - Trying to use the client without logging in needs to give a reasonable error
*   [SYNPY-331](https://sagebionetworks.jira.com/browse/SYNPY-331) - test_command_get_recursive_and_query occasionally fails
*   [SYNPY-337](https://sagebionetworks.jira.com/browse/SYNPY-337) - Download error on 10 Gb file.
*   [SYNPY-343](https://sagebionetworks.jira.com/browse/SYNPY-343) - Login failure
*   [SYNPY-351](https://sagebionetworks.jira.com/browse/SYNPY-351) - Versioning broken for sftp files
*   [SYNPY-352](https://sagebionetworks.jira.com/browse/SYNPY-352) - file upload max retries exceeded messages
*   [SYNPY-358](https://sagebionetworks.jira.com/browse/SYNPY-358) - upload failure from python client (threading)
*   [SYNPY-361](https://sagebionetworks.jira.com/browse/SYNPY-361) - file download fails midway without warning/error
*   [SYNPY-362](https://sagebionetworks.jira.com/browse/SYNPY-362) - setAnnotations bug when given synapse ID
*   [SYNPY-363](https://sagebionetworks.jira.com/browse/SYNPY-363) - problems using provenance during upload
*   [SYNPY-382](https://sagebionetworks.jira.com/browse/SYNPY-382) - Python client is truncating the entity id in download csv from table
*   [SYNPY-383](https://sagebionetworks.jira.com/browse/SYNPY-383) - Travis failing with paramiko.ssh_exception.SSHException: No hostkey
*   [SYNPY-384](https://sagebionetworks.jira.com/browse/SYNPY-384) - resuming a download after a ChunkedEncodingError created new file with correct size
*   [SYNPY-388](https://sagebionetworks.jira.com/browse/SYNPY-388) - Asynchronous creation of Team causes sporadic test failure
*   [SYNPY-391](https://sagebionetworks.jira.com/browse/SYNPY-391) - downloadTableColumns() function doesn't work when resultsAs=rowset is set for for syn.tableQuery()
*   [SYNPY-397](https://sagebionetworks.jira.com/browse/SYNPY-397) - Error in syncFromSynapse() integration test on Windows
*   [SYNPY-399](https://sagebionetworks.jira.com/browse/SYNPY-399) - python client not compatible with newly released Pandas 0.19