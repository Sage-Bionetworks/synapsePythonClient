# Release notes - Synapse Python Client - Version 1.8.0

**Install Instructions:** `pip install --upgrade synapseclient` or see [http://docs.synapse.org/python/#installation](http://docs.synapse.org/python/#installation)  
**Documentation:** [http://docs.synapse.org/python/](http://docs.synapse.org/python/)

**Release Date:** 7-May-2018 

--------------------------------------------------

This release has 2 major changes:
* The client will no longer store your saved credentials in your synapse cache (`~/synapseCache/.session`). The python client now relies on [keyring](https://pypi.org/project/keyring/) to handle credential storage of your Synapse credentials. 

* The client also now uses connection pooling, which means that all method calls that connect to Synapse should now be faster.

The remaining changes are bugfixes and cleanup of test code.


Below are the full list of issues addressed by this release:

Bug
---

*   \[[SYNPY-654](https://sagebionetworks.jira.com/browse/SYNPY-654)\] \- syn.getColumns does not terminate
*   \[[SYNPY-658](https://sagebionetworks.jira.com/browse/SYNPY-658)\] \- Security vunerability on clusters
*   \[[SYNPY-689](https://sagebionetworks.jira.com/browse/SYNPY-689)\] \- Wiki's attachments cannot be None
*   \[[SYNPY-692](https://sagebionetworks.jira.com/browse/SYNPY-692)\] \- synapseutils.sync.generateManifest() sets contentType incorrectly
*   \[[SYNPY-693](https://sagebionetworks.jira.com/browse/SYNPY-693)\] \- synapseutils.sync.generateManifest() UnicodeEncodingError in python 2

Task
----

*   \[[SYNPY-617](https://sagebionetworks.jira.com/browse/SYNPY-617)\] \- Remove use of deprecated service to delete table rows
*   \[[SYNPY-673](https://sagebionetworks.jira.com/browse/SYNPY-673)\] \- Fix Integration Tests being run on Appveyor
*   \[[SYNPY-683](https://sagebionetworks.jira.com/browse/SYNPY-683)\] \- Clean up print()s used in unit/integration tests

Improvement
-----------

*   \[[SYNPY-408](https://sagebionetworks.jira.com/browse/SYNPY-408)\] \- Add bettter error messages when /filehandle/batch fails.
*   \[[SYNPY-647](https://sagebionetworks.jira.com/browse/SYNPY-647)\] \- Use connection pooling for Python client's requests
