Release Notes - Synapse Python Client - Version py-1.7.3

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

Release notes - Synapse Python Client - Version py-1.7.3

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
