# Release notes - Synapse Python Client - Version 1.8.2

**Install Instructions:** `pip install --upgrade synapseclient` or see [http://docs.synapse.org/python/#installation](http://docs.synapse.org/python/#installation)  
**Documentation:** [http://sage-bionetworks.github.io/synapsePythonClient](http://sage-bionetworks.github.io/synapsePythonClient)

**Release Date:** 17-August-2018 


--------------------------------------------------

In this release, we have been performed some house-keeping on the code base. The two major changes are:
 * making `syn.move()` available to move an entity to a new parent in Synapse. For example:
```
import synapseclient
from synapseclient import Folder

syn = synapseclient.login()

file = syn.get("syn123")
folder = Folder("new folder", parent="syn456")
folder = syn.store(folder)

# moving file to the newly created folder
syn.move(file, folder)
```
 * exposing the ability to use the Synapse Python client with single threaded. This feature is useful when running Python script in an environment that does not support multi-threading. However, this will negatively impact upload speed. To use single threaded:

```
import synapseclient

synapseclient.config.single_threaded = True
```

Bug
---

*   \[[SYNPY-535](https://sagebionetworks.jira.com/browse/SYNPY-535)\] \- Synapse Table update: Connection Reset
*   \[[SYNPY-603](https://sagebionetworks.jira.com/browse/SYNPY-603)\] \- Python client and synapser cannot handle table column type LINK
*   \[[SYNPY-688](https://sagebionetworks.jira.com/browse/SYNPY-688)\] \- Recursive get (sync) broken for empty folders.
*   \[[SYNPY-744](https://sagebionetworks.jira.com/browse/SYNPY-744)\] \- KeyError when trying to download using Synapse Client 1.8.1
*   \[[SYNPY-750](https://sagebionetworks.jira.com/browse/SYNPY-750)\] \- Error in downloadTableColumns for file view
*   \[[SYNPY-758](https://sagebionetworks.jira.com/browse/SYNPY-758)\] \- docs in Sphinx don't show for synapseclient.table.RowSet
*   \[[SYNPY-760](https://sagebionetworks.jira.com/browse/SYNPY-760)\] \- Keyring related error on Linux
*   \[[SYNPY-766](https://sagebionetworks.jira.com/browse/SYNPY-766)\] \- as\_table\_columns() returns a list of columns out of order for python 3.5 and 2.7
*   \[[SYNPY-776](https://sagebionetworks.jira.com/browse/SYNPY-776)\] \- Cannot log in to Synapse - error(54, 'Connection reset by peer')
*   \[[SYNPY-795](https://sagebionetworks.jira.com/browse/SYNPY-795)\] \- Not recognizable column in query result

New Feature
-----------

*   \[[SYNPY-582](https://sagebionetworks.jira.com/browse/SYNPY-582)\] \- move file or folder in the client
*   \[[SYNPY-788](https://sagebionetworks.jira.com/browse/SYNPY-788)\] \- Add option to use syn.store() without exercising multithreads

Task
----

*   \[[SYNPY-729](https://sagebionetworks.jira.com/browse/SYNPY-729)\] \- Deprecate query() and chunkedQuery()
*   \[[SYNPY-797](https://sagebionetworks.jira.com/browse/SYNPY-797)\] \- Check Python client code base on using PLFM object model
*   \[[SYNPY-798](https://sagebionetworks.jira.com/browse/SYNPY-798)\] \- Using github.io to host documentation

Improvement
-----------

*   \[[SYNPY-646](https://sagebionetworks.jira.com/browse/SYNPY-646)\] \- Error output of synGet is non-informative
*   \[[SYNPY-743](https://sagebionetworks.jira.com/browse/SYNPY-743)\] \- lint the entire python client code base