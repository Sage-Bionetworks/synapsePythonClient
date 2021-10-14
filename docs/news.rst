=============
Release Notes
=============

2.5.0 (2021-10-05)
==================

Highlights
----------

- Added ability to generate a manifest file from your local directory structure.

  .. code-block:: bash

        # from the command line
        synapse manifest --parent-id syn123 --manifest-file ./manifest.tsv ./

- Added ability to pipe manifest stdout into sync function.

  .. code-block:: bash

        # from the command line
        synapse manifest --parent-id syn123 ./docs/ | synapse sync -

- Added ability to describe csv and tsv files stored in Synapse.

  .. code-block:: bash

        # from python
        import synapseutils
        synapseutils.describe(syn=syn, entity="syn12345")

- Next major release (3.0.0) will remove all non-standard single dash long
  command line interface (cli) parameters. Therefore, there will be cosmetic changes to the cli, such as removing
  all camel case parameters.
  Example: command line arguments like `-parent` will become
 `--parent`.  Commands that support camel case like `--parentId`
  might be changed to `--parent-id`.


Bug Fixes
---------
-  [`SYNPY-669 <https://sagebionetworks.jira.com/browse/SYNPY-669>`__] -
   Signing of Synapse authentication header does not correctly URL encode the URL path
-  [`SYNPY-770 <https://sagebionetworks.jira.com/browse/SYNPY-770>`__] -
   Files failing to upload using syncToSynapse
-  [`SYNPY-1123 <https://sagebionetworks.jira.com/browse/SYNPY-1123>`__] -
   All tables erroring when indexing
-  [`SYNPY-1146 <https://sagebionetworks.jira.com/browse/SYNPY-1146>`__] -
   Error writing Booleans from Python dataframes into Boolean columns in a Synapse table
-  [`SYNPY-1156 <https://sagebionetworks.jira.com/browse/SYNPY-1156>`__] -
   datetimes in a Pandas dataframe are not properly stored to Synapse

Stories
-------
-  [`SYNPY-726 <https://sagebionetworks.jira.com/browse/SYNPY-726>`__] -
   mirror local folder structure for bulk upload
-  [`SYNPY-1163 <https://sagebionetworks.jira.com/browse/SYNPY-1163>`__] -
   Expose synId with syn get -r
-  [`SYNPY-1165 <https://sagebionetworks.jira.com/browse/SYNPY-1165>`__] -
   Generate manifest template from local folder structure
-  [`SYNPY-1167 <https://sagebionetworks.jira.com/browse/SYNPY-1167>`__] -
   Support for Quick Summary Statistics on CSV and TSV files

Tasks
-----
-  [`SYNPY-1169 <https://sagebionetworks.jira.com/browse/SYNPY-1169>`__] -
   Integration tests failures in develop branch against stack-371
-  [`SYNPY-1172 <https://sagebionetworks.jira.com/browse/SYNPY-1172>`__] -
   Passing a pandas dataframe with a column called "read" breaks the type parsing in as_table_columns()
-  [`SYNPY-1173 <https://sagebionetworks.jira.com/browse/SYNPY-1173>`__] -
   Support DATE_LIST, ENTITYID_LIST, USERID_LIST table columns
-  [`SYNPY-1188 <https://sagebionetworks.jira.com/browse/SYNPY-1188>`__] -
   Support piping of `synapse manifest` stdout in `synapse sync` function

2.4.0 (2021-07-08)
==================

Highlights
----------

- Added ability to authenticate from a :code:`SYNAPSE_AUTH_TOKEN` environment variable set with a valid `personal access token <https://help.synapse.org/docs/Managing-Your-Account.2055405596.html#ManagingYourAccount-PersonalAccessTokens>`__.

  .. code-block:: bash

        # e.g. set environment variable prior to invoking a Synapse command or running a program that uses synapseclient
        SYNAPSE_AUTH_TOKEN='<my_personal_access_token>' synapse <subcommand options>

  The environment variable will take priority over credentials in the user's :code:`.synapseConfig` file
  or any credentials saved in a prior login using the remember me option.

  See `here <Credentials.html#use-environment-variable>`__ for more details on usage.

- Added ability to silence all console output.

  .. code-block:: bash

        # from the command line, use the --silent option with any synapse subcommand, here it will suppress the download progress indicator
        synapse --silent get <synid>

  .. code-block:: python3

        # from code using synapseclient, pass the silent option to the Synapse constructor
        import synapseclient

        syn = synapseclient.Synapse(silent=True)
        syn.login()
        syn.get(<synid>)

- Improved robustness during downloads with unstable connections. Specifically the client will automatically recover
  when encoutering some types of network errors that previously would have caused a download to start over as indicated by a
  reset progress bar.


Bug Fixes
---------
-  [`SYNPY-198 <https://sagebionetworks.jira.com/browse/SYNPY-198>`__] -
   get: Unmet access requirement should not raise error if entity not downloadable
-  [`SYNPY-959 <https://sagebionetworks.jira.com/browse/SYNPY-959>`__] -
   FileEntity 'path' property has wrong separator in Windows
-  [`SYNPY-1113 <https://sagebionetworks.jira.com/browse/SYNPY-1113>`__] -
   Confusing error when putting the positional FILE at the end of the synapse store command with an optional n-arg
-  [`SYNPY-1128 <https://sagebionetworks.jira.com/browse/SYNPY-1128>`__] -
   failures downloading 14G vcf file
-  [`SYNPY-1130 <https://sagebionetworks.jira.com/browse/SYNPY-1130>`__] -
   Migration tool trying to move URL-linked data
-  [`SYNPY-1134 <https://sagebionetworks.jira.com/browse/SYNPY-1134>`__] -
   500 error during part copy to AWS presigned url
-  [`SYNPY-1135 <https://sagebionetworks.jira.com/browse/SYNPY-1135>`__] -
   Exceeding part limit during AD Migration
-  [`SYNPY-1136 <https://sagebionetworks.jira.com/browse/SYNPY-1136>`__] -
   Connection aborted to AWS part copy to presigned  url during AD Migration
-  [`SYNPY-1141 <https://sagebionetworks.jira.com/browse/SYNPY-1141>`__] -
   synapse get command line nargs usage/error
-  [`SYNPY-1150 <https://sagebionetworks.jira.com/browse/SYNPY-1150>`__] -
   Boolean-like string columns being reformatted (TRUE/FALSE to True/False)
-  [`SYNPY-1158 <https://sagebionetworks.jira.com/browse/SYNPY-1158>`__] -
   race condition in test_caching.py#test_threaded_access
-  [`SYNPY-1159 <https://sagebionetworks.jira.com/browse/SYNPY-1159>`__] -
   logging in with an email address and an authToken gives spurious error
-  [`SYNPY-1161 <https://sagebionetworks.jira.com/browse/SYNPY-1161>`__] -
   ChunkEncodingError encountered from external collaborator during a synapseclient download

Improvements
------------
-  [`SYNPY-638 <https://sagebionetworks.jira.com/browse/SYNPY-638>`__] -
   add after date to cache purge
-  [`SYNPY-929 <https://sagebionetworks.jira.com/browse/SYNPY-929>`__] -
   silent parameter for all functions which default to writing to stdout
-  [`SYNPY-1068 <https://sagebionetworks.jira.com/browse/SYNPY-1068>`__] -
   Should show some progress indicator during upload md5 calculation
-  [`SYNPY-1125 <https://sagebionetworks.jira.com/browse/SYNPY-1125>`__] -
   Allow login with environment variables
-  [`SYNPY-1138 <https://sagebionetworks.jira.com/browse/SYNPY-1138>`__] -
   When using boto3 client to upload a file, also include ACL to give bucket owner full access

Tasks
-----
-  [`SYNPY-948 <https://sagebionetworks.jira.com/browse/SYNPY-948>`__] -
   command line client set-annotations does not return proper error code when there's a problem
-  [`SYNPY-1024 <https://sagebionetworks.jira.com/browse/SYNPY-1024>`__] -
   remove reference to deprecated 'status' field from Evaluation
-  [`SYNPY-1143 <https://sagebionetworks.jira.com/browse/SYNPY-1143>`__] -
   indicate in CLI doc's that select statement requires double quotes


2.3.1 (2021-04-13)
==================

Highlights
----------

- Entities can be annotated with boolean datatypes, for example:

  .. code-block::

    file = synapseclient.File('/path/to/file', parentId='syn123', synapse_is_great=True)
    syn.store(file)

- synapseclient is additionally packaged as a Python wheel.


Bug Fixes
---------

-  [`SYNPY-829 <https://sagebionetworks.jira.com/browse/SYNPY-829>`__] -
   syn.store always updates annotations
-  [`SYNPY-1033 <https://sagebionetworks.jira.com/browse/SYNPY-1033>`__] -
   If versionComment is left blank, previous version comment populates

Improvements
------------

-  [`SYNPY-1120 <https://sagebionetworks.jira.com/browse/SYNPY-1120>`__] -
   Build wheel distributions
-  [`SYNPY-1129 <https://sagebionetworks.jira.com/browse/SYNPY-1129>`__] -
   Support boolean annotations in Python client

2.3.0 (2021-03-03)
================

Highlights
----------

- The `index_files_for_migration <synapseutils.html#synapseutils.migrate_functions.index_files_for_migration>`__ and
  `migrate_indexed_files <synapseutils.html#synapseutils.migrate_functions.migrate_indexed_files>`__ functions are added
  to synapseutils to help migrate files in Synapse projects and folders between AWS S3 buckets in the same region.
  More details on using these utilities can be found `here <S3Storage.html#storage-location-migration>`__.

- This version supports login programatically and from the command line using personal access tokens that can be obtained
  from your synapse.org Settings. Additional documentation on login and be found `here <Credentials.html>`__.

  .. code-block::

   # programmatic
   syn = synapseclient.login(authToken=<token>)

  .. code-block::

   # command line
   synapse login -p <token>

- The location where downloaded entities are cached can be customized to a location other than the user's home directory.
  This is useful in environments where writing to a home directory is not appropriate (e.g. an AWS lambda).

  .. code-block::

   syn = synapseclient.Synapse(cache_root_dir=<directory path>)

- A `helper method <index.html#synapseclient.Synapse.is_certified>`__ on the Synapse object has been added to enable obtaining the Synapse certification quiz status of a user.

  .. code-block::

   passed = syn.is_certified(<username or user_id>)

- This version has been tested with Python 3.9.


Bug Fixes
---------

-  [`SYNPY-1039 <https://sagebionetworks.jira.com/browse/SYNPY-1039>`__] -
   tableQuery asDataFrame() results with TYPE_LIST columns should be lists and not literal strings
-  [`SYNPY-1109 <https://sagebionetworks.jira.com/browse/SYNPY-1109>`__] -
   unparseable synapse cacheMap raises JSONDecodeError
-  [`SYNPY-1110 <https://sagebionetworks.jira.com/browse/SYNPY-1110>`__] -
   Cleanup on Windows console login
-  [`SYNPY-1112 <https://sagebionetworks.jira.com/browse/SYNPY-1112>`__] -
   Concurrent migration of entities sharing the same file handle can result in an error
-  [`SYNPY-1114 <https://sagebionetworks.jira.com/browse/SYNPY-1114>`__] -
   Mitigate new Rust compiler dependency on Linux via transitive cryptography dependency
-  [`SYNPY-1118 <https://sagebionetworks.jira.com/browse/SYNPY-1118>`__] -
   Migration tool erroring when it shouldn't
New Features
------------

-  [`SYNPY-1058 <https://sagebionetworks.jira.com/browse/SYNPY-1058>`__] -
   Accept oauth access token for authentication to use Synapse REST services
-  [`SYNPY-1103 <https://sagebionetworks.jira.com/browse/SYNPY-1103>`__] -
   Multipart copy integration
-  [`SYNPY-1111 <https://sagebionetworks.jira.com/browse/SYNPY-1111>`__] -
   Add function to get user certification status

Improvements
------------

-  [`SYNPY-885 <https://sagebionetworks.jira.com/browse/SYNPY-885>`__] -
   Public interface to customize CACHE_ROOT_DIR
-  [`SYNPY-1102 <https://sagebionetworks.jira.com/browse/SYNPY-1102>`__] -
   syncToSynapse adds empty annotation values
-  [`SYNPY-1104 <https://sagebionetworks.jira.com/browse/SYNPY-1104>`__] -
   Python 3.9 support
-  [`SYNPY-1119 <https://sagebionetworks.jira.com/browse/SYNPY-1119>`__] -
   Add source storage location option to storage migrate functions

2.2.2 (2020-10-18)
==================

Highlights
----------

- This version addresses an issue with downloads being retried unsuccessfully after encountering certain types of errors.
- A `create_snapshot_version <index.html#synapseclient.Synapse.create_snapshot_version>`__ function is added for making table and view snapshots.

Bug Fixes
---------
-  [`SYNPY-1096 <https://sagebionetworks.jira.com/browse/SYNPY-1096>`__] -
   Fix link to Synapse on PyPI
-  [`SYNPY-1097 <https://sagebionetworks.jira.com/browse/SYNPY-1097>`__] -
   downloaded files are reset when disk space exhausted

New Features
------------

-  [`SYNPY-1041 <https://sagebionetworks.jira.com/browse/SYNPY-1041>`__] -
   Snapshot feature and programmatic clients

Improvements
------------

-  [`SYNPY-1063 <https://sagebionetworks.jira.com/browse/SYNPY-1063>`__] -
   Consolidate builds to GitHub Actions
-  [`SYNPY-1099 <https://sagebionetworks.jira.com/browse/SYNPY-1099>`__] -
   Replace usage of deprecated PUT /entity/{id}/version endpoint


2.2.0 (2020-08-31)
==================

Highlights
----------

- Files that are part of
  `syncFromSynapse <https://python-docs.synapse.org/build/html/synapseutils.html#synapseutils.sync.syncFromSynapse>`__
  and
  `syncToSynapse <https://python-docs.synapse.org/build/html/synapseutils.html#synapseutils.sync.syncToSynapse>`__
  operations (:code:`synapse get -r` and :code:`synapse sync` in the command line client, respectively) are
  transferred in in parallel threads rather than serially, substantially improving the performance of these operations.
- Table metadata from `synapse get -q` is automatically downloaded to a users working directory instead of to the Synapse cache (a hidden folder).
- Users can now pass their API key to `synapse login` in place of a password.

Bug Fixes
---------
-  [`SYNPY-1082 <https://sagebionetworks.jira.com/browse/SYNPY-1082>`__] -
   Downloading entity linked to URL fails: module 'urllib.parse' has no attribute 'urlretrieve'

Improvements
------------

-  [`SYNPY-1072 <https://sagebionetworks.jira.com/browse/SYNPY-1072>`__] -
   Improve throughput of multiple small file transfers
-  [`SYNPY-1073 <https://sagebionetworks.jira.com/browse/SYNPY-1073>`__] -
   Parellelize upload syncs
-  [`SYNPY-1074 <https://sagebionetworks.jira.com/browse/SYNPY-1074>`__] -
   Parallelize download syncs
-  [`SYNPY-1084 <https://sagebionetworks.jira.com/browse/SYNPY-1084>`__] -
   Allow anonymous usage for public APIs like GET /teamMembers/{id}
-  [`SYNPY-1088 <https://sagebionetworks.jira.com/browse/SYNPY-1088>`__] -
   Manifest is in cache with synapse get -q
-  [`SYNPY-1090 <https://sagebionetworks.jira.com/browse/SYNPY-1090>`__] -
   Command line client does not support apikey

Tasks
-----
-  [`SYNPY-1080 <https://sagebionetworks.jira.com/browse/SYNPY-1080>`__] -
   Remove Versionable from SchemaBase
-  [`SYNPY-1085 <https://sagebionetworks.jira.com/browse/SYNPY-1085>`__] -
   Move to pytest testing framework
-  [`SYNPY-1087 <https://sagebionetworks.jira.com/browse/SYNPY-1087>`__] -
   Improve synapseclient installation instructions

2.1.1 (2020-07-10)
==================

Highlights
----------

- This version includes a performance improvement for
  `syncFromSynapse <https://python-docs.synapse.org/build/html/synapseutils.html#synapseutils.sync.syncFromSynapse>`__
  downloads of deep folder hierarchies to local filesystem locations outside of the
  `Synapse cache <https://docs.synapse.org/articles/downloading_data.html#downloading-a-file>`__.

- Support is added for **SubmissionViews** that can be used to query and edit
  a set of submissions through table services.

  .. code-block:: python

   from synapseclient import SubmissionViewSchema

   project = syn.get("syn123")
   evaluation_id = '9876543'
   view = syn.store(SubmissionViewSchema(name='My Submission View', parent=project, scopes=[evaluation_id]))
   view_table = syn.tableQuery(f"select * from {view.id}")

Bug Fixes
---------

-  [`SYNPY-1075 <https://sagebionetworks.jira.com/browse/SYNPY-1075>`__] -
   Error in Python test (submission annotations)
-  [`SYNPY-1076 <https://sagebionetworks.jira.com/browse/SYNPY-1076>`__] -
   Upgrade/fix Pandas dependency

Improvements
------------

-  [`SYNPY-1070 <https://sagebionetworks.jira.com/browse/SYNPY-1070>`__] -
   Add support for submission views
-  [`SYNPY-1078 <https://sagebionetworks.jira.com/browse/SYNPY-1078>`__] -
   Improve syncFromSynapse performance for large folder structures synced to external paths


2.1.0 (2020-06-16)
==================

Highlights
----------

- A :code:`max_threads` property of the Synapse object has been added to customize the number of concurrent threads
  that will be used during file transfers.

  .. code-block:: python

    import synapseclient
    syn = synapseclient.login()
    syn.max_threads = 20

  If not customized the default value is (CPU count + 4). Adjusting this value
  higher may speed up file transfers if the local system resources can take advantage of the higher setting.
  Currently this value applies only to files whose underlying storage is AWS S3.

  Alternately, a value can be stored in the `synapseConfig configuration file <https://docs.synapse.org/articles/client_configuration.html>`__ that will automatically apply
  as the default if a value is not explicitly set.

  .. code-block::

     [transfer]
     max_threads=16

- This release includes support for directly accessing S3 storage locations using AWS Security Token Service
  credentials. This allows use of external AWS clients and libraries with Synapse storage, and can be used to
  accelerate file transfers under certain conditions. To create an STS enabled folder and set-up direct access to S3
  storage, see :ref:`here <sts_storage_locations>`.

- The :code:`getAnnotations` and :code:`setAnnotations` methods of the Synapse object have been **deprecated** in
  favor of newer :code:`get_annotations` and :code:`set_annotations` methods, respectively. The newer versions
  are parameterized with a typed :code:`Annotations` dictionary rather than a plain Python dictionary to prevent
  existing annotations from being accidentally overwritten. The expected usage for setting annotations is to first
  retrieve the existing :code:`Annotations` for an entity before saving changes by passing back a modified value.

  .. code-block::

     annos = syn.get_annotations('syn123')

     # set key 'foo' to have value of 'bar' and 'baz'
     annos['foo'] = ['bar', 'baz']
     # single values will automatically be wrapped in a list once stored
     annos['qwerty'] = 'asdf'

     annos = syn.set_annotations(annos)

  The deprecated annotations methods may be removed in a future release.

A full list of issues addressed in this release are below.

Bug Fixes
---------

-  [`SYNPY-913 <https://sagebionetworks.jira.com/browse/SYNPY-913>`__] -
   Travis Build badge for develop branch is pointing to pull request
-  [`SYNPY-960 <https://sagebionetworks.jira.com/browse/SYNPY-960>`__] -
   AppVeyor build badge appears to be failed while the builds are passed
-  [`SYNPY-1036 <https://sagebionetworks.jira.com/browse/SYNPY-1036>`__] -
   different users storing same file to same folder results in 403
-  [`SYNPY-1056 <https://sagebionetworks.jira.com/browse/SYNPY-1056>`__] -
   syn.getSubmissions fails due to new Annotation class in v2.1.0-rc

Improvements
------------

-  [`SYNPY-1036 <https://sagebionetworks.jira.com/browse/SYNPY-1029>`__] -
   Make upload speeds comparable to those of the AWS S3 CLI
-  [`SYNPY-1049 <https://sagebionetworks.jira.com/browse/SYNPY-1049>`__] -
   Expose STS-related APIs

Tasks
-----

-  [`SYNPY-1059 <https://sagebionetworks.jira.com/browse/SYNPY-1059>`__] -
   Use collections.abc instead of collections


2.0.0 (2020-03-23)
==================
**Python 2 is no longer supported as of this release.** This release requires Python 3.6+.

Highlights:
----------------

- Multi-threaded download of files from Synapse can be enabled by setting :code:`syn.multi_threaded` to :code:`True` on a
  :code:`synapseclient.Synapse` object. This will become the default implementation in the future,
  but to ensure stability for the first release of this feature, it must be intentionally enabled.

  .. code-block:: python

    import synapseclient
    syn = synapseclient.login()
    syn.multi_threaded = True
    # syn123 now will be downloaded via the multi-threaded implementation
    syn.get("syn123")

  Currently, multi-threaded download only works with files stored in AWS S3, where most files on Synapse reside.
  This also includes `custom storage locations <https://docs.synapse.org/articles/custom_storage_location.html>`__
  that point to an AWS S3 bucket.
  Files not stored in S3 will fall back to single-threaded download even if :code:`syn.multi_threaded==True`.
- :code:`synapseutils.copy()` now has limitations on what can be copied:
   - A user must have download permissions on the entity they want to copy.
   - Users cannot copy any entities that have `access requirements <https://docs.synapse.org/articles/access_controls.html>`__.
- :code:`contentTypes` and :code:`fileNames` are optional parameters in :code:`synapseutils.copyFileHandles()`

- Synapse Docker Repository(:code:`synapseclient.DockerRepository`) objects can now be submitted to Synapse evaluation
  queues using the :code:`entity` argument in :code:`synapseclient.Synapse.submit()`.
  An optional argument :code:`docker_tag="latest"` has also been added to :code:`synapseclient.Synapse.submit()`"
  to designate which tagged Docker image to submit.



A full list of issues addressed in this release are below.

Bugs Fixes
----------

-  [`SYNPY-271 <https://sagebionetworks.jira.com/browse/SYNPY-271>`__] -
   cache.remove fails to return the file handles we removed
-  [`SYNPY-1032 <https://sagebionetworks.jira.com/browse/SYNPY-1032>`__]
   - Support new columnTypes defined in backend

Tasks
-----

-  [`SYNPY-999 <https://sagebionetworks.jira.com/browse/SYNPY-999>`__] -
   Remove unsafe copy functions from client
-  [`SYNPY-1027 <https://sagebionetworks.jira.com/browse/SYNPY-1027>`__]
   - Copy function should copy things when users are part of a Team that
   has DOWNLOAD access

Improvements
------------

-  [`SYNPY-389 <https://sagebionetworks.jira.com/browse/SYNPY-389>`__] -
   submission of Docker repository
-  [`SYNPY-537 <https://sagebionetworks.jira.com/browse/SYNPY-537>`__] -
   synapseutils.copyFileHandles requires fields that does not require at
   rest
-  [`SYNPY-680 <https://sagebionetworks.jira.com/browse/SYNPY-680>`__] -
   synapseutils.changeFileMetaData() needs description in documentation
-  [`SYNPY-682 <https://sagebionetworks.jira.com/browse/SYNPY-682>`__] -
   improve download speeds to be comparable to AWS
-  [`SYNPY-807 <https://sagebionetworks.jira.com/browse/SYNPY-807>`__] -
   Drop support for Python 2
-  [`SYNPY-907 <https://sagebionetworks.jira.com/browse/SYNPY-907>`__] -
   Replace \`from <module> import ...\` with \`import <module>\`
-  [`SYNPY-962 <https://sagebionetworks.jira.com/browse/SYNPY-962>`__] -
   remove 'password' as an option in default synapse config file
-  [`SYNPY-972 <https://sagebionetworks.jira.com/browse/SYNPY-972>`__] -
   Link on Synapse Python Client Documentation points back at itself


1.9.4 (2019-06-28)
==================

Bug Fixes
---------

-  [`SYNPY-881 <https://sagebionetworks.jira.com/browse/SYNPY-881>`__] -
   Synu.copy fails when copying a file with READ permissions
-  [`SYNPY-888 <https://sagebionetworks.jira.com/browse/SYNPY-888>`__] -
   Docker repositories cannot be copied
-  [`SYNPY-927 <https://sagebionetworks.jira.com/browse/SYNPY-927>`__] -
   trying to create a project with name that already exists hangs
-  [`SYNPY-1005 <https://sagebionetworks.jira.com/browse/SYNPY-1005>`__]
   - cli docs missing sub-commands
-  [`SYNPY-1018 <https://sagebionetworks.jira.com/browse/SYNPY-1018>`__]
   - Synu.copy shouldn't copy any files with access restrictions

New Features
------------

-  [`SYNPY-851 <https://sagebionetworks.jira.com/browse/SYNPY-851>`__] -
   invite user or list of users to a team

Improvements
------------

-  [`SYNPY-608 <https://sagebionetworks.jira.com/browse/SYNPY-608>`__] -
   Add how to contribute md to github project
-  [`SYNPY-735 <https://sagebionetworks.jira.com/browse/SYNPY-735>`__] -
   command line for building a table
-  [`SYNPY-864 <https://sagebionetworks.jira.com/browse/SYNPY-864>`__] -
   docstring for the command line client doesn't have complete list of
   sub-commands available
-  [`SYNPY-926 <https://sagebionetworks.jira.com/browse/SYNPY-926>`__] -
   allow forceVersion false for command line client
-  [`SYNPY-1013 <https://sagebionetworks.jira.com/browse/SYNPY-1013>`__]
   - Documentation of "store" command for Synapse command line client
-  [`SYNPY-1021 <https://sagebionetworks.jira.com/browse/SYNPY-1021>`__]
   - change email contact for code of conduct

1.9.3 (2019-06-28)
==================

Bug Fixes
---------

-  [`SYNPY-993 <https://sagebionetworks.jira.com/browse/SYNPY-993>`__] -
   Fix `sendMessage` function
-  [`SYNPY-989 <https://sagebionetworks.jira.com/browse/SYNPY-989>`__] -
   Fix unstable test


1.9.2 (2019-02-15)
==================

In version 1.9.2, we improved Views' usability by exposing `set_entity_types()` function to change the entity types that will show up in a View::

    import synapseclient
    from synapseclient.table import EntityViewType

    syn = synapseclient.login()
    view = syn.get("syn12345")
    view.set_entity_types([EntityViewType.FILE, EntityViewType.FOLDER])
    view = syn.store(view)

Features
--------

-  [`SYNPY-919 <https://sagebionetworks.jira.com/browse/SYNPY-919>`__] -
   Expose a way to update entity types in a view using EntityViewType

Bug Fixes
---------

-  [`SYNPY-855 <https://sagebionetworks.jira.com/browse/SYNPY-855>`__] -
   Single thread uploading fails in Lambda python3.6 environment
-  [`SYNPY-910 <https://sagebionetworks.jira.com/browse/SYNPY-910>`__] -
   Store Wiki shows deprecation warning
-  [`SYNPY-920 <https://sagebionetworks.jira.com/browse/SYNPY-920>`__] -
   Project View turned into File View after using syndccutils template

Tasks
-----

-  [`SYNPY-790 <https://sagebionetworks.jira.com/browse/SYNPY-790>`__] -
   Pin to a fixed version of the request package
-  [`SYNPY-866 <https://sagebionetworks.jira.com/browse/SYNPY-866>`__] -
   Update Synapse logo in Python docs :)

Improvements
------------

-  [`SYNPY-783 <https://sagebionetworks.jira.com/browse/SYNPY-783>`__] -
   typos in comments and in stdout
-  [`SYNPY-916 <https://sagebionetworks.jira.com/browse/SYNPY-916>`__] -
   Wonky display on parameters
-  [`SYNPY-917 <https://sagebionetworks.jira.com/browse/SYNPY-917>`__] -
   Add instructions on how to login with API key
-  [`SYNPY-909 <https://sagebionetworks.jira.com/browse/SYNPY-909>`__] -
   Missing columnTypes in Column docstring



1.9.1 (2019-01-20)
==================

In version 1.9.1, we fix various bugs and added two new features:

* Python 3.7 is supported.
* Deprecation warnings are visible by default.

Features
--------

-  [`SYNPY-802 <https://sagebionetworks.jira.com/browse/SYNPY-802>`__] -
   Support Python 3.7
-  [`SYNPY-849 <https://sagebionetworks.jira.com/browse/SYNPY-849>`__] -
   Add deprecation warning that isn't filtered by Python

Bug Fixes
---------

-  [`SYNPY-454 <https://sagebionetworks.jira.com/browse/SYNPY-454>`__] -
   Some integration tests do not clean up after themselves
-  [`SYNPY-456 <https://sagebionetworks.jira.com/browse/SYNPY-456>`__] -
   Problems with updated query system
-  [`SYNPY-515 <https://sagebionetworks.jira.com/browse/SYNPY-515>`__] -
   sphinx documentation not showing for some new classes
-  [`SYNPY-526 <https://sagebionetworks.jira.com/browse/SYNPY-526>`__] -
   deprecate downloadTableFile()
-  [`SYNPY-578 <https://sagebionetworks.jira.com/browse/SYNPY-578>`__] -
   switch away from POST /entity/#/table/deleterows
-  [`SYNPY-594 <https://sagebionetworks.jira.com/browse/SYNPY-594>`__] -
   Getting error from dev branch in integration test against staging
-  [`SYNPY-796 <https://sagebionetworks.jira.com/browse/SYNPY-796>`__] -
   fix or remove PyPI downloads badge in readme
-  [`SYNPY-799 <https://sagebionetworks.jira.com/browse/SYNPY-799>`__] -
   Unstable test: Test PartialRow updates to entity views from rowset
   queries
-  [`SYNPY-846 <https://sagebionetworks.jira.com/browse/SYNPY-846>`__] -
   error if password stored in config file contains a '%'


Tasks
-----

-  [`SYNPY-491 <https://sagebionetworks.jira.com/browse/SYNPY-491>`__] -
   Figure out custom release note fitlers
-  [`SYNPY-840 <https://sagebionetworks.jira.com/browse/SYNPY-840>`__] -
   Install not working on latest python
-  [`SYNPY-847 <https://sagebionetworks.jira.com/browse/SYNPY-847>`__] -
   uploadFileHandle should not be deprecated nor removed
-  [`SYNPY-852 <https://sagebionetworks.jira.com/browse/SYNPY-852>`__] -
   Check and update docs.synapse.org to reflect the change in the Python
   client
-  [`SYNPY-860 <https://sagebionetworks.jira.com/browse/SYNPY-860>`__] -
   vignette for how to upload a new version of a file directly to a
   synapse entity
-  [`SYNPY-863 <https://sagebionetworks.jira.com/browse/SYNPY-863>`__] -
   Update public documentation to move away from the query services
-  [`SYNPY-866 <https://sagebionetworks.jira.com/browse/SYNPY-866>`__] -
   Update Synapse logo in Python docs :)
-  [`SYNPY-873 <https://sagebionetworks.jira.com/browse/SYNPY-873>`__] -
   consolidate integration testing to platform dev account

Improvements
------------

-  [`SYNPY-473 <https://sagebionetworks.jira.com/browse/SYNPY-473>`__] -
   Change syn.list to no longer use deprecated function chunkedQuery
-  [`SYNPY-573 <https://sagebionetworks.jira.com/browse/SYNPY-573>`__] -
   synapse list command line shouldn't list the parent container
-  [`SYNPY-581 <https://sagebionetworks.jira.com/browse/SYNPY-581>`__] -
   <entity>.annotations return object is inconsistent with
   getAnnotations()
-  [`SYNPY-612 <https://sagebionetworks.jira.com/browse/SYNPY-612>`__] -
   Rename view_type to viewType in EntityViewSchema for consistency
-  [`SYNPY-777 <https://sagebionetworks.jira.com/browse/SYNPY-777>`__] -
   Python client \_list still uses chunckedQuery and result seem out of
   date
-  [`SYNPY-804 <https://sagebionetworks.jira.com/browse/SYNPY-804>`__] -
   Update styling in the python docs to more closely match the Docs site
   styling
-  [`SYNPY-815 <https://sagebionetworks.jira.com/browse/SYNPY-815>`__] -
   Update the build to use test user instead of migrationAdmin
-  [`SYNPY-848 <https://sagebionetworks.jira.com/browse/SYNPY-848>`__] -
   remove outdated link to confluence for command line query
-  [`SYNPY-856 <https://sagebionetworks.jira.com/browse/SYNPY-856>`__] -
   build_table example in the docs does not look right
-  [`SYNPY-858 <https://sagebionetworks.jira.com/browse/SYNPY-858>`__] -
   Write file view documentation in python client that is similar to
   synapser
-  [`SYNPY-870 <https://sagebionetworks.jira.com/browse/SYNPY-870>`__] -
   Submitting to an evaluation queue can't accept team as int




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

* We have expanded the `synapseutils packages <python-docs.synapse.org/build/html/synapseutils.html#module-synapseutils>`_ to add the ability to:

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
