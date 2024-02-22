# Release Notes

## 4.1.0 (2024-02-21)

### Highlights
- **New Interfaces:**
    - Combines data and behavior into a single class, simplifying the understanding and usage of the system's models. Review the available [synchronous](./reference/oop/models.md) and [asynchronous](./reference/oop/models_async.md) models.
    - **New Interface:** Introduced a revamped interface in the Synapse Python Client, shifting from a functional programming approach to an object-oriented one.
    - **Enhanced Developer Experience:** This change highlights much needed quality of life updates for developers. Improved autocomplete, hoverdocs, and examples in docstrings provide a significantly better coding experience.
    - **Asyncio Support:** Introduced support for asyncio, enabling more efficient use of system resources and enhancing performance for IO-bound tasks.
    - **Extensibility:** Laying the foundation for an extensible platform, facilitating easier addition of new features, and improvements to the Synapse Python Client.

- **synapseutils.walk Improvement:**
    - Improved performance for [synapseutils.walk][].

- **Pandas Range Expansion:**
    - Expanded pandas range to `>=1.5, <3.0`.

- **Version Notation Support:**
    - Using `syn123.version` notation is now supported with [syn.get][synapseclient.Synapse.get], [synapseutils.syncFromSynapse][], and [syn.setProvenance][synapseclient.Synapse.setProvenance]. This enhances consistency in version management across various activities.


### Bug Fixes
-  \[[SYNPY-448](https://sagebionetworks.jira.com/browse/SYNPY-448)\] - synapseutils.changeFileMetaData should allow changing Synapse name (like web client) for consistency
-  \[[SYNPY-1253](https://sagebionetworks.jira.com/browse/SYNPY-1253)\] - syn.store(forceVersion=False) created new versions of the same file
-  \[[SYNPY-1398](https://sagebionetworks.jira.com/browse/SYNPY-1398)\] - syncToSynapse doesn't recognize a "valid" provenance synapse id
-  \[[SYNPY-1412](https://sagebionetworks.jira.com/browse/SYNPY-1412)\] - Issue importing synapseutils in notebook

### Stories
-  \[[SYNPY-1326](https://sagebionetworks.jira.com/browse/SYNPY-1326)\] - Update pandas dependency to support pandas 2.1
-  \[[SYNPY-1344](https://sagebionetworks.jira.com/browse/SYNPY-1344)\] - Implement 'Activity' model into OOP
-  \[[SYNPY-1347](https://sagebionetworks.jira.com/browse/SYNPY-1347)\] - Implement 'Team' model into OOP
-  \[[SYNPY-1348](https://sagebionetworks.jira.com/browse/SYNPY-1348)\] - Implement 'UserProfile' model into OOP
-  \[[SYNPY-1401](https://sagebionetworks.jira.com/browse/SYNPY-1401)\] - Avoid repeatedly calling syn.get in `_helpWalk`
-  \[[SYNPY-1414](https://sagebionetworks.jira.com/browse/SYNPY-1414)\] - Finish 'Project' OOP model
-  \[[SYNPY-1415](https://sagebionetworks.jira.com/browse/SYNPY-1415)\] - Finish 'Folder' OOP model
-  \[[SYNPY-1416](https://sagebionetworks.jira.com/browse/SYNPY-1416)\] - Finish 'File' OOP model
-  \[[SYNPY-1434](https://sagebionetworks.jira.com/browse/SYNPY-1434)\] - Release python client 4.1

## 4.0.0 (2024-01-12)

### Highlights

-   **Only authentication through Personal Access Token**
    **(aka: Authentication bearer token) is supported**. Review the
    [Authentication document](https://python-docs.synapse.org/tutorials/authentication/)
    for information on setting up your usage of a Personal Access Token to authenticate
    with Synapse.
-   **Date type Annotations on Synapse entities are now timezone aware**. Review our
    [reference documentation for Annotations](https://python-docs.synapse.org/reference/annotations/).
    The [`pytz` package](https://pypi.org/project/pytz/) is reccomended if you regularly
    work with data across time zones.
    - If you do not set the `tzinfo` field on a date or datetime instance we will use the
        timezone of the machine where the code is executing.
    - If you are using the
        [Manifest TSV](https://python-docs.synapse.org/explanations/manifest_tsv/#annotations)
        for bulk actions on your projects you'll now see that
        [synapseutils.sync.syncFromSynapse][] will store dates as `YYYY-MM-DDTHH:MM:SSZ`.
        Review our documentation for an
        [example manifest file](https://python-docs.synapse.org/explanations/manifest_tsv/#example-manifest-file).
        Additionally, if you'd like to upload an annotation in a specific timezone please
        make sure that it is in [ISO 8601 format](https://en.wikipedia.org/wiki/ISO_8601).
        If you do not specify a timezone it is assumed to use the timezone of the machine
        where the code is executing.
-   **Support for annotations with multiple values through the**
    [**Manifest TSV**](https://python-docs.synapse.org/explanations/manifest_tsv/#multiple-values-of-annotations-per-key)
    with the usage of a comma delimited bracket wrapped list. Any manifest files wishing
    to take advantage of multi-value annotations need to match this format. Examples:
    - `["Annotation, with a comma", another annotation]`
    - `[1,2,3]`
    - `[2023-12-04T07:00:00Z,2000-01-01T07:00:00Z]`
-   Migration and expansion of the docs site! You'll see that the look, feel, and flow
    of all of the information on this site has been touched. As we move forward we hope
    that you'll
    [provide the Data Processing and Engineering team feedback on areas we can improve](https://sagebionetworks.jira.com/servicedesk/customer/portal/5/group/7).
-   Expansion of the available Python Tutorials can be found
    [starting here](https://python-docs.synapse.org/tutorials/python_client/).

### Bug Fixes
-  \[[SYNPY-1357](https://sagebionetworks.jira.com/browse/SYNPY-1357)\] - Manifest does not support annotations with multiple values
-  \[[SYNPY-1358](https://sagebionetworks.jira.com/browse/SYNPY-1358)\] - Date and datetime annotations do not account for timezone
-  \[[SYNPY-1387](https://sagebionetworks.jira.com/browse/SYNPY-1387)\] - Update High level best practices for project structure document

### Stories
-  \[[SYNPY-955](https://sagebionetworks.jira.com/browse/SYNPY-955)\] - Remove the ability to login using session token
-  \[[SYNPY-1182](https://sagebionetworks.jira.com/browse/SYNPY-1182)\] - Remove ability to manage API keys and to use API keys for auth' in R/Py/CLI
-  \[[SYNPY-1225](https://sagebionetworks.jira.com/browse/SYNPY-1225)\] - Remove the use of all authentication methods except authToken in the python client for security
-  \[[SYNPY-1302](https://sagebionetworks.jira.com/browse/SYNPY-1302)\] - Deprecate getPermissions and create get_permissions
-  \[[SYNPY-1321](https://sagebionetworks.jira.com/browse/SYNPY-1321)\] - Benchmark download speeds
-  \[[SYNPY-1334](https://sagebionetworks.jira.com/browse/SYNPY-1334)\] - Review and revamp the getting started documentation for the client
-  \[[SYNPY-1365](https://sagebionetworks.jira.com/browse/SYNPY-1365)\] - Finish migration of docstrings from sphinx to google style
-  \[[SYNPY-1392](https://sagebionetworks.jira.com/browse/SYNPY-1392)\] - Remove all older functions that have been labeled to be deprecated
-  \[[SYNPY-1394](https://sagebionetworks.jira.com/browse/SYNPY-1394)\] - Python client release 4.0.0

## 3.2.0 (2023-11-27)

### Highlights

-   Introduction of OpenTelemetry within the client. See information in
    the README for further details.
-   Added 2 new functions `get_user_profile_by_username` and
    `get_user_profile_by_id` to handle for use cases when a
    username is a number.
-   Updated the local .cacheMap that is used to track local files
    downloaded from Synapse to include the MD5 of the file when there is
    a cache miss. This is used to determine if the file has changed
    since we cannot fully rely on the file's modified date only.
-   Include a progress indicator when downloading files via FTP.
-   Benchmarking of upload speeds to allow us to make data driven
    decisions and comparisons.

### Bug Fixes

-   \[[SYNPY-1186](https://sagebionetworks.jira.com/browse/SYNPY-1186)\]
    When a username is a number, getUserProfile cannot retrieve the
    user.
-   \[[SYNPY-1316](https://sagebionetworks.jira.com/browse/SYNPY-1316)\]
    Calling `syn.get` with
    `ifcollision='overwrite.local` does not always
    overwrite previous file
-   \[[SYNPY-1319](https://sagebionetworks.jira.com/browse/SYNPY-1319)\]
    Unstable test: test_download_file_false
-   \[[SYNPY-1333](https://sagebionetworks.jira.com/browse/SYNPY-1333)\]
    synapse get on a file in a ftp server doesn't seem to be
    downloading

### Stories

-   \[[SYNPY-816](https://sagebionetworks.jira.com/browse/SYNPY-816)\]
    Unstable test: integration.test_evaluations.test_teams
-   \[[SYNPY-1274](https://sagebionetworks.jira.com/browse/SYNPY-1274)\]
    Set up pre-commit in github actions
-   \[[SYNPY-1305](https://sagebionetworks.jira.com/browse/SYNPY-1305)\]
    Collect trace data for integration tests
-   \[[SYNPY-1304](https://sagebionetworks.jira.com/browse/SYNPY-1304)\]
    Introduction of OpenTelemetry
-   \[[SYNPY-1320](https://sagebionetworks.jira.com/browse/SYNPY-1320)\]
    Benchmark upload speeds

## 3.1.1 (2023-10-30)

### Highlights

-   A fix to dowloads when file names match but content does not when
    `ifcollision=overwrite.local`

### Bug Fixes

-   \[[SYNPY-1316](https://sagebionetworks.jira.com/browse/SYNPY-1316)\]
    Calling `syn.get` with
    `ifcollision='overwrite.local` does not always
    overwrite previous file
-   \[[SYNPY-1298](https://sagebionetworks.jira.com/browse/SYNPY-1298)\]
    Resolve unstable integration tests

## 3.1.0 (2023-10-20)

### Highlights

-   A fix to authentication when using a Personal Access token or API
    Key. `synapse login` and `synapse config`
    correctly work as a result.
-   Replacement of custom `@memoize` decorator with
    `@functools.lru_cache` decorator.
-   Introduction of pipfile for easy creation of a virtual environment
    for development.

### Bug Fixes

-   \[[SYNPY-1283](https://sagebionetworks.jira.com/browse/SYNPY-1283)\]
    Fix dead link in docs
-   \[[SYNPY-1296](https://sagebionetworks.jira.com/browse/SYNPY-1296)\]
    Unable to log into synapse only with auth token

### Stories

-   \[[SYNPY-49](https://sagebionetworks.jira.com/browse/SYNPY-49)\]
    py: getPermissions and permissions via group membership
-   \[[SYNPY-967](https://sagebionetworks.jira.com/browse/SYNPY-967)\]
    Replace @memoize annotation in python client with
    @functools.lru_cache
-   \[[SYNPY-1282](https://sagebionetworks.jira.com/browse/SYNPY-1282)\]
    Add type hints to 5 functions in client.py
-   \[[SYNPY-1285](https://sagebionetworks.jira.com/browse/SYNPY-1285)\]
    Create pipenv lock file for synapsePythonClient
-   \[[SYNPY-1293](https://sagebionetworks.jira.com/browse/SYNPY-1293)\]
    Address Dependabot security vulnerabilities
-   \[[SYNPY-1295](https://sagebionetworks.jira.com/browse/SYNPY-1295)\]
    Doc: Add config command to authentication section in Synapse doc

## 3.0.0 (2023-09-09)

### Highlights

-   Removed all camel case or non-standard single dash long command line
    interface (cli) parameters. Example: command line arguments like
    `-parent` will become `--parent`. Commands
    that support camel case like `--parentId` will be
    changed to `--parent-id`.
-   Remove support for Python 3.7 due to its end of life and support
    Python 3.11
-   Support Synapse JSON schema services

### Bug Fixes

-   \[[SYNPY-570](https://sagebionetworks.jira.com/browse/SYNPY-570)\]
    warning messages when downloading if files in the cache already
-   \[[SYNPY-622](https://sagebionetworks.jira.com/browse/SYNPY-622)\]
    Constructor for SubmissionStatus is not correctly implemented
-   \[[SYNPY-1242](https://sagebionetworks.jira.com/browse/SYNPY-1242)\]
    \_loggedIn() function doesn't work
-   \[[SYNPY-1269](https://sagebionetworks.jira.com/browse/SYNPY-1269)\]
    Integration test failures against release-456
-   \[[SYNPY-1271](https://sagebionetworks.jira.com/browse/SYNPY-1271)\]
    Integration test failure against release-458

### Stories

-   \[[SYNPY-737](https://sagebionetworks.jira.com/browse/SYNPY-737)\]
    deprecate single dash, multi-letter named params in command line
    client
-   \[[SYNPY-1012](https://sagebionetworks.jira.com/browse/SYNPY-1012)\]
    Synapse Command Line documentation is not distinct from Synapse
    Python Client documentations
-   \[[SYNPY-1213](https://sagebionetworks.jira.com/browse/SYNPY-1213)\]
    Add \"public\" and \"registered users\" group to setPermissions doc
-   \[[SYNPY-1245](https://sagebionetworks.jira.com/browse/SYNPY-1245)\]
    Fix old links to synapse docs
-   \[[SYNPY-1246](https://sagebionetworks.jira.com/browse/SYNPY-1246)\]
    Support Python 3.11
-   \[[SYNPY-1122](https://sagebionetworks.jira.com/browse/SYNPY-1122)\]
    JSON Schema support
-   \[[SYNPY-1255](https://sagebionetworks.jira.com/browse/SYNPY-1255)\]
    Support pandas 2.0 in python client
-   \[[SYNPY-1270](https://sagebionetworks.jira.com/browse/SYNPY-1270)\]
    Deprecate the use of `date_parser` and
    `parse_date` in [pd.read_csv` in table
    module
-   \[[SYNPY-716](https://sagebionetworks.jira.com/browse/SYNPY-716)\]
    Deprecate and remove asInteger()
-   \[[SYNPY-1227](https://sagebionetworks.jira.com/browse/SYNPY-1227)\]
    Run `black` the Python auto formatter on the files

## 2.7.2 (2023-06-02)

### Highlights

-   \[[SYNPY-1267](https://sagebionetworks.jira.com/browse/SYNPY-1267)\]
    -Lock down urllib3
-   \[[SYNPY-1268](https://sagebionetworks.jira.com/browse/SYNPY-1268)\]
    -Add deprecation warning for non-support login arguments
-   Next major release (3.0.0)\...
    -   Support only pandas `\>=` 1.5
    -   Remove support for Python 3.7 due to its end of life.
    -   There will be major cosmetic changes to the cli such as removing
        all camel case or non-standard single dash long command line
        interface (cli) parameters. Example: command line arguments like
        `-parent` will become [\--parent`.
        Commands that support camel case like `\--parentId`
        will be changed to `\--parent-id`.

## 2.7.1 (2023-04-11)

### Highlights

-   Locked down pandas version to only support pandas `\<`
    1.5

-   Next major release (3.0.0)\...

    > -   Support only pandas `\>=` 1.5
    > -   Remove support for Python 3.7 due to its end of life.
    > -   There will be major cosmetic changes to the cli such as
    >     removing all camel case or non-standard single dash long
    >     command line interface (cli) parameters. Example: command line
    >     arguments like `-parent` will become
    >     `\--parent`. Commands that support camel case like
    >     `\--parentId` will be changed to
    >     `\--parent-id`.

## 2.7.0 (2022-09-16)

### Highlights

-   Added support for Datasets

    ``` python
    # from python
    import synapseclient
    import synapseutils
    syn = synapseclient.login()
    dataset_items = [
        {'entityId': "syn000", 'versionNumber': 1},
        {...},
    ]
    dataset = synapseclient.Dataset(
        name="My Dataset",
        parent=project,
        dataset_items=dataset_items
    )
    dataset = syn.store(dataset)
    # Add/remove specific Synapse IDs to/from the Dataset
    dataset.add_item({'entityId': "syn111", 'versionNumber': 1})
    dataset.remove_item("syn000")
    dataset = syn.store(dataset)
    # Add a single Folder to the Dataset
    # this will recursively add all the files in the folder
    dataset.add_folder("syn123")
    # Add a list of Folders, overwriting any existing files in the dataset
    dataset.add_folders(["syn456", "syn789"], force=True)
    dataset = syn.store(dataset)
    # Create snapshot version of dataset
    syn.create_snapshot_version(
        dataset.id,
        label="v1.0",
        comment="This is version 1"
    )
    ```

-   Added support for downloading from download cart. You can use this
    feature by first adding items to your download cart on Synapse.

    ``` python
    # from python
    import synapseclient
    import synapseutils
    syn = synapseclient.login()
    manifest_path = syn.get_download_list()
    ```

    ``` bash
    # from command line
    synapse get-download-list
    ```

-   Next major release (3.0.0) there will be major cosmetic changes to
    the cli such as removing all camel case or non-standard single dash
    long command line interface (cli) parameters. Example: command line
    arguments like `-parent` will become
    `\--parent`. Commands that support camel case like
    `\--parentId` will be changed to
    `\--parent-id`.

### Bug Fixes

-   \[[SYNPY-226](https://sagebionetworks.jira.com/browse/SYNPY-226)\]
    -isConsistent fails as parameter for table query
-   \[[SYNPY-562](https://sagebionetworks.jira.com/browse/SYNPY-562)\]
    -Make sure SQL functions, including \"year\", are quoted correctly
-   \[[SYNPY-1031](https://sagebionetworks.jira.com/browse/SYNPY-1031)\]
    -File version increments with 400 client error
-   \[[SYNPY-1219](https://sagebionetworks.jira.com/browse/SYNPY-1219)\]
    -Update Entity class to be compatible with the new Dataset entity
-   \[[SYNPY-1224](https://sagebionetworks.jira.com/browse/SYNPY-1224)\]
    -Correct SynapseUnmetAccessRestrictions message
-   \[[SYNPY-1237](https://sagebionetworks.jira.com/browse/SYNPY-1237)\]
    -as_table_columns function is mishandling mixed data types

### Stories

-   \[[SYNPY-63](https://sagebionetworks.jira.com/browse/SYNPY-63)\]
    -py: use metaclass to replace the \_entity_type_to_class hack
-   \[[SYNPY-992](https://sagebionetworks.jira.com/browse/SYNPY-992)\]
    -synapseutils changeFileMetadata missing syn parameter docstring
-   \[[SYNPY-1175](https://sagebionetworks.jira.com/browse/SYNPY-1175)\]
    -Programmatic Support for Download V2 via Py Client
-   \[[SYNPY-1193](https://sagebionetworks.jira.com/browse/SYNPY-1193)\]
    -Support Datasets functionality
-   \[[SYNPY-1221](https://sagebionetworks.jira.com/browse/SYNPY-1221)\]
    -Set up gh-action: black, the python auto formatter on the python
    client

### Tasks

-   \[[SYNPY-566](https://sagebionetworks.jira.com/browse/SYNPY-566)\]
    -Clarify expected list format for sync manifest
-   \[[SYNPY-1053](https://sagebionetworks.jira.com/browse/SYNPY-1053)\]
    -Increase documentation of forceVersion in syncToSynapse
-   \[[SYNPY-1145](https://sagebionetworks.jira.com/browse/SYNPY-1145)\]
    -Link to manifest format in CLI sync command usage help
-   \[[SYNPY-1226](https://sagebionetworks.jira.com/browse/SYNPY-1226)\]
    -Leverage `ViewBase` for Datasets instead of
    `SchemaBase`
-   \[[SYNPY-1235](https://sagebionetworks.jira.com/browse/SYNPY-1235)\]
    -Create codeql scanning workflow
-   \[[SYNPY-1207](https://sagebionetworks.jira.com/browse/SYNPY-1207)\]
    -Support syn.get() on a dataset

## 2.6.0 (2022-04-19)

### Highlights

-   Next major release (3.0.0) there will be major cosmetic changes to
    the cli such as removing all camel case or non-standard single dash
    long command line interface (cli) parameters. Example: command line
    arguments like `-parent` will become
    `\--parent`. Commands that support camel case like
    `\--parentId` will be changed to
    `\--parent-id`.

-   Added support for materialized views

    ``` python
    # from python
    import synapseclient
    import synapseutils
    syn = synapseclient.login()
    view = synapseclient.MaterializedViewSchema(
        name="test-material-view",
        parent="syn34234",
        definingSQL="SELECT * FROM syn111 F JOIN syn2222 P on (F.PATIENT_ID = P.PATIENT_ID)"
    )
    view_ent = syn.store(view)
    ```

-   Removed support for Python 3.6 and added support for Python 3.10

-   Add function to create Synapse config file

    ``` bash
    # from the command line
    synapse config
    ```

### Bug Fixes

-   \[[SYNPY-1204](https://sagebionetworks.jira.com/browse/SYNPY-1204)\]
    -Python 3.10 compatibility

### Stories

-   \[[SYNPY-728](https://sagebionetworks.jira.com/browse/SYNPY-728)\]
    -Improve error message when pandas is not available
-   \[[SYNPY-974](https://sagebionetworks.jira.com/browse/SYNPY-974)\]
    -Documentation for generateManifest
-   \[[SYNPY-1209](https://sagebionetworks.jira.com/browse/SYNPY-1209)\]
    -Support for MaterializedViews in Py Client

### Tasks

-   \[[SYNPY-1174](https://sagebionetworks.jira.com/browse/SYNPY-1174)\]
    -Add function to create Synapse config file
-   \[[SYNPY-1176](https://sagebionetworks.jira.com/browse/SYNPY-1176)\]
    -syncToSynapse aborted + silent failure of file upload
-   \[[SYNPY-1184](https://sagebionetworks.jira.com/browse/SYNPY-1184)\]
    -Add `includeTypes` to `synapseutils.walk()`
-   \[[SYNPY-1189](https://sagebionetworks.jira.com/browse/SYNPY-1189)\]
    -Document \"maximumListLength\" parameter for Column
-   \[[SYNPY-1196](https://sagebionetworks.jira.com/browse/SYNPY-1196)\]
    -Expose `forceVersion` on
    `changeFileMetadata`
-   \[[SYNPY-1205](https://sagebionetworks.jira.com/browse/SYNPY-1205)\]
    -Python 3.6 EOL - Remove support for 3.6
-   \[[SYNPY-1212](https://sagebionetworks.jira.com/browse/SYNPY-1212)\]
    -Include `dataset` as an entity type to return in
    getChildren()

## 2.5.1 (2021-12-02)

### Highlights

-   Next major release (3.0.0) there will be major cosmetic changes to
    the cli such as removing all camel case or non-standard single dash
    long command line interface (cli) parameters. Example: command line
    arguments like `-parent` will become
    `\--parent`. Commands that support camel case like
    `\--parentId` will be changed to
    `\--parent-id`.

### Bug Fixes

-   \[[SYNPY-1197](https://sagebionetworks.jira.com/browse/SYNPY-1197)\]
    -Schema is a string and strings don't have columns_to_store
    attributes

### Stories

-   \[[SYNPY-772](https://sagebionetworks.jira.com/browse/SYNPY-772)\]
    -update statement that appears on PyPi about Synapse to be
    consistent
-   \[[SYNPY-997](https://sagebionetworks.jira.com/browse/SYNPY-997)\]
    -Typos in Views documentation

## 2.5.0 (2021-10-05)

### Highlights

-   Added ability to generate a manifest file from your local directory
    structure.

    ``` bash
    # from the command line
    # write the manifest to manifest.tsv
    synapse manifest --parent-id syn123 --manifest-file ./manifest.tsv /path/to/local/directory
    # stdout
    synapse manifest --parent-id syn123 /path/to/local/directory
    ```

-   Added ability to pipe manifest stdout into sync function.

    ``` bash
    # from the command line
    synapse manifest --parent-id syn123 ./docs/ | synapse sync -
    ```

-   Added ability to return summary statistics of csv and tsv files
    stored in Synapse.

    ``` bash
    # from python
    import synapseclient
    import synapseutils
    syn = synapseclient.login()
    statistics = synapseutils.describe(syn=syn, entity="syn12345")
    print(statistics)
    {
        "column1": {
            "dtype": "object",
            "mode": "FOOBAR"
        },
        "column2": {
            "dtype": "int64",
            "mode": 1,
            "min": 1,
            "max": 2,
            "mean": 1.4
        },
        "column3": {
            "dtype": "bool",
            "mode": false,
            "min": false,
            "max": true,
            "mean": 0.5
        }
    }
    ```

-   Next major release (3.0.0) there will be major cosmetic changes to
    the cli such as removing all camel case or non-standard single dash
    long command line interface (cli) parameters. Example: command line
    arguments like `-parent` will become
    `\--parent`. Commands that support camel case like
    `\--parentId` will be changed to
    `\--parent-id`.

### Bug Fixes

-   \[[SYNPY-669](https://sagebionetworks.jira.com/browse/SYNPY-669)\]
    -Signing of Synapse authentication header does not correctly URL
    encode the URL path
-   \[[SYNPY-770](https://sagebionetworks.jira.com/browse/SYNPY-770)\]
    -Files failing to upload using syncToSynapse
-   \[[SYNPY-1123](https://sagebionetworks.jira.com/browse/SYNPY-1123)\]
    -All tables erroring when indexing
-   \[[SYNPY-1146](https://sagebionetworks.jira.com/browse/SYNPY-1146)\]
    -Error writing Booleans from Python dataframes into Boolean columns
    in a Synapse table
-   \[[SYNPY-1156](https://sagebionetworks.jira.com/browse/SYNPY-1156)\]
    -datetimes in a Pandas dataframe are not properly stored to Synapse

### Stories

-   \[[SYNPY-726](https://sagebionetworks.jira.com/browse/SYNPY-726)\]
    -mirror local folder structure for bulk upload
-   \[[SYNPY-1163](https://sagebionetworks.jira.com/browse/SYNPY-1163)\]
    -Expose synId with syn get -r
-   \[[SYNPY-1165](https://sagebionetworks.jira.com/browse/SYNPY-1165)\]
    -Generate manifest template from local folder structure
-   \[[SYNPY-1167](https://sagebionetworks.jira.com/browse/SYNPY-1167)\]
    -Support for Quick Summary Statistics on CSV and TSV files

### Tasks

-   \[[SYNPY-1169](https://sagebionetworks.jira.com/browse/SYNPY-1169)\]
    -Integration tests failures in develop branch against stack-371
-   \[[SYNPY-1172](https://sagebionetworks.jira.com/browse/SYNPY-1172)\]
    -Passing a pandas dataframe with a column called \"read\" breaks the
    type parsing in as_table_columns()
-   \[[SYNPY-1173](https://sagebionetworks.jira.com/browse/SYNPY-1173)\]
    -Support DATE_LIST, ENTITYID_LIST, USERID_LIST table columns
-   \[[SYNPY-1188](https://sagebionetworks.jira.com/browse/SYNPY-1188)\]
    -Support piping of `synapse manifest` stdout in `synapse sync` function

## 2.4.0 (2021-07-08)

### Highlights

-   Added ability to authenticate from a `SYNAPSE_AUTH_TOKEN`
    environment variable set with a valid [personal access
    token](https://help.synapse.org/docs/Managing-Your-Account.2055405596.html#ManagingYourAccount-PersonalAccessTokens).

    ``` bash
    # e.g. set environment variable prior to invoking a Synapse command or running a program that uses synapseclient
    SYNAPSE_AUTH_TOKEN='<my_personal_access_token>' synapse <subcommand options>
    ```

    The environment variable will take priority over credentials in the
    user's `.synapseConfig` file or any credentials saved in a prior
    login using the remember me option.

    See [here](Credentials.html#use-environment-variable) for more
    details on usage.

-   Added ability to silence all console output.

    ``` bash
    # from the command line, use the --silent option with any synapse subcommand, here it will suppress the download progress indicator
    synapse --silent get <synid>
    ```

    ``` python3
    # from code using synapseclient, pass the silent option to the Synapse constructor
    import synapseclient

    syn = synapseclient.Synapse(silent=True)
    syn.login()
    syn.get(<synid>)
    ```

-   Improved robustness during downloads with unstable connections.
    Specifically the client will automatically recover when encoutering
    some types of network errors that previously would have caused a
    download to start over as indicated by a reset progress bar.

### Bug Fixes

-   \[[SYNPY-198](https://sagebionetworks.jira.com/browse/SYNPY-198)\]
    -get: Unmet access requirement should not raise error if entity not
    downloadable
-   \[[SYNPY-959](https://sagebionetworks.jira.com/browse/SYNPY-959)\]
    -FileEntity 'path' property has wrong separator in Windows
-   \[[SYNPY-1113](https://sagebionetworks.jira.com/browse/SYNPY-1113)\]
    -Confusing error when putting the positional FILE at the end of the
    synapse store command with an optional n-arg
-   \[[SYNPY-1128](https://sagebionetworks.jira.com/browse/SYNPY-1128)\]
    -failures downloading 14G vcf file
-   \[[SYNPY-1130](https://sagebionetworks.jira.com/browse/SYNPY-1130)\]
    -Migration tool trying to move URL-linked data
-   \[[SYNPY-1134](https://sagebionetworks.jira.com/browse/SYNPY-1134)\]
    -500 error during part copy to AWS presigned url
-   \[[SYNPY-1135](https://sagebionetworks.jira.com/browse/SYNPY-1135)\]
    -Exceeding part limit during AD Migration
-   \[[SYNPY-1136](https://sagebionetworks.jira.com/browse/SYNPY-1136)\]
    -Connection aborted to AWS part copy to presigned url during AD
    Migration
-   \[[SYNPY-1141](https://sagebionetworks.jira.com/browse/SYNPY-1141)\]
    -synapse get command line nargs usage/error
-   \[[SYNPY-1150](https://sagebionetworks.jira.com/browse/SYNPY-1150)\]
    -Boolean-like string columns being reformatted (TRUE/FALSE to
    True/False)
-   \[[SYNPY-1158](https://sagebionetworks.jira.com/browse/SYNPY-1158)\]
    -race condition in test_caching.py#test_threaded_access
-   \[[SYNPY-1159](https://sagebionetworks.jira.com/browse/SYNPY-1159)\]
    -logging in with an email address and an authToken gives spurious
    error
-   \[[SYNPY-1161](https://sagebionetworks.jira.com/browse/SYNPY-1161)\]
    -ChunkEncodingError encountered from external collaborator during a
    synapseclient download

### Improvements

-   \[[SYNPY-638](https://sagebionetworks.jira.com/browse/SYNPY-638)\]
    -add after date to cache purge
-   \[[SYNPY-929](https://sagebionetworks.jira.com/browse/SYNPY-929)\]
    -silent parameter for all functions which default to writing to
    stdout
-   \[[SYNPY-1068](https://sagebionetworks.jira.com/browse/SYNPY-1068)\]
    -Should show some progress indicator during upload md5 calculation
-   \[[SYNPY-1125](https://sagebionetworks.jira.com/browse/SYNPY-1125)\]
    -Allow login with environment variables
-   \[[SYNPY-1138](https://sagebionetworks.jira.com/browse/SYNPY-1138)\]
    -When using boto3 client to upload a file, also include ACL to give
    bucket owner full access

### Tasks

-   \[[SYNPY-948](https://sagebionetworks.jira.com/browse/SYNPY-948)\]
    -command line client set-annotations does not return proper error
    code when there's a problem
-   \[[SYNPY-1024](https://sagebionetworks.jira.com/browse/SYNPY-1024)\]
    -remove reference to deprecated 'status' field from Evaluation
-   \[[SYNPY-1143](https://sagebionetworks.jira.com/browse/SYNPY-1143)\]
    -indicate in CLI doc's that select statement requires double quotes

## 2.3.1 (2021-04-13)

### Highlights

-   Entities can be annotated with boolean datatypes, for example:

    ```
    file = synapseclient.File('/path/to/file', parentId='syn123', synapse_is_great=True)
    syn.store(file)
    ```

-   synapseclient is additionally packaged as a Python wheel.

### Bug Fixes

-   \[[SYNPY-829](https://sagebionetworks.jira.com/browse/SYNPY-829)\]
    -syn.store always updates annotations
-   \[[SYNPY-1033](https://sagebionetworks.jira.com/browse/SYNPY-1033)\]
    -If versionComment is left blank, previous version comment populates

### Improvements

-   \[[SYNPY-1120](https://sagebionetworks.jira.com/browse/SYNPY-1120)\]
    -Build wheel distributions
-   \[[SYNPY-1129](https://sagebionetworks.jira.com/browse/SYNPY-1129)\]
    -Support boolean annotations in Python client

## 2.3.0 (2021-03-03)

### Highlights

-   The
    [index_files_for_migration](synapseutils.html#synapseutils.migrate_functions.index_files_for_migration)
    and
    [migrate_indexed_files](synapseutils.html#synapseutils.migrate_functions.migrate_indexed_files)
    functions are added to synapseutils to help migrate files in Synapse
    projects and folders between AWS S3 buckets in the same region. More
    details on using these utilities can be found
    [here](S3Storage.html#storage-location-migration).

-   This version supports login programatically and from the command
    line using personal access tokens that can be obtained from your
    synapse.org Settings. Additional documentation on login and be found
    [here](Credentials.html).

    ```
    # programmatic
    syn = synapseclient.login(authToken=<token>)
    ```

    ```
    # command line
    synapse login -p <token>
    ```

-   The location where downloaded entities are cached can be customized
    to a location other than the user's home directory. This is useful
    in environments where writing to a home directory is not appropriate
    (e.g. an AWS lambda).

    ```
    syn = synapseclient.Synapse(cache_root_dir=<directory path>)
    ```

-   A [helper method](index.html#synapseclient.Synapse.is_certified) on
    the Synapse object has been added to enable obtaining the Synapse
    certification quiz status of a user.

    ```
    passed = syn.is_certified(<username or user_id>)
    ```

-   This version has been tested with Python 3.9.

### Bug Fixes

-   \[[SYNPY-1039](https://sagebionetworks.jira.com/browse/SYNPY-1039)\]
    -tableQuery asDataFrame() results with TYPE_LIST columns should be
    lists and not literal strings
-   \[[SYNPY-1109](https://sagebionetworks.jira.com/browse/SYNPY-1109)\]
    -unparseable synapse cacheMap raises JSONDecodeError
-   \[[SYNPY-1110](https://sagebionetworks.jira.com/browse/SYNPY-1110)\]
    -Cleanup on Windows console login
-   \[[SYNPY-1112](https://sagebionetworks.jira.com/browse/SYNPY-1112)\]
    -Concurrent migration of entities sharing the same file handle can
    result in an error
-   \[[SYNPY-1114](https://sagebionetworks.jira.com/browse/SYNPY-1114)\]
    -Mitigate new Rust compiler dependency on Linux via transitive
    cryptography dependency
-   \[[SYNPY-1118](https://sagebionetworks.jira.com/browse/SYNPY-1118)\]
    -Migration tool erroring when it shouldn't

### New Features

-   \[[SYNPY-1058](https://sagebionetworks.jira.com/browse/SYNPY-1058)\]
    -Accept oauth access token for authentication to use Synapse REST
    services
-   \[[SYNPY-1103](https://sagebionetworks.jira.com/browse/SYNPY-1103)\]
    -Multipart copy integration
-   \[[SYNPY-1111](https://sagebionetworks.jira.com/browse/SYNPY-1111)\]
    -Add function to get user certification status

### Improvements

-   \[[SYNPY-885](https://sagebionetworks.jira.com/browse/SYNPY-885)\]
    -Public interface to customize CACHE_ROOT_DIR
-   \[[SYNPY-1102](https://sagebionetworks.jira.com/browse/SYNPY-1102)\]
    -syncToSynapse adds empty annotation values
-   \[[SYNPY-1104](https://sagebionetworks.jira.com/browse/SYNPY-1104)\]
    -Python 3.9 support
-   \[[SYNPY-1119](https://sagebionetworks.jira.com/browse/SYNPY-1119)\]
    -Add source storage location option to storage migrate functions

## 2.2.2 (2020-10-18)

### Highlights

-   This version addresses an issue with downloads being retried
    unsuccessfully after encountering certain types of errors.
-   A
    [create_snapshot_version](index.html#synapseclient.Synapse.create_snapshot_version)
    function is added for making table and view snapshots.

### Bug Fixes

-   \[[SYNPY-1096](https://sagebionetworks.jira.com/browse/SYNPY-1096)\]
    -Fix link to Synapse on PyPI
-   \[[SYNPY-1097](https://sagebionetworks.jira.com/browse/SYNPY-1097)\]
    -downloaded files are reset when disk space exhausted

### New Features

-   \[[SYNPY-1041](https://sagebionetworks.jira.com/browse/SYNPY-1041)\]
    -Snapshot feature and programmatic clients

### Improvements

-   \[[SYNPY-1063](https://sagebionetworks.jira.com/browse/SYNPY-1063)\]
    -Consolidate builds to GitHub Actions
-   \[[SYNPY-1099](https://sagebionetworks.jira.com/browse/SYNPY-1099)\]
    -Replace usage of deprecated PUT /entity/{id}/version endpoint

## 2.2.0 (2020-08-31)

### Highlights

-   Files that are part of
    [syncFromSynapse](https://python-docs.synapse.org/build/html/synapseutils.html#synapseutils.sync.syncFromSynapse)
    and
    [syncToSynapse](https://python-docs.synapse.org/build/html/synapseutils.html#synapseutils.sync.syncToSynapse)
    operations (`synapse get -r` and `synapse sync` in the command line
    client, respectively) are transferred in in parallel threads rather
    than serially, substantially improving the performance of these
    operations.
-   Table metadata from [synapse get -q` is automatically
    downloaded to a users working directory instead of to the Synapse
    cache (a hidden folder).
-   Users can now pass their API key to [synapse login` in
    place of a password.

### Bug Fixes

-   \[[SYNPY-1082](https://sagebionetworks.jira.com/browse/SYNPY-1082)\]
    -Downloading entity linked to URL fails: module 'urllib.parse' has
    no attribute 'urlretrieve'

### Improvements

-   \[[SYNPY-1072](https://sagebionetworks.jira.com/browse/SYNPY-1072)\]
    -Improve throughput of multiple small file transfers
-   \[[SYNPY-1073](https://sagebionetworks.jira.com/browse/SYNPY-1073)\]
    -Parellelize upload syncs
-   \[[SYNPY-1074](https://sagebionetworks.jira.com/browse/SYNPY-1074)\]
    -Parallelize download syncs
-   \[[SYNPY-1084](https://sagebionetworks.jira.com/browse/SYNPY-1084)\]
    -Allow anonymous usage for public APIs like GET /teamMembers/{id}
-   \[[SYNPY-1088](https://sagebionetworks.jira.com/browse/SYNPY-1088)\]
    -Manifest is in cache with synapse get -q
-   \[[SYNPY-1090](https://sagebionetworks.jira.com/browse/SYNPY-1090)\]
    -Command line client does not support apikey

### Tasks

-   \[[SYNPY-1080](https://sagebionetworks.jira.com/browse/SYNPY-1080)\]
    -Remove Versionable from SchemaBase
-   \[[SYNPY-1085](https://sagebionetworks.jira.com/browse/SYNPY-1085)\]
    -Move to pytest testing framework
-   \[[SYNPY-1087](https://sagebionetworks.jira.com/browse/SYNPY-1087)\]
    -Improve synapseclient installation instructions

## 2.1.1 (2020-07-10)

### Highlights

-   This version includes a performance improvement for
    [syncFromSynapse](https://python-docs.synapse.org/build/html/synapseutils.html#synapseutils.sync.syncFromSynapse)
    downloads of deep folder hierarchies to local filesystem locations
    outside of the [Synapse
    cache](https://help.synapse.org/docs/Downloading-Data-Programmatically.2003796248.html#DownloadingDataProgrammatically-DownloadingFiles).

-   Support is added for **SubmissionViews** that can be used to query
    and edit a set of submissions through table services.

    ``` python
    from synapseclient import SubmissionViewSchema

    project = syn.get("syn123")
    evaluation_id = '9876543'
    view = syn.store(SubmissionViewSchema(name='My Submission View', parent=project, scopes=[evaluation_id]))
    view_table = syn.tableQuery(f"select * from {view.id}")
    ```

### Bug Fixes

-   \[[SYNPY-1075](https://sagebionetworks.jira.com/browse/SYNPY-1075)\]
    -Error in Python test (submission annotations)
-   \[[SYNPY-1076](https://sagebionetworks.jira.com/browse/SYNPY-1076)\]
    -Upgrade/fix Pandas dependency

### Improvements

-   \[[SYNPY-1070](https://sagebionetworks.jira.com/browse/SYNPY-1070)\]
    -Add support for submission views
-   \[[SYNPY-1078](https://sagebionetworks.jira.com/browse/SYNPY-1078)\]
    -Improve syncFromSynapse performance for large folder structures
    synced to external paths

## 2.1.0 (2020-06-16)

### Highlights

-   A `max_threads` property of the Synapse object has been added to
    customize the number of concurrent threads that will be used during
    file transfers.

    ``` python
    import synapseclient
    syn = synapseclient.login()
    syn.max_threads = 20
    ```

    If not customized the default value is (CPU count + 4). Adjusting
    this value higher may speed up file transfers if the local system
    resources can take advantage of the higher setting. Currently this
    value applies only to files whose underlying storage is AWS S3.

    Alternately, a value can be stored in the [synapseConfig
    configuration
    file](https://help.synapse.org/docs/Client-Configuration.1985446156.html)
    that will automatically apply as the default if a value is not
    explicitly set.

    ```
    [transfer]
    max_threads=16
    ```

-   This release includes support for directly accessing S3 storage
    locations using AWS Security Token Service credentials. This allows
    use of external AWS clients and libraries with Synapse storage, and
    can be used to accelerate file transfers under certain conditions.
    To create an STS enabled folder and set-up direct access to S3
    storage, see `here <sts_storage_locations>`{.interpreted-text
    role="ref"}.

-   The `getAnnotations` and `setAnnotations` methods of the Synapse
    object have been **deprecated** in favor of newer `get_annotations`
    and `set_annotations` methods, respectively. The newer versions are
    parameterized with a typed `Annotations` dictionary rather than a
    plain Python dictionary to prevent existing annotations from being
    accidentally overwritten. The expected usage for setting annotations
    is to first retrieve the existing `Annotations` for an entity before
    saving changes by passing back a modified value.

    ```
    annos = syn.get_annotations('syn123')

    # set key 'foo' to have value of 'bar' and 'baz'
    annos['foo'] = ['bar', 'baz']
    # single values will automatically be wrapped in a list once stored
    annos['qwerty'] = 'asdf'

    annos = syn.set_annotations(annos)
    ```

    The deprecated annotations methods may be removed in a future
    release.

A full list of issues addressed in this release are below.

### Bug Fixes

-   \[[SYNPY-913](https://sagebionetworks.jira.com/browse/SYNPY-913)\]
    -Travis Build badge for develop branch is pointing to pull request
-   \[[SYNPY-960](https://sagebionetworks.jira.com/browse/SYNPY-960)\]
    -AppVeyor build badge appears to be failed while the builds are
    passed
-   \[[SYNPY-1036](https://sagebionetworks.jira.com/browse/SYNPY-1036)\]
    -different users storing same file to same folder results in 403
-   \[[SYNPY-1056](https://sagebionetworks.jira.com/browse/SYNPY-1056)\]
    -syn.getSubmissions fails due to new Annotation class in v2.1.0-rc

### Improvements

-   \[[SYNPY-1036](https://sagebionetworks.jira.com/browse/SYNPY-1029)\]
    -Make upload speeds comparable to those of the AWS S3 CLI
-   \[[SYNPY-1049](https://sagebionetworks.jira.com/browse/SYNPY-1049)\]
    -Expose STS-related APIs

### Tasks

-   \[[SYNPY-1059](https://sagebionetworks.jira.com/browse/SYNPY-1059)\]
    -Use collections.abc instead of collections

## 2.0.0 (2020-03-23)

**Python 2 is no longer supported as of this release.** This release
requires Python 3.6+.

### Highlights:

-   Multi-threaded download of files from Synapse can be enabled by
    setting `syn.multi_threaded` to `True` on a `synapseclient.Synapse`
    object. This will become the default implementation in the future,
    but to ensure stability for the first release of this feature, it
    must be intentionally enabled.

    ``` python
    import synapseclient
    syn = synapseclient.login()
    syn.multi_threaded = True
    # syn123 now will be downloaded via the multi-threaded implementation
    syn.get("syn123")
    ```

    Currently, multi-threaded download only works with files stored in
    AWS S3, where most files on Synapse reside. This also includes
    [custom storage
    locations](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html)
    that point to an AWS S3 bucket. Files not stored in S3 will fall
    back to single-threaded download even if `syn.multi_threaded==True`.

-

    `synapseutils.copy()` now has limitations on what can be copied:

    :   -   A user must have download permissions on the entity they
            want to copy.
        -   Users cannot copy any entities that have [access
            requirements](https://help.synapse.org/docs/Sharing-Settings,-Permissions,-and-Conditions-for-Use.2024276030.html).

-   `contentTypes` and `fileNames` are optional parameters in
    `synapseutils.copyFileHandles()`

-   Synapse Docker Repository(`synapseclient.DockerRepository`) objects
    can now be submitted to Synapse evaluation queues using the `entity`
    argument in `synapseclient.Synapse.submit()`. An optional argument
    `docker_tag="latest"` has also been added to
    `synapseclient.Synapse.submit()`\" to designate which tagged Docker
    image to submit.

A full list of issues addressed in this release are below.

### Bugs Fixes

-   \[[SYNPY-271](https://sagebionetworks.jira.com/browse/SYNPY-271)\]
    -cache.remove fails to return the file handles we removed
-   \[[SYNPY-1032](https://sagebionetworks.jira.com/browse/SYNPY-1032)\]
    -   Support new columnTypes defined in backend

### Tasks

-   \[[SYNPY-999](https://sagebionetworks.jira.com/browse/SYNPY-999)\]
    -Remove unsafe copy functions from client
-   \[[SYNPY-1027](https://sagebionetworks.jira.com/browse/SYNPY-1027)\]
    \- Copy function should copy things when users are part of a Team
    that has DOWNLOAD access

### Improvements

-   \[[SYNPY-389](https://sagebionetworks.jira.com/browse/SYNPY-389)\]
    -submission of Docker repository
-   \[[SYNPY-537](https://sagebionetworks.jira.com/browse/SYNPY-537)\]
    -synapseutils.copyFileHandles requires fields that does not require
    at rest
-   \[[SYNPY-680](https://sagebionetworks.jira.com/browse/SYNPY-680)\]
    -synapseutils.changeFileMetaData() needs description in
    documentation
-   \[[SYNPY-682](https://sagebionetworks.jira.com/browse/SYNPY-682)\]
    -improve download speeds to be comparable to AWS
-   \[[SYNPY-807](https://sagebionetworks.jira.com/browse/SYNPY-807)\]
    -Drop support for Python 2
-   \[[SYNPY-907](https://sagebionetworks.jira.com/browse/SYNPY-907)\]
    -Replace \`from \<module\> import \...\` with \`import \<module\>\`
-   \[[SYNPY-962](https://sagebionetworks.jira.com/browse/SYNPY-962)\]
    -remove 'password' as an option in default synapse config file
-   \[[SYNPY-972](https://sagebionetworks.jira.com/browse/SYNPY-972)\]
    -Link on Synapse Python Client Documentation points back at itself

## 1.9.4 (2019-06-28)

### Bug Fixes

-   \[[SYNPY-881](https://sagebionetworks.jira.com/browse/SYNPY-881)\]
    -Synu.copy fails when copying a file with READ permissions
-   \[[SYNPY-888](https://sagebionetworks.jira.com/browse/SYNPY-888)\]
    -Docker repositories cannot be copied
-   \[[SYNPY-927](https://sagebionetworks.jira.com/browse/SYNPY-927)\]
    -trying to create a project with name that already exists hangs
-   \[[SYNPY-1005](https://sagebionetworks.jira.com/browse/SYNPY-1005)\]
    -   cli docs missing sub-commands
-   \[[SYNPY-1018](https://sagebionetworks.jira.com/browse/SYNPY-1018)\]
    -   Synu.copy shouldn't copy any files with access restrictions

### New Features

-   \[[SYNPY-851](https://sagebionetworks.jira.com/browse/SYNPY-851)\]
    -invite user or list of users to a team

### Improvements

-   \[[SYNPY-608](https://sagebionetworks.jira.com/browse/SYNPY-608)\]
    -Add how to contribute md to github project
-   \[[SYNPY-735](https://sagebionetworks.jira.com/browse/SYNPY-735)\]
    -command line for building a table
-   \[[SYNPY-864](https://sagebionetworks.jira.com/browse/SYNPY-864)\]
    -docstring for the command line client doesn't have complete list
    of sub-commands available
-   \[[SYNPY-926](https://sagebionetworks.jira.com/browse/SYNPY-926)\]
    -allow forceVersion false for command line client
-   \[[SYNPY-1013](https://sagebionetworks.jira.com/browse/SYNPY-1013)\]
    -   Documentation of \"store\" command for Synapse command line
        client
-   \[[SYNPY-1021](https://sagebionetworks.jira.com/browse/SYNPY-1021)\]
    -   change email contact for code of conduct

## 1.9.3 (2019-06-28)

### Bug Fixes

-   \[[SYNPY-993](https://sagebionetworks.jira.com/browse/SYNPY-993)\]
    -Fix [sendMessage` function
-   \[[SYNPY-989](https://sagebionetworks.jira.com/browse/SYNPY-989)\]
    -Fix unstable test

## 1.9.2 (2019-02-15)

In version 1.9.2, we improved Views' usability by exposing
[set_entity_types()` function to change the entity types
that will show up in a View:

    import synapseclient
    from synapseclient.table import EntityViewType

    syn = synapseclient.login()
    view = syn.get("syn12345")
    view.set_entity_types([EntityViewType.FILE, EntityViewType.FOLDER])
    view = syn.store(view)

### Features

-   \[[SYNPY-919](https://sagebionetworks.jira.com/browse/SYNPY-919)\]
    -Expose a way to update entity types in a view using EntityViewType

### Bug Fixes

-   \[[SYNPY-855](https://sagebionetworks.jira.com/browse/SYNPY-855)\]
    -Single thread uploading fails in Lambda python3.6 environment
-   \[[SYNPY-910](https://sagebionetworks.jira.com/browse/SYNPY-910)\]
    -Store Wiki shows deprecation warning
-   \[[SYNPY-920](https://sagebionetworks.jira.com/browse/SYNPY-920)\]
    -Project View turned into File View after using syndccutils template

### Tasks

-   \[[SYNPY-790](https://sagebionetworks.jira.com/browse/SYNPY-790)\]
    -Pin to a fixed version of the request package
-   \[[SYNPY-866](https://sagebionetworks.jira.com/browse/SYNPY-866)\]
    -Update Synapse logo in Python docs :)

### Improvements

-   \[[SYNPY-783](https://sagebionetworks.jira.com/browse/SYNPY-783)\]
    -typos in comments and in stdout
-   \[[SYNPY-916](https://sagebionetworks.jira.com/browse/SYNPY-916)\]
    -Wonky display on parameters
-   \[[SYNPY-917](https://sagebionetworks.jira.com/browse/SYNPY-917)\]
    -Add instructions on how to login with API key
-   \[[SYNPY-909](https://sagebionetworks.jira.com/browse/SYNPY-909)\]
    -Missing columnTypes in Column docstring

## 1.9.1 (2019-01-20)

In version 1.9.1, we fix various bugs and added two new features:

-   Python 3.7 is supported.
-   Deprecation warnings are visible by default.

### Features

-   \[[SYNPY-802](https://sagebionetworks.jira.com/browse/SYNPY-802)\]
    -Support Python 3.7
-   \[[SYNPY-849](https://sagebionetworks.jira.com/browse/SYNPY-849)\]
    -Add deprecation warning that isn't filtered by Python

### Bug Fixes

-   \[[SYNPY-454](https://sagebionetworks.jira.com/browse/SYNPY-454)\]
    -Some integration tests do not clean up after themselves
-   \[[SYNPY-456](https://sagebionetworks.jira.com/browse/SYNPY-456)\]
    -Problems with updated query system
-   \[[SYNPY-515](https://sagebionetworks.jira.com/browse/SYNPY-515)\]
    -sphinx documentation not showing for some new classes
-   \[[SYNPY-526](https://sagebionetworks.jira.com/browse/SYNPY-526)\]
    -deprecate downloadTableFile()
-   \[[SYNPY-578](https://sagebionetworks.jira.com/browse/SYNPY-578)\]
    -switch away from POST /entity/#/table/deleterows
-   \[[SYNPY-594](https://sagebionetworks.jira.com/browse/SYNPY-594)\]
    -Getting error from dev branch in integration test against staging
-   \[[SYNPY-796](https://sagebionetworks.jira.com/browse/SYNPY-796)\]
    -fix or remove PyPI downloads badge in readme
-   \[[SYNPY-799](https://sagebionetworks.jira.com/browse/SYNPY-799)\]
    -Unstable test: Test PartialRow updates to entity views from rowset
    queries
-   \[[SYNPY-846](https://sagebionetworks.jira.com/browse/SYNPY-846)\]
    -error if password stored in config file contains a '%'

### Tasks

-   \[[SYNPY-491](https://sagebionetworks.jira.com/browse/SYNPY-491)\]
    -Figure out custom release note fitlers
-   \[[SYNPY-840](https://sagebionetworks.jira.com/browse/SYNPY-840)\]
    -Install not working on latest python
-   \[[SYNPY-847](https://sagebionetworks.jira.com/browse/SYNPY-847)\]
    -uploadFileHandle should not be deprecated nor removed
-   \[[SYNPY-852](https://sagebionetworks.jira.com/browse/SYNPY-852)\]
    -Check and update help.synapse.org to reflect the change in the
    Python client
-   \[[SYNPY-860](https://sagebionetworks.jira.com/browse/SYNPY-860)\]
    -vignette for how to upload a new version of a file directly to a
    synapse entity
-   \[[SYNPY-863](https://sagebionetworks.jira.com/browse/SYNPY-863)\]
    -Update public documentation to move away from the query services
-   \[[SYNPY-866](https://sagebionetworks.jira.com/browse/SYNPY-866)\]
    -Update Synapse logo in Python docs :)
-   \[[SYNPY-873](https://sagebionetworks.jira.com/browse/SYNPY-873)\]
    -consolidate integration testing to platform dev account

### Improvements

-   \[[SYNPY-473](https://sagebionetworks.jira.com/browse/SYNPY-473)\]
    -Change syn.list to no longer use deprecated function chunkedQuery
-   \[[SYNPY-573](https://sagebionetworks.jira.com/browse/SYNPY-573)\]
    -synapse list command line shouldn't list the parent container
-   \[[SYNPY-581](https://sagebionetworks.jira.com/browse/SYNPY-581)\]
    -\<entity\>.annotations return object is inconsistent with
    getAnnotations()
-   \[[SYNPY-612](https://sagebionetworks.jira.com/browse/SYNPY-612)\]
    -Rename view_type to viewType in EntityViewSchema for consistency
-   \[[SYNPY-777](https://sagebionetworks.jira.com/browse/SYNPY-777)\]
    -Python client \_list still uses chunckedQuery and result seem out
    of date
-   \[[SYNPY-804](https://sagebionetworks.jira.com/browse/SYNPY-804)\]
    -Update styling in the python docs to more closely match the Docs
    site styling
-   \[[SYNPY-815](https://sagebionetworks.jira.com/browse/SYNPY-815)\]
    -Update the build to use test user instead of migrationAdmin
-   \[[SYNPY-848](https://sagebionetworks.jira.com/browse/SYNPY-848)\]
    -remove outdated link to confluence for command line query
-   \[[SYNPY-856](https://sagebionetworks.jira.com/browse/SYNPY-856)\]
    -build_table example in the docs does not look right
-   \[[SYNPY-858](https://sagebionetworks.jira.com/browse/SYNPY-858)\]
    -Write file view documentation in python client that is similar to
    synapser
-   \[[SYNPY-870](https://sagebionetworks.jira.com/browse/SYNPY-870)\]
    -Submitting to an evaluation queue can't accept team as int

## 1.9.0 (2018-09-28)

In version 1.9.0, we deprecated and removed `query()` and
`chunkedQuery()`. These functions used the old query
services which does not perform well. To query for entities filter by
annotations, please use `EntityViewSchema`.

We also deprecated the following functions and will remove them in
Synapse Python client version 2.0. In the `Activity` object:

-   `usedEntity()`
-   `usedURL()`

In the `Synapse` object:

-   `getEntity()`
-   `loadEntity()`
-   `createEntity()`
-   `updateEntity()`
-   `deleteEntity()`
-   `downloadEntity()`
-   `uploadFile()`
-   `uploadFileHandle()`
-   `uploadSynapseManagedFileHandle()`
-   `downloadTableFile()`

Please see our documentation for more details on how to migrate your
code away from these functions.

### Features

-   [SYNPY-806](https://sagebionetworks.jira.com/browse/SYNPY-806) -
    Support Folders and Tables in View

### Bug Fixes

-   [SYNPY-195](https://sagebionetworks.jira.com/browse/SYNPY-195) -
    Dangerous exception handling
-   [SYNPY-261](https://sagebionetworks.jira.com/browse/SYNPY-261) -
    error downloading data from synapse (python client)
-   [SYNPY-694](https://sagebionetworks.jira.com/browse/SYNPY-694) -
    Uninformative error in `copyWiki` function
-   [SYNPY-805](https://sagebionetworks.jira.com/browse/SYNPY-805) -
    Uninformative error when getting View that does not exist
-   [SYNPY-819](https://sagebionetworks.jira.com/browse/SYNPY-819) -
    command-line clients need to be updated to replace the EntityView
    'viewType' with 'viewTypeMask'

### Tasks

-   [SYNPY-759](https://sagebionetworks.jira.com/browse/SYNPY-759) -
    Look for all functions that are documented as \"Deprecated\" and
    apply the deprecation syntax
-   [SYNPY-812](https://sagebionetworks.jira.com/browse/SYNPY-812) - Add
    Github issue template
-   [SYNPY-824](https://sagebionetworks.jira.com/browse/SYNPY-824) -
    Remove the deprecated function query() and chunkedQuery()

### Improvements

-   [SYNPY-583](https://sagebionetworks.jira.com/browse/SYNPY-583) -
    Better error message for create Link object
-   [SYNPY-810](https://sagebionetworks.jira.com/browse/SYNPY-810) -
    simplify docs for deleting rows
-   [SYNPY-814](https://sagebionetworks.jira.com/browse/SYNPY-814) - fix
    docs links in python client \_\_init\_\_.py
-   [SYNPY-822](https://sagebionetworks.jira.com/browse/SYNPY-822) -
    Switch to use news.rst instead of multiple release notes files
-   [SYNPY-823](https://sagebionetworks.jira.com/browse/SYNPY-759) - Pin
    keyring to version 12.0.2 to use SecretStorage 2.x

## 1.8.2 (2018-08-17)

In this release, we have been performed some house-keeping on the code
base. The two major changes are:

> -   making `syn.move()` available to move an entity to a
>     new parent in Synapse. For example:
>
>         import synapseclient
>         from synapseclient import Folder
>
>         syn = synapseclient.login()
>
>         file = syn.get("syn123")
>         folder = Folder("new folder", parent="syn456")
>         folder = syn.store(folder)
>
>         # moving file to the newly created folder
>         syn.move(file, folder)
>
> -   exposing the ability to use the Synapse Python client with single
>     threaded. This feature is useful when running Python script in an
>     environment that does not support multi-threading. However, this
>     will negatively impact upload speed. To use single threaded:
>
>         import synapseclient
>         synapseclient.config.single_threaded = True

### Bug Fixes

-   [SYNPY-535](https://sagebionetworks.jira.com/browse/SYNPY-535) -
    Synapse Table update: Connection Reset
-   [SYNPY-603](https://sagebionetworks.jira.com/browse/SYNPY-603) -
    Python client and synapser cannot handle table column type LINK
-   [SYNPY-688](https://sagebionetworks.jira.com/browse/SYNPY-688) -
    Recursive get (sync) broken for empty folders.
-   [SYNPY-744](https://sagebionetworks.jira.com/browse/SYNPY-744) -
    KeyError when trying to download using Synapse Client 1.8.1
-   [SYNPY-750](https://sagebionetworks.jira.com/browse/SYNPY-750) -
    Error in downloadTableColumns for file view
-   [SYNPY-758](https://sagebionetworks.jira.com/browse/SYNPY-758) -
    docs in Sphinx don't show for synapseclient.table.RowSet
-   [SYNPY-760](https://sagebionetworks.jira.com/browse/SYNPY-760) -
    Keyring related error on Linux
-   [SYNPY-766](https://sagebionetworks.jira.com/browse/SYNPY-766) -
    as_table_columns() returns a list of columns out of order for python
    3.5 and 2.7
-   [SYNPY-776](https://sagebionetworks.jira.com/browse/SYNPY-776) -
    Cannot log in to Synapse - error(54, 'Connection reset by peer')
-   [SYNPY-795](https://sagebionetworks.jira.com/browse/SYNPY-795) - Not
    recognizable column in query result

### Features

-   [SYNPY-582](https://sagebionetworks.jira.com/browse/SYNPY-582) -
    move file or folder in the client
-   [SYNPY-788](https://sagebionetworks.jira.com/browse/SYNPY-788) - Add
    option to use syn.store() without exercising multithreads

### Tasks

-   [SYNPY-729](https://sagebionetworks.jira.com/browse/SYNPY-729) -
    Deprecate query() and chunkedQuery()
-   [SYNPY-797](https://sagebionetworks.jira.com/browse/SYNPY-797) -
    Check Python client code base on using PLFM object model
-   [SYNPY-798](https://sagebionetworks.jira.com/browse/SYNPY-798) -
    Using github.io to host documentation

### Improvements

-   [SYNPY-646](https://sagebionetworks.jira.com/browse/SYNPY-646) -
    Error output of synGet is non-informative
-   [SYNPY-743](https://sagebionetworks.jira.com/browse/SYNPY-743) -
    lint the entire python client code base

## 1.8.1 (2018-05-17)

This release is a hotfix for a bug. Please refer to 1.8.0 release notes
for information about additional changes.

### Bug Fixes

-   [SYNPY-706](https://sagebionetworks.jira.com/browse/SYNPY-706) -
    syn.username can cause attribute not found if user not logged in

## 1.8.0 (2018-05-07)

This release has 2 major changes:

-   The client will no longer store your saved credentials in your
    synapse cache (`\~/synapseCache/.session`). The python
    client now relies on [keyring](https://pypi.org/project/keyring/) to
    handle credential storage of your Synapse credentials.
-   The client also now uses connection pooling, which means that all
    method calls that connect to Synapse should now be faster.

The remaining changes are bug fixes and cleanup of test code.

Below are the full list of issues addressed by this release:

### Bug Fixes

-   [SYNPY-654](https://sagebionetworks.jira.com/browse/SYNPY-654) -
    syn.getColumns does not terminate
-   [SYNPY-658](https://sagebionetworks.jira.com/browse/SYNPY-658) -
    Security vunerability on clusters
-   [SYNPY-689](https://sagebionetworks.jira.com/browse/SYNPY-689) -
    Wiki's attachments cannot be None
-   [SYNPY-692](https://sagebionetworks.jira.com/browse/SYNPY-692) -
    synapseutils.sync.generateManifest() sets contentType incorrectly
-   [SYNPY-693](https://sagebionetworks.jira.com/browse/SYNPY-693) -
    synapseutils.sync.generateManifest() UnicodeEncodingError in python
    2

### Tasks

-   [SYNPY-617](https://sagebionetworks.jira.com/browse/SYNPY-617) -
    Remove use of deprecated service to delete table rows
-   [SYNPY-673](https://sagebionetworks.jira.com/browse/SYNPY-673) - Fix
    Integration Tests being run on Appveyor
-   [SYNPY-683](https://sagebionetworks.jira.com/browse/SYNPY-683) -
    Clean up print()s used in unit/integration tests

### Improvements

-   [SYNPY-408](https://sagebionetworks.jira.com/browse/SYNPY-408) - Add
    bettter error messages when /filehandle/batch fails.
-   [SYNPY-647](https://sagebionetworks.jira.com/browse/SYNPY-647) - Use
    connection pooling for Python client's requests

## 1.7.5 (2018-01-31)

v1.7.4 release was broken for new users that installed from pip. v1.7.5
has the same changes as v1.7.4 but fixes the pip installation.

## 1.7.4 (2018-01-29)

This release mostly includes bugfixes and improvements for various Table classes:

:   -   Fixed bug where you couldn't store a table converted to a
        `pandas.Dataframe` if it had a INTEGER column with
        some missing values.
    -   `EntityViewSchema` can now automatically add all
        annotations within your defined `scopes` as columns.
        Just set the view's `addAnnotationColumns=True`
        before calling `syn.store()`. This attribute
        defaults to `True` for all newly created
        `EntityViewSchemas`. Setting
        `addAnnotationColumns=True` on existing tables will
        only add annotation columns that are not already a part of your
        schema.
    -   You can now use `synapseutils.notifyMe` as a
        decorator to notify you by email when your function has
        completed. You will also be notified of any Errors if they are
        thrown while your function runs.

We also added some new features:

:   -   `syn.findEntityId()` function that allows you to
        find an Entity by its name and parentId, set parentId to
        `None` to search for Projects by name.
    -   The bulk upload functionality of
        `synapseutils.syncToSynapse` is available from the
        command line using: `synapse sync`.

Below are the full list of issues addressed by this release:

### Features

-   [SYNPY-506](https://sagebionetworks.jira.com/browse/SYNPY-506) -
    need convenience function for /entity/child
-   [SYNPY-517](https://sagebionetworks.jira.com/browse/SYNPY-517) -
    sync command line

### Improvements

-   [SYNPY-267](https://sagebionetworks.jira.com/browse/SYNPY-267) -
    Update Synapse tables for integer types
-   [SYNPY-304](https://sagebionetworks.jira.com/browse/SYNPY-304) -
    Table objects should implement len()
-   [SYNPY-416](https://sagebionetworks.jira.com/browse/SYNPY-416) -
    warning message for recursive get when a non-Project of Folder
    entity is passed
-   [SYNPY-482](https://sagebionetworks.jira.com/browse/SYNPY-482) -
    Create a sample synapseConfig if none is present
-   [SYNPY-489](https://sagebionetworks.jira.com/browse/SYNPY-489) - Add
    a boolean parameter in EntityViewSchema that will indicate whether
    the client should create columns based on annotations in the
    specified scopes
-   [SYNPY-494](https://sagebionetworks.jira.com/browse/SYNPY-494) -
    Link should be able to take an entity object as the parameter and
    derive its id
-   [SYNPY-511](https://sagebionetworks.jira.com/browse/SYNPY-511) -
    improve exception handling
-   [SYNPY-512](https://sagebionetworks.jira.com/browse/SYNPY-512) -
    Remove the use of PaginatedResult's totalNumberOfResult
-   [SYNPY-539](https://sagebionetworks.jira.com/browse/SYNPY-539) -
    When creating table Schemas, enforce a limit on the number of
    columns that can be created.

### Bug Fixes

-   [SYNPY-235](https://sagebionetworks.jira.com/browse/SYNPY-235) -
    can't print Row objects with dates in them
-   [SYNPY-272](https://sagebionetworks.jira.com/browse/SYNPY-272) - bug
    syn.storing rowsets containing Python datetime objects
-   [SYNPY-297](https://sagebionetworks.jira.com/browse/SYNPY-297) -
    as_table_columns shouldn't give fractional max size
-   [SYNPY-404](https://sagebionetworks.jira.com/browse/SYNPY-404) -
    when we get a SynapseMd5MismatchError we should delete the
    downloaded file
-   [SYNPY-425](https://sagebionetworks.jira.com/browse/SYNPY-425) -
    onweb doesn't work for tables
-   [SYNPY-438](https://sagebionetworks.jira.com/browse/SYNPY-438) -
    Need to change 'submit' not to use
    evaluation/id/accessRequirementUnfulfilled
-   [SYNPY-496](https://sagebionetworks.jira.com/browse/SYNPY-496) -
    monitor.NotifyMe can not be used as an annotation decorator
-   [SYNPY-521](https://sagebionetworks.jira.com/browse/SYNPY-521) -
    inconsistent error message when username/password is wrong on login
-   [SYNPY-536](https://sagebionetworks.jira.com/browse/SYNPY-536) -
    pre-signed upload URL expired warnings using Python client sync
    function
-   [SYNPY-555](https://sagebionetworks.jira.com/browse/SYNPY-555) -
    EntityViewSchema is missing from sphinx documentation
-   [SYNPY-558](https://sagebionetworks.jira.com/browse/SYNPY-558) -
    synapseutils.sync.syncFromSynapse throws error when syncing a Table
    object
-   [SYNPY-595](https://sagebionetworks.jira.com/browse/SYNPY-595) - Get
    recursive folders filled with Links fails
-   [SYNPY-605](https://sagebionetworks.jira.com/browse/SYNPY-605) -
    Update documentation for getUserProfile to include information about
    refreshing and memoization

### Tasks

-   [SYNPY-451](https://sagebionetworks.jira.com/browse/SYNPY-451) - Add
    limit and offset for accessApproval and accessRequirement API calls
    and remove 0x400 flag default when calling GET /entity/{id}/bundle
-   [SYNPY-546](https://sagebionetworks.jira.com/browse/SYNPY-546) -
    Change warning message when user does not DOWNLOAD permissions.

## 1.7.3 (2017-12-08)

Release 1.7.3 introduces fixes and quality of life changes to Tables and
synapseutils:

-   Changes to Tables:

    > -   You no longer have to include the `etag` column in
    >     your SQL query when using a `tableQuery()` to
    >     update File/Project Views. just `SELECT` the
    >     relevant columns and etags will be resolved automatically.
    > -   The new `PartialRowSet` class allows you to only
    >     have to upload changes to individual cells of a table instead
    >     of every row that had a value changed. It is recommended to
    >     use the `PartialRowSet.from_mapping()` classmethod
    >     instead of the `PartialRowSet` constructor.

-   Changes to synapseutils:

    > -   Improved documentation
    > -   You can now use `\~` to refer to your home
    >     directory in your manifest.tsv

We also added improved debug logging and use Python's builtin
`logging` module instead of printing directly to
`sys.stderr`

Below are the full list of issues addressed by this release:

### Bug Fixes

-   [SYNPY-419](https://sagebionetworks.jira.com/browse/SYNPY-419) -
    support object store from client
-   [SYNPY-499](https://sagebionetworks.jira.com/browse/SYNPY-499) -
    metadata manifest file name spelled wrong
-   [SYNPY-504](https://sagebionetworks.jira.com/browse/SYNPY-504) -
    downloadTableFile changed return type with no change in
    documentation or mention in release notes
-   [SYNPY-508](https://sagebionetworks.jira.com/browse/SYNPY-508) -
    syncToSynapse does not work if \"the file path in \"used\" or
    \"executed\" of the manifest.tsv uses home directory shortcut \"\~\"
-   [SYNPY-516](https://sagebionetworks.jira.com/browse/SYNPY-516) -
    synapse sync file does not work if file is a URL
-   [SYNPY-525](https://sagebionetworks.jira.com/browse/SYNPY-525) -
    Download CSV file of Synapse Table - 416 error
-   [SYNPY-572](https://sagebionetworks.jira.com/browse/SYNPY-572) -
    Users should only be prompted for updates if the first or second
    part of the version number is changed.

### Features

-   [SYNPY-450](https://sagebionetworks.jira.com/browse/SYNPY-450) -
    Create convenience functions for synapse project settings
-   [SYNPY-517](https://sagebionetworks.jira.com/browse/SYNPY-517) -
    sync command line
-   [SYNPY-519](https://sagebionetworks.jira.com/browse/SYNPY-519) -
    Clean up doc string for Sync
-   [SYNPY-545](https://sagebionetworks.jira.com/browse/SYNPY-545) - no
    module botocore
-   [SYNPY-577](https://sagebionetworks.jira.com/browse/SYNPY-577) -
    Expose new view etags in command line clients

### Tasks

-   [SYNPY-569](https://sagebionetworks.jira.com/browse/SYNPY-569) -
    'includeEntityEtag' should be True for Async table csv query
    downloads

### Improvements

-   [SYNPY-304](https://sagebionetworks.jira.com/browse/SYNPY-304) -
    Table objects should implement len()
-   [SYNPY-511](https://sagebionetworks.jira.com/browse/SYNPY-511) -
    improve exception handling
-   [SYNPY-518](https://sagebionetworks.jira.com/browse/SYNPY-518) -
    Clean up sync interface
-   [SYNPY-590](https://sagebionetworks.jira.com/browse/SYNPY-590) -
    Need better logging of errors that occur in the Python client.
-   [SYNPY-597](https://sagebionetworks.jira.com/browse/SYNPY-597) - Add
    ability to create PartialRowset updates

## 1.7.1 (2017-11-17)

Release 1.7 is a large bugfix release with several new features. The
main ones include:

-   We have expanded the [synapseutils
    packages](python-docs.synapse.org/build/html/synapseutils.html#module-synapseutils)
    to add the ability to:

    > -   Bulk upload files to synapse (synapseutils.syncToSynapse).
    > -   Notify you via email on the progress of a function (useful for
    >     jobs like large file uploads that may take a long time to
    >     complete).
    > -   The syncFromSynapse function now creates a \"manifest\" which
    >     contains the metadata of downloaded files. (These can also be
    >     used to update metadata with the bulk upload function.

-   File View tables can now be created from the python client using
    EntityViewSchema. See [fileviews
    documentation](https://help.synapse.org/docs/Views.2011070739.html).

-   The python client is now able to upload to user owned S3 Buckets.
    [Click here for instructions on linking your S3 bucket to
    synapse](https://help.synapse.org/docs/Custom-Storage-Locations.2048327803.html).

We've also made various improvements to existing features:

-   The LARGETEXT type is now supported in Tables allowing for strings
    up to 2Mb.
-   The `\--description` argument when creating/updating
    entities from the command line client will now create a
    `Wiki` for that entity. You can also use
    `\--descriptionFile` to write the contents of a markdown
    file as the entity's `Wiki`
-   Two member variables of the File object,
    `file_entity.cacheDir` and
    `file_entity.files` is being DEPRECATED in favor of
    `file_entity.path` for finding the location of a
    downloaded `File`
-   `pandas` `dataframe\`s containing
    \`datetime` values can now be properly converted into
    csv and uploaded to Synapse.

We also added a optional `convert_to_datetime` parameter to
`CsvFileTable.asDataFrame()` that will automatically convert
Synapse DATE columns into `datetime` objects instead of
leaving them as `long` unix timestamps

Below are the full list of bugs and issues addressed by this release:

### Features

-   [SYNPY-53](https://sagebionetworks.jira.com/browse/SYNPY-53) -
    support syn.get of external FTP links in py client
-   [SYNPY-179](https://sagebionetworks.jira.com/browse/SYNPY-179) -
    Upload to user owned S3 bucket
-   [SYNPY-412](https://sagebionetworks.jira.com/browse/SYNPY-412) -
    allow query-based download based on view tables from command line
    client
-   [SYNPY-487](https://sagebionetworks.jira.com/browse/SYNPY-487) - Add
    remote monitoring of long running processes
-   [SYNPY-415](https://sagebionetworks.jira.com/browse/SYNPY-415) - Add
    Docker and TableViews into Entity.py
-   [SYNPY-89](https://sagebionetworks.jira.com/browse/SYNPY-89) -
    Python client: Bulk upload client/command
-   [SYNPY-413](https://sagebionetworks.jira.com/browse/SYNPY-413) -
    Update table views via python client
-   [SYNPY-301](https://sagebionetworks.jira.com/browse/SYNPY-301) -
    change actual file name from python client
-   [SYNPY-442](https://sagebionetworks.jira.com/browse/SYNPY-442) - set
    config file path on command line

### Improvements

-   [SYNPY-407](https://sagebionetworks.jira.com/browse/SYNPY-407) -
    support LARGETEXT in tables
-   [SYNPY-360](https://sagebionetworks.jira.com/browse/SYNPY-360) -
    Duplicate file handles are removed from BulkFileDownloadRequest
-   [SYNPY-187](https://sagebionetworks.jira.com/browse/SYNPY-187) -
    Move \--description in command line client to create wikis
-   [SYNPY-224](https://sagebionetworks.jira.com/browse/SYNPY-224) -
    When uploading to a managed external file handle (e.g. SFTP), fill
    in storageLocationId
-   [SYNPY-315](https://sagebionetworks.jira.com/browse/SYNPY-315) -
    Default behavior for files in cache dir should be replace
-   [SYNPY-381](https://sagebionetworks.jira.com/browse/SYNPY-381) -
    Remove references to \"files\" and \"cacheDir\".
-   [SYNPY-396](https://sagebionetworks.jira.com/browse/SYNPY-396) -
    Create filehandle copies in synapseutils.copy instead of downloading
-   [SYNPY-403](https://sagebionetworks.jira.com/browse/SYNPY-403) - Use
    single endpoint for all downloads
-   [SYNPY-435](https://sagebionetworks.jira.com/browse/SYNPY-435) -
    Convenience function for new service to get entity's children
-   [SYNPY-471](https://sagebionetworks.jira.com/browse/SYNPY-471) -
    docs aren't generated for synapseutils
-   [SYNPY-472](https://sagebionetworks.jira.com/browse/SYNPY-472) -
    References to wrong doc site
-   [SYNPY-347](https://sagebionetworks.jira.com/browse/SYNPY-347) -
    Missing dtypes in table.DTYPE_2\_TABLETYPE
-   [SYNPY-463](https://sagebionetworks.jira.com/browse/SYNPY-463) -
    When copying filehandles we should add the files to the cache if we
    already donwloaded them
-   [SYNPY-475](https://sagebionetworks.jira.com/browse/SYNPY-475) -
    Store Tables timeout error

### Bug Fixes

-   [SYNPY-190](https://sagebionetworks.jira.com/browse/SYNPY-190) -
    syn.login('asdfasdfasdf') should fail
-   [SYNPY-344](https://sagebionetworks.jira.com/browse/SYNPY-344) -
    weird cache directories
-   [SYNPY-346](https://sagebionetworks.jira.com/browse/SYNPY-346) -
    ValueError: cannot insert ROW_ID, already exists in CsvTableFile
    constructor
-   [SYNPY-351](https://sagebionetworks.jira.com/browse/SYNPY-351) -
    Versioning broken for sftp files
-   [SYNPY-366](https://sagebionetworks.jira.com/browse/SYNPY-366) -
    file URLs leads to wrong path
-   [SYNPY-393](https://sagebionetworks.jira.com/browse/SYNPY-393) - New
    cacheDir causes cache to be ignored(?)
-   [SYNPY-409](https://sagebionetworks.jira.com/browse/SYNPY-409) -
    Python client cannot depend on parsing Amazon pre-signed URLs
-   [SYNPY-418](https://sagebionetworks.jira.com/browse/SYNPY-418) -
    Integration test failure against 167
-   [SYNPY-421](https://sagebionetworks.jira.com/browse/SYNPY-421) -
    syn.getWikiHeaders has a return limit of 50 (Need to return all
    headers)
-   [SYNPY-423](https://sagebionetworks.jira.com/browse/SYNPY-423) -
    upload rate is off or incorrect
-   [SYNPY-424](https://sagebionetworks.jira.com/browse/SYNPY-424) -
    File entities don't handle local_state correctly for setting
    datafilehandleid
-   [SYNPY-426](https://sagebionetworks.jira.com/browse/SYNPY-426) -
    multiple tests failing because of filenameOveride
-   [SYNPY-427](https://sagebionetworks.jira.com/browse/SYNPY-427) -
    test dependent on config file
-   [SYNPY-428](https://sagebionetworks.jira.com/browse/SYNPY-428) -
    sync function error
-   [SYNPY-431](https://sagebionetworks.jira.com/browse/SYNPY-431) -
    download ending early and not restarting from previous spot
-   [SYNPY-443](https://sagebionetworks.jira.com/browse/SYNPY-443) -
    tests/integration/integration_test_Entity.py:test_get_with_downloadLocation_and_ifcollision
    AssertionError
-   [SYNPY-461](https://sagebionetworks.jira.com/browse/SYNPY-461) - On
    Windows, command line client login credential prompt fails (python
    2.7)
-   [SYNPY-465](https://sagebionetworks.jira.com/browse/SYNPY-465) -
    Update tests that set permissions to also include 'DOWNLOAD'
    permission and tests that test functions using queries
-   [SYNPY-468](https://sagebionetworks.jira.com/browse/SYNPY-468) -
    Command line client incompatible with cache changes
-   [SYNPY-470](https://sagebionetworks.jira.com/browse/SYNPY-470) -
    default should be read, download for setPermissions
-   [SYNPY-483](https://sagebionetworks.jira.com/browse/SYNPY-483) -
    integration test fails for most users
-   [SYNPY-484](https://sagebionetworks.jira.com/browse/SYNPY-484) - URL
    expires after retries
-   [SYNPY-486](https://sagebionetworks.jira.com/browse/SYNPY-486) -
    Error in integration tests
-   [SYNPY-488](https://sagebionetworks.jira.com/browse/SYNPY-488) -
    sync tests for command line client puts file in working directory
-   [SYNPY-142](https://sagebionetworks.jira.com/browse/SYNPY-142) - PY:
    Error in login with rememberMe=True
-   [SYNPY-464](https://sagebionetworks.jira.com/browse/SYNPY-464) -
    synapse get syn4988808 KeyError: u'preSignedURL'

### Tasks

-   [SYNPY-422](https://sagebionetworks.jira.com/browse/SYNPY-422) -
    reduce default page size for GET
    /evaluation/{evalId}/submission/bundle/all
-   [SYNPY-437](https://sagebionetworks.jira.com/browse/SYNPY-437) -
    Remove tests for access restrictions on evaluations
-   [SYNPY-402](https://sagebionetworks.jira.com/browse/SYNPY-402) - Add
    release notes to Github release tag

## 1.6.1 (2016-11-02)

### What is New

In version 1.6 we introduce a new sub-module \_[synapseutils]() that
provide convenience functions for more complicated operations in Synapse
such as copying of files wikis and folders. In addition we have
introduced several improvements in downloading content from Synapse. As
with uploads we are now able to recover from an interrupted download and
will retry on network failures.

-   [SYNPY-48](https://sagebionetworks.jira.com/browse/SYNPY-48) -
    Automate build and test of Python client on Python 3.x
-   [SYNPY-180](https://sagebionetworks.jira.com/browse/SYNPY-180) -
    Pass upload destination in chunked file upload
-   [SYNPY-349](https://sagebionetworks.jira.com/browse/SYNPY-349) -
    Link Class
-   [SYNPY-350](https://sagebionetworks.jira.com/browse/SYNPY-350) -
    Copy Function
-   [SYNPY-370](https://sagebionetworks.jira.com/browse/SYNPY-370) -
    Building to new doc site for Synapse
-   [SYNPY-371](https://sagebionetworks.jira.com/browse/SYNPY-371) -
    Support paths in syn.onweb

### Improvements

We have improved download robustness and error checking, along with
extensive recovery on failed operations. This includes the ability for
the client to pause operation when Synapse is updated.

-   [SYNPY-270](https://sagebionetworks.jira.com/browse/SYNPY-270) -
    Synapse READ ONLY mode should cause pause in execution
-   [SYNPY-308](https://sagebionetworks.jira.com/browse/SYNPY-308) - Add
    md5 checking after downloading a new file handle
-   [SYNPY-309](https://sagebionetworks.jira.com/browse/SYNPY-309) - Add
    download recovery by using the 'Range': 'bytes=xxx-xxx' header
-   [SYNPY-353](https://sagebionetworks.jira.com/browse/SYNPY-353) -
    Speed up downloads of fast connections
-   [SYNPY-356](https://sagebionetworks.jira.com/browse/SYNPY-356) - Add
    support for version flag in synapse cat command line
-   [SYNPY-357](https://sagebionetworks.jira.com/browse/SYNPY-357) -
    Remove failure message on retry in multipart_upload
-   [SYNPY-380](https://sagebionetworks.jira.com/browse/SYNPY-380) - Add
    speed meter to downloads/uploads
-   [SYNPY-387](https://sagebionetworks.jira.com/browse/SYNPY-387) - Do
    exponential backoff on 429 status and print explanatory error
    message from server
-   [SYNPY-390](https://sagebionetworks.jira.com/browse/SYNPY-390) -
    Move recursive download to Python client utils

### Bug Fixes

-   [SYNPY-154](https://sagebionetworks.jira.com/browse/SYNPY-154) - 500
    Server Error when storing new version of file from command line
-   [SYNPY-168](https://sagebionetworks.jira.com/browse/SYNPY-168) -
    Failure on login gives an ugly error message
-   [SYNPY-253](https://sagebionetworks.jira.com/browse/SYNPY-253) -
    Error messages on upload retry inconsistent with behavior
-   [SYNPY-261](https://sagebionetworks.jira.com/browse/SYNPY-261) -
    error downloading data from synapse (python client)
-   [SYNPY-274](https://sagebionetworks.jira.com/browse/SYNPY-274) -
    Trying to use the client without logging in needs to give a
    reasonable error
-   [SYNPY-331](https://sagebionetworks.jira.com/browse/SYNPY-331) -
    test_command_get_recursive_and_query occasionally fails
-   [SYNPY-337](https://sagebionetworks.jira.com/browse/SYNPY-337) -
    Download error on 10 Gb file.
-   [SYNPY-343](https://sagebionetworks.jira.com/browse/SYNPY-343) -
    Login failure
-   [SYNPY-351](https://sagebionetworks.jira.com/browse/SYNPY-351) -
    Versioning broken for sftp files
-   [SYNPY-352](https://sagebionetworks.jira.com/browse/SYNPY-352) -
    file upload max retries exceeded messages
-   [SYNPY-358](https://sagebionetworks.jira.com/browse/SYNPY-358) -
    upload failure from python client (threading)
-   [SYNPY-361](https://sagebionetworks.jira.com/browse/SYNPY-361) -
    file download fails midway without warning/error
-   [SYNPY-362](https://sagebionetworks.jira.com/browse/SYNPY-362) -
    setAnnotations bug when given synapse ID
-   [SYNPY-363](https://sagebionetworks.jira.com/browse/SYNPY-363) -
    problems using provenance during upload
-   [SYNPY-382](https://sagebionetworks.jira.com/browse/SYNPY-382) -
    Python client is truncating the entity id in download csv from table
-   [SYNPY-383](https://sagebionetworks.jira.com/browse/SYNPY-383) -
    Travis failing with paramiko.ssh_exception.SSHException: No hostkey
-   [SYNPY-384](https://sagebionetworks.jira.com/browse/SYNPY-384) -
    resuming a download after a ChunkedEncodingError created new file
    with correct size
-   [SYNPY-388](https://sagebionetworks.jira.com/browse/SYNPY-388) -
    Asynchronous creation of Team causes sporadic test failure
-   [SYNPY-391](https://sagebionetworks.jira.com/browse/SYNPY-391) -
    downloadTableColumns() function doesn't work when resultsAs=rowset
    is set for for syn.tableQuery()
-   [SYNPY-397](https://sagebionetworks.jira.com/browse/SYNPY-397) -
    Error in syncFromSynapse() integration test on Windows
-   [SYNPY-399](https://sagebionetworks.jira.com/browse/SYNPY-399) -
    python client not compatible with newly released Pandas 0.19
