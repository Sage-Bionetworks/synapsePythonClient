# Command Line Client

The Synapse Python Client can be used from the command line via the `synapse` command.

> **Note:** The command line client is installed along with Installation of the Synapse Python client.

## Usage

For help, type:

```bash
synapse -h
```

For help on specific commands, type:

```bash
synapse [command] -h
```

Test your login credentials with an auth token environment variable:
<!-- termynal -->
```
> synapse login -p $MY_SYNAPSE_TOKEN
Welcome, First Last!

Logged in as: username (1234567)
```

The usage is as follows:

```bash
synapse [-h] [--version] [-u SYNAPSEUSER] [-p SYNAPSE_AUTH_TOKEN] [-c CONFIGPATH] [--debug] [--silent] [-s]
        [--otel {console,otlp}]
        {get,manifest,sync,store,add,mv,cp,get-download-list,associate,delete,query,submit,show,cat,list,config,set-provenance,get-provenance,set-annotations,get-annotations,create,store-table,onweb,login,test-encoding,get-sts-token,migrate}
        ...
```

## Options

| Name                | Type    | Description                                                               | Default               |
|---------------------|---------|---------------------------------------------------------------------------|-----------------------|
| `--version`         | Flag    | Show program’s version number and exit                                    |                       |
| `-u, --username`    | Option  | Username used to connect to Synapse                                       |                       |
| `-p, --password`    | Option  | Auth Token used to connect to Synapse                                     |                       |
| `-c, --configPath`  | Option  | Path to configuration file used to connect to Synapse                     | “~/.synapseConfig”    |
| `--debug`           | Flag    | Set to debug mode, additional output and error messages are printed to the console | False             |
| `--silent`          | Flag    | Set to silent mode, console output is suppressed                          | False                 |
| `-s, --skip-checks` | Flag    | Suppress checking for version upgrade messages and endpoint redirection   | False                 |
| `--otel`            | Option  | Enable the usage of OpenTelemetry for tracing. Possible choices: console, otlp |                     |

## Subcommands

- [get](#get): downloads a file from Synapse
- [manifest](#manifest): Generate manifest for uploading directory tree to Synapse.
- [sync](#sync): Synchronize files described in a manifest to Synapse
- [store](#store): uploads and adds a file to Synapse
- [add](#add): uploads and adds a file to Synapse
- [mv](#mv): Moves a file/folder in Synapse
- [cp](#cp): Copies specific versions of synapse content such as files, folders and projects by recursively copying all sub-content
- [get-download-list](#get-download-list): Download files from the Synapse download cart
- [associate](#associate): Associate local files with the files stored in Synapse so that calls to “synapse get” and “synapse show” don’t re-download the files but use the already existing file.
- [delete](#delete): removes a dataset from Synapse
- [query](#query): Performs SQL like queries on Synapse
- [submit](#submit): submit an entity or a file for evaluation
- [show](#show): show metadata for an entity
- [cat](#cat): prints a dataset from Synapse
- [list](#list): List Synapse entities contained by the given Project or Folder. Note: May not be supported in future versions of the client.
- [config](#config): Create or modify a Synapse configuration file
- [set-provenance](#set-provenance): create provenance records
- [get-provenance](#get-provenance): show provenance records
- [set-annotations](#set-annotations): create annotations records
- [get-annotations](#get-annotations): show annotations records
- [create](#create): Creates folders or projects on Synapse
- [store-table](#store-table): Creates a Synapse Table given a csv
- [onweb](#onweb): opens Synapse website for Entity
- [login](#login): Verify credentials can be used to login to Synapse.
        This does not need to be used prior to executing other commands.
- [test-encoding](#test-encoding): test character encoding to help diagnose problems
- [get-sts-token](#get-sts-token): Get an STS token for access to AWS S3 storage underlying Synapse
- [migrate](#migrate): Migrate Synapse entities to a different storage location



### `get`

```bash
synapse get [-h] [-q queryString] [-v VERSION] [-r] [--followLink] [--limitSearch projId] [--downloadLocation path]
            [--multiThreaded] [--manifest {all,root,suppress}]
            [local path]
```

| Name                | Type    | Description                                                               | Default               |
|---------------------|---------|---------------------------------------------------------------------------|-----------------------|
| `local path`        | Positional | Synapse ID of form syn123 of desired data object.                         |                       |
| `-q, --query`       | Named  | Optional query parameter, will fetch all of the entities returned by a query. |                       |
| `-v, --version`     | Named  | Synapse version number of entity to retrieve.                             | Most recent version   |
| `-r, --recursive`   | Named  | Fetches content in Synapse recursively contained in the parentId specified by id. | False             |
| `--followLink`      | Named  | Determines whether the link returns the target Entity.                    | False                 |
| `--limitSearch`     | Named  | Synapse ID of a container such as project or folder to limit search for files if using a path. |                       |
| `--downloadLocation`| Named  | Directory to download file to.                                            | “./”                  |
| `--multiThreaded`   | Named  | Download file using a multiple threaded implementation.                   | True                  |
| `--manifest`        | Named  | Determines whether creating manifest file automatically.                  | “all”                 |


### `manifest`

Generate manifest for uploading directory tree to Synapse.

```bash
synapse manifest [-h] --parent-id syn123 [--manifest-file OUTPUT] PATH
```

| Name                | Type    | Description                                                               | Default               |
|---------------------|---------|---------------------------------------------------------------------------|-----------------------|
| `PATH`              | Positional | A path to a file or folder whose manifest will be generated.              |                       |
| `--parent-id`       | Named  | Synapse ID of project or folder where to upload data.                     |                       |
| `--manifest-file`   | Named  | A TSV output file path where the generated manifest is stored.            | stdout                |


### `sync`

Synchronize files described in a manifest to Synapse.

```bash
synapse sync [-h] [--dryRun] [--sendMessages] [--retries INT] FILE
```

| Name                | Type    | Description                                                               | Default               |
|---------------------|---------|---------------------------------------------------------------------------|-----------------------|
| `FILE`              | Positional | A tsv file with file locations and metadata to be pushed to Synapse. See [synapseutils.sync.syncToSynapse][] for details on the format of a manifest. |                       |
| `--dryRun`          | Named  | Perform validation without uploading.                                     | False                 |
| `--sendMessages`    | Named  | Send notifications via Synapse messaging (email) at specific intervals, on errors and on completion. | False                 |
| `--retries`         | Named  | Number of retries for failed uploads.                                     | 4                     |


### `store`

Uploads and adds a file to Synapse.

```bash
synapse store [-h] (--parentid syn123 | --id syn123 | --type TYPE) [--name NAME]
              [--description DESCRIPTION | --descriptionFile DESCRIPTION_FILE_PATH] [--used [target [target ...]]]
              [--executed [target [target ...]]] [--limitSearch projId] [--noForceVersion] [--annotations ANNOTATIONS]
              [--replace]
              [FILE]
```

| Name                | Type    | Description                                                               | Default               |
|---------------------|---------|---------------------------------------------------------------------------|-----------------------|
| `FILE`              | Positional | File to be added to synapse.                                              |                       |
| `--parentid, --parentId` | Named  | Synapse ID of project or folder where to upload data (must be specified if –id is not used). |                       |
| `--id`              | Named  | Optional Id of entity in Synapse to be updated.                           |                       |
| `--type`            | Named  | Type of object, such as “File”, “Folder”, or “Project”, to create in Synapse. | “File”               |
| `--name`            | Named  | Name of data object in Synapse.                                           |                       |
| `--description`     | Named  | Description of data object in Synapse.                                    |                       |
| `--descriptionFile` | Named  | Path to a markdown file containing description of project/folder.          |                       |
| `--used`            | Named  | Synapse ID, a url, or a local file path (of a file previously uploaded to Synapse) from which the specified entity is derived. |                       |
| `--executed`        | Named  | Synapse ID, a url, or a local file path (of a file previously uploaded to Synapse) that was executed to generate the specified entity. |                       |
| `--limitSearch`     | Named  | Synapse ID of a container such as project or folder to limit search for provenance files. |                       |
| `--noForceVersion`  | Named  | Do not force a new version to be created if the contents of the file have not changed. | False                 |
| `--annotations`     | Named  | Annotations to add as a JSON formatted string, should evaluate to a dictionary (key/value pairs). Example: ‘{“foo”: 1, “bar”:”quux”}’ |                       |
| `--replace`         | Named  | Replace all existing annotations with the given annotations.                | False                 |


### `add`

Uploads and adds a file to Synapse.

```bash
synapse add [-h] (--parentid syn123 | --id syn123 | --type TYPE) [--name NAME]
            [--description DESCRIPTION | --descriptionFile DESCRIPTION_FILE_PATH] [--used [target [target ...]]]
            [--executed [target [target ...]]] [--limitSearch projId] [--noForceVersion] [--annotations ANNOTATIONS] [--replace]
            [FILE]
```

| Name                | Type    | Description                                                               | Default               |
|---------------------|---------|---------------------------------------------------------------------------|-----------------------|
| `FILE`              | Positional | File to be added to synapse.                                              |                       |
| `--parentid, --parentId` | Named  | Synapse ID of project or folder where to upload data (must be specified if –id is not used). |                       |
| `--id`              | Named  | Optional Id of entity in Synapse to be updated.                           |                       |
| `--type`            | Named  | Type of object, such as “File”, “Folder”, or “Project”, to create in Synapse. | “File”               |
| `--name`            | Named  | Name of data object in Synapse.                                           |                       |
| `--description`     | Named  | Description of data object in Synapse.                                    |                       |
| `--descriptionFile` | Named  | Path to a markdown file containing description of project/folder.          |                       |
| `--used`            | Named  | Synapse ID, a url, or a local file path (of a file previously uploaded to Synapse) from which the specified entity is derived. |                       |
| `--executed`        | Named  | Synapse ID, a url, or a local file path (of a file previously uploaded to Synapse) that was executed to generate the specified entity. |                       |
| `--limitSearch`     | Named  | Synapse ID of a container such as project or folder to limit search for provenance files. |                       |
| `--noForceVersion`  | Named  | Do not force a new version to be created if the contents of the file have not changed. | False                 |
| `--annotations`     | Named  | Annotations to add as a JSON formatted string, should evaluate to a dictionary (key/value pairs). Example: ‘{“foo”: 1, “bar”:”quux”}’ |                       |
| `--replace`         | Named  | Replace all existing annotations with the given annotations.                | False                 |

### `mv`

Moves a file/folder in Synapse.

```bash
synapse mv [-h] --id syn123 --parentid syn123
```

| Name                | Type    | Description                                                               |
|---------------------|---------|---------------------------------------------------------------------------|
| `--id`              | Named  | Id of entity in Synapse to be moved.                                      |
| `--parentid, --parentId` | Named  | Synapse ID of project or folder where file/folder will be moved.          |


### `cp`

Copies specific versions of synapse content such as files, folders and projects by recursively copying all sub-content.

```bash
synapse cp [-h] --destinationId syn123 [--version 1] [--setProvenance traceback] [--updateExisting] [--skipCopyAnnotations]
           [--excludeTypes [file table [file table ...]]] [--skipCopyWiki]
           syn123
```

| Name                | Type    | Description                                                               | Default               |
|---------------------|---------|---------------------------------------------------------------------------|-----------------------|
| `syn123`            | Positional | Id of entity in Synapse to be copied.                                     |                       |
| `--destinationId`   | Named  | Synapse ID of project or folder where file will be copied to.              |                       |
| `--version, -v`     | Named  | Synapse version number of File or Link to retrieve. This parameter cannot be used when copying Projects or Folders. Defaults to most recent version. | Most recent version |
| `--setProvenance`   | Named  | Has three values to set the provenance of the copied entity-traceback: Sets to the source entityexisting: Sets to source entity’s original provenance (if it exists)None/none: No provenance is set | "traceback"         |
| `--updateExisting`  | Named  | Will update the file if there is already a file that is named the same in the destination | False                 |
| `--skipCopyAnnotations` | Named  | Do not copy the annotations                                               | False                 |
| `--excludeTypes`    | Named  | Accepts a list of entity types (file, table, link) which determines which entity types to not copy. | []                    |
| `--skipCopyWiki`    | Named  | Do not copy the wiki pages                                                 | False                 |

### `get-download-list`

Download files from the Synapse download cart.

```bash
synapse get-download-list [-h] [--downloadLocation path]
```

| Name                | Type    | Description                                                               | Default               |
|---------------------|---------|---------------------------------------------------------------------------|-----------------------|
| `--downloadLocation`| Named  | Directory to download file to.                                            | "./"                  |

### `associate`

Associate local files with the files stored in Synapse so that calls to “synapse get” and “synapse show” don’t re-download the files but use the already existing file.

```bash
synapse associate [-h] [--limitSearch projId] [-r] path
```

| Name                | Type    | Description                                                               | Default               |
|---------------------|---------|---------------------------------------------------------------------------|-----------------------|
| `path`              | Positional | Local file path.                                                          |                       |
| `--limitSearch`     | Named  | Synapse ID of a container such as project or folder to limit search to.    |                       |
| `-r`                | Named  | Perform recursive association with all local files in a folder.            | False                 |


### `delete`

Removes a dataset from Synapse.

```bash
synapse delete [-h] [--version VERSION] syn123
```

| Name                | Type    | Description                                                               |
|---------------------|---------|---------------------------------------------------------------------------|
| `syn123`            | Positional | Synapse ID of form syn123 of desired data object.                         |
| `--version`         | Named  | Version number to delete of given entity.                                 |


### `query`

Performs SQL like queries on Synapse.

```bash
synapse query [-h] [string [string ...]]
```

| Name                | Type    | Description                                                               |
|---------------------|---------|---------------------------------------------------------------------------|
| `string`            | Positional | A query string. Note that when using the command line query strings must be passed intact as a single string. In most shells this can mean wrapping the query in quotes as appropriate and escaping any quotes that may appear within the query string itself. Example: `synapse query "select \"column has spaces\" from syn123"`. See [Table Examples](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/web/controller/TableExamples.html) for more information. |


### `submit`

Submit an entity or a file for evaluation.

```bash
synapse submit [-h] [--evaluationID EVALUATIONID] [--evaluationName EVALUATIONNAME] [--entity ENTITY] [--file FILE]
               [--parentId PARENTID] [--name NAME] [--teamName TEAMNAME] [--submitterAlias ALIAS] [--used [target [target ...]]]
               [--executed [target [target ...]]] [--limitSearch projId]
```

| Name                | Type    | Description                                                               |
|---------------------|---------|---------------------------------------------------------------------------|
| `--evaluationID, --evaluationId, --evalID` | Named  | Evaluation ID where the entity/file will be submitted.                    |
| `--evaluationName, --evalN` | Named  | Evaluation Name where the entity/file will be submitted.                  |
| `--entity, --eid, --entityId, --id` | Named  | Synapse ID of the entity to be submitted.                                 |
| `--file, -f` | Named  | File to be submitted to the challenge.                                    |
| `--parentId, --parentid, --parent` | Named  | Synapse ID of project or folder where to upload data.                     |
| `--name` | Named  | Name of the submission.                                                    |
| `--teamName, --team` | Named  | Submit on behalf of a registered team.                                    |
| `--submitterAlias, --alias` | Named  | A nickname, possibly for display in leaderboards.                         |
| `--used` | Named  | Synapse ID, a url, or a local file path (of a file previously uploaded to Synapse) from which the specified entity is derived. |
| `--executed` | Named  | Synapse ID, a url, or a local file path (of a file previously uploaded to Synapse) that was executed to generate the specified entity. |
| `--limitSearch` | Named  | Synapse ID of a container such as project or folder to limit search for provenance files. |

### `show`

Show metadata for an entity.

```bash
synapse show [-h] [--limitSearch projId] syn123
```

| Name                | Type    | Description                                                               |
|---------------------|---------|---------------------------------------------------------------------------|
| `syn123`            | Positional | Synapse ID of form syn123 of desired synapse object.                      |
| `--limitSearch`     | Named  | Synapse ID of a container such as project or folder to limit search for provenance files. |


### `cat`

Prints a dataset from Synapse.

```bash
synapse cat [-h] [-v VERSION] syn123
```

| Name                | Type    | Description                                                               | Default               |
|---------------------|---------|---------------------------------------------------------------------------|-----------------------|
| `syn123`            | Positional | Synapse ID of form syn123 of desired data object.                         |                       |
| `-v, --version`     | Named  | Synapse version number of entity to display.                              | Most recent version   |


### `list`

List Synapse entities contained by the given Project or Folder. Note: May not be supported in future versions of the client.

```bash
synapse list [-h] [-r] [-l] [-m] syn123
```

| Name                | Type    | Description                                                               | Default               |
|---------------------|---------|---------------------------------------------------------------------------|-----------------------|
| `syn123`            | Positional | Synapse ID of a project or folder.                                        |                       |
| `-r, --recursive`   | Named  | Recursively list contents of the subtree descending from the given Synapse ID. | False                 |
| `-l, --long`        | Named  | List synapse entities in long format.                                     | False                 |
| `-m, --modified`    | Named  | List modified by and modified date.                                       | False                 |


### `config`

Create or modify a Synapse configuration file. This command interactively prompts for a username and a Synapse Personal Access Token (Auth Token) and writes them to the ~/.synapseConfig file.
Supports multiple profiles.

```bash
synapse config [-h] [--profile PROFILE_NAME]
```

| Name        | Type    | Description                                                               |
|-------------|---------|---------------------------------------------------------------------------|
| `-h`        | Named  | Show the help message and exit.                                           |
| `--profile` | Named  | Optional name of the Synapse profile to create or update in the config file. <br/>If omitted, modifies the default [authentication] section.                                          |

### `set-provenance`

Create provenance records.

```bash
synapse set-provenance [-h] --id syn123 [--name NAME] [--description DESCRIPTION] [-o [OUTPUT_FILE]]
                       [--used [target [target ...]]] [--executed [target [target ...]]] [--limitSearch projId]
```

| Name                | Type    | Description                                                               |
|---------------------|---------|---------------------------------------------------------------------------|
| `--id`              | Named  | Synapse ID of entity whose provenance we are accessing.                   |
| `--name`            | Named  | Name of the activity that generated the entity.                           |
| `--description`     | Named  | Description of the activity that generated the entity.                    |
| `-o, --output`      | Named  | Output the provenance record in JSON format.                              |
| `--used`            | Named  | Synapse ID, a url, or a local file path (of a file previously uploaded to Synapse) from which the specified entity is derived. |
| `--executed`        | Named  | Synapse ID, a url, or a local file path (of a file previously uploaded to Synapse) that was executed to generate the specified entity. |
| `--limitSearch`     | Named  | Synapse ID of a container such as project or folder to limit search for provenance files. |


### `get-provenance`

Show provenance records.

```bash
synapse get-provenance [-h] --id syn123 [--version version] [-o [OUTPUT_FILE]]
```

| Name                | Type    | Description                                                               |
|---------------------|---------|---------------------------------------------------------------------------|
| `--id`              | Named  | Synapse ID of entity whose provenance we are accessing.                   |
| `--version`         | Named  | Version of Synapse entity whose provenance we are accessing.              |
| `-o, --output`      | Named  | Output the provenance record in JSON format.                              |


### `set-annotations`

Create annotations records.

```bash
synapse set-annotations [-h] --id syn123 --annotations ANNOTATIONS [-r]
```

| Name                | Type    | Description                                                               | Default               |
|---------------------|---------|---------------------------------------------------------------------------|-----------------------|
| `--id`              | Named  | Synapse ID of entity whose annotations we are accessing.                  |                       |
| `--annotations`     | Named  | Annotations to add as a JSON formatted string, should evaluate to a dictionary (key/value pairs). Example: ‘{“foo”: 1, “bar”:”quux”}’. |                       |
| `-r, --replace`     | Named  | Replace all existing annotations with the given annotations.               | False                 |


### `get-annotations`

Show annotations records.

```bash
synapse get-annotations [-h] --id syn123 [-o [OUTPUT_FILE]]
```

| Name                | Type    | Description                                                               |
|---------------------|---------|---------------------------------------------------------------------------|
| `--id`              | Named  | Synapse ID of entity whose annotations we are accessing.                  |
| `-o, --output`      | Named  | Output the annotations record in JSON format.                             |


### `create`

Creates folders or projects on Synapse.

```bash
synapse create [-h] [--parentid syn123] --name NAME [--description DESCRIPTION | --descriptionFile DESCRIPTION_FILE_PATH] type
```

| Name                | Type    | Description                                                               |
|---------------------|---------|---------------------------------------------------------------------------|
| `type`              | Positional | Type of object to create in synapse one of {Project, Folder}.             |
| `--parentid, --parentId` | Named  | Synapse ID of project or folder where to place folder [not used with project]. |
| `--name`            | Named  | Name of folder/project.                                                   |
| `--description`     | Named  | Description of project/folder.                                            |
| `--descriptionFile` | Named  | Path to a markdown file containing description of project/folder.         |


### `store-table`

Creates a Synapse Table given a csv.

```bash
synapse store-table [-h] --name NAME [--parentid syn123] [--csv foo.csv]
```

| Name                | Type    | Description                                                               |
|---------------------|---------|---------------------------------------------------------------------------|
| `--name`            | Named  | Name of Table.                                                            |
| `--parentid, --parentId` | Named  | Synapse ID of project.                                                    |
| `--csv`             | Named  | Path to csv.                                                              |


### `onweb`

Opens Synapse website for Entity.

```bash
synapse onweb [-h] id
```

| Name                | Type    | Description                                                               |
|---------------------|---------|---------------------------------------------------------------------------|
| `id`                | Positional | Synapse id.                                                               |


### `login`

Verify credentials can be used to login to Synapse.
This does not need to be used prior to executing other commands.

```bash
synapse login [-h] [-u SYNAPSEUSER] [-p SYNAPSE_AUTH_TOKEN] [--profile PROFILE_NAME]
```

| Name             | Type    | Description                                                                | Default               |
|------------------|---------|----------------------------------------------------------------------------|-----------------------|
| `-u, --username` | Named  | Username used to connect to Synapse.                                       |                       |
| `-p, --password` | Named  | Synapse Auth Token (aka: Personal Access Token) used to connect to Synapse |                       |
| `--profile`      | Named  | Name of the Synapse profile (from .synapseConfig) to log in under          |                       |

If --profile is provided, the credentials will be validated and the login will be associated with the given profile.
If no profile is specified, the default '[default]' section in ~/.synapseConfig will be used.

### `test-encoding`

Test character encoding to help diagnose problems.

```bash
synapse test-encoding [-h]
```

| Name                | Type    | Description                                                               |
|---------------------|---------|---------------------------------------------------------------------------|
| `-h`                | Named  | Show the help message and exit.                                           |


### `get-sts-token`

Get an STS token for access to AWS S3 storage underlying Synapse.

```bash
synapse get-sts-token [-h] [-o {json,boto,shell,bash,cmd,powershell}] id {read_write,read_only}
```

| Name                | Type    | Description                                                               | Default               |
|---------------------|---------|---------------------------------------------------------------------------|-----------------------|
| `id`                | Positional | Synapse id.                                                               |                       |
| `permission`        | Positional | Possible choices: read_write, read_only.                                 |                       |
| `-o, --output`      | Named  | Possible choices: json, boto, shell, bash, cmd, powershell.               | "shell"               |

### `migrate`

Migrate Synapse entities to a different storage location.

```bash
synapse migrate [-h] [--source_storage_location_ids [SOURCE_STORAGE_LOCATION_IDS [SOURCE_STORAGE_LOCATION_IDS ...]]]
                [--file_version_strategy FILE_VERSION_STRATEGY] [--include_table_files] [--continue_on_error]
                [--csv_log_path CSV_LOG_PATH] [--dryRun] [--force]
                id dest_storage_location_id db_path
```

| Name                | Type    | Description                                                               | Default               |
|---------------------|---------|---------------------------------------------------------------------------|-----------------------|
| `id`                | Positional | Synapse id.                                                               |                       |
| `dest_storage_location_id` | Positional | Destination Synapse storage location id.                                 |                       |
| `db_path`           | Positional | Local system path where a record keeping file can be stored.              |                       |
| `--source_storage_location_ids` | Named  | Source Synapse storage location ids. If specified only files in these storage locations will be migrated. |                       |
| `--file_version_strategy` | Named  | One of ‘new’, ‘latest’, ‘all’, ‘skip’. New creates a new version of each entity, latest migrates the most recent version, all migrates all versions, skip avoids migrating file entities (use when exclusively targeting table attached files. | "new"               |
| `--include_table_files` | Named  | Include table attached files when migrating.                              | False                 |
| `--continue_on_error` | Named  | Whether to continue processing other entities if migration of one fails.  | False                 |
| `--csv_log_path`    | Named  | Path where to log a csv documenting the changes from the migration.       |                       |
| `--dryRun`          | Named  | Dry run, files will be indexed by not migrated.                           | False                 |
| `--force`           | Named  | Bypass interactive prompt confirming migration.                           | False                 |
