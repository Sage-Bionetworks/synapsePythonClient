# Manifest
The manifest is a tsv file with file locations and metadata to be pushed to Synapse. The purpose is to allow bulk actions through a TSV without the need to manually execute commands for every requested action.

## Manifest file format

The format of the manifest file is a tab delimited file with one row per file to upload and columns describing the file. The minimum required columns are **path** and **parent** where path is the local file path and parent is the Synapse Id of the project or folder where the file is uploaded to.

In addition to these columns you can specify any of the parameters to the File constructor (**name**, **synapseStore**, **contentType**) as well as parameters to the [syn.store][synapseclient.Synapse.store] command (**used**, **executed**, **activityName**, **activityDescription**, **forceVersion**).

For only updating annotations without uploading new versions of unchanged files, the [syn.store][synapseclient.Synapse.store] parameter forceVersion should be included in the manifest with the value set to False.

Used and executed can be semi-colon (";") separated lists of Synapse ids, urls and/or local filepaths of files already stored in Synapse (or being stored in Synapse by the manifest). If you leave a space, like "syn1234; syn2345" the white space from " syn2345" will be stripped.

Any additional columns will be added as annotations.

### Required fields:

| Field | Meaning | Example |
| --- | --- | --- |
| path | local file path or URL | /path/to/local/file.txt |
| parent | synapse id | syn1235 |

### Common fields:

| Field | Meaning | Example |
| --- | --- | --- |
| name | name of file in Synapse | Example_file |
| forceVersion | whether to update version | False |

### Activity/Provenance fields:

Each of these are individual examples and is what you would find in a row in each of these columns. To clarify, "syn1235;/path/to_local/file.txt" below states that you would like both "syn1234" and "/path/to_local/file.txt" added as items used to generate a file. You can also specify one item by specifying "syn1234"

| Field | Meaning | Example |
| --- | --- | --- |
| used | List of items used to generate file | "syn1235;/path/to_local/file.txt" |
| executed | List of items executed | "https://github.org/;/path/to_local/code.py" |
| activityName | Name of activity in provenance | "Ran normalization" |
| activityDescription | Text description on what was done | "Ran algorithm xyx with parameters..." |

See:

- [Activity][synapseclient.Activity]

### Annotations:

Any columns that are not in the reserved names described above will be interpreted as annotations of the file

Adding 4 annotations to each row:

| path | parent | annot1 | annot2 | annot3 | annot4 |
| --- | --- | --- | --- | --- | --- |
| /path/file1.txt | syn1243 | bar | 3.1415 | aaaa, bbbb | 14,27,30 |
| /path/file2.txt | syn12433 | baz | 2.71 | value_1,value_2 | 1,2,3 |
| /path/file3.txt | syn12455 | zzz | 3.52 | value_3,value_4 | 42, 56, 77 |

#### Multiple values of annotations per key
Using multiple values for a single annotation should be used sparingly as it makes it more
difficult for you to manage the data. However, it is supported.

**Annotations can be comma (",") separated lists. **

If you have a string that requires a `,` to be used in the formatting of the string you
may escape the comma in your string with a `\`. For example:

`Hello\, my first string, Hello\, my second string`

You can also escape the `\` with another `\`. For example:

`An escaped backslash \\ there`



See:

- [Annotations][synapseclient.annotations.Annotations]

### Other optional fields:

| Field | Meaning | Example |
| --- | --- | --- |
| synapseStore | Boolean describing whether to upload files | True |
| contentType | content type of file to overload defaults | text/html |

### Example manifest file

| path | parent | annot1 | annot2 | collection_date | used | executed |
| --- | --- | --- | --- | --- | --- | --- |
| /path/file1.txt | syn1243 | "bar" | 3.1415 | 2023-12-04 07:00:00+00:00 | "syn124;/path/file2.txt" | "https://github.org/foo/bar" |
| /path/file2.txt | syn12433 | "baz" | 2.71 | 2001-01-01 15:00:00+07:00 | "" | "https://github.org/foo/baz" |
| /path/file3.txt | syn12455 | "zzz" | 3.52 | 2023-12-04T07:00:00Z | "" | "https://github.org/foo/zzz" |

### Dates in the manifest file
Dates within the manifest file will always be written as [ISO 8601](https://en.wikipedia.org/wiki/ISO_8601) format in UTC without milliseconds. For example: `2023-12-20T16:55:08Z`.

Dates can be written in other formats specified in ISO 8601 and it will be reconginzed, however, the [synapseutils.syncFromSynapse][] will always write this in the UTC format specified above. For example you may want to specify a datetime at a specific timezone like: `2023-12-20 23:55:08-07:00` and this will be recognized as a valid datetime.


## Refernces:

- [synapseutils.syncFromSynapse][]
- [synapseutils.syncToSynapse][]
- [Managing custom metadata at scale](https://help.synapse.org/docs/Managing-Custom-Metadata-at-Scale.2004254976.html#ManagingCustomMetadataatScale-BatchUploadFileswithAnnotations)
