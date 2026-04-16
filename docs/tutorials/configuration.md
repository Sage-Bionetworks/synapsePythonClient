# Configuration

The Synapse Python client can be configured either programmatically or by using a configuration file.

**The default configuration file does not need to be modified for most use-cases**.

When installing the Synapse Python client, the `.synapseConfig` file is added to your home directory if it doesn't exist already. This file stores configuration options including your Synapse auth token, cache location, multi-threading settings, and storage credentials.

A full annotated example `.synapseConfig` can be found in the [GitHub repository](https://github.com/Sage-Bionetworks/synapsePythonClient/blob/develop/synapseclient/.synapseConfig).

## `.synapseConfig` sections

### `[default]` and `[profile <name>]`

Holds Synapse login credentials. `[default]` is used when no profile is specified; named profiles use `[profile <name>]` syntax. See the [authentication](./authentication.md) document for full details including how to create tokens, select profiles, and use environment variables.

### `[sftp://hostname]`

Credentials for files stored on SFTP servers. Use one section per server; the section name is the full SFTP URL.

| Key | Description |
| --- | --- |
| `username` | Username for the SFTP server. |
| `password` | Password for the SFTP server. |

```ini
[sftp://some.sftp.url.com]
username = sftpuser
password = sftppassword
```

### `[https://s3.amazonaws.com/bucket_name]`

Credentials for files stored in AWS S3 or S3-compatible storage that Synapse does not manage access for. Use one section per bucket; the section name is the full endpoint URL including the bucket name.

| Key | Description |
| --- | --- |
| `profile_name` | Name of an AWS CLI profile from `~/.aws/credentials`. If omitted, the `default` AWS profile is used. |

```ini
[https://s3.amazonaws.com/bucket_name]
profile_name = local_credential_profile_name
```

For more information on AWS credentials files, see the [AWS CLI documentation](https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html).

### `[cache]`

Downloaded files are cached to avoid repeat downloads of the same file.

| Key | Description |
| --- | --- |
| `location` | Path to the cache directory. Supports `~` and environment variables. Default: `~/.synapseCache`. |

```ini
[cache]
location = ~/.synapseCache
```

### `[debug]`

When this section is present (no keys required), the client prints debug-level log output. Equivalent to passing `debug=True` to the `Synapse()` constructor.

```ini
[debug]
```

### `[endpoints]`

Override the default Synapse production service endpoints. Useful for testing against staging or development environments.

| Key | Description |
| --- | --- |
| `repoEndpoint` | Synapse repository REST API endpoint. |
| `authEndpoint` | Synapse authentication service endpoint. |
| `fileHandleEndpoint` | Synapse file service endpoint. |
| `portalEndpoint` | Synapse web portal URL. |

Note: The following are the default endpoints.

```ini
[endpoints]
repoEndpoint = https://repo-prod.prod.sagebase.org/repo/v1
authEndpoint = https://auth-prod.prod.sagebase.org/auth/v1
fileHandleEndpoint = https://file-prod.prod.sagebase.org/file/v1
portalEndpoint = https://www.synapse.org/
```

### `[transfer]`

Settings to configure how Synapse uploads and downloads data.

| Key | Description |
| --- | --- |
| `max_threads` | Number of concurrent threads/connections for file transfers. Applies to AWS S3 transfers (uploads and downloads). Default: `min(cpu_count + 4, 128)`. Maximum: `128`. Minimum: `1`. |
| `use_boto_sts` | If `true`, use AWS STS (Security Token Service) to obtain temporary credentials for S3 transfers instead of using stored AWS credentials directly. Valid values: `true` or `false` (case-insensitive). Default: `false`. |

```ini
[transfer]
max_threads = 16
use_boto_sts = false
```

You may also set `max_threads` programmatically:

```python
import synapseclient
syn = synapseclient.login()
syn.max_threads = 10
```
