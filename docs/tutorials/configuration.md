# Configuration

The synapse python client can be configured either programmatically or by using a configuration file.

**The default configuration file does not need to be modified for most use-cases**.


When installing the Synapse Python client, the `.synapseConfig` is added to your home directory. This configuration file is used to store a number of configuration options, including your Synapse authtoken, cache, and multi-threading settings.

A full example `.synapseConfig` can be found in the [github repository](https://github.com/Sage-Bionetworks/synapsePythonClient/blob/develop/synapseclient/.synapseConfig).

## `.synapseConfig` sections

### `[authentication]`

See details on this section in the [authentication](./authentication.md) document.

### `[cache]`

Your downloaded files are cached to avoid repeat downloads of the same file. Change 'location' to use a different folder on your computer as the cache location

### `[endpoints]`

Configuring these will cause the Python client to use these as Synapse service endpoints instead of the default prod endpoints.

### `[transfer]`

Settings to configure how Synapse uploads/downloads data.

You may also set the `max_threads` programmatically via:

```python
import synapseclient
syn = synapseclient.login()
syn.max_threads = 10
```
