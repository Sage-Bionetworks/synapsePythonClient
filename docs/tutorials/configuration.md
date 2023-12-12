# Configuration

The synapse python client can be configured either programmatically or by using a configuration file. When installing the Synapse Python client, the `.synapseConfig` is added to your home directory. This configuration file is used to store a number of configuration options, including your Synapse authtoken, cache, and multi-threading settings.

A full example `.synapseConfig` can be found in the [github repository](https://github.com/Sage-Bionetworks/synapsePythonClient/blob/develop/synapseclient/.synapseConfig).

## `.synapseConfig` sections

### `[authentication]`

See details on this section in the [credentials](credentials.md) document.

### `[cache]`

Your downloaded files are cached to avoid repeat downloads of the same file. change 'location' to use a different folder on your computer as the cache location

### `[endpoints]`

Configuring these will cause the Python client to use these as Synapse service endpoints instead of the default prod endpoints.

### `[transfer]`

Settings to configure how Synapse uploads/downloads data.

The current recommended `max_threads` is around 50. This was the best balance in stability and performance. See the results of our benchmarking [here](../articles/benchmarking.md).

You may also set the `max_threads` programmatically via:

```python
import synapseclient
syn = synapseclient.login()
syn.max_threads = 50
```
