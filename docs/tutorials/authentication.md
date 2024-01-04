# Authentication

There are multiple ways one can login to Synapse. We recommend users to choose the method that fits their workflow.

## Prerequisites

* Create a [personal access token](https://help.synapse.org/docs/Managing-Your-Account.2055405596.html#ManagingYourAccount-PersonalAccessTokens) token obtained from synapse.org under your Settings.
    * Note that a token must minimally have the **view** scope to be used with the Synapse Python Client.
    * Include **Download** and **Modify** permissions if you are using the Synapse Python Client to follow any subsequent tutorials.
* Once a personal access token has been created it can be used for any of the options below.

## One Time Login

### Python

Use the [synapseclient.login][synapseclient.Synapse.login] function

```python
import synapseclient
syn = synapseclient.login(authToken="authtoken")
```

### Command Line Client

Use the [`synapse login`](./command_line_client.md#login) command

<!-- termynal -->
```
> synapse login -p $MY_SYNAPSE_TOKEN
Welcome, First Last!

Logged in as: username (1234567)
```

## Use `.synapseConfig`

For writing code using the Synapse Python client that is easy to share with others, please do not include your credentials in the code. Instead, please use the `~/.synapseConfig` file to manage your credentials.

When installing the Synapse Python client, the `~/.synapseConfig` is added to your home directory.

### Automatically modifying the `~/.synapseConfig` file with the Command line Client
You may modify the `~/.synapseConfig` file by utilizing the [command line client command and following the interactive prompts](./command_line_client.md#config):

<!-- termynal -->
```
> synapse config
Synapse username (Optional):

Auth token: $MY_SYNAPSE_TOKEN
```

### Manually modifying the `~/.synapseConfig` file
The following describes how to add your credentials to the `~/.synapseConfig` file without the use of the `synapse config` command.

Open the `~/.synapseConfig` file and find the following section:

```
#[authentication]
#username = <username>
#authtoken = <authtoken>
```

To enable this section, uncomment it. You don't need to specify your username when using authtoken as a pair, but if you do, it will be used to verify your identity. A personal access token generated from your synapse.org Settings can be used as your .synapseConfig authtoken.
```
[authentication]
authtoken = <authtoken>
```

Now, you can login without specifying any arguments:

```python
import synapseclient
syn = synapseclient.login()
```

## Use Environment Variable

Setting the `SYNAPSE_AUTH_TOKEN` environment variable will allow you to login to Synapse with a [personal access token](https://help.synapse.org/docs/Managing-Your-Account.2055405596.html#ManagingYourAccount-PersonalAccessTokens)

The environment variable will take priority over credentials in the user's `.synapseConfig` file.

In your shell, you can pass an environment variable to Python inline by defining it before the command:

```bash
SYNAPSE_AUTH_TOKEN='<my_personal_access_token>' python3
```

Alternatively you may export it first, then start: Python

```bash
export SYNAPSE_AUTH_TOKEN='<my_personal_access_token>'
python3
```

Once you are inside Python, you may simply login without passing any arguments:

```python
import synapseclient
syn = synapseclient.login()
```

To use the environment variable with the command line client, simply substitute `python` for the `synapse` command

```bash
SYNAPSE_AUTH_TOKEN='<my_personal_access_token>' synapse get syn123
SYNAPSE_AUTH_TOKEN='<my_personal_access_token>' synapse store --parentid syn123 ~/foobar.txt
```

Or alternatively, for multiple commands:

```bash
export SYNAPSE_AUTH_TOKEN='<my_personal_access_token>'
synapse get syn123
synapse store --parentid syn123 ~/foobar.txt
```

### For more information, see:

- [synapseclient.Synapse][]
- [synapseclient.Synapse.login][]
- [synapseclient.Synapse.logout][]
- [synapse config](./configuration.md)
