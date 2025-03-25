# Authentication

There are multiple ways one can login to Synapse. We recommend users to choose the method that fits their workflow.

## Prerequisites

* Create a [Personal Access Token](https://help.synapse.org/docs/Managing-Your-Account.2055405596.html#ManagingYourAccount-PersonalAccessTokens) (**aka: Synapse Auth Token**) token obtained
from synapse.org under your Settings.
    * Note that a token must minimally have the **view** scope to be used with the Synapse Python Client.
    * Include **Download** and **Modify** permissions if you are using the Synapse Python Client to follow any subsequent tutorials.
* Once a personal access token has been created it can be used for any of the options below.

## One Time Login

### Python

Use the [synapseclient.login][synapseclient.Synapse.login] function

```python
import synapseclient
syn = synapseclient.login(authToken="authtoken")
#returns Welcome, First Last! 
```

### Command Line Client

Use the [`synapse login`](./command_line_client.md#login) command

<!-- termynal -->
```
> synapse login -p $MY_SYNAPSE_TOKEN
Welcome, First Last!

Logged in as: username (1234567)
```

## Use `.synapseConfig` for Multiple Profiles

For writing code using the Synapse Python client that is easy to share with others, please do not include your credentials in the code. Instead, please use the `~/.synapseConfig` file to manage your credentials.

The Synapse Python Client supports multiple profiles within the ~/.synapseConfig file, enabling users to manage credentials for multiple accounts.

Each profile is defined in its own '[profile <profile_name>]' section. A default profile can still be defined using '[default]'.

When installing the Synapse Python client, the `~/.synapseConfig` is added to your home directory.

### Automatically modifying the `~/.synapseConfig` file with the Command line Client
You may modify the `~/.synapseConfig` file by utilizing the [command line client command and following the interactive prompts](./command_line_client.md#config):

<!-- termynal -->
```
#For Default profile
> synapse --profile default config
Synapse username (Optional): $MY_USERNAME

Auth token: $MY_SYNAPSE_TOKEN

#For additional profiles
> synapse --profile $MY_PROFILE_NAME config
Synapse username (Optional): $MY_USERNAME

Auth token: $MY_SYNAPSE_TOKEN
```

### Manually modifying the `~/.synapseConfig` file
The following describes how to add your credentials to the `~/.synapseConfig` file without the use of the `synapse config` command.

Open the `~/.synapseConfig` file (can use 'nano ~/.synapseConfig' to edit or 'cat ~/.synapseConfig' to read from the CLI)
and find the following section:

```
#[default]
#username = default_user
#authtoken = default_auth_token

#[profile user1]
#username = user1
#authtoken = user1_auth_token

#[profile user2]
#username = user2
#authtoken = user2_auth_token
```

To enable this section, uncomment it. You don't need to specify your username when using authtoken as a pair, but if you do, it will be used to verify your identity. A personal access token generated from your synapse.org Settings can be used as your .synapseConfig authtoken.
```
[default]
username = default_user
#authtoken = default_auth_token

[profile user1]
username = user1
authtoken = user1_auth_token

[profile user2]
username = user2
authtoken = user2_auth_token
```

If logging in without specifying any profiles, it will default to the default profile:
Now, you can login without specifying any arguments:

```python
import synapseclient
syn = synapseclient.login()
```

If logging in specifying the profile, it will log in with said profile:

```python
import synapseclient
syn = synapseclient.login(profile="user1")
```

## Use Environment Variable

Setting the `SYNAPSE_AUTH_TOKEN` environment variable will allow you to login to Synapse with a [Personal Access Token](https://help.synapse.org/docs/Managing-Your-Account.2055405596.html#ManagingYourAccount-PersonalAccessTokens)

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

Once you are inside Python, you may simply login without passing any arguments, or pass a profile argument to access a specific profile :

```python
import synapseclient
syn = synapseclient.login()

import synapseclient
syn = synapseclient.login(profile="user1")
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
