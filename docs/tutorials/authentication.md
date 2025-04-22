# Authentication

There are multiple ways one can login to Synapse. We recommend users choose the method that fits their workflow best.

## Prerequisites

* Create a [Personal Access Token](https://help.synapse.org/docs/Managing-Your-Account.2055405596.html#ManagingYourAccount-PersonalAccessTokens) (**aka: Synapse Auth Token**) obtained
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

## Use `.synapseConfig`

For writing code using the Synapse Python client that is easy to share with others, please do not include your credentials in the code. Instead, please use the `~/.synapseConfig` file to manage your credentials.

The Synapse Python Client supports multiple profiles within the `~/.synapseConfig` file, enabling users to manage credentials for multiple accounts. Each profile is defined in its own `[profile <profile_name>]` section. A default profile can still be defined using `[default]`.

When installing the Synapse Python client, the `~/.synapseConfig` is added to your home directory.

### Automatically modifying the `~/.synapseConfig` file with the Command line Client
You may modify the `~/.synapseConfig` file by utilizing the [command line client command and following the interactive prompts](./command_line_client.md#config):

#### Modifying the synapse config for multiple profiles

<!-- termynal -->
```
> synapse config

Synapse username (Optional): $MY_USERNAME

Auth token: $MY_SYNAPSE_TOKEN

Configuration profile name (Optional, 'default' used if not specified): $MY_CONFIG_PROFILE
```

#### Adding or updating a specific profile passed in as a command line argument
<!-- termynal -->
```
> synapse --profile $MY_PROFILE_NAME config

Synapse username (Optional): $MY_USERNAME

Auth token: $MY_SYNAPSE_TOKEN
```

Note: If you encounter a PermissionError
(e.g., `[Errno 13] Permission denied: '/Users/username/.synapseConfig'`), it is likely that the user does not have write permissions to the `~/.synapseConfig` file.
To resolve this, ensure that you have the necessary permissions to modify this file.
You can change the permissions using the following command:

`chmod u+w ~/.synapseConfig`


### Manually modifying the `~/.synapseConfig` file
The following describes how to add your credentials to the `~/.synapseConfig` file without the use of the `synapse config` command.

Open the `~/.synapseConfig` file using your preferred text editing tool and find/insert the following section(s):

```
[default]
username = default_user
authtoken = default_auth_token

[profile user1]
username = user1
authtoken = user1_auth_token

[profile user2]
username = user2
authtoken = user2_auth_token

# This section is deprecated. It will be used if a `default` profile or a specific profile is not present in the config file
#[authentication]
#username = default_user
#authtoken = default_auth_token
```

`username` is optional when using `authtoken`, but if provided, an additional check to verify the `authtoken` matches the `username` is performed.

The `authoken` is also know as a personal access token. It is generated from your [synapse.org Settings](https://help.synapse.org/docs/Managing-Your-Account.2055405596.html#ManagingYourAccount-PersonalAccessTokens)..

### Transitioning from One Profile to Multiple

If you're currently using a single profile (under the `[default]` or `[authentication]` section) and wish to start using multiple profiles,
simply add new sections for each profile with a unique profile name. For example, you can add a profile for user1 and user2 as shown below.
The Synapse Python client will allow you to choose which profile to use at login.

```
[default]
username = default_user
authtoken = default_auth_token

[profile user1]
username = user1
authtoken = user1_auth_token

[profile user2]
username = user2
authtoken = user2_auth_token
```

## Logging in with your ~/.synapseConfig file

**Note:** If no profile is specified the `default` section will be used. Additionally, to support backwards compatibility, `authentication` will continue to function. `authentication` will be used if no profile is used and `default` is not present in the configuration file.

### Logging in via python code

```python
import synapseclient
syn = synapseclient.login()
```

If you want to log in with a specific profile, simply pass the profile name as an argument to `login()`:

```python
import synapseclient
syn = synapseclient.login(profile="user1")
```

### Logging in via the command line

Logs in with the `default` profile, or the profile set in the `SYNAPSE_PROFILE` environment variable:

<!-- termynal -->
```
#For default login
> synapse login

returns Welcome, last first!
```

Logging in with the `profile_name` given:

<!-- termynal -->
```
#For profile login

> synapse --profile profile_name login

Welcome, last first! You are using the 'profile_name' profile.
```

## Use Environment Variable

Setting the `SYNAPSE_AUTH_TOKEN` environment variable will allow you to login to Synapse with a [Personal Access Token](https://help.synapse.org/docs/Managing-Your-Account.2055405596.html#ManagingYourAccount-PersonalAccessTokens)

The environment variable will take priority over credentials in the user's `.synapseConfig` file.

In your shell, you can pass an environment variable to Python inline by defining it before the command:

```bash
SYNAPSE_AUTH_TOKEN='<my_personal_access_token>' python3
```

Alternatively you may export it first, then start Python:

```bash
export SYNAPSE_AUTH_TOKEN='<my_personal_access_token>'
python3
```

Setting the `SYNAPSE_PROFILE` environment variable will allow you to log into Synapse using a specific authentication profile present in your `.synapseConfig` file. This allows you to have multiple profiles present in a single configuration file that you may swap between. Alternatively, you may use the `profile` parameter in Python, or the `--profile` command line flag for all commands like `synapse --profile <profile_name> COMMAND`.

Once you are inside Python, you may simply login without passing any arguments, or pass a profile argument to access a specific profile:

```python
import synapseclient
syn = synapseclient.login()

import synapseclient
syn = synapseclient.login(profile="user1")
```

To use the environment variable with the command line client, simply substitute `python` for the `synapse` command:

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
