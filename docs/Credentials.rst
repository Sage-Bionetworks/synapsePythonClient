==========================
Manage Synapse Credentials
==========================

There are multiple ways one can login to Synapse. We recommend users to choose the method that fits their workflow.

One Time Login
==============
Use `username` and `password` to login as follows::

    import synapseclient
    syn = synapseclient.login("username", "password")

Alternately you can login using a personal access token obtained from synapse.org under your Settings. Note that a token must minimally have the *view* scope to be used with the Synapse Python Client.

::

    syn = synapseclient.login(authToken="authtoken")

Use `.synapseConfig`
====================
For writing code using the Synapse Python client that is easy to share with others, please do not include your credentials in the code. Instead, please use `.synapseConfig` file to manage your credentials.

When installing the Synapse Python client, the `.synapseConfig` is added to your home directory. Open the `.synapseConfig` file and find the following section::

    #[authentication]
    #username = <username>
    #password = <password>
    #authtoken = <authtoken>

To enable this section, uncomment it. You will only need to specify either a `username` and `password` pair, or an `authtoken`. For security purposes, we recommend that you use `authtoken` instead of your `password`. A personal access token generated from your synapse.org Settings can be used as your *.synapseConfig* authtoken.

::

    [authentication]
    authtoken = <authtoken>

Now, you can login without specifying your `username` and `password`::

    import synapseclient
    syn = synapseclient.login()

The .synapseConfig also supports a legacy `apikey` which can be used with a `username` instead of the `password` or `authtoken`, however api key support in the synapseConfig is considered deprecated in favor of personal access tokens which
can be scoped to certain functions and which are revocable. If needed your legacy `apikey` can also be obtained from your synapse.org Settings.

Letting the Operating System Manage Your Synapse Credentials
============================================================

For users who would like to save their credentials and let other OS configured applications (like keychain in Mac) manage credentials for them, when logging in for the first time, use::

    import synapseclient
    syn = synapseclient.login("username", "password", rememberMe=True)

The application (keychain in Mac) will then prompt you to allow Python to access these credentials. Please choose “Yes” or “OK”.

The second time you login, you will not have to enter your `username` or `password`::

    import synapseclient
    syn = synapseclient.login()
