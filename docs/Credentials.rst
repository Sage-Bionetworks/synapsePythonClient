==========================
Manage Synapse Credentials
==========================

There are multiple ways one can login to Synapse. We recommend users to choose the method that fits their workflow.

One Time Login
==============
Use `username` and `password` to login as follows::

    import synapseclient
    syn = synapseclient.login("username", "password")


Use `.synapseConfig`
====================
For writing code using the Synapse Python client, that is easy to share with others, please do not include your credentials in the code. Instead, please use `.synapseConfig` file to manage your credentials.

When installing the Synapse Python client, the `.synapseConfig` is added to your home directory. Open the `.synapseConfig` file and find the following section::

    #[authentication]
    #username = <username>
    #password = <password>
    #apikey = <apikey>

To enable this section, uncomment it. You will only need to specify either `username` and `password` or `username` and `apikey`. For security purposes, we recommend that you use Synapse `apikey` instead of your `password`::

    [authentication]
    username = <username>
    apikey = <apikey>

Now, you can login without specifying your `username` and `password`::

    import synapseclient
    syn = synapseclient.login()


Letting the Operating System Manage Your Synapse Credentials
============================================================

For users who would like to save their credentials and let other OS configured applications (like keychain in Mac) manage credentials for them, when logging in for the first time, use::

    import synapseclient
    syn = synapseclient.login("username", "password", rememberMe=True)

The application (keychain in Mac) will then prompt you to allow Python to access these credentials. Please choose “Yes” or “OK”.

The second time you login, you will not have to enter your `username` or `password`::

    import synapseclient
    syn = synapseclient.login()
