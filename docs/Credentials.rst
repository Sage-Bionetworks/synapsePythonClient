==========================
Manage Synapse Credentials
==========================

There are multiple ways one can login to Synapse. We recommend users to choose the method that fits their workflow.

One Time Login
==============
Use :code:`username` and :code:`password` to login as follows::

    import synapseclient
    syn = synapseclient.login("username", "password")

Alternately you can login using a personal access token obtained from synapse.org under your Settings. Note that a token must minimally have the *view* scope to be used with the Synapse Python Client.

::

    syn = synapseclient.login(authToken="authtoken")

Use Environment Variable
=========================

Setting the :code:`SYNAPSE_ACCESS_TOKEN` environment variable will allow you to login
to synapse with a personal access token.

The environment variable will take priority over credentials in the user's :code:`.synapseConfig` file
or any credentials saved in a prior login using :code:`syn.login(rememberMe=True)`.

.. TODO: Once documentation for it is written, link to documentation about generating a personal access token

In your shell, you can pass an environment variable to Python inline by defining it before the command:

.. code-block:: bash

    SYNAPSE_ACCESS_TOKEN='<my_personal_access_token>' python3

Alternatively you may export it first, then start Python

.. code-block:: bash

    export SYNAPSE_ACCESS_TOKEN='<my_personal_access_token>'
    python3
Once you are inside Python, you may simply login without passing any arguments:

.. code-block:: python3

    import synapseclient
    syn = synapseclient.login()


Use :code:`.synapseConfig`
====================
For writing code using the Synapse Python client that is easy to share with others, please do not include your credentials in the code. Instead, please use `.synapseConfig` file to manage your credentials.

When installing the Synapse Python client, the `.synapseConfig` is added to your home directory. Open the `.synapseConfig` file and find the following section::

    #[authentication]
    #username = <username>
    #password = <password>
    #authtoken = <authtoken>

To enable this section, uncomment it. You will only need to specify either a :code:`username` and :code:`password` pair, or an :code:`authtoken`. For security purposes, we recommend that you use :code:`authtoken` instead of your :code:`password`. A personal access token generated from your synapse.org Settings can be used as your *.synapseConfig* authtoken.

::

    [authentication]
    authtoken = <authtoken>

Now, you can login without specifying your :code:`username` and :code:`password`::

    import synapseclient
    syn = synapseclient.login()

The .synapseConfig also supports a legacy :code:`apikey` which can be used with a :code:`username` instead of the :code:`password` or :code:`authtoken`, however api key support in the synapseConfig is considered deprecated in favor of personal access tokens which
can be scoped to certain functions and which are revocable. If needed your legacy :code:`apikey` can also be obtained from your synapse.org Settings.

Letting the Operating System Manage Your Synapse Credentials
============================================================

For users who would like to save their credentials and let other OS configured applications (like keychain in Mac) manage credentials for them, when logging in for the first time, use::

    import synapseclient
    syn = synapseclient.login("username", "password", rememberMe=True)

The application (keychain in Mac) will then prompt you to allow Python to access these credentials. Please choose "Yes" or "OK".

The second time you login, you will not have to enter your :code:`username` or :code:`password`::

    import synapseclient
    syn = synapseclient.login()

