"""Unit tests that pin the order of the default credential provider chain."""

from synapseclient.core.credentials.credential_provider import (
    DEFAULT_CREDENTIAL_PROVIDER_CHAIN,
    AWSParameterStoreCredentialsProvider,
    ConfigFileCredentialsProvider,
    EnvironmentVariableCredentialsProvider,
    UserArgsCredentialsProvider,
    get_default_credential_chain,
)


async def test_default_credential_provider_chain_order() -> None:
    # GIVEN the default credential provider chain
    chain = get_default_credential_chain()

    # THEN it is the module-level default chain
    assert chain is DEFAULT_CREDENTIAL_PROVIDER_CHAIN

    # AND the providers are checked in the documented order. If this order changes,
    # update Synapse.login() in client.py and docs/tutorials/authentication.md
    assert [type(provider) for provider in chain.cred_providers] == [
        UserArgsCredentialsProvider,
        ConfigFileCredentialsProvider,
        EnvironmentVariableCredentialsProvider,
        AWSParameterStoreCredentialsProvider,
    ]
