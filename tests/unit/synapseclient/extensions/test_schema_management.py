import json
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

import pytest

from synapseclient.extensions.curator import register_jsonschema_async
from synapseclient.extensions.curator.schema_management import fix_name


@pytest.fixture
def mock_synapse_client():
    mock_client = MagicMock()
    mock_client.logger = MagicMock()
    return mock_client


@pytest.fixture
def mock_jsonschema():
    with patch("synapseclient.models.organization.JSONSchema") as MockSchema:
        instance = MockSchema.return_value
        instance.store_async = AsyncMock()
        instance.uri = "syn123.456"
        yield MockSchema


@pytest.mark.asyncio
async def test_register_jsonschema_async(mock_synapse_client, mock_jsonschema):
    schema_path = "mock_path.json"
    schema_content = {"$id": "test_schema", "type": "object"}
    org_name = "test.org"
    schema_name = "my-schema_name"
    version = "1.0.0"

    m_open = mock_open(read_data=json.dumps(schema_content))

    with (
        patch("builtins.open", m_open),
        patch("synapseclient.Synapse.get_client", return_value=mock_synapse_client),
        patch("json.load", return_value=schema_content),
    ):
        result = await register_jsonschema_async(
            schema_path=schema_path,
            organization_name=org_name,
            schema_name=schema_name,
            schema_version=version,
            synapse_client=mock_synapse_client,
        )

        # Verify the name was used as-is
        mock_jsonschema.assert_called_once_with(
            name=schema_name, organization_name=org_name
        )

        result.store_async.assert_awaited_once_with(
            schema_body=schema_content,
            version=version,
            dry_run=False,
            synapse_client=mock_synapse_client,
        )

        assert result.uri == "syn123.456"


@pytest.mark.asyncio
async def test_register_jsonschema_async_fix_schema_name(
    mock_synapse_client, mock_jsonschema
):
    schema_path = "mock_path.json"
    schema_content = {"$id": "test_schema", "type": "object"}
    org_name = "test.org"
    schema_name = "my-schema_name"
    fixed_schema_name = "my.schema.name"
    version = "1.0.0"

    m_open = mock_open(read_data=json.dumps(schema_content))

    with (
        patch("builtins.open", m_open),
        patch("synapseclient.Synapse.get_client", return_value=mock_synapse_client),
        patch("json.load", return_value=schema_content),
    ):
        result = await register_jsonschema_async(
            schema_path=schema_path,
            organization_name=org_name,
            schema_name=schema_name,
            fix_schema_name=True,
            schema_version=version,
            synapse_client=mock_synapse_client,
        )

        # Verify the name was fixed (dashes/underscores to dots)
        mock_jsonschema.assert_called_once_with(
            name=fixed_schema_name, organization_name=org_name
        )

        result.store_async.assert_awaited_once_with(
            schema_body=schema_content,
            version=version,
            dry_run=False,
            synapse_client=mock_synapse_client,
        )

        assert result.uri == "syn123.456"


async def test_register_jsonschema_async_dry_run(mock_synapse_client, mock_jsonschema):
    # GIVEN a schema file and a JSONSchema whose dry-run store leaves it unchanged
    schema_content = {"$id": "test_schema", "type": "object"}
    mock_jsonschema.return_value.uri = None

    with (
        patch("builtins.open", mock_open(read_data=json.dumps(schema_content))),
        patch("synapseclient.Synapse.get_client", return_value=mock_synapse_client),
    ):
        # WHEN I register it as a dry run
        result = await register_jsonschema_async(
            schema_path="mock_path.json",
            organization_name="test.org",
            schema_name="my.schema",
            schema_version="1.0.0",
            dry_run=True,
            synapse_client=mock_synapse_client,
        )

    # THEN the store is a dry run
    result.store_async.assert_awaited_once_with(
        schema_body=schema_content,
        version="1.0.0",
        dry_run=True,
        synapse_client=mock_synapse_client,
    )
    # AND the log says nothing was stored, without reporting a URI
    messages = [call.args[0] for call in mock_synapse_client.logger.info.call_args_list]
    assert any("Nothing was stored" in message for message in messages)
    assert not any("Schema URI" in message for message in messages)


@pytest.mark.parametrize(
    "name, expected_fixed_name",
    [
        ("name", "name"),
        ("name.name", "name.name"),
        ("name..name", "name.name"),
        ("name-name", "name.name"),
        ("name--name", "name.name"),
        ("name_name", "name.name"),
        ("name-_name", "name.name"),
    ],
    ids=[
        "No special characters",
        "One period",
        "Multiple periods",
        "One dash",
        "Multiple dashes",
        "Underscore",
        "Mixed special characters",
    ],
)
def test_fix_name(name, expected_fixed_name):
    assert fix_name(name) == expected_fixed_name
