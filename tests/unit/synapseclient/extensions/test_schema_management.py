import json
from unittest.mock import AsyncMock, MagicMock, mock_open, patch

import pytest

from synapseclient.extensions.curator import register_jsonschema_async


@pytest.fixture
def mock_synapse_client():
    mock_client = MagicMock()
    mock_client.logger = MagicMock()
    return mock_client


@pytest.fixture
def mock_jsonschema():
    with patch("synapseclient.models.schema_organization.JSONSchema") as MockSchema:
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

    with patch("builtins.open", m_open), patch(
        "synapseclient.Synapse.get_client", return_value=mock_synapse_client
    ), patch("json.load", return_value=schema_content):
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

    with patch("builtins.open", m_open), patch(
        "synapseclient.Synapse.get_client", return_value=mock_synapse_client
    ), patch("json.load", return_value=schema_content):
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
            synapse_client=mock_synapse_client,
        )

        assert result.uri == "syn123.456"
