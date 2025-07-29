from unittest.mock import AsyncMock, MagicMock, patch

import synapseclient.api.json_schema_services as jss


@patch("synapseclient.Synapse")
async def test_bind_json_schema_to_entity(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client.rest_put_async.return_value = {"result": "ok"}
    result = await jss.bind_json_schema_to_entity(
        "syn1", "schema$id", synapse_client=None
    )
    assert result == {"result": "ok"}
    mock_client.rest_put_async.assert_awaited_once()


@patch("synapseclient.Synapse")
async def test_get_json_schema_from_entity(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client.rest_get_async.return_value = {"schema": "data"}
    result = await jss.get_json_schema_from_entity("syn1", synapse_client=None)
    assert result == {"schema": "data"}
    mock_client.rest_get_async.assert_awaited_once()


@patch("synapseclient.Synapse")
async def test_delete_json_schema_from_entity(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client.rest_delete_async.return_value = None
    result = await jss.delete_json_schema_from_entity("syn1", synapse_client=None)
    assert result is None
    mock_client.rest_delete_async.assert_awaited_once()


@patch("synapseclient.Synapse")
async def test_validate_entity_with_json_schema(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client.rest_get_async.return_value = {"isValid": True}
    result = await jss.validate_entity_with_json_schema("syn1", synapse_client=None)
    assert result == {"isValid": True}
    mock_client.rest_get_async.assert_awaited_once()


@patch("synapseclient.Synapse")
async def test_get_json_schema_validation_statistics(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client.rest_get_async.return_value = {"stats": 123}
    result = await jss.get_json_schema_validation_statistics(
        "syn1", synapse_client=None
    )
    assert result == {"stats": 123}
    mock_client.rest_get_async.assert_awaited_once()


@patch("synapseclient.api.json_schema_services.rest_post_paginated_async")
async def test_get_invalid_json_schema_validation(mock_rest_post):
    async def async_gen():
        yield {"objectId": "syn1"}
        yield {"objectId": "syn2"}

    mock_rest_post.return_value = async_gen()
    results = []
    async for item in jss.get_invalid_json_schema_validation(
        "syn1", synapse_client=None
    ):
        results.append(item)
    assert results == [{"objectId": "syn1"}, {"objectId": "syn2"}]
    mock_rest_post.assert_called_once()


@patch("synapseclient.Synapse")
def test_get_invalid_json_schema_validation_sync(mock_synapse):
    mock_client = MagicMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client._POST_paginated.return_value = iter(
        [
            {"objectId": "syn1"},
            {"objectId": "syn2"},
        ]
    )
    results = list(
        jss.get_invalid_json_schema_validation_sync("syn1", synapse_client=None)
    )
    assert results == [{"objectId": "syn1"}, {"objectId": "syn2"}]
    mock_client._POST_paginated.assert_called_once()


@patch("synapseclient.Synapse")
async def test_get_json_schema_derived_keys(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client.rest_get_async.return_value = ["key1", "key2"]
    result = await jss.get_json_schema_derived_keys("syn1", synapse_client=None)
    assert result == ["key1", "key2"]
    mock_client.rest_get_async.assert_awaited_once()


@patch("synapseclient.Synapse")
async def test_create_organization(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client.rest_post_async.return_value = {"id": "1", "name": "org"}
    result = await jss.create_organization("org", synapse_client=None)
    assert result == {"id": "1", "name": "org"}
    mock_client.rest_post_async.assert_awaited_once()


@patch("synapseclient.Synapse")
async def test_get_organization(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client.rest_get_async.return_value = {"id": "1", "name": "org"}
    result = await jss.get_organization("org", synapse_client=None)
    assert result == {"id": "1", "name": "org"}
    mock_client.rest_get_async.assert_awaited_once()


@patch("synapseclient.Synapse")
async def test_list_organizations(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    with patch(
        "synapseclient.api.json_schema_services.rest_post_paginated_async"
    ) as mock_rest_post:

        async def async_gen():
            yield {"id": "1"}
            yield {"id": "2"}

        mock_rest_post.return_value = async_gen()
        result = []
        async for item in jss.list_organizations(synapse_client=None):
            result.append(item)
        assert result == [{"id": "1"}, {"id": "2"}]
        mock_rest_post.assert_called_once()


@patch("synapseclient.Synapse")
def test_list_organizations_sync(mock_synapse):
    mock_client = MagicMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client._POST_paginated.return_value = iter(
        [
            {"id": "1"},
            {"id": "2"},
        ]
    )
    result = list(jss.list_organizations_sync(synapse_client=None))
    assert result == [{"id": "1"}, {"id": "2"}]
    mock_client._POST_paginated.assert_called_once()


@patch("synapseclient.Synapse")
async def test_delete_organization(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client.rest_delete_async.return_value = None
    result = await jss.delete_organization("1", synapse_client=None)
    assert result is None
    mock_client.rest_delete_async.assert_awaited_once()


@patch("synapseclient.Synapse")
async def test_get_organization_acl(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client.rest_get_async.return_value = {"id": "1", "etag": "abc"}
    result = await jss.get_organization_acl("1", synapse_client=None)
    assert result == {"id": "1", "etag": "abc"}
    mock_client.rest_get_async.assert_awaited_once()


@patch("synapseclient.Synapse")
async def test_update_organization_acl(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client.rest_put_async.return_value = {"id": "1", "etag": "abc"}
    result = await jss.update_organization_acl(
        "1", [{"principalId": 1, "accessType": ["READ"]}], "abc", synapse_client=None
    )
    assert result == {"id": "1", "etag": "abc"}
    mock_client.rest_put_async.assert_awaited_once()


@patch("synapseclient.Synapse")
async def test_list_json_schemas(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    with patch(
        "synapseclient.api.json_schema_services.rest_post_paginated_async"
    ) as mock_rest_post:

        async def async_gen():
            yield {"schemaId": "1"}
            yield {"schemaId": "2"}

        mock_rest_post.return_value = async_gen()
        result = await jss.list_json_schemas("org", synapse_client=None)
        assert result == [{"schemaId": "1"}, {"schemaId": "2"}]
        mock_rest_post.assert_called_once()


@patch("synapseclient.Synapse")
async def test_list_json_schema_versions(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    with patch(
        "synapseclient.api.json_schema_services.rest_post_paginated_async"
    ) as mock_rest_post:

        async def async_gen():
            yield {"versionId": "1"}
            yield {"versionId": "2"}

        mock_rest_post.return_value = async_gen()
        result = await jss.list_json_schema_versions(
            "org", "schema", synapse_client=None
        )
        assert result == [{"versionId": "1"}, {"versionId": "2"}]
        mock_rest_post.assert_called_once()


@patch("synapseclient.Synapse")
async def test_get_json_schema_body(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client.rest_get_async.return_value = {"$id": "schema$id"}
    result = await jss.get_json_schema_body("schema$id", synapse_client=None)
    assert result == {"$id": "schema$id"}
    mock_client.rest_get_async.assert_awaited_once()


@patch("synapseclient.Synapse")
async def test_delete_json_schema(mock_synapse):
    mock_client = AsyncMock()
    mock_synapse.get_client.return_value = mock_client
    mock_client.rest_delete_async.return_value = None
    result = await jss.delete_json_schema("schema$id", synapse_client=None)
    assert result is None
    mock_client.rest_delete_async.assert_awaited_once()
