"""Unit tests for synapseclient.core.download.download_functions"""

import os
from unittest.mock import patch

import pytest

from synapseclient import File, Synapse
from synapseclient.core import utils
from synapseclient.core.download import (
    download_file_entity,
    ensure_download_location_is_directory,
)


def test_ensure_download_location_is_directory() -> None:
    download_location = "/foo/bar/baz"
    with patch("synapseclient.core.download.download_functions.os") as mock_os:
        mock_os.path.isfile.return_value = False
        ensure_download_location_is_directory(download_location)

        mock_os.path.isfile.return_value = True
        with pytest.raises(ValueError):
            ensure_download_location_is_directory(download_location)


async def test_download_file_entity_correct_local_state(syn: Synapse) -> None:
    mock_cache_path = utils.normalize_path("/i/will/show/you/the/path/yi.txt")
    file_entity = File(parentId="syn123")
    file_entity.dataFileHandleId = 123
    with patch.object(syn.cache, "get", return_value=mock_cache_path):
        await download_file_entity(
            download_location=None,
            entity=file_entity,
            if_collision="overwrite.local",
            submission=None,
            synapse_client=syn,
        )
        assert mock_cache_path == utils.normalize_path(file_entity.path)
        assert os.path.dirname(mock_cache_path) == file_entity.cacheDir
        assert 1 == len(file_entity.files)
        assert os.path.basename(mock_cache_path) == file_entity.files[0]
