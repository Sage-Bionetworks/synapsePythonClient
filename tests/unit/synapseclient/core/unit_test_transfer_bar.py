"""Unit tests for the silent-aware progress bar factory in transfer_bar."""

from unittest.mock import patch

from synapseclient import Synapse
from synapseclient.core.exceptions import SynapseError
from synapseclient.core.transfer_bar import create_progress_bar


class TestCreateProgressBar:
    """Tests for :func:`create_progress_bar`."""

    def test_disabled_when_silent(self, syn: Synapse) -> None:
        # GIVEN a client running in silent mode
        with patch.object(syn, "silent", True):
            # WHEN a progress bar is created
            progress_bar = create_progress_bar(
                total=100, desc="Uploading", synapse_client=syn
            )

            # THEN the bar is disabled so it renders nothing
            assert progress_bar.disable is True

    def test_enabled_when_not_silent(self, syn: Synapse) -> None:
        # GIVEN a client that is not silent
        with patch.object(syn, "silent", False):
            # WHEN a progress bar is created
            progress_bar = create_progress_bar(
                total=100, desc="Uploading", synapse_client=syn
            )

            # THEN the bar is enabled with the standardized configuration
            assert progress_bar.disable is False
            assert progress_bar.total == 100
            assert progress_bar.desc == "Uploading"

    def test_disabled_bar_accepts_mutation(self, syn: Synapse) -> None:
        # GIVEN a disabled (silent) progress bar
        with patch.object(syn, "silent", True):
            progress_bar = create_progress_bar(total=None, desc="", synapse_client=syn)

            # WHEN callers mutate and drive the bar directly (as the live sites do)
            progress_bar.desc = "processing"
            progress_bar.total = 50
            progress_bar.update(25)
            progress_bar.refresh()
            progress_bar.close()

            # THEN no error is raised and nothing is rendered
            assert progress_bar.disable is True

    def test_shows_bar_when_no_client_available(self) -> None:
        # GIVEN no client is passed and none can be resolved
        with patch.object(Synapse, "get_client", side_effect=SynapseError("no client")):
            # WHEN a progress bar is created without a client
            progress_bar = create_progress_bar(total=100, desc="Uploading")

            # THEN the bar is shown rather than propagating the error
            assert progress_bar.disable is False
