"""
The purpose of this module is to provide any functions that are needed to interact with
annotations that are not cleanly provided by the synapseclient library.
"""
import json

from dataclasses import asdict

from typing import TYPE_CHECKING, Optional
from synapseclient import Synapse
from opentelemetry import context

if TYPE_CHECKING:
    from synapseclient.models import Annotations


def set_annotations(
    annotations: "Annotations",
    synapse_client: Optional[Synapse] = None,
    opentelemetry_context: Optional[context.Context] = None,
):
    """Call to synapse and set the annotations for the given input.

    :param annotations: The annotations to set. This is expected to have the id, etag, and annotations filled in.
    :param synapse_client: If not passed in or None this will use the last client from the `.login()` method.
    :param opentelemetry_context: OpenTelemetry context to propogate to this function to use for tracing. Used
                                    cases where concurrent operations need to be linked to parent spans.
    :return: _description_
    """
    annotations_dict = asdict(annotations)

    # TODO: Is there a more elegant way to handle this - This is essentially being used
    # TODO: to remove any fields that are not expected by the REST API.
    filtered_dict = {
        k: v for k, v in annotations_dict.items() if v is not None and k != "is_loaded"
    }

    # TODO: This `restPUT` returns back a dict (or string) - Could we use:
    # TODO: https://github.com/konradhalas/dacite to convert the dict to an object?
    return Synapse.get_client(synapse_client=synapse_client).restPUT(
        f"/entity/{annotations.id}/annotations2",
        body=json.dumps(filtered_dict),
        opentelemetry_context=opentelemetry_context,
    )
