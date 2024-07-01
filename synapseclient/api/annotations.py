"""
The purpose of this module is to provide any functions that are needed to interact with
annotations that are not cleanly provided by the synapseclient library.
"""

import json
from dataclasses import asdict
from typing import TYPE_CHECKING, Optional

from synapseclient.annotations import _convert_to_annotations_list

if TYPE_CHECKING:
    from synapseclient import Synapse
    from synapseclient.models import Annotations


def set_annotations(
    annotations: "Annotations",
    *,
    synapse_client: Optional["Synapse"] = None,
):
    """Call to synapse and set the annotations for the given input.

    Arguments:
        annotations: The annotations to set. This is expected to have the id, etag,
            and annotations filled in.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns: The annotations set in Synapse.
    """
    annotations_dict = asdict(annotations)

    synapse_annotations = _convert_to_annotations_list(
        annotations_dict["annotations"] or {}
    )
    from synapseclient import Synapse

    return Synapse.get_client(synapse_client=synapse_client).restPUT(
        f"/entity/{annotations.id}/annotations2",
        body=json.dumps(
            {
                "id": annotations.id,
                "etag": annotations.etag,
                "annotations": synapse_annotations,
            }
        ),
    )


async def set_annotations_async(
    annotations: "Annotations",
    *,
    synapse_client: Optional["Synapse"] = None,
):
    """Call to synapse and set the annotations for the given input.

    Arguments:
        annotations: The annotations to set. This is expected to have the id, etag,
            and annotations filled in.
        synapse_client: If not passed in or None this will use the last client from
            the `.login()` method.

    Returns: The annotations set in Synapse.
    """
    annotations_dict = asdict(annotations)

    synapse_annotations = _convert_to_annotations_list(
        annotations_dict["annotations"] or {}
    )
    from synapseclient import Synapse

    return await Synapse.get_client(synapse_client=synapse_client).rest_put_async(
        f"/entity/{annotations.id}/annotations2",
        body=json.dumps(
            {
                "id": annotations.id,
                "etag": annotations.etag,
                "annotations": synapse_annotations,
            }
        ),
    )
