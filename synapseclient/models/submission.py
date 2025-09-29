from collections import OrderedDict
from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional, Protocol, TypeVar, Union

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.core.async_utils import async_to_sync
from synapseclient.core.constants import concrete_types
from synapseclient.core.utils import delete_none_keys
from synapseclient.models import Activity, Annotations
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.mixins.table_components import (
    DeleteMixin, 
    GetMixin,
)


class SubmissionSynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for Submission operations."""

    def get(
        self,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """
        Retrieve a Submission from Synapse.

        Arguments:
            include_activity: Whether to include the activity in the returned submission.
                Defaults to False. Setting this to True will include the activity
                record associated with this submission.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Submission instance retrieved from Synapse.

        Example: Retrieving a submission by ID.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            submission = Submission(id="syn1234").get()
            print(submission)
            ```
        """
        return self

    def delete(self, *, synapse_client: Optional[Synapse] = None) -> None:
        """
        Delete a Submission from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Example: Delete a submission.
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            submission = Submission(id="syn1234")
            submission.delete()
            print("Deleted Submission.")
            ```
        """
        pass


@dataclass
@async_to_sync
class Submission(
    SubmissionSynchronousProtocol,
    AccessControllable,
    GetMixin,
    DeleteMixin,
):
    """A `Submission` object represents a Synapse Submission, which is created when a user
    submits an entity to an evaluation queue.
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/evaluation/model/Submission.html>

    Attributes:
        id: The unique ID of this Submission.
        user_id: The ID of the user that submitted this Submission.
        submitter_alias: The name of the user that submitted this Submission.
        entity_id: The ID of the entity being submitted.
        version_number: The version number of the entity at submission.
        evaluation_id: The ID of the Evaluation to which this Submission belongs.
        name: The name of this Submission.
        created_on: The date this Submission was created.
        team_id: The ID of the team that submitted this submission (if it's a team submission).
        contributors: User IDs of team members who contributed to this submission (if it's a team submission).
        submission_status: The status of this Submission.
        entity_bundle_json: The bundled entity information at submission. This includes the entity, annotations,
            file handles, and other metadata.
        docker_repository_name: For Docker repositories, the repository name.
        docker_digest: For Docker repositories, the digest of the submitted Docker image.
        activity: The Activity model represents the main record of Provenance in Synapse.
        
    Example: Retrieve a Submission.
        ```python
        from synapseclient import Synapse
        from synapseclient.models import Submission

        syn = Synapse()
        syn.login()

        submission = Submission(id="syn123456").get()
        print(submission)
        ```
    """

    id: Optional[str] = None
    """
    The unique ID of this Submission.
    """

    user_id: Optional[str] = None
    """
    The ID of the user that submitted this Submission.
    """

    submitter_alias: Optional[str] = None
    """
    The name of the user that submitted this Submission.
    """

    entity_id: Optional[str] = None
    """
    The ID of the entity being submitted.
    """

    version_number: Optional[int] = field(default=None, compare=False)
    """
    The version number of the entity at submission.
    """

    evaluation_id: Optional[str] = None
    """
    The ID of the Evaluation to which this Submission belongs.
    """

    name: Optional[str] = None
    """
    The name of this Submission.
    """

    created_on: Optional[str] = field(default=None, compare=False)
    """
    The date this Submission was created.
    """

    team_id: Optional[str] = None
    """
    The ID of the team that submitted this submission (if it's a team submission).
    """

    contributors: List[str] = field(default_factory=list)
    """
    User IDs of team members who contributed to this submission (if it's a team submission).
    """

    submission_status: Optional[Dict] = None
    """
    The status of this Submission.
    """

    entity_bundle_json: Optional[str] = None
    """
    The bundled entity information at submission. This includes the entity, annotations,
    file handles, and other metadata.
    """

    docker_repository_name: Optional[str] = None
    """
    For Docker repositories, the repository name.
    """

    docker_digest: Optional[str] = None
    """
    For Docker repositories, the digest of the submitted Docker image.
    """

    activity: Optional[Activity] = field(default=None, compare=False)
    """The Activity model represents the main record of Provenance in Synapse. It is
    analogous to the Activity defined in the
    [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance."""

    _last_persistent_instance: Optional["Submission"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""
