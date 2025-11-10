from dataclasses import dataclass, field
from typing import Dict, List, Optional, Protocol, Union

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.api import evaluation_services
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.models.mixins.access_control import AccessControllable
from synapseclient.models.mixins.table_components import DeleteMixin, GetMixin


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

    # TODO
    activity: Optional[Dict] = field(default=None, compare=False)
    """The Activity model represents the main record of Provenance in Synapse. It is
    analogous to the Activity defined in the
    [W3C Specification](https://www.w3.org/TR/prov-n/) on Provenance."""

    _last_persistent_instance: Optional["Submission"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    def fill_from_dict(
        self, synapse_submission: Dict[str, Union[bool, str, int, List]]
    ) -> "Submission":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_submission: The response from the REST API.

        Returns:
            The Submission object.
        """
        self.id = synapse_submission.get("id", None)
        self.user_id = synapse_submission.get("userId", None)
        self.submitter_alias = synapse_submission.get("submitterAlias", None)
        self.entity_id = synapse_submission.get("entityId", None)
        self.version_number = synapse_submission.get("versionNumber", None)
        self.evaluation_id = synapse_submission.get("evaluationId", None)
        self.name = synapse_submission.get("name", None)
        self.created_on = synapse_submission.get("createdOn", None)
        self.team_id = synapse_submission.get("teamId", None)
        self.contributors = synapse_submission.get("contributors", [])
        self.submission_status = synapse_submission.get("submissionStatus", None)
        self.entity_bundle_json = synapse_submission.get("entityBundleJSON", None)
        self.docker_repository_name = synapse_submission.get(
            "dockerRepositoryName", None
        )
        self.docker_digest = synapse_submission.get("dockerDigest", None)

        activity_dict = synapse_submission.get("activity", None)
        if activity_dict:
            # TODO: Implement Activity class and its fill_from_dict method
            self.activity = {}

        return self

    def to_synapse_request(self) -> Dict:
        """Creates a request body expected of the Synapse REST API for the Submission model.

        Returns:
            A dictionary containing the request body for creating a submission.

        Raises:
            ValueError: If any required attributes are missing.
        """
        # These attributes are required for creating a submission
        required_attributes = ["entity_id", "evaluation_id"]

        for attribute in required_attributes:
            if not getattr(self, attribute):
                raise ValueError(
                    f"Your submission object is missing the '{attribute}' attribute. This attribute is required to create a submission"
                )

        # Build a request body for creating a submission
        request_body = {
            "entityId": self.entity_id,
            "evaluationId": self.evaluation_id,
        }

        # Add optional fields if they are set
        if self.name is not None:
            request_body["name"] = self.name
        if self.team_id is not None:
            request_body["teamId"] = self.team_id
        if self.contributors:
            request_body["contributors"] = self.contributors
        if self.docker_repository_name is not None:
            request_body["dockerRepositoryName"] = self.docker_repository_name
        if self.docker_digest is not None:
            request_body["dockerDigest"] = self.docker_digest

        return request_body

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Submission_Store: {self.id if self.id else 'new_submission'}"
    )
    async def store_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Submission":
        """
        Store the submission in Synapse. This creates a new submission in an evaluation queue.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Submission object with the ID set.

        Raises:
            ValueError: If the submission is missing required fields.

        Example: Creating a submission
            &nbsp;
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            submission = Submission(
                entity_id="syn123456",
                evaluation_id="9614543",
                name="My Submission"
            )
            submission = await submission.store_async()
            print(submission.id)
            ```
        """
        # Create the submission using the new to_synapse_request method
        request_body = self.to_synapse_request()

        # Create the submission using the service
        response = await evaluation_services.create_submission(
            request_body=request_body, synapse_client=synapse_client
        )

        # Update this object with the response
        self.fill_from_dict(response)
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Submission_Get: {self.id}"
    )
    async def get_async(
        self,
        include_activity: bool = False,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Submission":
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

        Raises:
            ValueError: If the submission does not have an ID to get.

        Example: Retrieving a submission by ID
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            submission = await Submission(id="syn1234").get_async()
            print(submission)
            ```
        """
        if not self.id:
            raise ValueError("The submission must have an ID to get.")

        # Get the submission using the service
        response = await evaluation_services.get_submission(
            submission_id=self.id, synapse_client=synapse_client
        )

        # Update this object with the response
        self.fill_from_dict(response)

        # Handle activity if requested
        if include_activity and self.activity:
            # The activity should be included in the response by default
            # but if we need to fetch it separately, we would do it here
            pass

        return self

    @staticmethod
    async def get_evaluation_submissions_async(
        evaluation_id: str,
        status: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict:
        """
        Retrieves all Submissions for a specified Evaluation queue.

        Arguments:
            evaluation_id: The ID of the evaluation queue.
            status: Optionally filter submissions by a submission status, such as SCORED, VALID,
                    INVALID, OPEN, CLOSED or EVALUATION_IN_PROGRESS.
            limit: Limits the number of submissions in a single response. Default to 20.
            offset: The offset index determines where this page will start from.
                    An index of 0 is the first submission. Default to 0.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A response JSON containing a paginated list of submissions for the evaluation queue.

        Example: Getting submissions for an evaluation
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            response = await Submission.get_evaluation_submissions_async(
                evaluation_id="9614543",
                status="SCORED",
                limit=10
            )
            print(f"Found {len(response['results'])} submissions")
            ```
        """
        return await evaluation_services.get_evaluation_submissions(
            evaluation_id=evaluation_id,
            status=status,
            limit=limit,
            offset=offset,
            synapse_client=synapse_client,
        )

    @staticmethod
    async def get_user_submissions_async(
        evaluation_id: str,
        user_id: Optional[str] = None,
        limit: int = 20,
        offset: int = 0,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict:
        """
        Retrieves Submissions for a specified Evaluation queue and user.
        If user_id is omitted, this returns the submissions of the caller.

        Arguments:
            evaluation_id: The ID of the evaluation queue.
            user_id: Optionally specify the ID of the user whose submissions will be returned.
                    If omitted, this returns the submissions of the caller.
            limit: Limits the number of submissions in a single response. Default to 20.
            offset: The offset index determines where this page will start from.
                    An index of 0 is the first submission. Default to 0.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A response JSON containing a paginated list of user submissions for the evaluation queue.

        Example: Getting user submissions
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            response = await Submission.get_user_submissions_async(
                evaluation_id="9614543",
                user_id="123456",
                limit=10
            )
            print(f"Found {len(response['results'])} user submissions")
            ```
        """
        return await evaluation_services.get_user_submissions(
            evaluation_id=evaluation_id,
            user_id=user_id,
            limit=limit,
            offset=offset,
            synapse_client=synapse_client,
        )

    @staticmethod
    async def get_submission_count_async(
        evaluation_id: str,
        status: Optional[str] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict:
        """
        Gets the number of Submissions for a specified Evaluation queue, optionally filtered by submission status.

        Arguments:
            evaluation_id: The ID of the evaluation queue.
            status: Optionally filter submissions by a submission status, such as SCORED, VALID,
                    INVALID, OPEN, CLOSED or EVALUATION_IN_PROGRESS.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A response JSON containing the submission count.

        Example: Getting submission count
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            response = await Submission.get_submission_count_async(
                evaluation_id="9614543",
                status="SCORED"
            )
            print(f"Found {response['count']} submissions")
            ```
        """
        return await evaluation_services.get_submission_count(
            evaluation_id=evaluation_id, status=status, synapse_client=synapse_client
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Submission_Delete: {self.id}"
    )
    async def delete_submission_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Delete a Submission from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Raises:
            ValueError: If the submission does not have an ID to delete.

        Example: Delete a submission
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            submission = Submission(id="syn1234")
            await submission.delete_submission_async()
            print("Deleted Submission.")
            ```
        """
        if not self.id:
            raise ValueError("The submission must have an ID to delete.")

        await evaluation_services.delete_submission(
            submission_id=self.id, synapse_client=synapse_client
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Submission_Cancel: {self.id}"
    )
    async def cancel_submission_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Submission":
        """
        Cancel a Submission. Only the user who created the Submission may cancel it.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The updated Submission object.

        Raises:
            ValueError: If the submission does not have an ID to cancel.

        Example: Cancel a submission
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            submission = Submission(id="syn1234")
            canceled_submission = await submission.cancel_submission_async()
            print(f"Canceled submission: {canceled_submission.id}")
            ```
        """
        if not self.id:
            raise ValueError("The submission must have an ID to cancel.")

        response = await evaluation_services.cancel_submission(
            submission_id=self.id, synapse_client=synapse_client
        )

        # Update this object with the response
        self.fill_from_dict(response)
        return self
