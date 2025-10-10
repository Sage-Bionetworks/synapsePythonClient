from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Dict, List, Optional, Protocol, Union

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.api import submission_services
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.utils import delete_none_keys
from synapseclient.models import Annotations
from synapseclient.models.mixins.access_control import AccessControllable


class SubmissionStatusSynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for SubmissionStatus operations."""

    def get(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """
        Retrieve a SubmissionStatus from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The SubmissionStatus instance retrieved from Synapse.

        Example: Retrieving a submission status by ID.
            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionStatus

            syn = Synapse()
            syn.login()

            status = SubmissionStatus(id="syn1234").get()
            print(status)
            ```
        """
        return self

    def store(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """
        Store (update) the SubmissionStatus in Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The updated SubmissionStatus instance.

        Example: Update a submission status.
            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionStatus

            syn = Synapse()
            syn.login()

            status = SubmissionStatus(id="syn1234").get()
            status.status = "SCORED"
            status = status.store()
            print("Updated SubmissionStatus.")
            ```
        """
        return self

    @staticmethod
    def get_all_submission_statuses(
        evaluation_id: str,
        status: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict:
        """
        Gets a collection of SubmissionStatuses to a specified Evaluation.

        Arguments:
            evaluation_id: The ID of the specified Evaluation.
            status: Optionally filter submission statuses by status.
            limit: Limits the number of entities that will be fetched for this page.
                   When null it will default to 10, max value 100. Default to 10.
            offset: The offset index determines where this page will start from.
                    An index of 0 is the first entity. Default to 0.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A PaginatedResults<SubmissionStatus> object as a JSON dict containing
            a paginated list of submission statuses for the evaluation queue.

        Example: Getting all submission statuses for an evaluation
            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionStatus

            syn = Synapse()
            syn.login()

            response = SubmissionStatus.get_all_submission_statuses(
                evaluation_id="9614543",
                status="SCORED",
                limit=50
            )
            print(f"Found {len(response['results'])} submission statuses")
            ```
        """
        return {}

    @staticmethod
    def batch_update_submission_statuses(
        evaluation_id: str,
        statuses: List["SubmissionStatus"],
        is_first_batch: bool = True,
        is_last_batch: bool = True,
        batch_token: Optional[str] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict:
        """
        Update multiple SubmissionStatuses. The maximum batch size is 500.

        Arguments:
            evaluation_id: The ID of the Evaluation to which the SubmissionStatus objects belong.
            statuses: List of SubmissionStatus objects to update.
            is_first_batch: Boolean indicating if this is the first batch in the series. Default True.
            is_last_batch: Boolean indicating if this is the last batch in the series. Default True.
            batch_token: Token from previous batch response (required for all but first batch).
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A BatchUploadResponse object as a JSON dict containing the batch token
            and other response information.

        Example: Batch update submission statuses
            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionStatus

            syn = Synapse()
            syn.login()

            # Prepare list of status updates
            statuses = [
                SubmissionStatus(id="syn1", status="SCORED", submission_annotations={"score": [90.0]}),
                SubmissionStatus(id="syn2", status="SCORED", submission_annotations={"score": [85.0]})
            ]

            response = SubmissionStatus.batch_update_submission_statuses(
                evaluation_id="9614543",
                statuses=statuses,
                is_first_batch=True,
                is_last_batch=True
            )
            print(f"Batch update completed: {response}")
            ```
        """
        return {}


@dataclass
@async_to_sync
class SubmissionStatus(
    SubmissionStatusSynchronousProtocol,
    AccessControllable,
):
    """A SubmissionStatus is a secondary, mutable object associated with a Submission.
    This object should be used to contain scoring data about the Submission.
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/evaluation/model/SubmissionStatus.html>

    Attributes:
        id: The unique, immutable Synapse ID of the Submission.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. The eTag changes every time a SubmissionStatus is updated;
            it is used to detect when a client's copy of a SubmissionStatus is out-of-date.
        modified_on: The date on which this SubmissionStatus was last modified.
        status: The possible states of a Synapse Submission (e.g., RECEIVED, VALIDATED, SCORED).
        score: This field is deprecated and should not be used. Use the 'submission_annotations' field instead.
        report: This field is deprecated and should not be used. Use the 'submission_annotations' field instead.
        annotations: Primary container object for Annotations on a Synapse object.
        submission_annotations: Annotations are additional key-value pair metadata that are associated with an object.
        entity_id: The Synapse ID of the Entity in this Submission.
        version_number: The version number of the Entity in this Submission.
        status_version: A version of the status, auto-generated and auto-incremented by the system and read-only to the client.
        can_cancel: Can this submission be cancelled? By default, this will be set to False. Users can read this value.
            Only the queue's scoring application can change this value.
        cancel_requested: Has user requested to cancel this submission? By default, this will be set to False.
            Submission owner can read and request to change this value.

    Example: Retrieve and update a SubmissionStatus.
        ```python
        from synapseclient import Synapse
        from synapseclient.models import SubmissionStatus

        syn = Synapse()
        syn.login()

        # Get a submission status
        status = SubmissionStatus(id="syn123456").get()
        
        # Update the status
        status.status = "SCORED"
        status.submission_annotations = {"score": [85.5], "feedback": ["Good work!"]}
        status = status.store()
        print(status)
        ```
    """

    id: Optional[str] = None
    """
    The unique, immutable Synapse ID of the Submission.
    """

    etag: Optional[str] = field(default=None, compare=False)
    """
    Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. The eTag changes every time a SubmissionStatus is updated;
    it is used to detect when a client's copy of a SubmissionStatus is out-of-date.
    """

    modified_on: Optional[str] = field(default=None, compare=False)
    """
    The date on which this SubmissionStatus was last modified.
    """

    status: Optional[str] = None
    """
    The possible states of a Synapse Submission (e.g., RECEIVED, VALIDATED, SCORED).
    """

    score: Optional[float] = None
    """
    This field is deprecated and should not be used. Use the 'submission_annotations' field instead.
    """

    report: Optional[str] = None
    """
    This field is deprecated and should not be used. Use the 'submission_annotations' field instead.
    """

    annotations: Optional[
        Dict[
            str,
            Union[
                List[str],
                List[bool],
                List[float],
                List[int],
                List[date],
                List[datetime],
            ],
        ]
    ] = field(default_factory=dict, compare=False)
    """Primary container object for Annotations on a Synapse object."""

    submission_annotations: Optional[
        Dict[
            str,
            Union[
                List[str],
                List[bool],
                List[float],
                List[int],
                List[date],
                List[datetime],
            ],
        ]
    ] = field(default_factory=dict, compare=False)
    """Annotations are additional key-value pair metadata that are associated with an object."""

    entity_id: Optional[str] = None
    """
    The Synapse ID of the Entity in this Submission.
    """

    version_number: Optional[int] = field(default=None, compare=False)
    """
    The version number of the Entity in this Submission.
    """

    status_version: Optional[int] = field(default=None, compare=False)
    """
    A version of the status, auto-generated and auto-incremented by the system and read-only to the client.
    """

    can_cancel: Optional[bool] = field(default=False, compare=False)
    """
    Can this submission be cancelled? By default, this will be set to False. Users can read this value.
    Only the queue's scoring application can change this value.
    """

    cancel_requested: Optional[bool] = field(default=False, compare=False)
    """
    Has user requested to cancel this submission? By default, this will be set to False.
    Submission owner can read and request to change this value.
    """

    _last_persistent_instance: Optional["SubmissionStatus"] = field(
        default=None, repr=False, compare=False
    )
    """The last persistent instance of this object. This is used to determine if the
    object has been changed and needs to be updated in Synapse."""

    def has_changed(self) -> bool:
        """Determines if the object has been changed and needs to be updated in Synapse."""
        return (
            not self._last_persistent_instance or self._last_persistent_instance != self
        )

    def _set_last_persistent_instance(self) -> None:
        """Stash the last time this object interacted with Synapse. This is used to
        determine if the object has been changed and needs to be updated in Synapse."""
        import dataclasses
        del self._last_persistent_instance
        self._last_persistent_instance = dataclasses.replace(self)

    def fill_from_dict(
        self, synapse_submission_status: Dict[str, Union[bool, str, int, float, List]]
    ) -> "SubmissionStatus":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_submission_status: The response from the REST API.

        Returns:
            The SubmissionStatus object.
        """
        self.id = synapse_submission_status.get("id", None)
        self.etag = synapse_submission_status.get("etag", None)
        self.modified_on = synapse_submission_status.get("modifiedOn", None)
        self.status = synapse_submission_status.get("status", None)
        self.score = synapse_submission_status.get("score", None)
        self.report = synapse_submission_status.get("report", None)
        self.entity_id = synapse_submission_status.get("entityId", None)
        self.version_number = synapse_submission_status.get("versionNumber", None)
        self.status_version = synapse_submission_status.get("statusVersion", None)
        self.can_cancel = synapse_submission_status.get("canCancel", False)
        self.cancel_requested = synapse_submission_status.get("cancelRequested", False)

        # Handle annotations
        annotations_dict = synapse_submission_status.get("annotations", {})
        if annotations_dict:
            self.annotations = Annotations.from_dict(annotations_dict)

        # Handle submission annotations
        submission_annotations_dict = synapse_submission_status.get("submissionAnnotations", {})
        if submission_annotations_dict:
            self.submission_annotations = Annotations.from_dict(submission_annotations_dict)

        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"SubmissionStatus_Get: {self.id}"
    )
    async def get_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "SubmissionStatus":
        """
        Retrieve a SubmissionStatus from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The SubmissionStatus instance retrieved from Synapse.

        Raises:
            ValueError: If the submission status does not have an ID to get.

        Example: Retrieving a submission status by ID
            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionStatus

            syn = Synapse()
            syn.login()

            status = await SubmissionStatus(id="syn1234").get_async()
            print(status)
            ```
        """
        if not self.id:
            raise ValueError("The submission status must have an ID to get.")

        response = await submission_services.get_submission_status(
            submission_id=self.id,
            synapse_client=synapse_client
        )

        self.fill_from_dict(response)
        self._set_last_persistent_instance()
        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"SubmissionStatus_Store: {self.id if self.id else 'new_status'}"
    )
    async def store_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "SubmissionStatus":
        """
        Store (update) the SubmissionStatus in Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The updated SubmissionStatus object.

        Raises:
            ValueError: If the submission status is missing required fields.

        Example: Update a submission status
            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionStatus

            syn = Synapse()
            syn.login()

            # Get existing status
            status = await SubmissionStatus(id="syn1234").get_async()
            
            # Update fields
            status.status = "SCORED"
            status.submission_annotations = {"score": [85.5]}
            
            # Store the update
            status = await status.store_async()
            print(f"Updated status: {status.status}")
            ```
        """
        if not self.id:
            raise ValueError("The submission status must have an ID to update.")

        # Prepare request body
        request_body = delete_none_keys({
            "id": self.id,
            "etag": self.etag,
            "status": self.status,
            "score": self.score,
            "report": self.report,
            "entityId": self.entity_id,
            "versionNumber": self.version_number,
            "canCancel": self.can_cancel,
            "cancelRequested": self.cancel_requested,
        })

        # Add annotations if present
        if self.annotations:
            # Convert annotations to the format expected by the API
            request_body["annotations"] = self.annotations

        # Add submission annotations if present
        if self.submission_annotations:
            # Convert submission annotations to the format expected by the API
            request_body["submissionAnnotations"] = self.submission_annotations

        # Update the submission status using the service
        response = await submission_services.update_submission_status(
            submission_id=self.id,
            request_body=request_body,
            synapse_client=synapse_client
        )

        # Update this object with the response
        self.fill_from_dict(response)
        self._set_last_persistent_instance()
        return self

    @staticmethod
    async def get_all_submission_statuses_async(
        evaluation_id: str,
        status: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict:
        """
        Gets a collection of SubmissionStatuses to a specified Evaluation.

        Arguments:
            evaluation_id: The ID of the specified Evaluation.
            status: Optionally filter submission statuses by status.
            limit: Limits the number of entities that will be fetched for this page.
                   When null it will default to 10, max value 100. Default to 10.
            offset: The offset index determines where this page will start from.
                    An index of 0 is the first entity. Default to 0.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A PaginatedResults<SubmissionStatus> object as a JSON dict containing
            a paginated list of submission statuses for the evaluation queue.

        Example: Getting all submission statuses for an evaluation
            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionStatus

            syn = Synapse()
            syn.login()

            response = await SubmissionStatus.get_all_submission_statuses_async(
                evaluation_id="9614543",
                status="SCORED",
                limit=50
            )
            print(f"Found {len(response['results'])} submission statuses")
            ```
        """
        return await submission_services.get_all_submission_statuses(
            evaluation_id=evaluation_id,
            status=status,
            limit=limit,
            offset=offset,
            synapse_client=synapse_client
        )

    @staticmethod
    async def batch_update_submission_statuses_async(
        evaluation_id: str,
        statuses: List["SubmissionStatus"],
        is_first_batch: bool = True,
        is_last_batch: bool = True,
        batch_token: Optional[str] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict:
        """
        Update multiple SubmissionStatuses. The maximum batch size is 500.

        Arguments:
            evaluation_id: The ID of the Evaluation to which the SubmissionStatus objects belong.
            statuses: List of SubmissionStatus objects to update.
            is_first_batch: Boolean indicating if this is the first batch in the series. Default True.
            is_last_batch: Boolean indicating if this is the last batch in the series. Default True.
            batch_token: Token from previous batch response (required for all but first batch).
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A BatchUploadResponse object as a JSON dict containing the batch token
            and other response information.

        Example: Batch update submission statuses
            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionStatus

            syn = Synapse()
            syn.login()

            # Prepare list of status updates
            statuses = [
                SubmissionStatus(id="syn1", status="SCORED", submission_annotations={"score": [90.0]}),
                SubmissionStatus(id="syn2", status="SCORED", submission_annotations={"score": [85.0]})
            ]

            response = await SubmissionStatus.batch_update_submission_statuses_async(
                evaluation_id="9614543",
                statuses=statuses,
                is_first_batch=True,
                is_last_batch=True
            )
            print(f"Batch update completed: {response}")
            ```
        """
        # Convert SubmissionStatus objects to dictionaries
        status_dicts = []
        for status in statuses:
            status_dict = delete_none_keys({
                "id": status.id,
                "etag": status.etag,
                "status": status.status,
                "score": status.score,
                "report": status.report,
                "entityId": status.entity_id,
                "versionNumber": status.version_number,
                "canCancel": status.can_cancel,
                "cancelRequested": status.cancel_requested,
            })

            # Add annotations if present
            if status.annotations:
                status_dict["annotations"] = status.annotations

            # Add submission annotations if present
            if status.submission_annotations:
                status_dict["submissionAnnotations"] = status.submission_annotations

            status_dicts.append(status_dict)

        # Prepare the batch request body
        request_body = {
            "statuses": status_dicts,
            "isFirstBatch": is_first_batch,
            "isLastBatch": is_last_batch,
        }

        # Add batch token if provided (required for all but first batch)
        if batch_token:
            request_body["batchToken"] = batch_token

        return await submission_services.batch_update_submission_statuses(
            evaluation_id=evaluation_id,
            request_body=request_body,
            synapse_client=synapse_client
        )
