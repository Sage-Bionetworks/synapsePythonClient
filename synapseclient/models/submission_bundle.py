from dataclasses import dataclass
from typing import TYPE_CHECKING, Dict, List, Optional, Protocol, Union

from synapseclient import Synapse
from synapseclient.api import evaluation_services
from synapseclient.core.async_utils import async_to_sync

if TYPE_CHECKING:
    from synapseclient.models.submission import Submission
    from synapseclient.models.submission_status import SubmissionStatus


class SubmissionBundleSynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for SubmissionBundle operations."""

    @staticmethod
    def get_evaluation_submission_bundles(
        evaluation_id: str,
        status: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List["SubmissionBundle"]:
        """
        Gets a collection of bundled Submissions and SubmissionStatuses to a given Evaluation.

        Arguments:
            evaluation_id: The ID of the specified Evaluation.
            status: Optionally filter submission bundles by status.
            limit: Limits the number of entities that will be fetched for this page.
                   When null it will default to 10, max value 100. Default to 10.
            offset: The offset index determines where this page will start from.
                    An index of 0 is the first entity. Default to 0.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A list of SubmissionBundle objects containing the submission bundles
            for the evaluation queue.

        Note:
            The caller must be granted the ACCESS_TYPE.READ_PRIVATE_SUBMISSION on the specified Evaluation.

        Example: Getting submission bundles for an evaluation
            &nbsp;
            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionBundle

            syn = Synapse()
            syn.login()

            bundles = SubmissionBundle.get_evaluation_submission_bundles(
                evaluation_id="9614543",
                status="SCORED",
                limit=50
            )
            print(f"Found {len(bundles)} submission bundles")
            for bundle in bundles:
                print(f"Submission ID: {bundle.submission.id if bundle.submission else 'N/A'}")
            ```
        """
        return []

    @staticmethod
    def get_user_submission_bundles(
        evaluation_id: str,
        limit: int = 10,
        offset: int = 0,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List["SubmissionBundle"]:
        """
        Gets the requesting user's bundled Submissions and SubmissionStatuses to a specified Evaluation.

        Arguments:
            evaluation_id: The ID of the specified Evaluation.
            limit: Limits the number of entities that will be fetched for this page.
                   When null it will default to 10. Default to 10.
            offset: The offset index determines where this page will start from.
                    An index of 0 is the first entity. Default to 0.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A list of SubmissionBundle objects containing the requesting user's
            submission bundles for the evaluation queue.

        Example: Getting user submission bundles
            &nbsp;
            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionBundle

            syn = Synapse()
            syn.login()

            bundles = SubmissionBundle.get_user_submission_bundles(
                evaluation_id="9999999",
                limit=25
            )
            print(f"Found {len(bundles)} user submission bundles")
            for bundle in bundles:
                print(f"Submission ID: {bundle.submission.id}")
            ```
        """
        return []


@dataclass
@async_to_sync
class SubmissionBundle(SubmissionBundleSynchronousProtocol):
    """A `SubmissionBundle` object represents a bundle containing a Synapse Submission
    and its accompanying SubmissionStatus. This bundle provides convenient access to both
    the submission data and its current status in a single object.

    <https://rest-docs.synapse.org/rest/org/sagebionetworks/evaluation/model/SubmissionBundle.html>

    Attributes:
        submission: A Submission to a Synapse Evaluation is a pointer to a versioned Entity.
            Submissions are immutable, so we archive a copy of the EntityBundle at the time of submission.
        submission_status: A SubmissionStatus is a secondary, mutable object associated with a Submission.
            This object should be used to contain scoring data about the Submission.

    Example: Retrieve submission bundles for an evaluation.
        &nbsp;
        ```python
        from synapseclient import Synapse
        from synapseclient.models import SubmissionBundle

        syn = Synapse()
        syn.login()

        # Get all submission bundles for an evaluation
        bundles = SubmissionBundle.get_evaluation_submission_bundles(
            evaluation_id="9614543",
            status="SCORED"
        )

        for bundle in bundles:
            print(f"Submission ID: {bundle.submission.id if bundle.submission else 'N/A'}")
            print(f"Status: {bundle.submission_status.status if bundle.submission_status else 'N/A'}")
        ```

    Example: Retrieve user submission bundles for an evaluation.
        &nbsp;
        ```python
        from synapseclient import Synapse
        from synapseclient.models import SubmissionBundle

        syn = Synapse()
        syn.login()

        # Get current user's submission bundles for an evaluation
        user_bundles = SubmissionBundle.get_user_submission_bundles(
            evaluation_id="9999999",
            limit=25
        )

        for bundle in user_bundles:
            print(f"Submission ID: {bundle.submission.id}")
            print(f"Status: {bundle.submission_status.status}")
        ```
    """

    submission: Optional["Submission"] = None
    """
    A Submission to a Synapse Evaluation is a pointer to a versioned Entity.
    Submissions are immutable, so we archive a copy of the EntityBundle at the time of submission.
    """

    submission_status: Optional["SubmissionStatus"] = None
    """
    A SubmissionStatus is a secondary, mutable object associated with a Submission.
    This object should be used to contain scoring data about the Submission.
    """

    def fill_from_dict(
        self,
        synapse_submission_bundle: Dict[str, Union[bool, str, int, Dict]],
    ) -> "SubmissionBundle":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_submission_bundle: The response from the REST API.

        Returns:
            The SubmissionBundle object.
        """
        from synapseclient.models.submission import Submission
        from synapseclient.models.submission_status import SubmissionStatus

        submission_dict = synapse_submission_bundle.get("submission", None)
        if submission_dict:
            self.submission = Submission().fill_from_dict(submission_dict)
        else:
            self.submission = None

        submission_status_dict = synapse_submission_bundle.get("submissionStatus", None)
        if submission_status_dict:
            self.submission_status = SubmissionStatus().fill_from_dict(submission_status_dict)
            # Manually set evaluation_id from the submission data if available
            if self.submission_status and self.submission and self.submission.evaluation_id:
                self.submission_status.evaluation_id = self.submission.evaluation_id
        else:
            self.submission_status = None

        return self

    @staticmethod
    async def get_evaluation_submission_bundles_async(
        evaluation_id: str,
        status: Optional[str] = None,
        limit: int = 10,
        offset: int = 0,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List["SubmissionBundle"]:
        """
        Gets a collection of bundled Submissions and SubmissionStatuses to a given Evaluation.

        Arguments:
            evaluation_id: The ID of the specified Evaluation.
            status: Optionally filter submission bundles by status.
            limit: Limits the number of entities that will be fetched for this page.
                   When null it will default to 10, max value 100. Default to 10.
            offset: The offset index determines where this page will start from.
                    An index of 0 is the first entity. Default to 0.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A list of SubmissionBundle objects containing the submission bundles
            for the evaluation queue.

        Note:
            The caller must be granted the ACCESS_TYPE.READ_PRIVATE_SUBMISSION on the specified Evaluation.

        Example: Getting submission bundles for an evaluation
            &nbsp;
            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionBundle

            syn = Synapse()
            syn.login()

            bundles = await SubmissionBundle.get_evaluation_submission_bundles_async(
                evaluation_id="9999999",
                status="SCORED",
                limit=50
            )
            print(f"Found {len(bundles)} submission bundles")
            for bundle in bundles:
                print(f"Submission ID: {bundle.submission.id}")
            ```
        """
        response = await evaluation_services.get_evaluation_submission_bundles(
            evaluation_id=evaluation_id,
            status=status,
            limit=limit,
            offset=offset,
            synapse_client=synapse_client,
        )

        bundles = []
        for bundle_dict in response.get("results", []):
            bundle = SubmissionBundle()
            bundle.fill_from_dict(bundle_dict)
            bundles.append(bundle)

        return bundles

    @staticmethod
    async def get_user_submission_bundles_async(
        evaluation_id: str,
        limit: int = 10,
        offset: int = 0,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List["SubmissionBundle"]:
        """
        Gets the requesting user's bundled Submissions and SubmissionStatuses to a specified Evaluation.

        Arguments:
            evaluation_id: The ID of the specified Evaluation.
            limit: Limits the number of entities that will be fetched for this page.
                   When null it will default to 10. Default to 10.
            offset: The offset index determines where this page will start from.
                    An index of 0 is the first entity. Default to 0.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A list of SubmissionBundle objects containing the requesting user's
            submission bundles for the evaluation queue.

        Example: Getting user submission bundles
            &nbsp;

            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionBundle

            syn = Synapse()
            syn.login()

            bundles = await SubmissionBundle.get_user_submission_bundles_async(
                evaluation_id="9999999",
                limit=25
            )
            print(f"Found {len(bundles)} user submission bundles")
            for bundle in bundles:
                print(f"Submission ID: {bundle.submission.id}")
            ```
        """
        response = await evaluation_services.get_user_submission_bundles(
            evaluation_id=evaluation_id,
            limit=limit,
            offset=offset,
            synapse_client=synapse_client,
        )

        # Convert response to list of SubmissionBundle objects
        bundles = []
        for bundle_dict in response.get("results", []):
            bundle = SubmissionBundle()
            bundle.fill_from_dict(bundle_dict)
            bundles.append(bundle)

        return bundles
