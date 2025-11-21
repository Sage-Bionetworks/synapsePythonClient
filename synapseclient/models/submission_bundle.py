from dataclasses import dataclass
from typing import TYPE_CHECKING, AsyncGenerator, Dict, Generator, List, Optional, Protocol, Union

from synapseclient import Synapse
from synapseclient.api import evaluation_services
from synapseclient.core.async_utils import async_to_sync, skip_async_to_sync, wrap_async_generator_to_sync_generator

if TYPE_CHECKING:
    from synapseclient.models.submission import Submission
    from synapseclient.models.submission_status import SubmissionStatus


class SubmissionBundleSynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for SubmissionBundle operations."""

    @classmethod
    def get_evaluation_submission_bundles(
        cls,
        evaluation_id: str,
        status: Optional[str] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Generator["SubmissionBundle", None, None]:
        """
        Retrieves bundled Submissions and SubmissionStatuses for a given Evaluation.

        Arguments:
            evaluation_id: The ID of the specified Evaluation.
            status: Optionally filter submission bundles by status.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            SubmissionBundle objects as they are retrieved from the API.

        Note:
            The caller must be granted the ACCESS_TYPE.READ_PRIVATE_SUBMISSION on the specified Evaluation.

        Example: Getting submission bundles for an evaluation
            &nbsp;
            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionBundle

            syn = Synapse()
            syn.login()

            bundles = list(SubmissionBundle.get_evaluation_submission_bundles(
                evaluation_id="9614543",
                status="SCORED"
            ))
            print(f"Found {len(bundles)} submission bundles")
            for bundle in bundles:
                print(f"Submission ID: {bundle.submission.id if bundle.submission else 'N/A'}")
            ```
        """
        yield from wrap_async_generator_to_sync_generator(
            async_gen_func=cls.get_evaluation_submission_bundles_async,
            evaluation_id=evaluation_id,
            status=status,
            synapse_client=synapse_client,
        )

    @classmethod
    def get_user_submission_bundles(
        cls,
        evaluation_id: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Generator["SubmissionBundle", None, None]:
        """
        Retrieves all user bundled Submissions and SubmissionStatuses for a specified Evaluation.

        Arguments:
            evaluation_id: The ID of the specified Evaluation.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            SubmissionBundle objects as they are retrieved from the API.

        Example: Getting user submission bundles
            &nbsp;
            ```python
            from synapseclient import Synapse
            from synapseclient.models import SubmissionBundle

            syn = Synapse()
            syn.login()

            bundles = list(SubmissionBundle.get_user_submission_bundles(
                evaluation_id="9999999"
            ))
            print(f"Found {len(bundles)} user submission bundles")
            for bundle in bundles:
                print(f"Submission ID: {bundle.submission.id}")
            ```
        """
        yield from wrap_async_generator_to_sync_generator(
            async_gen_func=cls.get_user_submission_bundles_async,
            evaluation_id=evaluation_id,
            synapse_client=synapse_client,
        )


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
            self.submission_status = SubmissionStatus().fill_from_dict(
                submission_status_dict
            )
            # Manually set evaluation_id from the submission data if available
            if (
                self.submission_status
                and self.submission
                and self.submission.evaluation_id
            ):
                self.submission_status.evaluation_id = self.submission.evaluation_id
        else:
            self.submission_status = None

        return self

    @skip_async_to_sync
    @classmethod
    async def get_evaluation_submission_bundles_async(
        cls,
        evaluation_id: str,
        status: Optional[str] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> AsyncGenerator["SubmissionBundle", None]:
        """
        Generator to get all bundled Submissions and SubmissionStatuses for a given Evaluation.

        Arguments:
            evaluation_id: The ID of the specified Evaluation.
            status: Optionally filter submission bundles by status.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Yields:
            Individual SubmissionBundle objects from each page of the response.

        Note:
            The caller must be granted the ACCESS_TYPE.READ_PRIVATE_SUBMISSION on the specified Evaluation.

        Example: Getting submission bundles for an evaluation
            &nbsp;
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import SubmissionBundle

            syn = Synapse()
            syn.login()

            async def get_submission_bundles_example():
                bundles = []
                async for bundle in SubmissionBundle.get_evaluation_submission_bundles_async(
                    evaluation_id="9999999",
                    status="SCORED"
                ):
                    bundles.append(bundle)
                print(f"Found {len(bundles)} submission bundles")
                for bundle in bundles:
                    print(f"Submission ID: {bundle.submission.id}")

            asyncio.run(get_submission_bundles_example())
            ```
        """
        async for bundle_data in evaluation_services.get_evaluation_submission_bundles(
            evaluation_id=evaluation_id,
            status=status,
            synapse_client=synapse_client,
        ):
            bundle = cls()
            bundle.fill_from_dict(bundle_data)
            yield bundle

    @skip_async_to_sync
    @classmethod
    async def get_user_submission_bundles_async(
        cls,
        evaluation_id: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> AsyncGenerator["SubmissionBundle", None]:
        """
        Generator to get all user bundled Submissions and SubmissionStatuses for a specified Evaluation.

        Arguments:
            evaluation_id: The ID of the specified Evaluation.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Yields:
            Individual SubmissionBundle objects from each page of the response.

        Example: Getting user submission bundles
            &nbsp;

            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import SubmissionBundle

            syn = Synapse()
            syn.login()

            async def get_user_submission_bundles_example():
                bundles = []
                async for bundle in SubmissionBundle.get_user_submission_bundles_async(
                    evaluation_id="9999999"
                ):
                    bundles.append(bundle)
                print(f"Found {len(bundles)} user submission bundles")
                for bundle in bundles:
                    print(f"Submission ID: {bundle.submission.id}")

            asyncio.run(get_user_submission_bundles_example())
            ```
        """
        async for bundle_data in evaluation_services.get_user_submission_bundles(
            evaluation_id=evaluation_id,
            synapse_client=synapse_client,
        ):
            bundle = cls()
            bundle.fill_from_dict(bundle_data)
            yield bundle
