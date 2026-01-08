from dataclasses import dataclass, field
from typing import AsyncGenerator, Generator, Optional, Protocol, Union

from typing_extensions import Self

from synapseclient import Synapse
from synapseclient.api import evaluation_services
from synapseclient.core.async_utils import (
    async_to_sync,
    otel_trace_method,
    skip_async_to_sync,
    wrap_async_generator_to_sync_generator,
)


class SubmissionSynchronousProtocol(Protocol):
    """Protocol defining the synchronous interface for Submission operations."""

    def store(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """
        Store the submission in Synapse. This creates a new submission in an evaluation queue.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Submission object with the ID set.

        Raises:
            ValueError: If the submission is missing required fields, or if unable to fetch entity etag.

        Example: Creating a submission
            &nbsp;
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            submission = Submission(
                entity_id="syn123456",
                evaluation_id="9999999",
                name="My Submission"
            ).store()
            print(submission.id)
            ```
        """
        return self

    def get(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
        """
        Retrieve a Submission from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Submission instance retrieved from Synapse.

        Raises:
            ValueError: If the submission does not have an ID to get.

        Example: Retrieving a submission by ID
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

        Raises:
            ValueError: If the submission does not have an ID to delete.

        Example: Delete a submission
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

    def cancel(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Self":
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
            &nbsp;
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            submission = Submission(id="syn1234")
            canceled_submission = submission.cancel()
            ```
        """
        return self

    @classmethod
    def get_evaluation_submissions(
        cls,
        evaluation_id: str,
        status: Optional[str] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Generator["Submission", None, None]:
        """
        Retrieves all Submissions for a specified Evaluation queue.

        Arguments:
            evaluation_id: The ID of the evaluation queue.
            status: Optionally filter submissions by a submission status, such as SCORED, VALID,
                    INVALID, OPEN, CLOSED or EVALUATION_IN_PROGRESS.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Submission objects as they are retrieved from the API.

        Example: Getting submissions for an evaluation
            &nbsp;
            Get SCORED submissions from a specific evaluation.
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            submissions = list(Submission.get_evaluation_submissions(
                evaluation_id="9999999",
                status="SCORED"
            ))
            print(f"Found {len(submissions)} submissions")
            ```
        """
        yield from wrap_async_generator_to_sync_generator(
            async_gen_func=cls.get_evaluation_submissions_async,
            evaluation_id=evaluation_id,
            status=status,
            synapse_client=synapse_client,
        )

    @classmethod
    def get_user_submissions(
        cls,
        evaluation_id: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Generator["Submission", None, None]:
        """
        Retrieves all user Submissions for a specified Evaluation queue.

        Arguments:
            evaluation_id: The ID of the evaluation queue.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Submission objects as they are retrieved from the API.

        Example: Getting user submissions
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            submissions = list(Submission.get_user_submissions(
                evaluation_id="9999999"
            ))
            print(f"Found {len(submissions)} user submissions")
            ```
        """
        yield from wrap_async_generator_to_sync_generator(
            async_gen_func=cls.get_user_submissions_async,
            evaluation_id=evaluation_id,
            synapse_client=synapse_client,
        )

    @staticmethod
    def get_submission_count(
        evaluation_id: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> dict:
        """
        Gets the number of Submissions for a specified Evaluation queue, optionally filtered by submission status.

        Arguments:
            evaluation_id: The ID of the evaluation queue.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A response JSON containing the submission count.

        Example: Getting submission count
            &nbsp;
            Get the total number of submissions from a specific evaluation.
            ```python
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            response = Submission.get_submission_count(
                evaluation_id="9999999"
            )
            print(f"Found {response} submissions")
            ```
        """
        return {}


@dataclass
@async_to_sync
class Submission(SubmissionSynchronousProtocol):
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
        evaluation_round_id: The ID of the EvaluationRound to which this was submitted (auto-filled upon creation).
        name: The name of this Submission.
        created_on: The date this Submission was created.
        team_id: The ID of the team that submitted this submission (if it's a team submission).
        contributors: User IDs of team members who contributed to this submission (if it's a team submission).
        submission_status: The status of this Submission.
        entity_bundle_json: The bundled entity information at submission. This includes the entity, annotations,
            file handles, and other metadata.
        docker_repository_name: For Docker repositories, the repository name.
        docker_digest: For Docker repositories, the digest of the submitted Docker image.

    Example: Retrieve a Submission.
        &nbsp;
        ```python
        from synapseclient import Synapse
        from synapseclient.models import Submission

        syn = Synapse()
        syn.login()

        submission = Submission(id="syn123456").get()
        print(submission)
        ```

    Example: Create and store a new Submission.
        &nbsp;
        ```python
        from synapseclient import Synapse
        from synapseclient.models import Submission

        syn = Synapse()
        syn.login()

        # Create a new submission
        submission = Submission(
            entity_id="syn123456",
            evaluation_id="9999999",
            name="My Submission"
        )

        # Store the submission
        stored_submission = submission.store()
        print(f"Created submission with ID: {stored_submission.id}")
        ```

    Example: Get all submissions for a user.
        &nbsp;
        ```python
        from synapseclient import Synapse
        from synapseclient.models import Submission

        syn = Synapse()
        syn.login()

        # Get submissions for the current user
        my_submissions = list(Submission.get_user_submissions(
            evaluation_id="9999999"
        ))
        print(f"Found {len(my_submissions)} of my submissions")
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
    The version number of the entity at submission. If not provided, it will be automatically retrieved from the entity. If entity is a Docker repository, this attribute should be ignored in favor of `docker_digest` or `docker_tag`.
    """

    evaluation_id: Optional[str] = None
    """
    The ID of the Evaluation to which this Submission belongs.
    """

    evaluation_round_id: Optional[str] = field(default=None, compare=False)
    """
    The ID of the EvaluationRound to which this was submitted. DO NOT specify a value for this. It will be filled in automatically upon creation of the Submission if the Evaluation is configured with an EvaluationRound.
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

    contributors: list[str] = field(default_factory=list)
    """
    User IDs of team members who contributed to this submission (if it's a team submission).
    """

    submission_status: Optional[dict] = None
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

    docker_tag: Optional[str] = None
    """
    For Docker repositories, the tag of the submitted Docker image.
    """

    etag: Optional[str] = None
    """The current eTag of the Entity being submitted. If not provided, it will be automatically retrieved."""

    def fill_from_dict(
        self, synapse_submission: dict[str, Union[bool, str, int, list]]
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
        self.evaluation_round_id = synapse_submission.get("evaluationRoundId", None)
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

        return self

    async def _get_latest_docker_tag(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> Optional[dict]:
        """
        Fetch the latest Docker tag information for a Docker repository entity.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                    `Synapse.allow_client_caching(False)` this will use the last created
                    instance from the Synapse class constructor.

        Returns:
            Dictionary containing the latest Docker tag information, or None if no tags found.

        Raises:
            Exception: If there's an error fetching the Docker tag information.
        """
        from synapseclient.api import docker_commit_services

        docker_tag_response = await docker_commit_services.get_docker_tag(
            entity_id=self.entity_id, synapse_client=synapse_client
        )

        # Get the latest digest from the docker tag results
        if "results" in docker_tag_response and docker_tag_response["results"]:
            # Sort by createdOn timestamp to get the latest entry
            # Convert ISO timestamp strings to datetime objects for comparison
            from datetime import datetime

            latest_result = max(
                docker_tag_response["results"],
                key=lambda x: datetime.fromisoformat(
                    x["createdOn"].replace("Z", "+00:00")
                ),
            )

            return latest_result

        return None

    async def _fetch_latest_entity(
        self, *, synapse_client: Optional[Synapse] = None
    ) -> dict:
        """
        Fetch the latest entity information from Synapse.

        <https://rest-docs.synapse.org/rest/GET/entity/id.html>

        If the object is a DockerRepository, this will also fetch the DockerCommit object with the latest createdOn value
        and attach it to the final dictionary:

        <https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/docker/DockerCommit.html>

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            Dictionary containing entity information from the REST API.

        Raises:
            ValueError: If entity_id is not set or if unable to fetch entity information.
        """
        if not self.entity_id:
            raise ValueError("entity_id must be set to fetch entity information")

        from synapseclient import Synapse

        client = Synapse.get_client(synapse_client=synapse_client)

        try:
            from synapseclient.api import entity_services

            entity_info = await entity_services.get_entity(
                entity_id=self.entity_id, synapse_client=client
            )

            # If this is a DockerRepository, fetch docker image tag & digest, and add it to the entity_info dict
            from synapseclient.core.constants.concrete_types import DOCKER_REPOSITORY

            if entity_info.get("concreteType") == DOCKER_REPOSITORY:
                latest_docker_tag = await self._get_latest_docker_tag(
                    synapse_client=client
                )

                if latest_docker_tag:
                    # Add the latest result to entity_info
                    entity_info.update(latest_docker_tag)

            return entity_info
        except Exception as e:
            raise LookupError(
                f"Unable to fetch entity information for {self.entity_id}: {e}"
            ) from e

    def to_synapse_request(self) -> dict:
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
            "versionNumber": self.version_number,
        }

        # Add optional fields if they are set
        optional_fields = {
            "name": "name",
            "team_id": "teamId",
            "contributors": "contributors",
            "docker_repository_name": "dockerRepositoryName",
            "docker_digest": "dockerDigest",
        }

        for field_name, api_field_name in optional_fields.items():
            field_value = getattr(self, field_name)
            if field_value is not None and (
                field_name != "contributors" or field_value
            ):
                request_body[api_field_name] = field_value

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
            ValueError: If the submission is missing required fields, or if unable to fetch entity etag.

        Example: Creating a submission
            &nbsp;
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            async def create_submission_example():

                submission = Submission(
                    entity_id="syn123456",
                    evaluation_id="9999999",
                    name="My Submission"
                )
                submission = await submission.store_async()
                print(submission.id)

            asyncio.run(create_submission_example())
            ```
        """

        if not self.entity_id:
            raise ValueError("entity_id is required to create a submission")

        entity_info = await self._fetch_latest_entity(synapse_client=synapse_client)

        self.entity_etag = entity_info.get("etag")

        if not self.entity_etag:
            raise ValueError("Unable to fetch etag for entity")

        if (
            entity_info.get("concreteType")
            == "org.sagebionetworks.repo.model.FileEntity"
        ):
            self.version_number = entity_info.get("versionNumber")
        elif (
            entity_info.get("concreteType")
            == "org.sagebionetworks.repo.model.docker.DockerRepository"
        ):
            self.docker_repository_name = entity_info.get("repositoryName")
            self.docker_digest = entity_info.get("digest")
            self.docker_tag = entity_info.get("tag")
            # All docker repositories are assigned version number 1, even if they have multiple tags
            self.version_number = 1

        # Build the request body now that all the necessary dataclass attributes are set
        request_body = self.to_synapse_request()

        response = await evaluation_services.create_submission(
            request_body, self.entity_etag, synapse_client=synapse_client
        )
        self.fill_from_dict(response)

        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Submission_Get: {self.id}"
    )
    async def get_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Submission":
        """
        Retrieve a Submission from Synapse.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The Submission instance retrieved from Synapse.

        Raises:
            ValueError: If the submission does not have an ID to get.

        Example: Retrieving a submission by ID
            &nbsp;
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            async def get_submission_example():

                submission = await Submission(id="9999999").get_async()
                print(submission)

            asyncio.run(get_submission_example())
            ```
        """
        if not self.id:
            raise ValueError("The submission must have an ID to get.")

        response = await evaluation_services.get_submission(
            submission_id=self.id, synapse_client=synapse_client
        )

        self.fill_from_dict(response)

        return self

    @skip_async_to_sync
    @classmethod
    async def get_evaluation_submissions_async(
        cls,
        evaluation_id: str,
        status: Optional[str] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> AsyncGenerator["Submission", None]:
        """
        Generator to get all Submissions for a specified Evaluation queue.

        Arguments:
            evaluation_id: The ID of the evaluation queue.
            status: Optionally filter submissions by a submission status, such as SCORED, VALID,
                    INVALID, OPEN, CLOSED or EVALUATION_IN_PROGRESS.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Yields:
            Individual Submission objects from each page of the response.

        Example: Getting SCORED submissions for an evaluation
            &nbsp;
            Get submissions from a specific evaluation.
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            async def get_evaluation_submissions_example():
                submissions = []
                async for submission in Submission.get_evaluation_submissions_async(
                    evaluation_id="9999999",
                    status="SCORED"
                ):
                    submissions.append(submission)
                print(f"Found {len(submissions)} submissions")

            asyncio.run(get_evaluation_submissions_example())
            ```
        """
        async for submission_data in evaluation_services.get_evaluation_submissions(
            evaluation_id=evaluation_id,
            status=status,
            synapse_client=synapse_client,
        ):
            submission_object = cls().fill_from_dict(synapse_submission=submission_data)
            yield submission_object

    @skip_async_to_sync
    @classmethod
    async def get_user_submissions_async(
        cls,
        evaluation_id: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> AsyncGenerator["Submission", None]:
        """
        Generator to get all user Submissions for a specified Evaluation queue.

        Arguments:
            evaluation_id: The ID of the evaluation queue.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Yields:
            Individual Submission objects from each page of the response.

        Example: Getting user submissions
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            async def get_user_submissions_example():
                submissions = []
                async for submission in Submission.get_user_submissions_async(
                    evaluation_id="9999999"
                ):
                    submissions.append(submission)
                print(f"Found {len(submissions)} user submissions")

            asyncio.run(get_user_submissions_example())
            ```
        """
        async for submission_data in evaluation_services.get_user_submissions(
            evaluation_id=evaluation_id,
            synapse_client=synapse_client,
        ):
            submission_object = cls().fill_from_dict(synapse_submission=submission_data)
            yield submission_object

    @staticmethod
    async def get_submission_count_async(
        evaluation_id: str,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> dict:
        """
        Gets the number of Submissions for a specified Evaluation queue, optionally filtered by submission status.

        Arguments:
            evaluation_id: The ID of the evaluation queue.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            A response JSON containing the submission count.

        Example: Getting submission count
            &nbsp;
            Get the total number of submissions from a specific evaluation.
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            async def get_submission_count_example():
                response = await Submission.get_submission_count_async(
                    evaluation_id="9999999"
                )
                print(f"Found {response} submissions")

            asyncio.run(get_submission_count_example())
            ```
        """
        return await evaluation_services.get_submission_count(
            evaluation_id=evaluation_id, synapse_client=synapse_client
        )

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Submission_Delete: {self.id}"
    )
    async def delete_async(
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
            &nbsp;
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            async def delete_submission_example():
                submission = Submission(id="9999999")
                await submission.delete_async()
                print("Submission deleted successfully")

            # Run the async function
            asyncio.run(delete_submission_example())
            ```
        """
        if not self.id:
            raise ValueError("The submission must have an ID to delete.")

        await evaluation_services.delete_submission(
            submission_id=self.id, synapse_client=synapse_client
        )

        from synapseclient import Synapse

        client = Synapse.get_client(synapse_client=synapse_client)
        logger = client.logger
        logger.info(f"Submission {self.id} has successfully been deleted.")

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Submission_Cancel: {self.id}"
    )
    async def cancel_async(
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
            &nbsp;
            ```python
            import asyncio
            from synapseclient import Synapse
            from synapseclient.models import Submission

            syn = Synapse()
            syn.login()

            async def cancel_submission_example():
                submission = Submission(id="syn1234")
                canceled_submission = await submission.cancel_async()
                print(f"Canceled submission: {canceled_submission.id}")

            # Run the async function
            asyncio.run(cancel_submission_example())
            ```
        """
        if not self.id:
            raise ValueError("The submission must have an ID to cancel.")

        await evaluation_services.cancel_submission(
            submission_id=self.id, synapse_client=synapse_client
        )

        from synapseclient import Synapse

        client = Synapse.get_client(synapse_client=synapse_client)
        logger = client.logger
        logger.info(f"A request to cancel Submission {self.id} has been submitted.")
