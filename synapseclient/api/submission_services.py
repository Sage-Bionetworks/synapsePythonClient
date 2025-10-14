# TODO: The functions here should be moved into the `evaluation_services.py` file, once this branch is rebased onto those changes.

import json
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse


async def create_submission(
    request_body: dict, synapse_client: Optional["Synapse"] = None
) -> dict:
    """
    Creates a Submission and sends a submission notification email to the submitter's team members.

    <https://rest-docs.synapse.org/rest/POST/evaluation/submission.html>

    Arguments:
        request_body: The request body to send to the server.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = "/evaluation/submission"

    response = await client.rest_post_async(uri, body=json.dumps(request_body))

    return response


async def get_submission(
    submission_id: str, synapse_client: Optional["Synapse"] = None
) -> dict:
    """
    Retrieves a Submission by its ID.

    <https://rest-docs.synapse.org/rest/GET/evaluation/submission/subId.html>

    Arguments:
        submission_id: The ID of the submission to fetch.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        The requested Submission.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/submission/{submission_id}"

    response = await client.rest_get_async(uri)

    return response


async def get_evaluation_submissions(
    evaluation_id: str,
    status: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    synapse_client: Optional["Synapse"] = None,
) -> dict:
    """
    Retrieves all Submissions for a specified Evaluation queue.

    <https://rest-docs.synapse.org/rest/GET/evaluation/evalId/submission/all.html>

    Arguments:
        evaluation_id: The ID of the evaluation queue.
        status: Optionally filter submissions by a submission status, such as SCORED, VALID,
                INVALID, OPEN, CLOSED or EVALUATION_IN_PROGRESS.
        limit: Limits the number of submissions in a single response. Default to 20.
        offset: The offset index determines where this page will start from.
                An index of 0 is the first submission. Default to 0.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)`
                        this will use the last created instance from the Synapse class constructor.

    Returns:
        # TODO: Support pagination in the return type.
        A response JSON containing a paginated list of submissions for the evaluation queue.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{evaluation_id}/submission/all"
    query_params = {"limit": limit, "offset": offset}

    if status:
        query_params["status"] = status

    response = await client.rest_get_async(uri, params=query_params)

    return response


async def get_user_submissions(
    evaluation_id: str,
    user_id: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    synapse_client: Optional["Synapse"] = None,
) -> dict:
    """
    Retrieves Submissions for a specified Evaluation queue and user.
    If user_id is omitted, this returns the submissions of the caller.

    <https://rest-docs.synapse.org/rest/GET/evaluation/evalId/submission.html>

    Arguments:
        evaluation_id: The ID of the evaluation queue.
        user_id: Optionally specify the ID of the user whose submissions will be returned.
                If omitted, this returns the submissions of the caller.
        limit: Limits the number of submissions in a single response. Default to 20.
        offset: The offset index determines where this page will start from.
                An index of 0 is the first submission. Default to 0.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)`
                        this will use the last created instance from the Synapse class constructor.

    Returns:
        A response JSON containing a paginated list of user submissions for the evaluation queue.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{evaluation_id}/submission"
    query_params = {"limit": limit, "offset": offset}

    if user_id:
        query_params["userId"] = user_id

    response = await client.rest_get_async(uri, params=query_params)

    return response


async def get_submission_count(
    evaluation_id: str,
    status: Optional[str] = None,
    synapse_client: Optional["Synapse"] = None,
) -> dict:
    """
    Gets the number of Submissions for a specified Evaluation queue, optionally filtered by submission status.

    <https://rest-docs.synapse.org/rest/GET/evaluation/evalId/submission/count.html>

    Arguments:
        evaluation_id: The ID of the evaluation queue.
        status: Optionally filter submissions by a submission status, such as SCORED, VALID,
                INVALID, OPEN, CLOSED or EVALUATION_IN_PROGRESS.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)`
                        this will use the last created instance from the Synapse class constructor.

    Returns:
        A response JSON containing the submission count.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{evaluation_id}/submission/count"
    query_params = {}

    if status:
        query_params["status"] = status

    response = await client.rest_get_async(uri, params=query_params)

    return response


async def delete_submission(
    submission_id: str, synapse_client: Optional["Synapse"] = None
) -> None:
    """
    Deletes a Submission and its SubmissionStatus.

    <https://rest-docs.synapse.org/rest/DELETE/evaluation/submission/subId.html>

    Arguments:
        submission_id: The ID of the submission to delete.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)`
                        this will use the last created instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/submission/{submission_id}"

    await client.rest_delete_async(uri)


async def cancel_submission(
    submission_id: str, synapse_client: Optional["Synapse"] = None
) -> dict:
    """
    Cancels a Submission. Only the user who created the Submission may cancel it.

    <https://rest-docs.synapse.org/rest/PUT/evaluation/submission/subId/cancellation.html>

    Arguments:
        submission_id: The ID of the submission to cancel.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)`
                        this will use the last created instance from the Synapse class constructor.

    Returns:
        The Submission response object for the canceled submission as a JSON dict.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/submission/{submission_id}/cancellation"

    response = await client.rest_put_async(uri)

    return response


async def get_submission_status(
    submission_id: str, synapse_client: Optional["Synapse"] = None
) -> dict:
    """
    Gets the SubmissionStatus object associated with a specified Submission.

    <https://rest-docs.synapse.org/rest/GET/evaluation/submission/subId/status.html>

    Arguments:
        submission_id: The ID of the submission to get the status for.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The SubmissionStatus object as a JSON dict.

    Note:
        The caller must be granted the ACCESS_TYPE.READ on the specified Evaluation.
        Furthermore, the caller must be granted the ACCESS_TYPE.READ_PRIVATE_SUBMISSION
        to see all data marked as "private" in the SubmissionStatus.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/submission/{submission_id}/status"

    response = await client.rest_get_async(uri)

    return response


async def update_submission_status(
    submission_id: str, request_body: dict, synapse_client: Optional["Synapse"] = None
) -> dict:
    """
    Updates a SubmissionStatus object.

    <https://rest-docs.synapse.org/rest/PUT/evaluation/submission/subId/status.html>

    Arguments:
        submission_id: The ID of the SubmissionStatus being updated.
        request_body: The SubmissionStatus object to update as a dictionary.
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        The updated SubmissionStatus object as a JSON dict.

    Note:
        Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
        concurrent updates. Each time a SubmissionStatus is updated a new etag will be
        issued to the SubmissionStatus. When an update is requested, Synapse will compare
        the etag of the passed SubmissionStatus with the current etag of the SubmissionStatus.
        If the etags do not match, then the update will be rejected with a PRECONDITION_FAILED
        (412) response. When this occurs, the caller should fetch the latest copy of the
        SubmissionStatus and re-apply any changes, then re-attempt the SubmissionStatus update.

        The caller must be granted the ACCESS_TYPE.UPDATE_SUBMISSION on the specified Evaluation.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/submission/{submission_id}/status"

    response = await client.rest_put_async(uri, body=json.dumps(request_body))

    return response


async def get_all_submission_statuses(
    evaluation_id: str,
    status: Optional[str] = None,
    limit: int = 10,
    offset: int = 0,
    synapse_client: Optional["Synapse"] = None,
) -> dict:
    """
    Gets a collection of SubmissionStatuses to a specified Evaluation.

    <https://rest-docs.synapse.org/rest/GET/evaluation/evalId/submission/status/all.html>

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

    Note:
        The caller must be granted the ACCESS_TYPE.READ on the specified Evaluation.
        Furthermore, the caller must be granted the ACCESS_TYPE.READ_PRIVATE_SUBMISSION
        to see all data marked as "private" in the SubmissionStatuses.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{evaluation_id}/submission/status/all"
    query_params = {"limit": limit, "offset": offset}

    if status:
        query_params["status"] = status

    response = await client.rest_get_async(uri, params=query_params)

    return response


async def batch_update_submission_statuses(
    evaluation_id: str, request_body: dict, synapse_client: Optional["Synapse"] = None
) -> dict:
    """
    Update multiple SubmissionStatuses. The maximum batch size is 500.

    <https://rest-docs.synapse.org/rest/PUT/evaluation/evalId/statusBatch.html>

    Arguments:
        evaluation_id: The ID of the Evaluation to which the SubmissionStatus objects belong.
        request_body: The SubmissionStatusBatch object as a dictionary containing:
            - statuses: List of SubmissionStatus objects to update
            - isFirstBatch: Boolean indicating if this is the first batch in the series
            - isLastBatch: Boolean indicating if this is the last batch in the series
            - batchToken: Token from previous batch response (required for all but first batch)
        synapse_client: If not passed in and caching was not disabled by
            `Synapse.allow_client_caching(False)` this will use the last created
            instance from the Synapse class constructor.

    Returns:
        A BatchUploadResponse object as a JSON dict containing the batch token
        and other response information.

    Note:
        To allow upload of more than the maximum batch size (500), the system supports
        uploading a series of batches. Synapse employs optimistic concurrency on the
        series in the form of a batch token. Each request (except the first) must include
        the 'batch token' returned in the response to the previous batch. If another client
        begins batch upload simultaneously, a PRECONDITION_FAILED (412) response will be
        generated and upload must restart from the first batch.

        After the final batch is uploaded, the data for the Evaluation queue will be
        mirrored to the tables which support querying. Therefore uploaded data will not
        appear in Evaluation queries until after the final batch is successfully uploaded.

        It is the client's responsibility to note in each batch request:
        1. Whether it is the first batch in the series (isFirstBatch)
        2. Whether it is the last batch (isLastBatch)

        For a single batch both flags are set to 'true'.

        The caller must be granted the ACCESS_TYPE.UPDATE_SUBMISSION on the specified Evaluation.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{evaluation_id}/statusBatch"

    response = await client.rest_put_async(uri, body=json.dumps(request_body))

    return response


async def get_evaluation_submission_bundles(
    evaluation_id: str,
    status: Optional[str] = None,
    limit: int = 10,
    offset: int = 0,
    synapse_client: Optional["Synapse"] = None,
) -> dict:
    """
    Gets a collection of bundled Submissions and SubmissionStatuses to a given Evaluation.

    <https://rest-docs.synapse.org/rest/GET/evaluation/evalId/submission/bundle/all.html>

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
        A PaginatedResults<SubmissionBundle> object as a JSON dict containing
        a paginated list of submission bundles for the evaluation queue.

    Note:
        The caller must be granted the ACCESS_TYPE.READ_PRIVATE_SUBMISSION on the specified Evaluation.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{evaluation_id}/submission/bundle/all"
    query_params = {"limit": limit, "offset": offset}

    if status:
        query_params["status"] = status

    response = await client.rest_get_async(uri, params=query_params)

    return response


async def get_user_submission_bundles(
    evaluation_id: str,
    limit: int = 10,
    offset: int = 0,
    synapse_client: Optional["Synapse"] = None,
) -> dict:
    """
    Gets the requesting user's bundled Submissions and SubmissionStatuses to a specified Evaluation.

    <https://rest-docs.synapse.org/rest/GET/evaluation/evalId/submission/bundle.html>

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
        A PaginatedResults<SubmissionBundle> object as a JSON dict containing
        a paginated list of the requesting user's submission bundles for the evaluation queue.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{evaluation_id}/submission/bundle"
    query_params = {"limit": limit, "offset": offset}

    response = await client.rest_get_async(uri, params=query_params)

    return response
