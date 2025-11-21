"""
This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/index.html#org.sagebionetworks.repo.web.controller.EvaluationController>
"""

import json
from typing import TYPE_CHECKING, Any, AsyncGenerator, Dict, List, Optional

from synapseclient.api.api_client import rest_get_paginated_async

if TYPE_CHECKING:
    from synapseclient import Synapse


async def create_or_update_evaluation(
    request_body: dict,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> dict:
    """
    Create or update an Evaluation on Synapse.

    <https://rest-docs.synapse.org/rest/POST/evaluation.html>
    <https://rest-docs.synapse.org/rest/PUT/evaluation/evalId.html>

    Arguments:
        request_body: A dictionary containing the evaluation data
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        The Evaluation response object as a raw JSON dict.

    Raises:
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """

    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)
    logger = client.logger

    if request_body.get("id"):
        evaluation_id = request_body.get("id")
        uri = f"/evaluation/{evaluation_id}"
        response = await client.rest_put_async(uri, body=json.dumps(request_body))

        logger.info(
            f"Evaluation '{request_body.get('name')}' (ID: {evaluation_id}) has been updated"
        )

    else:
        uri = "/evaluation"
        response = await client.rest_post_async(uri, body=json.dumps(request_body))

        logger.info(
            f"Evaluation '{request_body.get('name')}' has been created with ID: {response.get('id')}"
        )

    return response


async def get_evaluation(
    evaluation_id: Optional[str] = None,
    name: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> dict:
    """
    Get an Evaluation by its ID or name.

    <https://rest-docs.synapse.org/rest/GET/evaluation/evalId.html>
    <https://rest-docs.synapse.org/rest/GET/evaluation/name/name.html>

    Arguments:
        id: The ID of the Evaluation to retrieve (e.g., "9614112"). If provided, this takes precedence over `name`.
        name: The name of the Evaluation to retrieve. Used only if `id` is not provided.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        The requested Evaluation as a raw JSON dict.

    Raises:
        ValueError: If neither `id` nor `name` is provided.
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    if evaluation_id:
        uri = f"/evaluation/{evaluation_id}"
    elif name:
        uri = f"/evaluation/name/{name}"
    else:
        raise ValueError("Either 'evaluation_id' or 'name' must be provided")

    response = await client.rest_get_async(uri)

    return response


async def get_evaluations_by_project(
    project_id: str,
    access_type: Optional[str] = None,
    active_only: Optional[bool] = None,
    evaluation_ids: Optional[List[str]] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[dict]:
    """
    Gets Evaluations tied to a project.
    Note: The response will contain only those Evaluations on which the caller is granted the ACCESS_TYPE.READ permission, unless specified otherwise with the accessType parameter.

    <https://rest-docs.synapse.org/rest/GET/entity/id/evaluation.html>

    Arguments:
        project_id: The ID of the project (e.g., "syn123456").
        access_type: The type of access for the user to filter for, optional and defaults to ACCESS_TYPE.READ.
        active_only: If True then return only those evaluations with rounds defined and for which the current time is in one of the rounds.
        evaluation_ids: An optional list of evaluation IDs to which the response is limited.
        offset: The offset index determines where this page will start from. An index of 0 is the first entity. When null it will default to 0.
        limit: Limits the number of entities that will be fetched for this page. When null it will default to 10.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        List[dict]: A list of Evaluation objects as raw JSON dicts tied to the project.

    Raises:
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Build query parameters
    query_params = {}
    if access_type is not None:
        query_params["accessType"] = access_type
    if active_only is not None:
        query_params["activeOnly"] = "true" if active_only else "false"
    if evaluation_ids is not None:
        query_params["evaluationIds"] = ",".join(evaluation_ids)
    if offset is not None:
        query_params["offset"] = offset
    if limit is not None:
        query_params["limit"] = limit

    uri = f"/entity/{project_id}/evaluation"
    evaluation_list = await client.rest_get_async(uri, params=query_params)

    return evaluation_list


async def get_all_evaluations(
    access_type: Optional[str] = None,
    active_only: Optional[bool] = None,
    evaluation_ids: Optional[List[str]] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[dict]:
    """
    Get a list of all Evaluations, within a given range.
    Note: The response will contain only those Evaluations on which the caller is granted the ACCESS_TYPE.READ permission, unless specified otherwise with the accessType parameter.

    <https://rest-docs.synapse.org/rest/GET/evaluation.html>

    Arguments:
        access_type: The type of access for the user to filter for, optional and defaults to ACCESS_TYPE.READ.
        active_only: If True then return only those evaluations with rounds defined and for which the current time is in one of the rounds.
        evaluation_ids: An optional list of evaluation IDs to which the response is limited.
        offset: The offset index determines where this page will start from. An index of 0 is the first entity. When null it will default to 0.
        limit: Limits the number of entities that will be fetched for this page. When null it will default to 10.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        List[dict]: A list of Evaluation objects as raw JSON dicts.

    Raises:
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Build query parameters
    query_params = {}
    if access_type is not None:
        query_params["accessType"] = access_type
    if active_only is not None:
        query_params["activeOnly"] = "true" if active_only else "false"
    if evaluation_ids is not None:
        query_params["evaluationIds"] = ",".join(evaluation_ids)
    if offset is not None:
        query_params["offset"] = offset
    if limit is not None:
        query_params["limit"] = limit

    uri = "/evaluation"
    evaluation_list = await client.rest_get_async(uri, params=query_params)

    return evaluation_list


async def get_available_evaluations(
    active_only: Optional[bool] = None,
    evaluation_ids: Optional[List[str]] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[dict]:
    """
    Get a list of Evaluations to which the user has SUBMIT permission, within a given range.
    Note: The response will contain only those Evaluations on which the caller is granted the ACCESS_TYPE.SUBMIT permission.

    <https://rest-docs.synapse.org/rest/GET/evaluation/available.html>

    Arguments:
        active_only: If True then return only those evaluations with rounds defined and for which the current time is in one of the rounds.
        evaluation_ids: An optional list of evaluation IDs to which the response is limited.
        offset: The offset index determines where this page will start from. An index of 0 is the first evaluation. When null it will default to 0.
        limit: Limits the number of entities that will be fetched for this page. When null it will default to 10.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        List[dict]: A list of available evaluations as raw JSON dicts.

    Raises:
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Build query parameters
    query_params = {}
    if active_only is not None:
        query_params["activeOnly"] = "true" if active_only else "false"
    if evaluation_ids is not None:
        query_params["evaluationIds"] = ",".join(evaluation_ids)
    if offset is not None:
        query_params["offset"] = offset
    if limit is not None:
        query_params["limit"] = limit

    uri = "/evaluation/available"
    evaluation_list = await client.rest_get_async(uri, params=query_params)

    return evaluation_list


async def delete_evaluation(
    evaluation_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Delete an Evaluation.

    <https://rest-docs.synapse.org/rest/DELETE/evaluation/evalId.html>

    Arguments:
        evaluation_id: The ID of the Evaluation to delete.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        None

    Raises:
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{evaluation_id}"
    await client.rest_delete_async(uri)


async def get_evaluation_acl(
    evaluation_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> dict:
    """
    Get the access control list (ACL) governing the given evaluation.
    The user should have the proper permissions to read the ACL.

    <https://rest-docs.synapse.org/rest/GET/evaluation/evalId/acl.html>

    Arguments:
        evaluation_id: The ID of the evaluation whose ACL is being retrieved.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        The AccessControlList response object as a raw JSON dict.

    Raises:
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{evaluation_id}/acl"
    acl_json = await client.rest_get_async(uri)

    return acl_json


async def update_evaluation_acl(
    acl: dict,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> dict:
    """
    Update the supplied access control list (ACL) for an evaluation.
    The ACL to be updated should have the ID of the evaluation.
    The user should have the proper permissions in order to update the ACL.

    <https://rest-docs.synapse.org/rest/PUT/evaluation/acl.html>

    Arguments:
        acl: An AccessControlList object or dictionary containing the ACL data to update.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        The AccessControlList response object as a raw JSON dict.

    Raises:
        ValueError: If the ACL object is invalid or missing required fields.
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

    Example:
        Update the ACL for an evaluation:

            acl = await get_evaluation_acl("9614112")
            # Modify ACL
            updated_acl = await update_evaluation_acl(acl)
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = "/evaluation/acl"
    response = await client.rest_put_async(uri, body=json.dumps(acl))

    return response


async def get_evaluation_permissions(
    evaluation_id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> dict:
    """
    Get the user permissions for the specified evaluation.

    <https://rest-docs.synapse.org/rest/GET/evaluation/evalId/permissions.html>

    Arguments:
        evaluation_id: The ID of the evaluation over which the user permissions are being retrieved.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        dict: The permissions for the current user.

    Raises:
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{evaluation_id}/permissions"

    return await client.rest_get_async(uri)


async def create_submission(
    request_body: dict, etag: str, synapse_client: Optional["Synapse"] = None
) -> dict:
    """
    Creates a Submission and sends a submission notification email to the submitter's team members.

    <https://rest-docs.synapse.org/rest/POST/evaluation/submission.html>

    Arguments:
        request_body: The request body to send to the server.
        etag: The current eTag of the Entity being submitted.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = "/evaluation/submission"

    # Add etag as query parameter if provided
    params = {"etag": etag}

    response = await client.rest_post_async(
        uri, body=json.dumps(request_body), params=params
    )

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
    *,
    synapse_client: Optional["Synapse"] = None,
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Generator to get all Submissions for a specified Evaluation queue.

    <https://rest-docs.synapse.org/rest/GET/evaluation/evalId/submission/all.html>

    Arguments:
        evaluation_id: The ID of the evaluation queue.
        status: Optionally filter submissions by a submission status, such as SCORED, VALID,
                INVALID, OPEN, CLOSED or EVALUATION_IN_PROGRESS.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)`
                        this will use the last created instance from the Synapse class constructor.

    Yields:
        Individual Submission objects from each page of the response.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{evaluation_id}/submission/all"
    query_params = {}

    if status:
        query_params["status"] = status

    async for item in rest_get_paginated_async(
        uri=uri, params=query_params, synapse_client=client
    ):
        yield item


async def get_user_submissions(
    evaluation_id: str,
    user_id: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> AsyncGenerator[Dict[str, Any], None]:
    """
    Generator to get all user Submissions for a specified Evaluation queue.
    If user_id is omitted, this returns the submissions of the caller.

    <https://rest-docs.synapse.org/rest/GET/evaluation/evalId/submission.html>

    Arguments:
        evaluation_id: The ID of the evaluation queue.
        user_id: Optionally specify the ID of the user whose submissions will be returned.
                If omitted, this returns the submissions of the caller.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)`
                        this will use the last created instance from the Synapse class constructor.

    Yields:
        Individual Submission objects from each page of the response.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{evaluation_id}/submission"
    query_params = {}

    if user_id:
        query_params["userId"] = user_id

    async for item in rest_get_paginated_async(
        uri=uri, params=query_params, synapse_client=client
    ):
        yield item


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
