# TODO: The functions here should be moved into the `evaluation_services.py` file, once this branch is rebased onto those changes.

import json
from typing import TYPE_CHECKING, List, Optional

if TYPE_CHECKING:
    from synapseclient import Synapse


async def create_submission(request_body: dict, synapse_client: Optional["Synapse"] = None) -> dict:
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


async def get_submission(submission_id: str, synapse_client: Optional["Synapse"] = None) -> dict:
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
    synapse_client: Optional["Synapse"] = None
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
    query_params = {
        "limit": limit,
        "offset": offset
    }
    
    if status:
        query_params["status"] = status
    
    response = await client.rest_get_async(uri, **query_params)

    return response


async def get_user_submissions(
    evaluation_id: str,
    user_id: Optional[str] = None,
    limit: int = 20,
    offset: int = 0,
    synapse_client: Optional["Synapse"] = None
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
    query_params = {
        "limit": limit,
        "offset": offset
    }
    
    if user_id:
        query_params["userId"] = user_id
    
    response = await client.rest_get_async(uri, **query_params)

    return response


async def get_submission_count(
    evaluation_id: str,
    status: Optional[str] = None,
    synapse_client: Optional["Synapse"] = None
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
    
    response = await client.rest_get_async(uri, **query_params)

    return response


async def delete_submission(
    submission_id: str,
    synapse_client: Optional["Synapse"] = None
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
    submission_id: str,
    synapse_client: Optional["Synapse"] = None
) -> dict:
    """
    Cancels a Submission. Only the user who created the Submission may cancel it.

    <https://rest-docs.synapse.org/rest/PUT/evaluation/submission/subId/cancellation.html>

    Arguments:
        submission_id: The ID of the submission to cancel.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` 
                        this will use the last created instance from the Synapse class constructor.
                        
    Returns:
        The canceled Submission.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/submission/{submission_id}/cancellation"
    
    response = await client.rest_put_async(uri)

    return response