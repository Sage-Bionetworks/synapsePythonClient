"""
This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/index.html#org.sagebionetworks.repo.web.controller.EvaluationController>
"""

import json
from typing import List, Optional, Union

from synapseclient.core import utils
from synapseclient.models.access_control import AccessControlList
from synapseclient.models.evaluation import Evaluation


async def create_evaluation_async(
    name: str,
    description: str,
    content_source: str,
    submission_instructions_message: str,
    submission_receipt_message: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Evaluation:
    """
    Create a new Evaluation.

    <https://rest-docs.synapse.org/rest/POST/evaluation.html>

    Arguments:
        name: The name of this Evaluation.
        description: A text description of this Evaluation.
        content_source: The Synapse ID of the Entity to which this Evaluation belongs, e.g. a reference to a Synapse project.
        submission_instructions_message: Message to display to users detailing acceptable formatting for Submissions to this Evaluation.
        submission_receipt_message: Message to display to users upon successful submission to this Evaluation.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        The created Evaluation object.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    request_body = {
        "name": name,
        "description": description,
        "contentSource": content_source,
        "submissionInstructionsMessage": submission_instructions_message,
        "submissionReceiptMessage": submission_receipt_message,
    }

    uri = "/evaluation"
    response = await client.rest_post_async(uri, body=json.dumps(request_body))

    return Evaluation(**response)


async def get_evaluation_async(
    id: Optional[str] = None,
    name: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Evaluation:
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
        The requested Evaluation.

    Raises:
        ValueError: If neither `id` nor `name` is provided.
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    if id:
        uri = f"/evaluation/{id}"
    elif name:
        uri = f"/evaluation/name/{name}"
    else:
        raise ValueError("Either 'id' or 'name' must be provided")

    response = await client._async_get_json(uri)

    return Evaluation(**response)


async def get_evaluations_by_project_async(
    project_id: str,
    access_type: Optional[str] = None,
    active_only: Optional[bool] = None,
    evaluation_ids: Optional[List[str]] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[Evaluation]:
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
        List[Evaluation]: A list of Evaluations tied to the project.

    Raises:
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Build query parameters
    params = []
    if access_type is not None:
        params.append(f"accessType={access_type}")
    if active_only is not None:
        params.append(f"activeOnly={'true' if active_only else 'false'}")
    if evaluation_ids is not None:
        params.append(f"evaluationIds={','.join(evaluation_ids)}")
    if offset is not None:
        params.append(f"offset={offset}")
    if limit is not None:
        params.append(f"limit={limit}")

    # Build URI with query parameters
    uri = f"/entity/{project_id}/evaluation"
    if params:
        uri += "?" + "&".join(params)

    evaluation_list = await client._async_get_json(uri)

    return [Evaluation(**evaluation) for evaluation in evaluation_list]


async def get_all_evaluations_async(
    access_type: Optional[str] = None,
    active_only: Optional[bool] = None,
    evaluation_ids: Optional[List[str]] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[Evaluation]:
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
        List[Evaluation]: A list of all evaluations.

    Raises:
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Build query parameters
    params = []
    if access_type is not None:
        params.append(f"accessType={access_type}")
    if active_only is not None:
        params.append(f"activeOnly={'true' if active_only else 'false'}")
    if evaluation_ids is not None:
        params.append(f"evaluationIds={','.join(evaluation_ids)}")
    if offset is not None:
        params.append(f"offset={offset}")
    if limit is not None:
        params.append(f"limit={limit}")

    # Build URI with query parameters
    uri = "/evaluation"
    if params:
        uri += "?" + "&".join(params)

    evaluation_list = await client._async_get_json(uri)

    return [Evaluation(**evaluation) for evaluation in evaluation_list]


async def get_available_evaluations_async(
    active_only: Optional[bool] = None,
    evaluation_ids: Optional[List[str]] = None,
    offset: Optional[int] = None,
    limit: Optional[int] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> List[Evaluation]:
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
        List[Evaluation]: A list of available evaluations.

    Raises:
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Build query parameters
    params = []
    if active_only is not None:
        params.append(f"activeOnly={'true' if active_only else 'false'}")
    if evaluation_ids is not None:
        params.append(f"evaluationIds={','.join(evaluation_ids)}")
    if offset is not None:
        params.append(f"offset={offset}")
    if limit is not None:
        params.append(f"limit={limit}")

    # Build URI with query parameters
    uri = "/evaluation/available"
    if params:
        uri += "?" + "&".join(params)

    evaluation_list = await client._async_get_json(uri)

    return [Evaluation(**evaluation) for evaluation in evaluation_list]


async def update_evaluation_async(
    id: str,
    name: Optional[str] = None,
    description: Optional[str] = None,
    content_source: Optional[str] = None,
    submission_instructions_message: Optional[str] = None,
    submission_receipt_message: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> Evaluation:
    """
    Update an Evaluation.

    Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent updates. Each time an Evaluation is updated a new etag will be issued to the Evaluation. When an update is requested, Synapse will compare the etag of the passed Evaluation with the current etag of the Evaluation. If the etags do not match, then the update will be rejected with a PRECONDITION_FAILED (412) response. When this occurs, the caller should fetch the latest copy of the Evaluation and re-apply any changes, then re-attempt the Evaluation update.

    Note: The caller must be granted the ACCESS_TYPE.UPDATE on the specified Evaluation.

    <https://rest-docs.synapse.org/rest/PUT/evaluation/evalId.html>

    Arguments:
        id: The ID of the Evaluation being updated.
        name: The human-readable name of the Evaluation.
        description: A short description of the Evaluation's purpose.
        content_source: The ID of the Project or Entity this Evaluation belongs to (e.g., "syn123").
        submission_instructions_message: Instructions presented to submitters when creating a submission.
        submission_receipt_message: A confirmation message shown after a successful submission.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        The updated Evaluation object.

    Raises:
        ValueError: If evaluation_id is not provided.
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs (including PRECONDITION_FAILED 412).


    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    # Build request body with only provided fields
    request_body = {}
    if name is not None:
        request_body["name"] = name
    if description is not None:
        request_body["description"] = description
    if content_source is not None:
        request_body["contentSource"] = content_source
    if submission_instructions_message is not None:
        request_body["submissionInstructionsMessage"] = submission_instructions_message
    if submission_receipt_message is not None:
        request_body["submissionReceiptMessage"] = submission_receipt_message

    uri = f"/evaluation/{id}"
    response = await client.rest_put_async(uri, body=json.dumps(request_body))

    return Evaluation(**response)


async def delete_evaluation_async(
    id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> None:
    """
    Delete an Evaluation.

    <https://rest-docs.synapse.org/rest/DELETE/evaluation/evalId.html>

    Arguments:
        id: The ID of the Evaluation to delete.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        None

    Raises:
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{id}"
    await client.rest_delete_async(uri)


async def get_evaluation_acl_async(
    id: str,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> AccessControlList:
    """
    Get the access control list (ACL) governing the given evaluation.
    The user should have the proper permissions to read the ACL.

    <https://rest-docs.synapse.org/rest/GET/evaluation/evalId/acl.html>
    TODO: SHould this already be a mixin?

    Arguments:
        id: The ID of the evaluation whose ACL is being retrieved.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        AccessControlList: The ACL for the Evaluation.

    Raises:
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{id}/acl"
    acl_json = await client._async_get_json(uri)

    return AccessControlList(**acl_json)


async def update_evaluation_acl_async(
    acl: Union[AccessControlList, dict],
    *,
    synapse_client: Optional["Synapse"] = None,
) -> AccessControlList:
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
        AccessControlList: The updated ACL.

    Raises:
        ValueError: If the ACL object is invalid or missing required fields.
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

    Example:
        Update the ACL for an evaluation:

            acl = await get_evaluation_acl_async("9614112")
            # Modify ACL
            updated_acl = await update_evaluation_acl_async(acl)
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    if not isinstance(acl, (AccessControlList, dict)):
        raise ValueError("Expected AccessControlList or dict")

    acl_json = utils._getRawJSON(acl)
    acl_json = utils._clean_json_for_request(acl_json)

    uri = "/evaluation/acl"
    response = await client.rest_put_async(uri, body=json.dumps(acl_json))

    return AccessControlList(**response)


async def get_evaluation_permissions_async(
    id: str,
    principal_id: Optional[str] = None,
    *,
    synapse_client: Optional["Synapse"] = None,
) -> dict:
    """
    Get the user permissions for the specified evaluation.

    <https://rest-docs.synapse.org/rest/GET/evaluation/evalId/permissions.html>

    Arguments:
        evaluation_id: The ID of the evaluation over which the user permissions are being retrieved.
        principal_id: The principal ID to get permissions for. Defaults to the current user.
        synapse_client: If not passed in and caching was not disabled by `Synapse.allow_client_caching(False)` this will use the last created
                        instance from the Synapse class constructor.

    Returns:
        dict: The permissions for the specified user.

    Raises:
        SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
    """
    from synapseclient import Synapse

    client = Synapse.get_client(synapse_client=synapse_client)

    uri = f"/evaluation/{id}/permissions"
    if principal_id:
        uri += f"?principalId={principal_id}"

    return await client._async_get_json(uri)
