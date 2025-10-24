"""
This module is responsible for exposing the services defined at:
<https://rest-docs.synapse.org/rest/index.html#org.sagebionetworks.repo.web.controller.EvaluationController>
"""

import json
from typing import TYPE_CHECKING, List, Optional

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
