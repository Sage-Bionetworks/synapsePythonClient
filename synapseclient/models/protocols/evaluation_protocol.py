"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Dict, List, Optional, Protocol, Union

from synapseclient import Synapse

if TYPE_CHECKING:
    from synapseclient.models import Evaluation


class EvaluationSynchronousProtocol(Protocol):
    """
    The protocol for methods that are asynchronous but also
    have a synchronous counterpart that may also be called.
    """

    def store(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Evaluation":
        """
        Create a new Evaluation or update an existing one in Synapse.

        If the Evaluation object has an ID and etag, it will be updated.
        Otherwise, a new Evaluation will be created.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The created or updated Evaluation object.

        Raises:
            ValueError: If required fields are missing.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Create a new evaluation in a project with ID "syn123456"
            &nbsp;
            Create a new evaluation on Synapse by storing an evaluation object with the required fields. If there are any fields missing, an error will be raised.
            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            evaluation = Evaluation(
                name="My Challenge Evaluation",
                description="Evaluation for my data challenge",
                content_source="syn123456",
                submission_instructions_message="Submit CSV files only",
                submission_receipt_message="Thank you for your submission!"
            ).store()
            ```

        Example: Update an evaluation that was retrieved from Synapse
            &nbsp;
            You can use the store method to create and update evaluations.
            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            evaluation = Evaluation(id="9999999").get()
            evaluation.description = "Updated description for my evaluation"
            updated_evaluation = evaluation.store()
            ```

        Raises:
            ValueError: If required fields are missing.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
        """
        return self

    def get(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "Evaluation":
        """
        Get this Evaluation from Synapse by its ID or name.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The retrieved Evaluation object.

        Raises:
            ValueError: If neither id nor name is set.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Get an evaluation by ID
            &nbsp;

            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            evaluation = Evaluation(id="9999999").get()
            ```

        Example: Get an evaluation by name
            &nbsp;

            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            evaluation = Evaluation(name="My Challenge Evaluation").get()
            ```

        Raises:
            ValueError: If neither id nor name is set.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
        """
        return self

    def delete(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> None:
        """
        Delete this Evaluation from Synapse. ID must be set in order to delete the Evaluation.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            None

        Raises:
            ValueError: If evaluation_id is not set.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Delete an evaluation by ID
            &nbsp;

            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            Evaluation(id="9614112").delete()
            ```

        Example: Get and then delete an evaluation
            &nbsp;
            If you do not have the ID of the evaluation, you can first retrieve it from Synapse by name.
            That will populate the ID attribute in your Evaluation object, at which point you can delete it.
            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            # First get the evaluation by name, so the ID attribute is set in your
            # Evaluation object, then delete it.
            evaluation = Evaluation(name="My Challenge Evaluation").get()
            evaluation.delete()
            ```

        Raises:
            ValueError: If evaluation_id is not set.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
        """
        return None

    def get_acl(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict:
        """
        Get the access control list (ACL) governing this evaluation.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The ACL for this Evaluation.

        Raises:
            ValueError: If evaluation_id is not set.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Get the ACL for an evaluation
            &nbsp;

            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            evaluation = Evaluation(id="9999999").get()
            acl = evaluation.get_acl()
            ```
        """
        return {}

    def update_acl(
        self,
        principal_id: Optional[Union[str, int]] = None,
        access_type: Optional[List[str]] = None,
        acl: Optional[dict] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict:
        """
        Update the access control list (ACL) for this evaluation.

        You can either: <br>
        1. Provide a `principal_id` and `access_type` list to update permissions for a specific user/team <br>
        2. Provide a complete ACL dictionary to update all permissions at once

        To remove a principal from the ACL completely, provide an empty list for access_type.

        The available access types are:

        - 'CREATE'
        - 'SUBMIT'
        - 'READ_PRIVATE_SUBMISSION'
        - 'DELETE_SUBMISSION'
        - 'UPDATE_SUBMISSION'
        - 'CHANGE_PERMISSIONS'
        - 'READ'
        - 'DELETE'
        - 'UPDATE'

        Arguments:
            acl: An AccessControlList object or dictionary containing the ACL data to update.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The updated ACL.

        Raises:
            ValueError: If neither (principal_id and access_type) nor acl is provided, or if the ACL object is invalid.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Update the ACL for an evaluation
            &nbsp;

            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            evaluation = Evaluation(id="9999999").get()
            updated_acl = evaluation.update_acl(principal_id="12345", acl=["READ", "SUBMIT"])
            ```

        Raises:
            ValueError: If the ACL object is invalid or missing required fields.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
        """
        return {}

    def get_permissions(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict:
        """
        Get the user permissions for this evaluation.

        Arguments:
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The permissions for the specified user.

        Raises:
            ValueError: If evaluation_id is not set.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Get permissions for the current user
            &nbsp;

            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            evaluation = Evaluation(id="9999999").get()
            my_permissions = evaluation.get_permissions()
            ```

        Raises:
            ValueError: If evaluation_id is not set.
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
        """
        return {}

    @staticmethod
    def get_all_evaluations(
        access_type: Optional[str] = None,
        active_only: Optional[bool] = None,
        evaluation_ids: Optional[List[str]] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List["Evaluation"]:
        """
        Get a list of all Evaluations, within a given range.

        Arguments:
            access_type: The type of access for the user to filter for, optional and defaults to ACCESS_TYPE.READ.
            active_only: If True then return only those evaluations with rounds defined and for which the current time is in one of the rounds.
            evaluation_ids: An optional list of evaluation IDs to which the response is limited.
            offset: The offset index determines where this page will start from. An index of 0 is the first entity. When null it will default to 0.
            limit: Limits the number of entities that will be fetched for this page. When null it will default to 10.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            List[Evaluation]: A list of all evaluations.

        Raises:
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Get all evaluations the user has at least READ access to
            &nbsp;
            A default call will return evaluations where the user has READ access, without needing to specify access type.
            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            all_evaluations = Evaluation.get_all_evaluations()
            ```

        Example: Get only active evaluations with a limit
            &nbsp;

            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            active_evaluations = Evaluation.get_all_evaluations(
                active_only=True,
                limit=20
            )
            ```

        Raises:
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
        """
        return []

    @staticmethod
    def get_available_evaluations(
        active_only: Optional[bool] = None,
        evaluation_ids: Optional[List[str]] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List["Evaluation"]:
        """
        Get a list of Evaluations to which the user has SUBMIT permission, within a given range.

        Arguments:
            active_only: If True then return only those evaluations with rounds defined and for which the current time is in one of the rounds.
            evaluation_ids: An optional list of evaluation IDs to which the response is limited.
            offset: The offset index determines where this page will start from. An index of 0 is the first entity. When null it will default to 0.
            limit: Limits the number of entities that will be fetched for this page. When null it will default to 10.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            List[Evaluation]: A list of available evaluations.

        Raises:
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Get all evaluations where the current user has SUBMIT permission
            &nbsp;
            A default call will return evaluations where the user has SUBMIT permission, without needing to specify access type.
            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            available_evaluations = Evaluation.get_available_evaluations()
            ```

        Example: Get the first 5 evaluations where the current user has SUBMIT permission
            &nbsp;

            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            limited_evaluations = Evaluation.get_available_evaluations(
                limit=5
            )
            ```

        Raises:
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
        """
        return []

    @staticmethod
    def get_evaluations_by_project(
        project_id: str,
        access_type: Optional[str] = None,
        active_only: Optional[bool] = None,
        evaluation_ids: Optional[List[str]] = None,
        offset: Optional[int] = None,
        limit: Optional[int] = None,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> List["Evaluation"]:
        """
        Get Evaluations tied to a project.

        Arguments:
            project_id: The ID of the project (e.g., "syn123456").
            access_type: The type of access for the user to filter for, optional and defaults to ACCESS_TYPE.READ.
            active_only: If True then return only those evaluations with rounds defined and for which the current time is in one of the rounds.
            evaluation_ids: An optional list of evaluation IDs to which the response is limited.
            offset: The offset index determines where this page will start from. An index of 0 is the first entity. When null it will default to 0.
            limit: Limits the number of entities that will be fetched for this page. When null it will default to 10.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            List[Evaluation]: A list of Evaluations tied to the project.

        Raises:
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.

        Example: Get all evaluations for a project
            &nbsp;
            The user must have at least READ access to the evaluations to retrieve the evaluations from a given project.
            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            project_evaluations = Evaluation.get_evaluations_by_project(
                project_id="syn123456"
            )
            ```

        Example: Get a limited set of evaluations for a project
            &nbsp;

            ```python
            from synapseclient.models import Evaluation
            from synapseclient import Synapse

            syn = Synapse()
            syn.login()

            limited_project_evaluations = Evaluation.get_evaluations_by_project(
                project_id="syn123456",
                limit=5
            )
            ```

        Raises:
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
        """
        return []
