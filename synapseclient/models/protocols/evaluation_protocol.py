"""Protocol for the specific methods of this class that have synchronous counterparts
generated at runtime."""

from typing import TYPE_CHECKING, Dict, List, Optional, Protocol

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

        Example: Using this method to create a new evaluation
            Create a new evaluation in a project with ID "syn123456":

                evaluation = Evaluation(
                    name="My Challenge Evaluation",
                    description="Evaluation for my data challenge",
                    content_source="syn123456",
                    submission_instructions_message="Submit CSV files only",
                    submission_receipt_message="Thank you for your submission!"
                ).store()

        Example: Using this method to update an existing evaluation
            Update an evaluation that was retrieved from Synapse:

                evaluation = Evaluation(id="9999999").get()
                evaluation.description = "Updated description for my evaluation"
                updated_evaluation = evaluation.store()
        
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

        Example: Using this method
            Get an evaluation by ID:

                evaluation = Evaluation(id="9999999").get()

            Get an evaluation by name:

                evaluation = Evaluation(name="My Challenge Evaluation").get()

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

        Example: Using this method
            Delete an evaluation by ID:

                Evaluation(id="9614112").delete()

            Get and then delete an evaluation:

                # First get the evaluation by name, so the ID attribute is set in your
                # Evaluation object, then delete it.
                evaluation = Evaluation(name="My Challenge Evaluation").get()
                evaluation.delete()

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

        Example: Using this method
            Get the ACL for an evaluation:

                evaluation = Evaluation(id="9999999").get()
                acl = evaluation.get_acl()
        """
        return {}
        
    def update_acl(
        self,
        acl: Dict,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> Dict:
        """
        Update the access control list (ACL) for this evaluation.

        Arguments:
            acl: An AccessControlList object or dictionary containing the ACL data to update.
            synapse_client: If not passed in and caching was not disabled by
                `Synapse.allow_client_caching(False)` this will use the last created
                instance from the Synapse class constructor.

        Returns:
            The updated ACL.

        Example: Using this method
            Update the ACL for an evaluation:

                evaluation = Evaluation(id="9999999").get()
                acl = evaluation.get_acl()

                # Modify the ACL - this is just an example, actual structure may vary
                acl["resourceAccess"].append({
                    "principalId": "12345",
                    "accessType": ["READ", "SUBMIT"]
                })

                updated_acl = evaluation.update_acl(acl)

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

        Example: Using this method
            Get permissions for the current user:

                evaluation = Evaluation(id="9999999").get()
                my_permissions = evaluation.get_permissions()

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

        Example: Using this method
            Get all evaluations the user has at least READ access to:

                all_evaluations = Evaluation.get_all_evaluations()

            Get only active evaluations with a limit:

                active_evaluations = Evaluation.get_all_evaluations(
                    active_only=True,
                    limit=20
                )

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

        Example: Using this method
            Get all evaluations where the current user has SUBMIT permission:

                available_evaluations = Evaluation.get_available_evaluations()

            Get the first 5 evaluations where the current user has SUBMIT permission:

                limited_evaluations = Evaluation.get_available_evaluations(
                    limit=5
                )

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

        Example: Using this method
            Get all evaluations for a project:

                project_evaluations = Evaluation.get_evaluations_by_project(
                    project_id="syn123456"
                )

            Get a limited set of evaluations for a project:

                limited_project_evaluations = Evaluation.get_evaluations_by_project(
                    project_id="syn123456",
                    limit=5
                )

        Raises:
            SynapseHTTPError: If the service rejects the request or an HTTP error occurs.
        """
        return []
