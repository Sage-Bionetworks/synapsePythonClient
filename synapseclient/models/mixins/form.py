from dataclasses import dataclass
from enum import Enum
from typing import Any, Optional


@dataclass
class FormGroup:
    """Dataclass representing a FormGroup.s"""

    group_id: Optional[str] = None
    """Unique identifier provided by the system."""

    name: Optional[str] = None
    """Unique name for the group provided by the caller."""

    created_by: Optional[str] = None
    """Id of the user that created this group"""

    created_on: Optional[str] = None
    """The date this object was originally created."""

    def fill_from_dict(self, synapse_response: dict[str, Any]) -> "FormGroup":
        """Converts a response from the REST API into this dataclass."""
        self.group_id = synapse_response.get("groupId", None)
        self.name = synapse_response.get("name", None)
        self.created_by = synapse_response.get("createdBy", None)
        self.created_on = synapse_response.get("createdOn", None)

        return self


@dataclass
class FormChangeRequest:
    """Dataclass representing a FormChangeRequest."""

    name: Optional[str] = None
    """The name of the form. Required for FormData create. Optional for FormData update. Between 3 and 256 characters"""

    file_handle_id: Optional[str] = None
    """The fileHandleId for the data of the form."""

    def to_dict(self) -> dict[str, Any]:
        """Converts this dataclass into a dictionary for REST API requests."""
        request_dict: dict[str, Any] = {}
        if self.name is not None:
            request_dict["name"] = self.name
        if self.file_handle_id is not None:
            request_dict["fileHandleId"] = self.file_handle_id
        return request_dict


class StateEnum(str, Enum):
    """
    The enumeration of possible FormData submission states.
    """

    WAITING_FOR_SUBMISSION = "WAITING_FOR_SUBMISSION"
    """Indicates that the FormData is waiting for the creator to submit it."""

    SUBMITTED_WAITING_FOR_REVIEW = "SUBMITTED_WAITING_FOR_REVIEW"
    """Indicates the FormData has been submitted and is now awaiting review."""

    ACCEPTED = "ACCEPTED"
    """The submitted FormData has been reviewed and accepted."""

    REJECTED = "REJECTED"
    """The submitted FormData has been reviewed but was not accepted. See the rejection message for more details."""


@dataclass
class FormSubmissionStatus:
    """
    The status of a submitted FormData object.
    """

    submitted_on: Optional[str] = None
    """The date when the object was submitted."""

    reviewed_on: Optional[str] = None
    """The date when this submission was reviewed."""

    reviewed_by: Optional[str] = None
    """The id of the service user that reviewed the submission."""

    state: Optional[StateEnum] = None
    """The enumeration of possible FormData submission states."""

    rejection_message: Optional[str] = None
    """The message provided by the reviewer when a submission is rejected."""

    def fill_from_dict(
        self, synapse_response: dict[str, Any]
    ) -> "FormSubmissionStatus":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response dictionary from the Synapse REST API.

        Returns:
            This FormSubmissionStatus object with populated fields.
        """
        self.submitted_on = synapse_response.get("submittedOn", None)
        self.reviewed_on = synapse_response.get("reviewedOn", None)
        self.reviewed_by = synapse_response.get("reviewedBy", None)

        # Handle enum conversion
        self.state = (
            StateEnum(synapse_response.get("state", None))
            if synapse_response.get("state", None)
            else None
        )
        self.rejection_message = synapse_response.get("rejectionMessage", None)

        return self


@dataclass
class FormData:
    """
    Represents a FormData object in Synapse.
    """

    form_data_id: Optional[str] = None
    """The system issued identifier that uniquely identifies this object."""

    etag: Optional[str] = None
    """Will change whenever there is a change to this data or its status."""

    group_id: Optional[str] = None
    """The identifier of the group that manages this data. Required."""

    name: Optional[str] = None
    """User provided name for this submission. Required."""

    created_by: Optional[str] = None
    """Id of the user that created this object."""

    created_on: Optional[str] = None
    """The date this object was originally created."""

    modified_on: Optional[str] = None
    """The date this object was last modified."""

    data_file_handle_id: Optional[str] = None
    """The identifier of the data FileHandle for this object."""

    submission_status: Optional[FormSubmissionStatus] = None
    """The status of a submitted FormData object."""

    def fill_from_dict(self, synapse_response: dict[str, Any]) -> "FormData":
        """
        Converts a response from the REST API into this dataclass.

        Arguments:
            synapse_response: The response dictionary from the Synapse REST API.

        Returns:
            This FormData object with populated fields.
        """
        self.form_data_id = synapse_response.get("formDataId", None)
        self.etag = synapse_response.get("etag", None)
        self.group_id = synapse_response.get("groupId", None)
        self.name = synapse_response.get("name", None)
        self.created_by = synapse_response.get("createdBy", None)
        self.created_on = synapse_response.get("createdOn", None)
        self.modified_on = synapse_response.get("modifiedOn", None)
        self.data_file_handle_id = synapse_response.get("dataFileHandleId", None)

        if (
            "submissionStatus" in synapse_response
            and synapse_response["submissionStatus"] is not None
        ):
            self.submission_status = FormSubmissionStatus().fill_from_dict(
                synapse_response["submissionStatus"]
            )

        return self
