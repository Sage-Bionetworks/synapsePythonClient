class EvaluationSynchronousProtocol(Protocol):
    """
    This is the protocol for methods that are asynchronous
    but also have a synchronous counterpart that may also be called.
    """


@dataclass
@async_to_sync
class Evaluation(EvaluationSynchronousProtocol):
    """
    An Evaluation is the core object of the Evaluation API, used to support collaborative data analysis challenges in Synapse.

    An `Evaluation` object represents an evaluation queue in Synapse:
    <https://rest-docs.synapse.org/rest/org/sagebionetworks/evaluation/model/Evaluation.html>

    Attributes:
        id: The unique immutable ID for this Evaluation.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent updates.
              The eTag changes every time an Evaluation is updated; it is used to detect when a client's copy
              of an Evaluation is out-of-date.
        name: The name of this Evaluation.
        description: A text description of this Evaluation.
        owner_id: The ID of the Synapse user who created this Evaluation.
        created_on: The date on which Evaluation was created.
        content_source: The Synapse ID of the Entity to which this Evaluation belongs,
                        e.g. a reference to a Synapse project.
        submission_instructions_message: Message to display to users detailing acceptable formatting for Submissions to this Evaluation.
        submission_receipt_message: Message to display to users upon successful submission to this Evaluation.

    Example:
    """

    id: Optional[str] = None
    """The unique immutable ID for this Evaluation."""

    etag: Optional[str] = None
    """Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle concurrent updates.
    The eTag changes every time an Evaluation is updated; it is used to detect when a client's copy
    of an Evaluation is out-of-date."""

    name: Optional[str] = None
    """The name of this Evaluation."""

    description: Optional[str] = None
    """A text description of this Evaluation."""

    owner_id: Optional[str] = None
    """The ID of the Synapse user who created this Evaluation."""

    created_on: Optional[str] = None
    """The date on which Evaluation was created."""

    content_source: Optional[str] = None
    """The Synapse ID of the Entity to which this Evaluation belongs,
    e.g. a reference to a Synapse project."""

    submission_instructions_message: Optional[str] = None
    """Message to display to users detailing acceptable formatting for Submissions to this Evaluation."""

    submission_receipt_message: Optional[str] = None
    """Message to display to users upon successful submission to this Evaluation."""

    def fill_from_dict(self, entity: dict) -> "Evaluation":
        """
        Converts the data coming from the Synapse Evaluation API into this datamodel.

        Arguments:
            entity: The data coming from the Synapse Evaluation API

        Returns:
            The Evaluation object instance.
        """
        self.id = entity.get("id", None)
        self.etag = entity.get("etag", None)
        self.name = entity.get("name", None)
        self.description = entity.get("description", None)
        self.owner_id = entity.get("ownerId", None)
        self.created_on = entity.get("createdOn", None)
        self.content_source = entity.get("contentSource", None)
        self.submission_instructions_message = entity.get(
            "submissionInstructionsMessage", None
        )
        self.submission_receipt_message = entity.get("submissionReceiptMessage", None)

        return self
