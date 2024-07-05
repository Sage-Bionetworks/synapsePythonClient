import asyncio
from dataclasses import dataclass, field
from typing import Dict, List, Optional, Union

from opentelemetry import context

from synapseclient import Synapse
from synapseclient.core.async_utils import async_to_sync, otel_trace_method
from synapseclient.core.utils import run_and_attach_otel_context
from synapseclient.models.protocols.user_protocol import UserProfileSynchronousProtocol
from synapseclient.team import UserGroupHeader as Synapse_UserGroupHeader
from synapseclient.team import UserProfile as Synapse_UserProfile


@dataclass
class UserGroupHeader:
    """
    Select metadata about a Synapse principal.
    In practice the constructor is not called directly by the client.

    Attributes:
        owner_id: A foreign key to the ID of the 'principal' object for the user.
        first_name: First Name
        last_name: Last Name
        user_name: A name chosen by the user that uniquely identifies them.
        email: User's current email address
        is_individual: True if this is a user, false if it is a group
    """

    owner_id: Optional[int] = None
    """A foreign key to the ID of the 'principal' object for the user."""

    first_name: Optional[str] = None
    """First Name"""

    last_name: Optional[str] = None
    """Last Name"""

    user_name: Optional[str] = None
    """A name chosen by the user that uniquely identifies them."""

    is_individual: Optional[bool] = None
    """True if this is a user, false if it is a group"""

    email: Optional[str] = None
    """User's current email address"""

    def fill_from_dict(
        self, synapse_user_group_header: Union[Synapse_UserGroupHeader, Dict[str, str]]
    ) -> "UserGroupHeader":
        self.owner_id = synapse_user_group_header.get("ownerId", None)
        self.first_name = synapse_user_group_header.get("firstName", None)
        self.last_name = synapse_user_group_header.get("lastName", None)
        self.user_name = synapse_user_group_header.get("userName", None)
        self.email = synapse_user_group_header.get("email", None)
        self.is_individual = synapse_user_group_header.get("isIndividual", None)
        return self


@dataclass()
class UserPreference:
    """
    A UserPreference represents a user preference in the system.

    Attributes:
        name: The name of the user preference.
        value: The value of the user preference.
    """

    name: Optional[str] = None
    """The name of the user preference."""

    value: Optional[bool] = None
    """The value of the user preference."""


@dataclass()
@async_to_sync
class UserProfile(UserProfileSynchronousProtocol):
    """
    UserProfile represents a user's profile in the system.

    Attributes:
        id: A foreign key to the ID of the 'principal' object for the user.
        etag: Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
            concurrent updates. Since the E-Tag changes every time an entity is updated
            it is used to detect when a client's currentrepresentation of an entity is
            out-of-date.
        first_name: This person's given name (forename)
        last_name: This person's family name (surname)
        emails: The list of user email addresses registered to this user.
        open_ids: The list of OpenIds bound to this user's account.
        username: A name chosen by the user that uniquely identifies them.
        display_name: This field is deprecated and will always be null.
        r_studio_url: URL for RStudio server assigned to the user.
        summary: A summary description about this person.
        position: This person's current position title.
        location: This person's location.
        industry: The industry/discipline that this person is associated with.
        company: This person's current affiliation.
        profile_picure_file_handle_id: The File Handle id of the user's profile picture.
        url: A link to more information about this person.
        team_name: This person's default team name.
        notification_settings: Contains a user's notification settings.
        preferences: User preferences
        created_on: The date this profile was created.
    """

    id: Optional[int] = None
    """A foreign key to the ID of the 'principal' object for the user."""

    username: Optional[str] = None
    """A name chosen by the user that uniquely identifies them."""

    etag: Optional[str] = None
    """
    Synapse employs an Optimistic Concurrency Control (OCC) scheme to handle
    concurrent updates. Since the E-Tag changes every time an entity is updated it is
    used to detect when a client's current representation of an entity is out-of-date.
    """

    first_name: Optional[str] = None
    """This person's given name (forename)"""

    last_name: Optional[str] = None
    """This person's family name (surname)"""

    emails: List[str] = field(default_factory=list)
    """The list of user email addresses registered to this user."""

    open_ids: List[str] = field(default_factory=list)
    """The list of OpenIds bound to this user's account."""

    r_studio_url: Optional[str] = None
    """URL for RStudio server assigned to the user"""

    summary: Optional[str] = None
    """A summary description about this person"""

    position: Optional[str] = None
    """This person's current position title"""

    location: Optional[str] = None
    """This person's location"""

    industry: Optional[str] = None
    """The industry/discipline that this person is associated with"""

    company: Optional[str] = None
    """This person's current affiliation"""

    profile_picure_file_handle_id: Optional[str] = None
    """The File Handle id of the user's profile picture"""

    url: Optional[str] = None
    """A link to more information about this person"""

    team_name: Optional[str] = None
    """This person's default team name"""

    send_email_notifications: Optional[bool] = True
    """Should the user receive email notifications? Default true."""

    mark_emailed_messages_as_read: Optional[bool] = False
    """Should messages that are emailed to the user be marked as
    READ in Synapse? Default false."""

    preferences: Optional[List[UserPreference]] = field(default_factory=list)
    """User preferences"""

    created_on: Optional[str] = None
    """The date this profile was created."""

    def fill_from_dict(
        self, synapse_user_profile: Union[Synapse_UserProfile, Dict]
    ) -> "UserProfile":
        """Fills the UserProfile object from a dictionary.

        Arguments:
            synapse_user_profile: The dictionary to fill the UserProfile object from.
                Typically filled from a
                [Synapse UserProfile](https://rest-docs.synapse.org/rest/org/sagebionetworks/repo/model/UserProfile.html) object.
        """
        self.id = (
            int(synapse_user_profile.get("ownerId", None))
            if synapse_user_profile.get("ownerId", None)
            else None
        )
        self.etag = synapse_user_profile.get("etag", None)
        self.first_name = synapse_user_profile.get("firstName", None)
        self.last_name = synapse_user_profile.get("lastName", None)
        self.emails = synapse_user_profile.get("emails", [])
        self.open_ids = synapse_user_profile.get("openIds", [])
        self.username = synapse_user_profile.get("userName", None)
        self.r_studio_url = synapse_user_profile.get("rStudioUrl", None)
        self.summary = synapse_user_profile.get("summary", None)
        self.position = synapse_user_profile.get("position", None)
        self.location = synapse_user_profile.get("location", None)
        self.industry = synapse_user_profile.get("industry", None)
        self.company = synapse_user_profile.get("company", None)
        self.profile_picure_file_handle_id = synapse_user_profile.get(
            "profilePicureFileHandleId", None
        )
        self.url = synapse_user_profile.get("url", None)
        self.team_name = synapse_user_profile.get("teamName", None)
        notification_settings = synapse_user_profile.get("notificationSettings", {})
        self.send_email_notifications = notification_settings.get(
            "sendEmailNotifications", True
        )
        self.mark_emailed_messages_as_read = notification_settings.get(
            "markEmailedMessagesAsRead", False
        )
        synapse_preferences_list = synapse_user_profile.get("preferences", [])
        preferences = []
        for preference in synapse_preferences_list:
            preferences.append(
                UserPreference(
                    name=preference.get("name", None),
                    value=preference.get("value", None),
                )
            )
        self.preferences = preferences
        self.created_on = synapse_user_profile.get("createdOn", None)

        return self

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Profile_Get: Username: {self.username}, id: {self.id}"
    )
    async def get_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "UserProfile":
        """
        Gets a UserProfile object using its id or username in that order. If an id
        and username is not specified this will retrieve the current user's profile.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The UserProfile object.

        """
        loop = asyncio.get_event_loop()

        current_context = context.get_current()
        if self.id:
            synapse_user_profile = await loop.run_in_executor(
                None,
                lambda: run_and_attach_otel_context(
                    lambda: Synapse.get_client(
                        synapse_client=synapse_client
                    ).get_user_profile_by_id(id=self.id),
                    current_context,
                ),
            )
        elif self.username:
            synapse_user_profile = await loop.run_in_executor(
                None,
                lambda: run_and_attach_otel_context(
                    lambda: Synapse.get_client(
                        synapse_client=synapse_client
                    ).get_user_profile_by_username(username=self.username),
                    current_context,
                ),
            )
        else:
            synapse_user_profile = await loop.run_in_executor(
                None,
                lambda: run_and_attach_otel_context(
                    lambda: Synapse.get_client(
                        synapse_client=synapse_client
                    ).get_user_profile_by_username(),
                    current_context,
                ),
            )

        self.fill_from_dict(synapse_user_profile=synapse_user_profile)
        return self

    @classmethod
    @otel_trace_method(
        method_to_trace_name=lambda cls, user_id, **kwargs: f"Profile_From_Id: {user_id}"
    )
    async def from_id_async(
        cls, user_id: int, *, synapse_client: Optional[Synapse] = None
    ) -> "UserProfile":
        """Gets UserProfile object using its integer id. Wrapper for the
        [get][synapseclient.models.UserProfile.get] method.

        Arguments:
            user_id: The id of the user.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The UserProfile object.
        """

        return await cls(id=user_id).get_async(synapse_client=synapse_client)

    @classmethod
    @otel_trace_method(
        method_to_trace_name=lambda cls, username, **kwargs: f"Profile_From_Username: {username}"
    )
    async def from_username_async(
        cls, username: str, *, synapse_client: Optional[Synapse] = None
    ) -> "UserProfile":
        """
        Gets UserProfile object using its string name. Wrapper for the
        [get][synapseclient.models.UserProfile.get] method.

        Arguments:
            username: A name chosen by the user that uniquely identifies them.
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            The UserProfile object.
        """
        return await cls(username=username).get_async(synapse_client=synapse_client)

    @otel_trace_method(
        method_to_trace_name=lambda self, **kwargs: f"Profile_is_certified: Username: {self.username}, id: {self.id}"
    )
    async def is_certified_async(
        self,
        *,
        synapse_client: Optional[Synapse] = None,
    ) -> "bool":
        """
        Determine whether a user is certified.

        Arguments:
            synapse_client: If not passed in or None this will use the last client
                from the `.login()` method.

        Returns:
            True if the user is certified, False otherwise.

        Raises:
            ValueError: If id nor username is specified.
        """
        loop = asyncio.get_event_loop()

        current_context = context.get_current()
        if self.id or self.username:
            is_certified = await loop.run_in_executor(
                None,
                lambda: run_and_attach_otel_context(
                    lambda: Synapse.get_client(
                        synapse_client=synapse_client
                    ).is_certified(user=self.id or self.username),
                    current_context,
                ),
            )
        else:
            raise ValueError("Must specify either id or username")

        return is_certified
