import abc
import base64
import json
import typing
from dataclasses import dataclass, field
from typing import Optional

import requests.auth

from synapseclient.core.exceptions import SynapseAuthenticationError


class SynapseCredentials(requests.auth.AuthBase, abc.ABC):
    @property
    @abc.abstractmethod
    def username(self) -> None:
        """The username associated with these credentials."""

    @property
    @abc.abstractmethod
    def secret(self) -> None:
        """The secret associated with these credentials."""

    @property
    @abc.abstractmethod
    def owner_id(self) -> None:
        """The owner id, or profile id, associated with these credentials."""

    @property
    @abc.abstractmethod
    def profile_name(self) -> None:
        """The name of the profile used to find these credentials. May be None."""


class SynapseAuthTokenCredentials(SynapseCredentials):
    @classmethod
    def get_keyring_service_name(
        cls,
    ) -> typing.Literal["SYNAPSE.ORG_CLIENT_AUTH_TOKEN"]:
        return "SYNAPSE.ORG_CLIENT_AUTH_TOKEN"

    @classmethod
    def _validate_token(cls, token):
        # decode the token to ensure it minimally has view scope.
        # if it doesn't raise an error, the client will not be useful without it.

        # if for any reason we are not able to decode the token and check its scopes
        # we do NOT raise an error. this is to accommodate the possibility of a changed
        # token format some day that this version of the client may still be able to
        # pass as a bearer token.
        try:
            token_body = json.loads(
                str(
                    base64.urlsafe_b64decode(
                        # we add padding to ensure that lack of padding won't prevent a decode error.
                        # the python base64 implementation will truncate extra padding so we can overpad
                        # rather than compute exactly how much padding we might need.
                        # https://stackoverflow.com/a/49459036
                        token.split(".")[1]
                        + "==="
                    ),
                    "utf-8",
                )
            )
            scopes = token_body.get("access", {}).get("scope")
            if scopes is not None and "view" not in scopes:
                raise SynapseAuthenticationError("A view scoped token is required")

        except (IndexError, ValueError):
            # possible errors if token is not encoded as expected:
            # IndexError if the token is not a '.' delimited base64 string with a header and body
            # ValueError if the split string is not base64 encoded or if the decoded base64 is not json
            pass

    def __init__(
        self, token: str, username: str = None, displayname: str = None
    ) -> None:
        self._validate_token(token)

        self._token = token
        self.username = username
        self.displayname = displayname
        self.owner_id = None

    @property
    def username(self) -> str:
        """The username associated with this token."""
        return self._username

    @username.setter
    def username(self, username: str) -> None:
        self._username = username

    @property
    def displayname(self) -> str:
        """The displayname associated with this token."""
        return self._displayname

    @displayname.setter
    def displayname(self, displayname: str) -> None:
        self._displayname = displayname

    @property
    def owner_id(self) -> str:
        """The owner id associated with this token."""
        return self._owner_id

    @owner_id.setter
    def owner_id(self, owner_id: str) -> None:
        self._owner_id = owner_id

    @property
    def secret(self) -> str:
        """The bearer token."""
        return self._token

    @property
    def profile_name(self) -> str:
        """The name of the profile used to find these credentials."""
        return self._profile_name

    @profile_name.setter
    def profile_name(self, profile_name: str) -> None:
        self._profile_name = profile_name

    def __call__(self, r):
        if self.secret:
            r.headers.update({"Authorization": f"Bearer {self.secret}"})
        return r

    def __repr__(self):
        return (
            f"SynapseAuthTokenCredentials("
            f"username='{self.username}', "
            f"displayname='{self.displayname}', "
            f"token='{self.secret}', "
            f"owner_id='{self.owner_id}')"
        )


@dataclass
class UserLoginArgs:
    """
    Data class representing user login arguments for authentication.

    Attributes:
        profile (Optional[str]): The profile name to use for authentication.
                                 Defaults to "default".
        username (Optional[str]): The Synapse username associated with the login profile.
                                  Defaults to None.
        auth_token (Optional[str]): The authentication token for logging in.
                                    Hidden from debug logs for security.
    """

    profile: Optional[str] = field(default="default")
    username: Optional[str] = field(default=None)
    auth_token: Optional[str] = field(
        default=None, repr=False
    )  # Hide auth_token from debug logs
