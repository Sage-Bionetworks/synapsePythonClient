import abc
import base64
import collections
import json
import typing

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
    def secret(self) -> str:
        """The bearer token."""
        return self._token

    def __call__(self, r):
        r.headers.update({"Authorization": f"Bearer {self.secret}"})
        return r

    def __repr__(self):
        return (
            f"SynapseAuthTokenCredentials("
            f"username='{self.username}', "
            f"displayname='{self.displayname}', "
            f"token='{self.secret}')"
        )


# a class that just contains args passed form synapse client login
UserLoginArgs = collections.namedtuple(
    "UserLoginArgs",
    [
        "username",
        "auth_token",
    ],
)

# make the namedtuple's arguments optional instead of positional. All values default to None
# when we require Python 3.6.1 we can use typing.NamedTuple's built-in default support
UserLoginArgs.__new__.__defaults__ = (None,) * len(UserLoginArgs._fields)
