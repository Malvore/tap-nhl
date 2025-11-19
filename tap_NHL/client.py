"""REST client handling, including NHLStream base class."""

from __future__ import annotations

import decimal
import typing as t
from importlib import resources

from singer_sdk.authenticators import APIAuthenticatorBase
from singer_sdk.helpers.jsonpath import extract_jsonpath
from singer_sdk.pagination import JSONPathPaginator  # noqa: TC002
from singer_sdk.streams import RESTStream
from singer_sdk.streams.rest import _HTTPStream

from tap_NHL.constants import DEFAULT_API_URL

if t.TYPE_CHECKING:
    import requests
    from singer_sdk.helpers.types import Context

SCHEMAS_DIR = resources.files(__package__) / "schemas"


class NHLPaginator(JSONPathPaginator):
    """NHL pagination class."""
    pass


class NHLAuthenticator(APIAuthenticatorBase):
    """Authenticator class for NHL."""

    def __init__(self, stream: _HTTPStream, token: str) -> None:
        """Create a new authenticator.

        Args:
            stream: The stream instance to use with this authenticator.
            token: Authentication token.
        """
        super().__init__(stream=stream)
        auth_credentials = {"Authorization": f"Bearer {token}"}

        if self.auth_headers is None:
            self.auth_headers = {}  # type: ignore[unreachable]
        self.auth_headers.update(auth_credentials)

    @classmethod
    def create_for_stream(
            cls: type[NHLAuthenticator],
            stream: _HTTPStream,
            token: str,
    ) -> NHLAuthenticator:
        """Create an Authenticator object specific to the Stream class.

        Args:
            stream: The stream instance to use with this authenticator.
            token: Authentication token.

        Returns:
            BearerTokenAuthenticator: A new
                :class:`singer_sdk.authenticators.BearerTokenAuthenticator` instance.
        """
        return cls(stream=stream, token=token)


class NHLStream(RESTStream):
    """NHL stream class."""

    # Update this value if necessary or override `parse_response`.
    records_jsonpath = "$.standings[*]"

    # Update this value if necessary or override `get_new_paginator`.
    next_page_token_jsonpath = "$.pagination.nextCursor"  # noqa: S105

    @property
    def url_base(self) -> str:
        """Return the API URL root, configurable via tap settings."""
        return self.config.get("api_url") or DEFAULT_API_URL

    @property
    def http_headers(self) -> dict:
        """Return the http headers needed.

        Returns:
            A dictionary of HTTP headers.
        """
        # If not using an authenticator, you may also provide inline auth headers:
        # headers["Private-Token"] = self.config.get("auth_token")  # noqa: ERA001
        headers = {"Content-type": "application/json", }
        return headers

    def get_new_paginator(self) -> NHLPaginator:
        """Create a new pagination helper instance.

        If the source API can make use of the `next_page_token_jsonpath`
        attribute, or it contains a `X-Next-Page` header in the response
        then you can remove this method.

        If you need custom pagination that uses page numbers, "next" links, or
        other approaches, please read the guide: https://sdk.meltano.com/en/v0.25.0/guides/pagination-classes.html.

        Returns:
            A pagination helper instance.
        """
        return NHLPaginator(jsonpath="pagination.nextCursor")

    def get_url_params(
            self,
            context: Context | None,  # noqa: ARG002
            next_page_token: t.Any | None,  # noqa: ANN401
    ) -> dict[str, t.Any]:
        """Return a dictionary of values to be used in URL parameterization.

        Args:
            context: The stream context.
            next_page_token: The next page index or value.

        Returns:
            A dictionary of URL query parameters.
        """
        params: dict = {"limit": 1000,
                        }
        if next_page_token:
            params["cursor"] = next_page_token
        if self.replication_key:
            params["sortBy"] = self.replication_key
        return params

    def parse_response(self, response: requests.Response) -> t.Iterable[dict]:
        """Parse the response and return an iterator of result records.

        Args:
            response: The HTTP ``requests.Response`` object.

        Yields:
            Each record from the source.
        """
        yield from extract_jsonpath(
            self.records_jsonpath,
            input=response.json(parse_float=decimal.Decimal),
        )
