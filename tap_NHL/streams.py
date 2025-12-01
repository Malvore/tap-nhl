"""Stream type classes for tap-nhl."""

from __future__ import annotations

import typing as t
from importlib import resources
import time
from datetime import UTC, datetime

import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry

from tap_NHL.client import NHLStream
from tap_NHL.constants import (
    GOALIE_DISCOVERY_ENDPOINT,
    PLAYER_DISCOVERY_MAX_RETRIES,
    PLAYER_DISCOVERY_PAGE_SIZE,
    PLAYER_DISCOVERY_SEASON_END,
    PLAYER_DISCOVERY_SEASON_START,
    PLAYER_DISCOVERY_SEASONS,
    PLAYER_DISCOVERY_TIMEOUT,
    SKATER_DISCOVERY_ENDPOINT,
)

SCHEMAS_DIR = resources.files(__package__) / "schemas"


class PlayerLandingStream(NHLStream):
    """Shared implementation for the NHL landing endpoint."""

    path = "/v1/player/{player_id}/landing"
    primary_keys: t.ClassVar[list[str]] = ["playerId"]
    records_jsonpath = "$"
    discovery_endpoints: t.ClassVar[tuple[str, ...]] = ()
    config_player_ids_keys: t.ClassVar[tuple[str, ...]] = ("player_ids",)
    _auto_player_ids: list[int] | None = None
    _season_ids: list[int] | None = None
    _discovery_session: requests.Session | None = None
    _last_request_ts: float | None = None
    RATE_LIMIT_SECONDS = 0.35
    LOCALE_FIELDS = [
        "fullTeamName",
        "teamCommonName",
        "teamPlaceNameWithPreposition",
        "firstName",
        "lastName",
        "birthCity",
        "birthStateProvince",
    ]

    @property
    def partitions(self) -> list[dict[str, int]] | None:
        """Partition records by configured player IDs or discover all players."""
        player_ids = self._get_configured_player_ids()
        if not player_ids:
            player_ids = self._get_all_player_ids()
        partitions = [{"player_id": player_id} for player_id in player_ids]
        return partitions or None

    def get_records(
        self,
        context: dict | None,
    ) -> t.Iterable[dict] | t.Iterable[tuple[dict, dict | None]]:
        """Yield records for the configured player IDs only."""
        if not context or "player_id" not in context:
            self.logger.debug(
                "No player ID in context; skipping %s stream sync.",
                self.name,
            )
            return iter([])
        return super().get_records(context)

    def post_process(  # noqa: D401
        self,
        row: dict,
        context: dict | None = None,
    ) -> dict | None:
        """Flatten localized fields into plain strings."""
        row = super().post_process(row, context)
        if row is None:
            return None

        for field in self.LOCALE_FIELDS:
            if field in row:
                row[field] = self._extract_default_locale(row.get(field))

        # Flatten localized fields inside seasonTotals entries.
        for entry in row.get("seasonTotals") or []:
            for nested_field in (
                "teamName",
                "teamCommonName",
                "teamPlaceNameWithPreposition",
            ):
                if nested_field in entry:
                    entry[nested_field] = self._extract_default_locale(entry.get(nested_field))

        return row

    def _get_all_player_ids(self) -> list[int]:
        """Fetch player IDs for the entire NHL historical dataset."""
        if not hasattr(self, "_auto_player_ids") or self._auto_player_ids is None:
            self._auto_player_ids = self._fetch_all_player_ids()
        return self._auto_player_ids

    def _fetch_all_player_ids(self) -> list[int]:
        """Retrieve every player ID using the stats API summary endpoint."""
        player_ids: set[int] = set()
        session = self._get_discovery_session()
        if not self.discovery_endpoints:
            msg = "discovery_endpoints must be defined on PlayerLandingStream subclasses."
            raise ValueError(msg)
        endpoints = self.discovery_endpoints
        for season_id in self._fetch_season_ids():
            for endpoint in endpoints:
                start = 0
                while True:
                    params = {
                        "isAggregate": "false",
                        "isGame": "false",
                        "start": start,
                        "limit": PLAYER_DISCOVERY_PAGE_SIZE,
                        "cayenneExp": f"seasonId={season_id}",
                    }
                    response = session.get(
                        endpoint,
                        params=params,
                        timeout=PLAYER_DISCOVERY_TIMEOUT,
                    )
                    response.raise_for_status()
                    payload = response.json()
                    data = payload.get("data") or []
                    for row in data:
                        player_id = row.get("playerId")
                        if player_id is not None:
                            player_ids.add(int(player_id))
                    total = payload.get("total") or 0
                    start += PLAYER_DISCOVERY_PAGE_SIZE
                    if start >= total or not data:
                        break

        return sorted(player_ids)

    def _fetch_season_ids(self) -> list[int]:
        """Build the list of season IDs honoring the configured discovery range."""
        if self._season_ids is not None:
            return self._season_ids

        discovery_seasons = (
            self.config.get("discovery_seasons") or PLAYER_DISCOVERY_SEASONS
        )
        if discovery_seasons:
            season_ids = sorted(set(discovery_seasons))
        else:
            current_year = datetime.now(UTC).year + 1
            start_year = PLAYER_DISCOVERY_SEASON_START
            end_year = PLAYER_DISCOVERY_SEASON_END or current_year
            season_ids = [
                int(f"{year}{year + 1}")
                for year in range(start_year, end_year)
            ]

        self._season_ids = season_ids
        return season_ids

    def _get_discovery_session(self) -> requests.Session:
        """Create or return a session with retry/backoff for discovery calls."""
        if self._discovery_session is None:
            retry = Retry(
                total=PLAYER_DISCOVERY_MAX_RETRIES,
                read=PLAYER_DISCOVERY_MAX_RETRIES,
                connect=PLAYER_DISCOVERY_MAX_RETRIES,
                backoff_factor=2,
                status_forcelist=[429, 500, 502, 503, 504],
            )
            adapter = HTTPAdapter(max_retries=retry)
            session = requests.Session()
            session.mount("https://", adapter)
            session.mount("http://", adapter)
            self._discovery_session = session
        return self._discovery_session

    def _request(
        self,
        prepared_request: requests.PreparedRequest,
        context: dict | None,
    ) -> requests.Response:
        """Throttle outgoing API calls to avoid rate limits."""
        self._apply_rate_limit()
        return super()._request(prepared_request, context)

    def _apply_rate_limit(self) -> None:
        """Sleep briefly between requests to avoid 429 rate limits."""
        now = time.monotonic()
        if self._last_request_ts is not None:
            elapsed = now - self._last_request_ts
            if elapsed < self.RATE_LIMIT_SECONDS:
                time.sleep(self.RATE_LIMIT_SECONDS - elapsed)
        self._last_request_ts = time.monotonic()

    @staticmethod
    def _extract_default_locale(value: t.Any) -> t.Any:
        """Return the 'default' locale value if present."""
        if isinstance(value, dict):
            for key in ("default", "en", "eng"):
                if key in value and value[key]:
                    return value[key]
            # Fall back to first non-null value
            for val in value.values():
                if val:
                    return val
            return None
        return value

    def _get_configured_player_ids(self) -> list[int]:
        """Return the configured IDs for this stream, if provided."""
        for key in self.config_player_ids_keys:
            configured = self.config.get(key)
            if configured:
                return [int(player_id) for player_id in configured]
        return []


class SkatersStream(PlayerLandingStream):
    """Stream returning landing information for configured skaters."""

    name = "skaters"
    schema_filepath = SCHEMAS_DIR / "skaters.json"
    discovery_endpoints = (SKATER_DISCOVERY_ENDPOINT,)
    config_player_ids_keys = ("skater_ids", "player_ids")


class GoaliesStream(PlayerLandingStream):
    """Stream returning landing information for configured goalies."""

    name = "goalies"
    schema_filepath = SCHEMAS_DIR / "goalies.json"
    discovery_endpoints = (GOALIE_DISCOVERY_ENDPOINT,)
    config_player_ids_keys = ("goalie_ids",)
