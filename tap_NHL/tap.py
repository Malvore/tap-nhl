from __future__ import annotations

from singer_sdk import Tap
from singer_sdk import typing as th

from tap_NHL import streams
from tap_NHL.constants import DEFAULT_API_URL


class TapNHL(Tap):

    name = "tap-nhl"

    config_jsonschema = th.PropertiesList(
        th.Property(
            "api_url",
            th.StringType,
            title="API URL",
            default=DEFAULT_API_URL,
            description="The base URL for the NHL Stats API",
        ),
        th.Property(
            "skater_ids",
            th.ArrayType(th.IntegerType),
            title="Skater IDs",
            description=(
                "Optional override list of NHL skater IDs to sync. "
                "Leave empty to auto-discover every available skater."
            ),
            default=[],
        ),
        th.Property(
            "goalie_ids",
            th.ArrayType(th.IntegerType),
            title="Goalie IDs",
            description=(
                "Optional override list of NHL goalie IDs to sync. "
                "Leave empty to auto-discover every available goalie."
            ),
            default=[],
        ),
        th.Property(
            "player_ids",
            th.ArrayType(th.IntegerType),
            title="Player IDs (deprecated)",
            description=(
                "Backward-compatible alias for skater_ids. "
                "Prefer the skater_ids and goalie_ids options going forward."
            ),
            default=[],
        ),
    ).to_dict()

    def discover_streams(self) -> list[streams.NHLStream]:
        """Return a list of discovered streams.

        Returns:
            A list of discovered streams.
        """
        return [
            streams.SkatersStream(self),
            streams.GoaliesStream(self),
        ]


if __name__ == "__main__":
    TapNHL.cli()
