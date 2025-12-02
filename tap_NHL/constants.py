"""Module containing shared constants for tap-nhl."""

DEFAULT_API_URL = "https://api-web.nhle.com"
STATS_API_BASE_URL = "https://api.nhle.com/stats/rest/en"
SKATER_DISCOVERY_ENDPOINT = f"{STATS_API_BASE_URL}/skater/summary"
GOALIE_DISCOVERY_ENDPOINT = f"{STATS_API_BASE_URL}/goalie/summary"
PLAYER_DISCOVERY_PAGE_SIZE = 250
PLAYER_DISCOVERY_TIMEOUT = 60  # seconds
PLAYER_DISCOVERY_MAX_RETRIES = 5

# Configure season discovery:
# - If PLAYER_DISCOVERY_SEASONS is not empty, those season IDs are used.
# - Otherwise, the tap builds a full backfill run from the NHL's first season to current season.
PLAYER_DISCOVERY_SEASONS: list[int] = []
PLAYER_DISCOVERY_SEASON_START = 1917  # 1917 is the first NHL season.
PLAYER_DISCOVERY_SEASON_END: int | None = None    # None: Includes records through the current season.
