# tap-nhl

A Singer tap for extracting player landing data from the public NHL Stats API using the Meltano Singer SDK.

## Overview

`tap-nhl` discovers every skater and goalie ID published by the NHL Stats API (or a list you provide) and pulls landing-page records containing bios, featured stats, last-five-game summaries, and career totals. Separate streams keep goalie metrics (save %, goals-against, etc.) distinct from skater scoring metrics so you can land clean tables downstream.

## Supported Streams

- **skaters** – Autodiscovered or explicitly configured skater IDs.
- **goalies** – Autodiscovered or explicitly configured goalie IDs.

## Installation

### Prerequisites

- Linux or WSL2 host
- Python 3.12+
- [uv](https://github.com/astral-sh/uv) (Python package/env manager)
- [Meltano](https://meltano.com) if you plan to orchestrate ELT pipelines

### Clone and install dependencies

```bash
git clone <repository-url> tap-nhl
cd tap-nhl
uv sync           # creates a .venv with the tap's dependencies
```

### Install Meltano plugins

```bash
pipx install meltano   # if Meltano isn't already available
meltano install        # installs tap-nhl + targets listed in meltano.yml
```

---

## Configuration

`tap-nhl` accepts the following config properties (see `tests/sample_config.json` for an example):

- `api_url` *(string, default `https://api-web.nhle.com`)* – Base URL for the NHL Stats API.
- `skater_ids` *(array[int], optional)* – Explicit list of skater IDs to sync. Leave empty to auto-discover every skater for the configured seasons.
- `goalie_ids` *(array[int], optional)* – Explicit list of goalie IDs to sync.
- `player_ids` *(array[int], optional, deprecated)* – Backward-compatible alias for `skater_ids`.
- `discovery_seasons` *(array[int], optional)* – Explicit season IDs to use for player discovery (for example, `20232024`). These are full season IDs (year concatenated). Leave empty to scan the full range (1917 through current).

Autodiscovery seasons are controlled via `tap_NHL/constants.py`. Update `PLAYER_DISCOVERY_SEASON_START`, `PLAYER_DISCOVERY_SEASON_END`, or `PLAYER_DISCOVERY_SEASONS` to shrink or expand the window once, and every run will honor that range.

Environment variables from `.env` are automatically read when you run `tap-nhl --config ENV`.

To inspect all config options supported by the SDK, run:

```bash
uv run tap-nhl --about
```

---

## Running the tap directly (uv)

Create a config file (example):

```json
{
  "api_url": "https://api-web.nhle.com",
  "skater_ids": [8479318, 8478402],
  "goalie_ids": [8476945]
}
```

Run discovery and sync:

```bash
# Discover available streams/schemas
uv run tap-nhl --config config.json --discover > catalog.json

# Sync the selected streams
uv run tap-nhl --config config.json
```

`uv run` automatically uses the local `.venv` created by `uv sync`, so you don’t need to activate anything manually.

---

## Running with Meltano

After `meltano install`, you can operate the tap through Meltano just like any other plugin:

```bash
# (Optional) broaden selections once so both streams are loaded
meltano select tap-nhl '*.*'

# Run a full ELT into target-postgres
meltano run tap-nhl target-postgres

# Invoke the tap standalone
meltano invoke tap-nhl

# Re-run discovery through Meltano
meltano invoke tap-nhl --discover
```

Remember to run your `meltano select` anytime you add new streams (e.g., goalies) so the state includes them.

---

## Development workflow

```bash
# Install deps (one-time)
uv sync

# Run tests / linting
uv run pytest

# Launch the CLI directly
uv run tap-nhl --help
```

The repo ships with a `tests` folder containing SDK-based sanity checks plus a sample config you can copy for local runs.

---

## Notes on rate limiting

Both streams inherit a shared rate limiter that spaces landing-endpoint requests by ~0.35 seconds to avoid NHL API throttling. Adjust `RATE_LIMIT_SECONDS` in `tap_NHL/streams.py` if you want to run slower/faster.

---

## Postgres setup and Meltano pipeline example

The tap ships with a Meltano project already configured for `target-postgres`. If you want to follow a full pipeline tutorial (from installing Postgres through Dockerizing the project), here’s a condensed version of the guide I use when teaching Meltano workshops:

### 1. Install PostgreSQL locally (Ubuntu/Debian example)

```bash
sudo apt update
sudo apt install postgresql
sudo systemctl enable --now postgresql
```

Create the Meltano user/database the tap expects:

```bash
export TARGET_POSTGRES_HOST=127.0.0.1
export TARGET_POSTGRES_PORT=5432
export TARGET_POSTGRES_USER=meltano
export TARGET_POSTGRES_PASSWORD='meltanopassword'
export TARGET_POSTGRES_DATABASE=meltano_database

sudo -u postgres psql -c "CREATE ROLE \"$TARGET_POSTGRES_USER\" WITH LOGIN PASSWORD '$TARGET_POSTGRES_PASSWORD';"
sudo -u postgres psql -c "CREATE DATABASE \"$TARGET_POSTGRES_DATABASE\" OWNER \"$TARGET_POSTGRES_USER\";"
sudo -u postgres psql -c "GRANT ALL PRIVILEGES ON DATABASE \"$TARGET_POSTGRES_DATABASE\" TO \"$TARGET_POSTGRES_USER\";"
```

### 2. Configure Meltano

- Copy `.env.example` (or create `.env`) with the `TARGET_POSTGRES_*` values above.
- Run `meltano install` to install both the tap and `target-postgres`.

To invoke the tap + target:

```bash
meltano select tap-nhl '*.*'
meltano run tap-nhl target-postgres
```

The target will create schemas such as `tap_nhl` with `skaters` and `goalies` tables.

### 3. Inspect the database

```bash
sudo -u postgres psql -d "$TARGET_POSTGRES_DATABASE"
# inside psql
\dn                   -- list schemas (tap_nhl should appear)
\dt tap_nhl.*         -- list tables
SELECT * FROM tap_nhl.skaters LIMIT 5;
```

### 4. Optional: Dockerize the project

If you want to ship your Meltano pipeline in a container, there is a simple image in the root directory. This image installs basic tools (jq/curl/ping), and is set to perform a full table load each run:

```dockerfile
FROM meltano/meltano:latest-python3.12
WORKDIR /project
RUN apt update && apt install -y jq curl inetutils-ping && apt clean
COPY . .
RUN meltano install
ENV MELTANO_PROJECT_READONLY=1
ENTRYPOINT ["meltano"]
CMD ["run", "--full-refresh", "tap-nhl", "target-postgres"]
```

Build and run:

```bash
docker build -t tap-nhl:latest .
docker run --rm --network host \
  -e TARGET_POSTGRES_HOST=127.0.0.1 \
  -e TARGET_POSTGRES_PORT=5432 \
  -e TARGET_POSTGRES_USER=meltano \
  -e TARGET_POSTGRES_PASSWORD=meltanopassword \
  -e TARGET_POSTGRES_DATABASE=meltano_database \
  tap-nhl:latest
```

---

## Query examples using Postgres

The `postgres_queries/` directory contains SQL you can run against the `tap_nhl` schema to highlight skating/goalie insights. Currently available:

- [`postgres_queries/games_played.sql`](postgres_queries/games_played.sql) – lists the top regular-season and playoff ironmen for both skaters and goalies. Each snippet targets the flattened fields emitted by `target-postgres` (for example, `"careerTotals__regularSeason__gamesPlayed"`) and uses an adjustable `LIMIT`.

Run the full script after a Meltano sync finishes:

```bash
psql "$TARGET_POSTGRES_DATABASE" -f postgres_queries/games_played.sql
```
