
import json
import os
import subprocess
from pathlib import Path

import requests
from singer_sdk.testing import get_tap_test_class

from tap_NHL.constants import DEFAULT_API_URL
from tap_NHL.streams import GoaliesStream, SkatersStream
from tap_NHL.tap import TapNHL

# Set project root path
project_root = Path(__file__).resolve().parent.parent

# Set config path
config_path = project_root / "tests" / "sample_config.json"

# Set schema path
skaters_schema_path = project_root / "tap_NHL" / "schemas" / "skaters.json"
goalies_schema_path = project_root / "tap_NHL" / "schemas" / "goalies.json"

#---------------- Start Configuration Section ----------------
if not config_path.exists():
    raise FileNotFoundError("❌  Config file not found in the specified path")
else:
    print(f"✅  Config file found")

# Grab the API URL from the config file if present, otherwise default to the public NHL endpoint.
with config_path.open() as config_file:
    config = json.load(config_file)

api_url = config.get("api_url", DEFAULT_API_URL)
skater_ids = config.get("skater_ids") or config.get("player_ids", [])
goalie_ids = config.get("goalie_ids", [])

if not api_url:
    raise ValueError("❌  API URL not found in the specified path")
else:
    print(f"✅ Public API URL found")

if skater_ids:
    print(f"✅ Skater IDs found in config: {skater_ids}")
else:
    print("ℹ️ No skater IDs provided; tap will auto-discover the full roster.")

if goalie_ids:
    print(f"✅ Goalie IDs found in config: {goalie_ids}")
else:
    print("ℹ️ No goalie IDs provided; tap will auto-discover the full roster.")

# Sample configuration that the SDK would use to run the tap:
SAMPLE_CONFIG = {
  "api_url": api_url,
}
if skater_ids:
    SAMPLE_CONFIG["skater_ids"] = skater_ids
if goalie_ids:
    SAMPLE_CONFIG["goalie_ids"] = goalie_ids
#---------------- End Configuration Section ----------------

# ---------------- Start API Connection Test Section ----------------
def test_api_root_connection():
    """Test connection to the API with GET request to the root endpoint."""
    try:
        root_endpoint = api_url
        response = requests.get(root_endpoint)
        response.raise_for_status()
        print(f"✅  Connection to API root successful")
        print(f"✅  Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌  Connection to API root failed. Error: {e} ")

test_api_root_connection()

def test_player_endpoint_connection():
    """Test connection to the API with GET request to the player landing endpoint."""
    test_ids = skater_ids or goalie_ids
    if not test_ids:
        print("ℹ️ Skipping player endpoint connectivity check; no player IDs configured.")
        return
    try:
        player_endpoint = f"{api_url}/v1/player/{test_ids[0]}/landing"
        response = requests.get(player_endpoint)
        response.raise_for_status()
        print(f"✅  Connection to player endpoint successful")
        print(f"✅  Status code: {response.status_code}")
    except requests.exceptions.RequestException as e:
        print(f"❌  Connection to player endpoint failed")
        print(f"❌  Error: {e}")

test_player_endpoint_connection()
# ---------------- End API Connection Test Section ----------------

# ---------------- Start tap.py Test Section ----------------
def test_tap_metadata():
    """Confirm that the tap name is correct and that the config schema contains the 'api_url' property"""
    assert TapNHL.name == "tap-nhl", "❌ Tap name is incorrect."
    print(f"✅ Tap name is correct: {TapNHL.name}")
    assert "api_url" in TapNHL.config_jsonschema["properties"], "❌ 'api_url' not in config schema."
    print(f"✅ 'api_url' found in config schema.")
    for key in ("skater_ids", "goalie_ids", "player_ids"):
        assert key in TapNHL.config_jsonschema["properties"], f"❌ '{key}' not in config schema."
        print(f"✅ '{key}' found in config schema.")

test_tap_metadata()
# ---------------- End tap.py Test Section ----------------

# ---------------- Start streams.py Test Section ----------------
def test_skaters_stream_parameters():
    """Confirm that the SkatersStream class has the correct parameters"""
    assert SkatersStream.name == "skaters", "❌ Skaters stream name is incorrect."
    print(f"✅ Skaters stream name: {SkatersStream.name}")
    assert SkatersStream.path == "/v1/player/{player_id}/landing", "❌ Skaters stream path is incorrect."
    print(f"✅ Skaters stream path is correct: {SkatersStream.path}")
    assert SkatersStream.primary_keys == ["playerId"], "❌ Skaters stream primary key incorrect."
    print(f"✅ Skaters stream primary key is correct: {SkatersStream.primary_keys}")
    assert str(SkatersStream.schema_filepath).endswith("skaters.json"), "❌ Skaters stream schema path is incorrect."
    print(f"✅ Skaters stream schema path is correct: {SkatersStream.schema_filepath}")


def test_goalies_stream_parameters():
    """Confirm that the GoaliesStream class has the correct parameters"""
    assert GoaliesStream.name == "goalies", "❌ Goalies stream name is incorrect."
    print(f"✅ Goalies stream name: {GoaliesStream.name}")
    assert GoaliesStream.path == "/v1/player/{player_id}/landing", "❌ Goalies stream path is incorrect."
    print(f"✅ Goalies stream path is correct: {GoaliesStream.path}")
    assert GoaliesStream.primary_keys == ["playerId"], "❌ Goalies stream primary key incorrect."
    print(f"✅ Goalies stream primary key is correct: {GoaliesStream.primary_keys}")
    assert str(GoaliesStream.schema_filepath).endswith("goalies.json"), "❌ Goalies stream schema path is incorrect."
    print(f"✅ Goalies stream schema path is correct: {GoaliesStream.schema_filepath}")

test_skaters_stream_parameters()
test_goalies_stream_parameters()
# ---------------- End streams.py Test Section ----------------

# ---------------- Start Catalog Test Section ----------------
def test_streams_in_discover():
    # Set path to the project root directory to run discovery
    env = os.environ.copy()
    env["PYTHONPATH"] = str(project_root)

    # Run the Singer SDK discover command
    result = subprocess.run(
        ["uv", "run", "tap_NHL", "--config",  str(config_path), "--discover"],
        capture_output=True,
        text=True,
        check=True,
        cwd=str(project_root),
        env=env
    )

    discover_output = json.loads(result.stdout)
    stream_names = {stream["stream"] for stream in discover_output["streams"]}

    assert "skaters" in stream_names, "❌ 'skaters' stream not found in discovery output."
    assert "goalies" in stream_names, "❌ 'goalies' stream not found in discovery output."
    print("✅ 'skaters' and 'goalies' streams are present in discovery output.")

test_streams_in_discover()
# ---------------- End Catalog Test Section ----------------

# ---------------- Start Schema Test Section ----------------
def test_schema_files():
    """Confirm that the schema files exist"""
    assert skaters_schema_path.exists(), "❌ Skaters schema file does not exist."
    print(f"✅ Skaters schema file exists at: {skaters_schema_path}")
    assert goalies_schema_path.exists(), "❌ Goalies schema file does not exist."
    print(f"✅ Goalies schema file exists at: {goalies_schema_path}")

test_schema_files()
# ---------------- End Schema Test Section ----------------
#sdfsdf
#sdfsdfsdf
