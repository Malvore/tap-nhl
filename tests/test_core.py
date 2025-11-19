"""Tests standard tap features using the built-in SDK tests library."""

from singer_sdk.testing import get_tap_test_class
from tap_NHL.tap import TapNHL
import json
from pathlib import Path
import requests

# Search for config file in the .secrets directory, confirm its existence.
project_root = Path(__file__).resolve().parent.parent
config_path = project_root / ".secrets" / "config.json"

# Grab the API URL from the config file.
with config_path.open() as config_file:
    config = json.load(config_file)

api_url = config.get("api_url")

# Sample configuration that the SDK would use to run the tap:
SAMPLE_CONFIG = {
  "api_url": api_url,
}

# Run standard built-in tap tests from the SDK:
TestTapNHL = get_tap_test_class(
    tap_class=TapNHL,
    config=SAMPLE_CONFIG,
)

