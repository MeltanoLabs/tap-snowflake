"""Tests standard tap features using the built-in SDK tests library."""
import os

from singer_sdk.testing import get_tap_test_class

from tap_snowflake.tap import TapSnowflake

SAMPLE_CONFIG = {
    "user": os.environ["SF_USER"],
    "password": os.environ["SF_PASSWORD"],
    "account": os.environ["SF_ACCOUNT"],
    "database": os.getenv("SF_DATABASE"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "role": os.getenv("SF_ROLE"),
}


TestTapSnowflake = get_tap_test_class(tap_class=TapSnowflake, config=SAMPLE_CONFIG)
