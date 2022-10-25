"""Tests standard tap features using the built-in SDK tests library."""
import os

from singer_sdk.testing import get_standard_tap_tests

from tap_snowflake.tap import TapSnowflake

SAMPLE_CONFIG = {
    "user": os.getenv("SF_USER"),
    "password": os.getenv("SF_PASSWORD"),
    "account": os.getenv("SF_ACCOUNT"),
    "database": os.getenv("SF_DATABASE"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "role": os.getenv("SF_ROLE"),
}


# Run standard built-in tap tests from the SDK:
def test_standard_tap_tests():
    """Run standard tap tests from the SDK."""
    tests = get_standard_tap_tests(TapSnowflake, config=SAMPLE_CONFIG)
    for test in tests:
        test()
