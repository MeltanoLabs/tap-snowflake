"""Tests standard tap features using the built-in SDK tests library."""
import os

from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_snowflake.tap import TapSnowflake

SAMPLE_CONFIG = {
    "user": os.environ["SF_USER"],
    "password": os.environ["SF_PASSWORD"],
    "account": os.environ["SF_ACCOUNT"],
    "database": os.getenv("SF_DATABASE"),
    "warehouse": os.getenv("SF_WAREHOUSE"),
    "role": os.getenv("SF_ROLE"),
    "tables": [
        "tpch_sf1.customer",
        "tpch_sf1.lineitem",
        "tpch_sf1.nation",
        "tpch_sf1.orders",
        "tpch_sf1.part",
        "tpch_sf1.partsupp",
        "tpch_sf1.region",
        "tpch_sf1.supplier",
    ],
}


TestTapSnowflake = get_tap_test_class(
    tap_class=TapSnowflake,
    config=SAMPLE_CONFIG,
    suite_config=SuiteConfig(
        max_records_limit=100, ignore_no_records_for_streams=["tpch_sf1-lineitem"]
    ),
    catalog="tests/catalog.json",
)
