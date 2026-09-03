"""Tests standard tap features using the built-in SDK tests library."""

import json
from pathlib import Path

from singer_sdk.singerlib import Catalog
from singer_sdk.testing import SuiteConfig, get_tap_test_class

from tap_snowflake.tap import TapSnowflake

SAMPLE_CONFIG = {
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

CATALOG = Catalog.from_dict(json.loads(Path("tests/catalog.json").read_text()))


TestTapSnowflake = get_tap_test_class(
    tap_class=TapSnowflake,
    config=SAMPLE_CONFIG,
    suite_config=SuiteConfig(
        max_records_limit=100,
        ignore_no_records_for_streams=["tpch_sf1-lineitem"],
    ),
    catalog=CATALOG,
)
