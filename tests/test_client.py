"""Unit tests for SnowflakeStream._build_select."""

from __future__ import annotations

import json
from pathlib import Path
from unittest import mock

import sqlalchemy as sa
from singer_sdk.singerlib import Catalog

from tap_snowflake.tap import TapSnowflake

SAMPLE_CONFIG = {
    "tables": ["tpch_sf1.customer"],
    "user": "u",
    "password": "p",
    "account": "acct",
}


def _fake_table(
    properties: list[str],
    *,
    table_name: str,
    schema_name: str,
) -> sa.Table:
    """Build a Table mimicking Snowflake's uppercase unquoted-identifier casing."""
    meta = sa.MetaData()
    columns = [sa.Column(name.upper(), sa.Integer()) for name in properties]
    return sa.Table(table_name, meta, *columns, schema=schema_name)


def test_build_select_labels_columns_to_schema_property_casing():
    catalog = Catalog.from_dict(json.loads(Path("tests/catalog.json").read_text()))
    tap = TapSnowflake(config=SAMPLE_CONFIG, catalog=catalog, validate_config=False)
    stream = tap.streams["tpch_sf1-customer"]

    properties = list(stream.get_selected_schema()["properties"].keys())
    fake_table = _fake_table(properties, table_name="CUSTOMER", schema_name="TPCH_SF1")

    with mock.patch.object(stream.connector, "get_table", return_value=fake_table):
        query = stream._build_select()

    assert [col.name for col in query.selected_columns] == properties


def test_build_select_orders_by_replication_key_despite_casing_mismatch():
    catalog = Catalog.from_dict(
        json.loads(Path("tests/catalog-incremental.json").read_text()),
    )
    config = {**SAMPLE_CONFIG, "tables": ["tpch_sf1.supplier"]}
    tap = TapSnowflake(config=config, catalog=catalog, validate_config=False)
    stream = tap.streams["tpch_sf1-supplier"]

    assert stream.replication_key == "s_suppkey"

    properties = list(stream.get_selected_schema()["properties"].keys())
    fake_table = _fake_table(properties, table_name="SUPPLIER", schema_name="TPCH_SF1")

    with mock.patch.object(stream.connector, "get_table", return_value=fake_table):
        query = stream._build_select()

    assert [col.name for col in query.selected_columns] == properties
    order_by_names = [col.name for col in query._order_by_clauses]
    assert order_by_names == ["S_SUPPKEY"]
