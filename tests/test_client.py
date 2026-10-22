"""Unit tests for SnowflakeStream."""

from __future__ import annotations

import json
import logging
import typing as t
from pathlib import Path
from unittest import mock

import pyarrow as pa
import sqlalchemy as sa
from singer_sdk.helpers._batch import BatchConfig
from singer_sdk.singerlib import Catalog
from snowflake.sqlalchemy import snowdialect

from tap_snowflake.client import SnowflakeStream
from tap_snowflake.tap import TapSnowflake

if t.TYPE_CHECKING:
    import pytest

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


def test_build_select_quotes_aliases_so_snowflake_preserves_casing():
    """Snowflake uppercases a bare alias at runtime, so it must render quoted."""
    catalog = Catalog.from_dict(json.loads(Path("tests/catalog.json").read_text()))
    tap = TapSnowflake(config=SAMPLE_CONFIG, catalog=catalog, validate_config=False)
    stream = tap.streams["tpch_sf1-customer"]

    properties = list(stream.get_selected_schema()["properties"].keys())
    fake_table = _fake_table(properties, table_name="CUSTOMER", schema_name="TPCH_SF1")

    with mock.patch.object(stream.connector, "get_table", return_value=fake_table):
        query = stream._build_select()

    compiled = str(query.compile(dialect=snowdialect.SnowflakeDialect()))
    for name in properties:
        assert f'AS "{name}"' in compiled, compiled


def _bare_stream() -> SnowflakeStream:
    """A stream instance without a connection, enough to call `get_batch_config`."""
    stream = SnowflakeStream.__new__(SnowflakeStream)
    stream._logger = logging.getLogger("test")
    return stream


def test_batch_config_absent_stays_in_record_mode():
    assert _bare_stream().get_batch_config({}) is None


def test_batch_config_without_encoding_stays_in_record_mode():
    """Partial values (e.g. from stale Hub defaults) must not enable BATCH."""
    config = {
        "batch_config": {
            "batch_size": 100_000,
            "storage": {"root": "file://.batch_files"},
        },
    }
    assert _bare_stream().get_batch_config(config) is None


def test_batch_config_without_encoding_warns(caplog: pytest.LogCaptureFixture):
    config = {"batch_config": {"storage": {"root": "file://.batch_files"}}}
    with caplog.at_level(logging.WARNING):
        assert _bare_stream().get_batch_config(config) is None
    assert "Ignoring `batch_config` because it has no `encoding`" in caplog.text


def test_batch_config_with_encoding_applies_defaults():
    batch_config = _bare_stream().get_batch_config(
        {"batch_config": {"encoding": {"format": "arrow"}}},
    )
    assert batch_config is not None
    assert batch_config.batch_size == 100_000
    assert batch_config.storage.root == "file://.batch_files"


def test_batch_config_schema_has_no_required_properties():
    """The Hub turns nested `required` into mandatory settings, enabling BATCH."""
    schema = TapSnowflake.config_jsonschema["properties"]["batch_config"]
    assert "required" not in schema
    assert "required" not in schema["properties"]["encoding"]


def test_arrow_batches_publish_each_file_with_its_own_state(
    tmp_path: Path,
    capsys: pytest.CaptureFixture[str],
):
    """Each Arrow file gets a BATCH message, then STATE bookmarking its last row."""
    catalog = Catalog.from_dict(
        json.loads(Path("tests/catalog-incremental.json").read_text()),
    )
    config = {**SAMPLE_CONFIG, "tables": ["tpch_sf1.supplier"]}
    tap = TapSnowflake(config=config, catalog=catalog, validate_config=False)
    stream = tap.streams["tpch_sf1-supplier"]
    batch_config = BatchConfig.from_dict(
        {
            "encoding": {"format": "arrow"},
            "storage": {"root": f"file://{tmp_path}"},
            "batch_size": 2,
        },
    )
    fetched = [
        pa.table({"s_suppkey": [1, 2, 3], "s_name": ["a", "b", "c"]}),
        pa.table({"s_suppkey": [4, 5], "s_name": ["d", "e"]}),
    ]
    result = mock.MagicMock()
    result.cursor.fetch_arrow_batches.return_value = iter(fetched)

    with (
        mock.patch.object(stream, "_build_select"),
        mock.patch.object(stream.connector, "execute", return_value=result),
        mock.patch.object(stream.connector, "get_table_profile") as profile,
    ):
        stream._sync_batches(batch_config)

    profile.assert_not_called()
    messages = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    batches = [i for i, m in enumerate(messages) if m["type"] == "BATCH"]
    assert [len(messages[i]["manifest"]) for i in batches] == [1, 1, 1]
    bookmarks = [
        messages[i + 1]["value"]["bookmarks"]["tpch_sf1-supplier"][
            "replication_key_value"
        ]
        for i in batches
    ]
    assert bookmarks == [2, 4, 5]
