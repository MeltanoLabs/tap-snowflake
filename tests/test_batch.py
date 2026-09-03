"""Unit tests for SnowflakeArrowBatchWriter."""

from __future__ import annotations

import pyarrow as pa
from pyarrow import ipc
from singer_sdk.helpers._batch import StorageTarget

from tap_snowflake.batch import SnowflakeArrowBatchWriter


def _table(n: int, offset: int = 0) -> pa.Table:
    return pa.table({"id": list(range(offset, offset + n))})


def _read_all_rows(paths: list[str]) -> list[int]:
    rows: list[int] = []
    for url in paths:
        path = url.replace("file://", "")
        with ipc.open_file(path) as reader:
            rows.extend(reader.read_all().column("id").to_pylist())
    return rows


def test_flush_below_batch_size_is_noop_until_flush_called(tmp_path):
    writer = SnowflakeArrowBatchWriter(
        stream_name="widgets",
        storage=StorageTarget(root=f"file://{tmp_path}"),
        batch_size=10,
    )
    writer.write(_table(3))
    assert writer.files == []

    writer.flush()
    assert len(writer.files) == 1
    assert _read_all_rows(writer.files) == [0, 1, 2]


def test_write_flushes_automatically_at_batch_size(tmp_path):
    writer = SnowflakeArrowBatchWriter(
        stream_name="widgets",
        storage=StorageTarget(root=f"file://{tmp_path}"),
        batch_size=5,
    )
    writer.write(_table(5))
    assert len(writer.files) == 1
    assert _read_all_rows(writer.files) == [0, 1, 2, 3, 4]


def test_single_table_larger_than_batch_size_splits_into_multiple_files(tmp_path):
    writer = SnowflakeArrowBatchWriter(
        stream_name="widgets",
        storage=StorageTarget(root=f"file://{tmp_path}"),
        batch_size=4,
    )
    writer.write(_table(10))
    assert len(writer.files) == 2
    assert sorted(_read_all_rows(writer.files)) == list(range(8))

    writer.flush()
    assert len(writer.files) == 3
    assert sorted(_read_all_rows(writer.files)) == list(range(10))


def test_multiple_chunks_accumulate_across_writes(tmp_path):
    writer = SnowflakeArrowBatchWriter(
        stream_name="widgets",
        storage=StorageTarget(root=f"file://{tmp_path}"),
        batch_size=6,
    )
    writer.write(_table(2, offset=0))
    writer.write(_table(2, offset=2))
    assert writer.files == []

    writer.write(_table(2, offset=4))
    assert len(writer.files) == 1
    assert sorted(_read_all_rows(writer.files)) == list(range(6))


def test_empty_table_is_ignored(tmp_path):
    writer = SnowflakeArrowBatchWriter(
        stream_name="widgets",
        storage=StorageTarget(root=f"file://{tmp_path}"),
        batch_size=5,
    )
    writer.write(_table(0))
    assert writer.files == []

    writer.flush()
    assert writer.files == []


def test_flush_with_nothing_buffered_is_noop(tmp_path):
    writer = SnowflakeArrowBatchWriter(
        stream_name="widgets",
        storage=StorageTarget(root=f"file://{tmp_path}"),
        batch_size=5,
    )
    writer.flush()
    assert writer.files == []
