"""Unit tests for the Arrow BATCH file helpers."""

from __future__ import annotations

import pyarrow as pa
from pyarrow import ipc
from singer_sdk.helpers._batch import StorageTarget

from tap_snowflake.batch import rechunk, write_arrow_file


def _table(n: int, offset: int = 0) -> pa.Table:
    return pa.table({"id": list(range(offset, offset + n))})


def _ids(tables: list[pa.Table]) -> list[list[int]]:
    return [table.column("id").to_pylist() for table in tables]


def test_rechunk_smaller_than_batch_size_yields_one_table():
    assert _ids(list(rechunk([_table(3)], 10))) == [[0, 1, 2]]


def test_rechunk_exactly_batch_size_yields_one_table():
    assert _ids(list(rechunk([_table(5)], 5))) == [[0, 1, 2, 3, 4]]


def test_rechunk_splits_a_table_larger_than_batch_size():
    assert _ids(list(rechunk([_table(10)], 4))) == [
        [0, 1, 2, 3],
        [4, 5, 6, 7],
        [8, 9],
    ]


def test_rechunk_accumulates_small_tables():
    tables = [_table(2, offset=0), _table(2, offset=2), _table(3, offset=4)]
    assert _ids(list(rechunk(tables, 6))) == [[0, 1, 2, 3, 4, 5], [6]]


def test_rechunk_skips_empty_tables():
    assert list(rechunk([_table(0)], 5)) == []
    assert list(rechunk([], 5)) == []


def test_write_arrow_file_round_trips(tmp_path):
    url = write_arrow_file(
        _table(3),
        storage=StorageTarget(root=f"file://{tmp_path / 'batches'}"),
        stream_name="widgets",
    )

    assert url.endswith(".arrow")
    with ipc.open_file(url.replace("file://", "")) as reader:
        assert reader.read_all().column("id").to_pylist() == [0, 1, 2]
