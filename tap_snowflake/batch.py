"""Arrow BATCH file helpers for Snowflake streams."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pyarrow as pa
from pyarrow import ipc

if TYPE_CHECKING:
    from collections.abc import Iterable, Iterator

    from singer_sdk.helpers._batch import StorageTarget


def rechunk(tables: Iterable[pa.Table], batch_size: int) -> Iterator[pa.Table]:
    """Re-slice Arrow tables into tables of ``batch_size`` rows.

    Snowflake's native Arrow result chunks don't line up with a stream's
    configured ``batch_size`` -- a chunk may be smaller or much larger than
    the target batch size -- so incoming tables are sliced and buffered until
    enough rows have accumulated.

    Args:
        tables: Tables of rows fetched from Snowflake, in order.
        batch_size: The number of rows per output table. The last one may be
            smaller.

    Yields:
        Tables of at most ``batch_size`` rows.
    """
    buffered: list[pa.Table] = []
    buffered_rows = 0

    for table in tables:
        offset = 0
        while offset < table.num_rows:
            chunk = table.slice(offset, batch_size - buffered_rows)
            buffered.append(chunk)
            buffered_rows += chunk.num_rows
            offset += chunk.num_rows

            if buffered_rows >= batch_size:
                yield pa.concat_tables(buffered)
                buffered = []
                buffered_rows = 0

    if buffered:
        yield pa.concat_tables(buffered)


def write_arrow_file(
    table: pa.Table,
    *,
    storage: StorageTarget,
    stream_name: str,
) -> str:
    """Write a table to a new Arrow IPC file.

    Args:
        table: The rows to write.
        storage: The batch config's storage target. Files are always written
            to its local root path (matching the existing internal-stage
            JSON batch implementation, which shares this local-only limitation).
        stream_name: The name of the stream being batched, used in file names.

    Returns:
        The URL of the written file.
    """
    local_root = Path(storage.root.replace("file://", ""))
    local_root.mkdir(parents=True, exist_ok=True)

    filename = f"{stream_name}-{uuid4().hex}.arrow"
    with ipc.new_file(local_root / filename, table.schema) as writer:
        writer.write_table(table)

    return storage.get_url(filename)
