"""Arrow BATCH file writer for Snowflake streams."""

from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from uuid import uuid4

import pyarrow as pa
from pyarrow import ipc

if TYPE_CHECKING:
    from singer_sdk.helpers._batch import StorageTarget


class SnowflakeArrowBatchWriter:
    """Buffers Arrow tables and flushes them to Arrow IPC files by row count.

    Snowflake's native Arrow result chunks don't line up with a stream's
    configured ``batch_size`` -- a chunk may be smaller or much larger than
    the target batch size -- so incoming tables are sliced and buffered until
    enough rows have accumulated, then written out as a single Arrow IPC file.
    """

    def __init__(
        self,
        *,
        stream_name: str,
        storage: StorageTarget,
        batch_size: int,
    ) -> None:
        """Initialize the writer.

        Args:
            stream_name: The name of the stream being batched, used in file names.
            storage: The batch config's storage target. Files are always written
                to its local root path (matching the existing internal-stage
                JSON batch implementation, which shares this local-only limitation).
            batch_size: The max number of rows to buffer before flushing a file.
        """
        self.stream_name = stream_name
        self.files: list[str] = []

        self._storage = storage
        self._local_root = Path(storage.root.replace("file://", ""))
        self._local_root.mkdir(parents=True, exist_ok=True)
        self._batch_size = batch_size
        self._buffered: list[pa.Table] = []
        self._buffered_rows = 0

    def write(self, table: pa.Table) -> None:
        """Buffer an Arrow table, flushing whenever the batch size is reached.

        Args:
            table: A table of rows fetched from Snowflake.
        """
        if table.num_rows == 0:
            return

        offset = 0
        while offset < table.num_rows:
            capacity = self._batch_size - self._buffered_rows
            chunk = table.slice(offset, capacity)
            self._buffered.append(chunk)
            self._buffered_rows += chunk.num_rows
            offset += chunk.num_rows

            if self._buffered_rows >= self._batch_size:
                self.flush()

    def flush(self) -> None:
        """Write any buffered rows to a new Arrow IPC file."""
        if not self._buffered:
            return

        table = pa.concat_tables(self._buffered)
        filename = f"{self.stream_name}-{uuid4().hex}.arrow"
        with ipc.new_file(self._local_root / filename, table.schema) as writer:
            writer.write_table(table)

        self.files.append(self._storage.get_url(filename))
        self._buffered = []
        self._buffered_rows = 0
