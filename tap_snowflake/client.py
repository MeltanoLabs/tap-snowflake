"""SQL client handling.

This includes SnowflakeStream and SnowflakeConnector.
"""

from __future__ import annotations

import os
from pathlib import Path
from typing import Iterable, List, Tuple
from uuid import uuid4

from singer_sdk import SQLConnector, SQLStream
from singer_sdk.helpers._batch import BaseBatchFileEncoding, BatchConfig
from singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL
from snowflake.sqlalchemy import URL
from sqlalchemy.orm import load_only
from sqlalchemy.sql import text


class SnowflakeConnector(SQLConnector):
    """Connects to the Snowflake SQL source."""

    @classmethod
    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        params = {
            "account": config["account"],
            "user": config["user"],
            "password": config["password"],
        }

        for option in ["database", "schema", "warehouse", "role"]:
            if config.get(option):
                params[option] = config.get(option)

        return URL(**params)


class SelectableSQLStream(SQLStream):
    """Patches the parent classes get_records method to support stream property selection."""

    def get_selected_columns(self, table) -> List:
        """Filter column list according to selection criteria."""
        return [
            col.name for col in table.columns if self.mask[("properties", col.name)]
        ]

    def get_records(self, context: dict | None) -> Iterable[dict[str, Any]]:
        """Return a generator of record-type dictionary objects.

        If the stream has a replication_key value defined, records will be sorted by the
        incremental key. If the stream also has an available starting bookmark, the
        records will be filtered for values greater than or equal to the bookmark value.

        Args:
            context: If partition context is provided, will read specifically from this
                data slice.

        Yields:
            One dict per record.

        Raises:
            NotImplementedError: If partition is passed in context and the stream does
                not support partitioning.
        """
        if context:
            raise NotImplementedError(
                f"Stream '{self.name}' does not support partitioning."
            )

        table = self.connector.get_table(self.fully_qualified_name)
        query = table.select()
        selected_columns = self.get_selected_columns(table=table)
        query = query.options(load_only(*selected_columns))
        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            query = query.order_by(replication_key_col)
            start_val = (
                self.get_starting_timestamp(context=context)
                if self.is_timestamp_replication_key
                else self.get_starting_replication_key_value(context=context)
            )
            if start_val:
                query = query.filter(replication_key_col >= start_val)

        for record in self.connector.connection.execute(query):
            yield dict(record)


class SnowflakeStream(SelectableSQLStream):
    """Stream class for Snowflake streams."""

    connector_class = SnowflakeConnector

    def get_batches(
        self, batch_config: BatchConfig, context: dict | None = None
    ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Get batches of Records from Snowflake.

        Currently this returns records batches unloaded via an internal user stage.
        In future this can be updated to include new methods for unloading via external stages.
        For more details on batch unloading data from Snowflake, see the Snowflake docs:
        https://docs.snowflake.com/en/user-guide-data-unload.html
        """
        if context:
            raise NotImplementedError(
                f"Stream '{self.name}' does not support partitioning."
            )
        return self.get_batches_from_internal_user_stage(batch_config, context)

    @staticmethod
    def _get_full_table_copy_statement(
        sync_id: str, prefix: str, objects: str, table_name: str
    ) -> Tuple[text, dict]:
        """Get FULL_TABLE copy statement and key bindings."""
        return (
            text(
                f"copy into '@~/tap-snowflake/{sync_id}/{prefix}' from "
                + f"(select object_construct({', '.join(objects)}) from {table_name}) "
                + "file_format = (type='JSON', compression='GZIP') overwrite = TRUE"
            ),
            {},
        )

    def _get_incremental_copy_statement(
        self,
        sync_id: str,
        prefix: str,
        objects: str,
        table_name: str,
        table,
        replication_key_value,
    ) -> Tuple[text, dict]:
        """Get INCREMENTAL copy statement and key bindings."""
        return (
            text(
                f"copy into '@~/tap-snowflake/{sync_id}/{prefix}' from "
                + f"(select object_construct({', '.join(objects)}) from {table_name} where {self.replication_key} >= :replication_key_value order by {self.replication_key})"
                + "file_format = (type='JSON', compression='GZIP') overwrite = TRUE"
            ),
            {"replication_key_value": replication_key_value},
        )

    def _get_copy_statement(
        self, sync_id: str, prefix: str, context: dict | None = None
    ) -> Tuple[text, dict]:
        """Construct copy statement, taking into account stream property selection and incremental keys."""
        table_name = self.fully_qualified_name
        table = self.connector.get_table(table_name)
        selected_columns = self.get_selected_columns(table=table)
        objects = [f"'{col}', {col}" for col in selected_columns]

        if self.replication_method == REPLICATION_FULL_TABLE:
            return self._get_full_table_copy_statement()

        elif self.replication_method == REPLICATION_INCREMENTAL:
            replication_key_value = (
                self.get_starting_timestamp(context=context)
                if self.is_timestamp_replication_key
                else self.get_starting_replication_key_value(context=context)
            )
            if replication_key_value:
                return self._get_incremental_copy_statement(
                    sync_id=sync_id,
                    prefix=prefix,
                    objects=objects,
                    table_name=table_name,
                    table=table,
                    replication_key_value=replication_key_value,
                )
            else:
                return self._get_full_table_copy_statement(
                    sync_id=sync_id,
                    prefix=prefix,
                    objects=objects,
                    table_name=table_name,
                )
        else:
            raise NotImplementedError(
                "Only 'FULL_TABLE' and 'INCREMENTAL' replication strategies are supported by this tap."
            )

    def get_batches_from_internal_user_stage(
        self, batch_config: BatchConfig, context: dict | None = None
    ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Unload Snowflake table to User Internal Stage, and download files to local storage.

        This method uses the Internal stage type, into the Snowflake-managed stage created
        for each user.

        More details on how this works can be found in the Snowflake docs:
        https://docs.snowflake.com/en/user-guide/data-unload-snowflake.html#unloading-data-to-your-user-stage
        """
        root = batch_config.storage.root
        sync_id = f"{self.tap_name}--{self.name}-{uuid4()}"
        prefix = batch_config.storage.prefix or ""
        # prepare object_construct statement
        files = []
        try:
            # unload table into user internal stage
            copy_statement, kwargs = self._get_copy_statement(
                sync_id=sync_id, prefix=prefix, context=context
            )
            self.connector.connection.execute(copy_statement, **kwargs)
            # list available files
            results = self.connector.connection.execute(
                text(f"list '@~/tap-snowflake/{sync_id}/'")
            )
            # download available files
            local_path = f"{root.replace('file://', '')}/{sync_id}"
            Path(local_path).mkdir(parents=True, exist_ok=True)
            for result in results:
                stage_path = result[0]
                file_name = os.path.basename(stage_path)
                self.connector.connection.execute(
                    text(f"get '@~/{stage_path}' '{root}/{sync_id}'")
                )
                files.append(f"{root}/{sync_id}/{file_name}")
        finally:
            # remove staged files
            self.connector.connection.execute(
                text(f"remove '@~/tap-snowflake/{sync_id}/'")
            )
        yield (batch_config.encoding, files)
