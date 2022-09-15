"""SQL client handling.

This includes SnowflakeStream and SnowflakeConnector.
"""

from __future__ import annotations

import os
from typing import Any, Dict, Iterable, Optional
from uuid import uuid4

from singer_sdk import SQLConnector, SQLStream
from singer_sdk.helpers._batch import BaseBatchFileEncoding, BatchConfig
from snowflake.sqlalchemy import URL
from sqlalchemy.sql import text


class SnowflakeConnector(SQLConnector):
    """Connects to the Snowflake SQL source."""

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


class SnowflakeStream(SQLStream):
    """Stream class for Snowflake streams."""

    connector_class = SnowflakeConnector

    def get_batches(
        self, batch_config: BatchConfig, context: dict | None = None
    ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        return self.get_batches_from_internal_stage(batch_config, context)

    def get_batches_from_internal_stage(
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
        table_name = self.fully_qualified_name
        # unload table into user internal stage
        copy_statement = text(
            f"copy into '@~/tap-snowflake/{sync_id}/{prefix}' from (select object_construct(*) from {table_name}) file_format = (type='JSON', compression='GZIP') overwrite = TRUE"
        )
        self.connector.connection.execute(copy_statement)
        # list available files
        results = self.connector.connection.execute(
            text(f"list '@~/tap-snowflake/{sync_id}/'")
        )
        # download available files
        files = []
        for result in results:
            file_path = result[0]
            file_name = os.path.basename(file_path)
            self.connector.connection.execute(text(f"get '@~/{file_path}' '{root}/'"))
            files.append(f"{root}/{file_name}")
        # remove staged files
        self.connector.connection.execute(text(f"remove '@~/tap-snowflake/{sync_id}/'"))
        yield (batch_config.encoding, files)
