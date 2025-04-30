"""SQL client handling.

This includes SnowflakeStream and SnowflakeConnector.
"""

from __future__ import annotations

import logging
import os
from dataclasses import dataclass
from enum import Enum, auto
from pathlib import Path
from typing import Any, Iterable, List, Tuple
from uuid import uuid4
import datetime

from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
import sqlalchemy
from singer_sdk import SQLConnector, SQLStream, metrics
from singer_sdk.helpers._batch import BaseBatchFileEncoding, BatchConfig
from singer_sdk.streams.core import REPLICATION_FULL_TABLE, REPLICATION_INCREMENTAL
import singer_sdk.helpers._typing
from snowflake.sqlalchemy import URL
from sqlalchemy.sql import text


logger = logging.getLogger()

unpatched_conform = singer_sdk.helpers._typing._conform_primitive_property


def patched_conform(
    elem: Any,
    property_schema: dict,
) -> Any:
    """Overrides Singer SDK type conformance to prevent dates turning into datetimes.
    Converts a primitive (i.e. not object or array) to a json compatible type.
    Returns:
        The appropriate json compatible type.
    """
    if isinstance(elem, datetime.date):
        return elem.isoformat()
    return unpatched_conform(elem=elem, property_schema=property_schema)


singer_sdk.helpers._typing._conform_primitive_property = patched_conform


class ProfileStats(Enum):
    """Profile Statistics Enum."""

    TABLE_SIZE_IN_MB = auto()
    TABLE_ROW_COUNT = auto()
    COLUMN_MIN_VALUE = auto()
    COLUMN_MAX_VALUE = auto()
    COLUMN_NULL_VALUES = auto()
    COLUMN_NONNULL_VALUES = auto()


@dataclass
class ColumnProfile:
    """Column Profile."""

    min_value: int | None
    max_value: int | None
    null_values: int | None
    nonnull_values: int | None


@dataclass
class TableProfile:
    """Table Profile."""

    column_profiles: dict[str, ColumnProfile]
    row_count: int | None
    size_in_mb: int | None


class SnowflakeConnector(SQLConnector):
    """Connects to the Snowflake SQL source."""

    @classmethod
    def get_sqlalchemy_url(cls, config: dict) -> str:
        """Concatenate a SQLAlchemy URL for use in connecting to the source."""
        if config.get("password"):
            logger.info("Connecting to Snowflake using basic authentication")
        else:
            if config.get("private_key") is None:
                raise ValueError("tap-snowflake must be passed one of 'password' or 'private_key' to use for authentication")
            else:
                logger.info("Connecting to Snowflake using key pair authentication")

        params = {
            "account": config["account"],
            "user": config["user"],
        }

        for option in ["database", "schema", "warehouse", "role", "password"]:
            if config.get(option):
                params[option] = config.get(option)

        return URL(**params)

    def create_engine(self) -> sqlalchemy.engine.Engine:
        """Return a new SQLAlchemy engine using the provided config.

        Developers can generally override just one of the following:
        `sqlalchemy_engine`, sqlalchemy_url`.

        Returns:
             A newly created SQLAlchemy engine object.
        """
        connect_args = None
        if self.config.get("private_key"):
            key = self.config["private_key"]
            key_code = self.config["private_key_passphrase"]
            p_key = serialization.load_pem_private_key(
                bytes(key, "utf-8"), password=key_code.encode(), backend=default_backend()
            )
            pkb = p_key.private_bytes(
                encoding=serialization.Encoding.DER,
                format=serialization.PrivateFormat.PKCS8,
                encryption_algorithm=serialization.NoEncryption(),
            )
            connect_args = {'private_key': pkb, 
                            'client_session_keep_alive': True,
                           }

        return sqlalchemy.create_engine(
            self.sqlalchemy_url, echo=False, pool_timeout=10, connect_args=connect_args
        )

    # overridden to filter out the information_schema from catalog discovery
    def discover_catalog_entries(self) -> list[dict]:
        """Return a list of catalog entries from discovery.

        Returns:
            The discovered catalog entries as a list.
        """
        result: list[dict] = []
        tables = [t.lower() for t in self.config.get("tables", [])]
        engine = self.create_sqlalchemy_engine()
        inspected = sqlalchemy.inspect(engine)
        schema_names = [
            schema_name
            for schema_name in self.get_schema_names(engine, inspected)
            if schema_name.lower() != "information_schema"
        ]
        for schema_name in schema_names:
            # Iterate through each table and view
            for table_name, is_view in self.get_object_names(
                engine, inspected, schema_name
            ):
                if (not tables) or (f"{schema_name}.{table_name}" in tables):
                    catalog_entry = self.discover_catalog_entry(
                        engine, inspected, schema_name, table_name, is_view
                    )
                    result.append(catalog_entry.to_dict())

        return result

    def get_table_profile(
        self,
        full_table_name: str,
        stats: set[ProfileStats],
        profile_columns: list[str] | None = None,
    ) -> TableProfile:
        """Scan source system for a set of profile stats.

        This is a proposed implementation to eventually be included in the SDK.

        This generic implementation should be compatible with most/all SQL providers, as
        it only requires min(), max(), and count() support, which are all part of ANSI
        SQL.

        Developers may override this for performance improvements on their native SQL
        implementation.

        Any stats not requested but available 'for free' while collecting other stats
        may be included in the returned profile. For example, the base implementation
        cannot calculate COLUMN_NULL_VALUES without also calculating TABLE_ROW_COUNT and
        COLUMN_NONNULL_VALUES. The additional stats therefore would be returned along
        with the requested stats. The consumer should ignore or disregard any stats not
        needed.

        Note: Gathering stats can take a long time. The implementation should attempt
        to combine stats gathering into fewer tables scans where possible and only
        spend time pulling in requested stats.

        Returns:
            A TableProfile object. Stats may be left null if not requested, not
            available, or not implemented. Callers should check each field for null
            values and handle null as 'not available'.
        """
        profile_columns = profile_columns or []
        expressions: list[str] = []
        if (
            ProfileStats.TABLE_ROW_COUNT in stats
            or ProfileStats.COLUMN_NULL_VALUES in stats
        ):
            expressions.append("count(1) as row_count")
        if ProfileStats.TABLE_SIZE_IN_MB in stats:
            self.logger.debug(
                "TABLE_SIZE_IN_MB stats not implemented for this provider."
            )
        for col in profile_columns or []:
            if ProfileStats.COLUMN_MIN_VALUE in stats:
                expressions.append(f"min({col}) as min__{col}")
            if ProfileStats.COLUMN_MAX_VALUE in stats:
                expressions.append(f"max({col}) as max__{col}")
            if (
                ProfileStats.COLUMN_NONNULL_VALUES in stats
                or ProfileStats.COLUMN_NULL_VALUES in stats
            ):
                expressions.append(f"count({col}) as nonnull__{col}")
            if ProfileStats.COLUMN_NULL_VALUES in stats:
                expressions.append(f"count(1) - count({col}) as null__{col}")
        result_dict = (
            self.connection.execute(
                text(f"SELECT {', '.join(expressions)} FROM {full_table_name}")
            )
            .one()
            ._asdict()
        )
        return TableProfile(
            row_count=result_dict.get("row_count", None),
            size_in_mb=result_dict.get("size_in_mb", None),
            column_profiles={
                col: ColumnProfile(
                    min_value=result_dict.get(f"min__{col}", None),
                    max_value=result_dict.get(f"max__{col}", None),
                    null_values=result_dict.get(f"null__{col}", None),
                    nonnull_values=result_dict.get(f"nonnull__{col}", None),
                )
                for col in profile_columns
            },
        )


class SnowflakeStream(SQLStream):
    """Stream class for Snowflake streams."""

    connector_class = SnowflakeConnector

    @property
    def is_sorted(self):
        """Is sorted."""
        return bool(self.replication_key)

    def _sync_batches(
        self,
        batch_config: BatchConfig,
        context: dict | None = None,
    ) -> None:
        """Sync batches, emitting BATCH messages.

        This is a proposed replacement for the SDK internal SQLStream._sync_baches.
        Per: https://github.com/meltano/sdk/issues/976

        This version stores the max replication value before batch sync starts, and then
        increments the stream state with this value after the sync operation completes.

        Since any FULL_TABLE sync operations may subsequently be run as INCREMENTAL,
        the querying of the max value is not dependent upon running in INCREMENTAL mode.

        Args:
            batch_config: The batch configuration.
            context: Stream partition or context dictionary.
        """
        self._write_starting_replication_value(context)

        # New: Collect the max value for the replication column.
        max_replication_key_value = None
        if self.replication_key:
            table_profile: TableProfile = (
                self.connector.get_table_profile(  # type: ignore
                    full_table_name=self.fully_qualified_name,
                    stats={ProfileStats.COLUMN_MAX_VALUE},
                    profile_columns=[self.replication_key],
                )
            )
            max_replication_key_value = table_profile.column_profiles[
                self.replication_key
            ].max_value

        # Not chanded: Note that the STATE messages will not have an incremented
        # replication key value at this point.
        with metrics.batch_counter(self.name, context=context) as counter:
            for encoding, manifest in self.get_batches(batch_config, context):
                counter.increment()
                self._write_batch_message(encoding=encoding, manifest=manifest)
                self._write_state_message()

        # New: Increment and emit the final STATE message after sync has completed.
        if max_replication_key_value:
            self._increment_stream_state(
                latest_record={
                    self.replication_key: max_replication_key_value  # type: ignore
                },
                context=context,
            )
        self._write_state_message()

    def get_batches(
        self, batch_config: BatchConfig, context: dict | None = None
    ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Get batches of Records from Snowflake.

        Currently this returns records batches unloaded via an internal user stage.
        In future this can be updated to include new methods for unloading via
        external stages.

        For more details on batch unloading data from Snowflake,
        see the Snowflake docs:
        https://docs.snowflake.com/en/user-guide-data-unload.html
        """
        if context:
            raise NotImplementedError(
                f"Stream '{self.name}' does not support partitioning."
            )
        yield from self.get_batches_from_internal_user_stage(batch_config, context)

    def _get_full_table_copy_statement(
        self, sync_id: str, prefix: str, objects: List[str], table_name: str
    ) -> Tuple[text, dict]:
        """Get FULL_TABLE copy statement and key bindings."""
        statement = [f"copy into '@~/tap-snowflake/{sync_id}/{prefix}' from "]
        if self.replication_key:
            statement.append(
                f"(select object_construct({', '.join(objects)}) from {table_name} "
                f"order by {self.replication_key}) "
            )
        else:
            statement.append(
                f"(select object_construct({', '.join(objects)}) from {table_name}) "
            )
        statement.append(
            "file_format = (type='JSON', compression='GZIP') overwrite = TRUE"
        )
        return (
            text("".join(statement)),
            {},
        )

    def _get_incremental_copy_statement(
        self,
        sync_id: str,
        prefix: str,
        objects: List[str],
        table_name: str,
        replication_key_value,
    ) -> Tuple[text, dict]:
        """Get INCREMENTAL copy statement and key bindings."""
        return (
            text(
                f"copy into '@~/tap-snowflake/{sync_id}/{prefix}' from "
                + f"(select object_construct({', '.join(objects)}) "
                + f"from {table_name} "
                + f"where {self.replication_key} >= :replication_key_value "
                + f"order by {self.replication_key}) "
                + "file_format = (type='JSON', compression='GZIP') overwrite = TRUE"
            ),
            {"replication_key_value": replication_key_value},
        )

    def _get_copy_statement(
        self, sync_id: str, prefix: str, context: dict | None = None
    ) -> Tuple[text, dict]:
        """Construct copy statement.

        Takes into account stream property selection and incremental keys.
        """
        selected_schema = self.get_selected_schema()
        objects = [f"'{col}', {col}" for col in selected_schema["properties"].keys()]

        if self.replication_method == REPLICATION_FULL_TABLE:
            return self._get_full_table_copy_statement(
                sync_id=sync_id,
                prefix=prefix,
                objects=objects,
                table_name=self.fully_qualified_name,
            )

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
                    table_name=self.fully_qualified_name,
                    replication_key_value=replication_key_value,
                )
            else:
                return self._get_full_table_copy_statement(
                    sync_id=sync_id,
                    prefix=prefix,
                    objects=objects,
                    table_name=self.fully_qualified_name,
                )
        else:
            raise NotImplementedError(
                "Only 'FULL_TABLE' and 'INCREMENTAL' replication strategies "
                "are supported by this tap."
            )

    def get_batches_from_internal_user_stage(
        self, batch_config: BatchConfig, context: dict | None = None
    ) -> Iterable[tuple[BaseBatchFileEncoding, list[str]]]:
        """Unload Snowflake table to User Internal Stage, and download files to local storage.

        This method uses the Internal stage type, into the Snowflake-managed stage created
        for each user.

        More details on how this works can be found in the Snowflake docs:
        https://docs.snowflake.com/en/user-guide/data-unload-snowflake.html#unloading-data-to-your-user-stage  # noqa: E501
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
            self.connector.connection.execute(copy_statement, **kwargs).all()
            # list available files
            results = self.connector.connection.execute(
                text(f"list '@~/tap-snowflake/{sync_id}/'")
            ).all()
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

    # Get records from stream
    # Overridden to use native objects under `if start_val:`
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

        selected_column_names = self.get_selected_schema()["properties"].keys()
        table = self.connector.get_table(
            full_table_name=self.fully_qualified_name,
            column_names=selected_column_names,
        )
        query = table.select()

        if self.replication_key:
            replication_key_col = table.columns[self.replication_key]
            query = query.order_by(replication_key_col)

            start_val = self.get_starting_replication_key_value(context)
            if start_val:
                query = query.where(replication_key_col >= start_val)

        if self.ABORT_AT_RECORD_COUNT is not None:
            query = query.limit(self.ABORT_AT_RECORD_COUNT)

        for record in self.connector.connection.execute(query):
            yield dict(record)
